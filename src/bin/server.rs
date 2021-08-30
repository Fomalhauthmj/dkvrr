use std::sync::{Arc, Mutex};
use std::time::Duration;
use std::{collections::HashMap, net::SocketAddr};

use dkvrr::app::RaftMessageResponse;
use dkvrr::network::{Network, NetworkMessage, NetworkReceiver};
use protobuf::Message as PbMessage;
use raft::StateRole;
use raft::{prelude::*, storage::MemStorage};
use slog::{Drain, Logger, debug, error, info, o};
use tokio::runtime::Runtime;
use tokio::sync::mpsc::channel;
use tokio::time::interval;
use tokio_stream::wrappers::IntervalStream;
use tokio_stream::StreamExt;
struct ServerConfig {
    id: u64,
    raft_listen_addrs: HashMap<u64, SocketAddr>,
}
struct Server {
    id: u64,
    logger: Logger,
    runtime: Runtime,
    network: Network,
    network_receiver: NetworkReceiver,
}
impl Server {
    pub fn new(config: ServerConfig) -> Self {
        let (tx, rx) = channel(1024);
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .unwrap();
        let decorator = slog_term::TermDecorator::new().build();
        let drain = slog_term::FullFormat::new(decorator).build().fuse();
        let drain = slog_async::Async::new(drain)
            .chan_size(4096)
            .overflow_strategy(slog_async::OverflowStrategy::Block)
            .build()
            .fuse();
        Server {
            id: config.id,
            logger: slog::Logger::root(drain, o!("tag" => format!("[{}]", config.id))),
            runtime,
            network: Network::new(config.id, config.raft_listen_addrs, tx),
            network_receiver: rx,
        }
    }
    pub fn create_raw_node(&self) -> Arc<Mutex<RawNode<MemStorage>>> {
        // Create the configuration for the Raft node.
        let cfg = Config {
            // The unique ID for the Raft node.
            id: self.id,
            // Election tick is for how long the follower may campaign again after
            // it doesn't receive any message from the leader.
            election_tick: 10,
            // Heartbeat tick is for how long the leader needs to send
            // a heartbeat to keep alive.
            heartbeat_tick: 3,
            // The max size limits the max size of each appended message. Mostly, 1 MB is enough.
            max_size_per_msg: 1024 * 1024 * 1024,
            // Max inflight msgs that the leader sends messages to follower without
            // receiving ACKs.
            max_inflight_msgs: 256,
            // The Raft applied index.
            // You need to save your applied index when you apply the committed Raft logs.
            applied: 0,
            ..Default::default()
        };

        // Create the Raft node.
        let storage = MemStorage::new_with_conf_state(ConfState::from((vec![1], vec![])));
        let raw_node = RawNode::new(&cfg, storage, &self.logger).unwrap();
        Arc::new(Mutex::new(raw_node))
    }
    async fn start(&mut self) {
        let raw_node = self.create_raw_node();
        self.network.start(self.runtime.handle());
        self.runtime.spawn(ticker(raw_node.clone()));
        let sender = self.network.network_to_server.clone();
        let logger=self.logger.clone();
        self.runtime.spawn(async move {
            let mut interval = IntervalStream::new(interval(Duration::from_millis(100)));
            loop {
                let _ = interval.next().await;
                sender.send(NetworkMessage::AppRequest).await.unwrap();
                info!(logger,"send apprequest");
            }
        });
        loop {
            // Step raft messages.
            match self.network_receiver.recv().await {
                Some(msg) => match msg {
                    NetworkMessage::RaftRequestWithCallBack(m, cb) => {
                        raw_node.lock().unwrap().step(m).unwrap();
                        cb.send(NetworkMessage::RaftResponse(RaftMessageResponse::default()))
                            .unwrap();
                    }
                    NetworkMessage::AppRequest => {
                        raw_node.lock().unwrap().propose(vec![], vec![1]);
                    }
                    _ => {}
                },
                None => {
                    todo!()
                }
            }
            self.on_ready(raw_node.clone(), &self.logger).await;
        }
    }
    async fn handle_messages(&self, msgs: Vec<Message>) {
        for msg in msgs {
            self.network
                .send_to(msg.to, NetworkMessage::RaftRequest(msg))
                .await;
        }
    }
    async fn on_ready(&self, raw_node: Arc<Mutex<RawNode<MemStorage>>>, logger: &Logger) {
        let mut node = raw_node.lock().unwrap();
        if !node.has_ready() {
            return;
        }
        let store = node.raft.raft_log.store.clone();

        // Get the `Ready` with `RawNode::ready` interface.
        let mut ready = node.ready();

        if !ready.messages().is_empty() {
            // Send out the messages come from the node.
            self.handle_messages(ready.take_messages()).await;
        }

        // Apply the snapshot. It's necessary because in `RawNode::advance` we stabilize the snapshot.
        if *ready.snapshot() != Snapshot::default() {
            let s = ready.snapshot().clone();
            if let Err(e) = store.wl().apply_snapshot(s) {
                error!(
                    logger,
                    "apply snapshot fail: {:?}, need to retry or panic", e
                );
                return;
            }
        }

        let handle_committed_entries =
            |rn: &mut RawNode<MemStorage>, committed_entries: Vec<Entry>| {
                for entry in committed_entries {
                    if entry.data.is_empty() {
                        // From new elected leaders.
                        continue;
                    }
                    if let EntryType::EntryConfChange = entry.get_entry_type() {
                        // For conf change messages, make them effective.
                        let mut cc = ConfChange::default();
                        cc.merge_from_bytes(&entry.data).unwrap();
                        let cs = rn.apply_conf_change(&cc).unwrap();
                        store.wl().set_conf_state(cs);
                    } else {
                        // For normal proposals, extract the key-value pair and then
                        // insert them into the kv engine.
                        debug!(logger,"{:?}",entry);
                    }
                    if rn.raft.state == StateRole::Leader {
                        // The leader should response to the clients, tell them if their proposals
                        // succeeded or not.
                        //let proposal = proposals.lock().unwrap().pop_front().unwrap();
                        //proposal.propose_success.send(true).unwrap();
                    }
                }
            };
        // Apply all committed entries.
        handle_committed_entries(&mut node, ready.take_committed_entries());

        // Persistent raft logs. It's necessary because in `RawNode::advance` we stabilize
        // raft logs to the latest position.
        if let Err(e) = store.wl().append(ready.entries()) {
            error!(
                logger,
                "persist raft log fail: {:?}, need to retry or panic", e
            );
            return;
        }

        if let Some(hs) = ready.hs() {
            // Raft HardState changed, and we need to persist it.
            store.wl().set_hardstate(hs.clone());
        }

        if !ready.persisted_messages().is_empty() {
            // Send out the persisted messages come from the node.
            self.handle_messages(ready.take_persisted_messages()).await;
        }

        // Call `RawNode::advance` interface to update position flags in the raft.
        let mut light_rd = node.advance(ready);
        // Update commit index.
        if let Some(commit) = light_rd.commit_index() {
            store.wl().mut_hard_state().set_commit(commit);
        }
        // Send out the messages.
        self.handle_messages(light_rd.take_messages()).await;
        // Apply all committed entries.
        handle_committed_entries(&mut node, light_rd.take_committed_entries());
        // Advance the apply index.
        node.advance_apply();
        drop(node);
    }
}
async fn ticker(raw_node: Arc<Mutex<RawNode<MemStorage>>>) {
    let mut interval = IntervalStream::new(interval(Duration::from_millis(100)));
    loop {
        let _ = interval.next().await;
        match raw_node.lock() {
            Ok(mut r) => {
                r.tick();
            }
            Err(e) => {
                ();
            }
        }
    }
}
#[tokio::main]
async fn main() {
    let mut addrs: HashMap<u64, SocketAddr> = HashMap::new();
    addrs.insert(1, "127.0.0.1:10086".parse().unwrap());
    addrs.insert(2, "127.0.0.1:10087".parse().unwrap());
    addrs.insert(3, "127.0.0.1:10088".parse().unwrap());

    let config = ServerConfig {
        id: 1,
        raft_listen_addrs: addrs,
    };
    let mut server = Server::new(config);
    server.start().await;
}
