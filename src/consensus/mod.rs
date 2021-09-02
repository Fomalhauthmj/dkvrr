use std::{collections::HashMap, time::Duration, vec};

use protobuf::Message as PbMessage;
use raft::{
    eraftpb::{ConfChange, ConfState, Entry, EntryType, Message, Snapshot},
    storage::MemStorage,
    Config, RawNode, StateRole, Storage,
};
use slog::{error, trace, Logger};
use tokio::{select, time::interval};
use tokio_stream::{wrappers::IntervalStream, StreamExt};

use crate::{
    types::{Callback, TMessage, TReceiver, TRequest, TResponse, TSender},
    utils,
};
pub struct Consensus<S: Storage> {
    logger: Logger,
    raw_node: RawNode<S>,
    consensus_to_network_tx: TSender,
    callbacks: HashMap<u64, Callback>,
    consensus_to_state_machine_tx: TSender,
}
impl Consensus<MemStorage> {
    pub fn new(
        id: u64,
        conf_state: ConfState,
        logger: Logger,
        consensus_to_network_tx: TSender,
        consensus_to_state_machine_tx: TSender,
    ) -> Self {
        // Create the configuration for the Raft node.
        let cfg = Config {
            // The unique ID for the Raft node.
            id,
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
        let storage = MemStorage::new_with_conf_state(conf_state);
        let raw_node = RawNode::new(&cfg, storage, &logger).expect("raw node new error");
        Consensus {
            logger,
            raw_node,
            consensus_to_network_tx,
            callbacks: HashMap::new(),
            consensus_to_state_machine_tx,
        }
    }
    pub async fn start(
        &mut self,
        mut network_to_consensus_rx: TReceiver,
        mut state_machine_to_consensus_rx: TReceiver,
    ) {
        let mut ticker = IntervalStream::new(interval(Duration::from_millis(100)));
        loop {
            select! {
                tmsg=network_to_consensus_rx.recv()=>{
                    match tmsg{
                        Some(TMessage::RequestWithCallback(TRequest::App(msg),cb))=>{
                            //leader propose
                            if self.raw_node.raft.state==StateRole::Leader{
                                let id=msg.id;
                                self.callbacks.insert(msg.id,cb);
                                match self.raw_node.propose(vec![],utils::convert_req_to_propose_data(msg)){
                                    Ok(_)=>{}
                                    Err(e)=>{
                                        //TODO negative ack
                                        trace!(self.logger,"{}",e);
                                        self.callbacks.remove(&id);
                                    }
                                }
                            }
                        }
                        Some(TMessage::Request(TRequest::RaftMessage(msg)))=>{
                            match self.raw_node.step(msg){
                                Ok(_)=>{}
                                Err(e)=>{trace!(self.logger,"{}",e)}
                            }
                        }
                        Some(TMessage::Request(TRequest::RaftConfChangeV2(_msg)))=>{
                            //leader propose
                            unimplemented!()
                        }
                        None=>{
                            trace!(self.logger, "network to consensus closed");
                            break;
                        }
                        _=>{
                            error!(self.logger,"unsupported tmsg")
                        }
                 }
                }
                _interval=ticker.next()=>{
                    self.raw_node.tick();
                }
                tmsg=state_machine_to_consensus_rx.recv()=>{
                    match tmsg{
                        Some(TMessage::Response(TResponse::App(resp)))=>{
                            if self.raw_node.raft.state==StateRole::Leader{
                                let cb=self.callbacks.remove(&resp.id).unwrap();
                                match cb.send(TMessage::Response(TResponse::App(resp))){
                                    Ok(_)=>{},
                                    Err(_)=>error!(self.logger,"callback send error"),
                                }
                            }
                        }
                        None=>{
                            trace!(self.logger, "state_machine to consensus closed");
                            break;
                        }
                        _=>{
                            error!(self.logger,"unsupported tmsg")
                        }
                    }
                }
            };
            self.on_ready().await;
        }
    }
    async fn handle_messages(&self, msgs: Vec<Message>) {
        for msg in msgs {
            match self
                .consensus_to_network_tx
                .send(TMessage::Request(TRequest::RaftMessage(msg)))
                .await
            {
                Ok(_) => trace!(self.logger, "send raft message to network success"),
                Err(e) => error!(self.logger, "handle_messages error {}", e),
            }
        }
    }
    async fn handle_committed_entries(
        &mut self,
        store: &MemStorage,
        committed_entries: Vec<Entry>,
    ) {
        for entry in committed_entries {
            if entry.data.is_empty() {
                // From new elected leaders.
                continue;
            }
            if let EntryType::EntryConfChange = entry.get_entry_type() {
                // For conf change messages, make them effective.
                let mut cc = ConfChange::default();
                cc.merge_from_bytes(&entry.data).unwrap();
                let cs = self.raw_node.apply_conf_change(&cc).unwrap();
                store.wl().set_conf_state(cs);
            } else {
                let req = utils::convert_propose_data_to_req(entry.data);
                // For normal proposals, extract the key-value pair and then
                // insert them into the kv engine.
                match self
                    .consensus_to_state_machine_tx
                    .send(TMessage::Request(TRequest::App(req)))
                    .await
                {
                    Ok(_) => trace!(self.logger, "send committed entry to state machine success"),
                    Err(e) => error!(
                        self.logger,
                        "send committed entry to state machine error {}", e
                    ),
                }
            }
            if self.raw_node.raft.state == StateRole::Leader {
                // The leader should response to the clients, tell them if their proposals
                // succeeded or not.
                //let proposal = proposals.lock().unwrap().pop_front().unwrap();
                //proposal.propose_success.send(true).unwrap();
            }
        }
    }
    async fn on_ready(&mut self) {
        if !self.raw_node.has_ready() {
            return;
        }

        let store = self.raw_node.raft.raft_log.store.clone();

        // Get the `Ready` with `RawNode::ready` interface.
        let mut ready = self.raw_node.ready();

        if !ready.messages().is_empty() {
            // Send out the messages come from the node.
            self.handle_messages(ready.take_messages()).await;
        }

        // Apply the snapshot. It's necessary because in `RawNode::advance` we stabilize the snapshot.
        if *ready.snapshot() != Snapshot::default() {
            let s = ready.snapshot().clone();
            if let Err(e) = store.wl().apply_snapshot(s) {
                error!(
                    self.logger,
                    "apply snapshot fail: {:?}, need to retry or panic", e
                );
                return;
            }
        }

        // Apply all committed entries.
        self.handle_committed_entries(&store, ready.take_committed_entries())
            .await;

        // Persistent raft logs. It's necessary because in `RawNode::advance` we stabilize
        // raft logs to the latest position.
        if let Err(e) = store.wl().append(ready.entries()) {
            error!(
                self.logger,
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
        let mut light_rd = self.raw_node.advance(ready);
        // Update commit index.
        if let Some(commit) = light_rd.commit_index() {
            store.wl().mut_hard_state().set_commit(commit);
        }
        // Send out the messages.
        self.handle_messages(light_rd.take_messages()).await;
        // Apply all committed entries.
        self.handle_committed_entries(&store, light_rd.take_committed_entries())
            .await;
        // Advance the apply index.
        self.raw_node.advance_apply();
    }
}
