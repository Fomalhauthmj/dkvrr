use std::{collections::HashMap, sync::{Arc, Mutex}, time::Duration, vec};

use protobuf::Message as PbMessage;
use raft::{prelude::*, StateRole};
use slog::{Logger, error, info, warn};
use tokio::{select, time::interval};
use tokio_stream::{wrappers::IntervalStream, StreamExt};

use crate::{Callback, MemStorage, TMessage, TReceiver, TSender, utils};

pub struct Consensus {
    logger: Logger,
    raw_node: RawNode<MemStorage>,
    to_network: TSender,
    to_state_machine: TSender,
    from_network: TReceiver,
    from_state_machine: TReceiver,
    callbacks: HashMap<u64, Callback>,
}
impl Consensus {
    pub fn new(
        id: u64,
        kv_clone:Arc<Mutex<HashMap<String, String>>>,
        logger: Logger,
        to_network: TSender,
        to_state_machine: TSender,
        from_network: TReceiver,
        from_state_machine: TReceiver,
    ) -> Self {
        let cfg = Consensus::example_config(id);
        let raw_node = match id {
            //default leader
            1 => {
                let mut s = Snapshot::default();
                s.mut_metadata().index = 1;
                s.mut_metadata().term = 1;
                s.mut_metadata().mut_conf_state().voters = vec![1];
                let data={
                    let kv=kv_clone.lock().unwrap();
                    serde_json::to_vec(&*kv).unwrap()
                };
                s.set_data(data);       
                let storage = MemStorage::new(kv_clone);
                storage.wl().apply_snapshot(s).unwrap();
                RawNode::new(&cfg, storage, &logger).expect("create raw node error")
            }
            _ => {
                let storage = MemStorage::new(kv_clone);
                RawNode::new(&cfg, storage, &logger).expect("create raw node error")
            }
        };
        Consensus {
            logger,
            raw_node,
            to_network,
            to_state_machine,
            from_network,
            from_state_machine,
            callbacks: HashMap::new(),
        }
    }

    pub async fn run(&mut self) {
        let mut ticker = IntervalStream::new(interval(Duration::from_millis(100)));
        loop {
            select! {
                network_tmsg=self.from_network.recv()=>{
                    match network_tmsg{
                        Some(TMessage::AppRequestWithCallback(req,cb))=>{
                            if self.is_leader(){
                                let id=req.id;
                                self.callbacks.insert(req.id,cb);
                                self.raw_node.propose(vec![],utils::convert_req_to_propose_data(req)).unwrap_or_else(|e|{
                                    //TODO negative ack
                                    error!(self.logger,"propose app request {}",e);
                                    self.callbacks.remove(&id);
                                });
                            }
                        }
                        Some(TMessage::RaftMessage(msg))=>{
                            self.raw_node.step(msg).unwrap_or_else(|e|{
                                error!(self.logger,"step raft message {}",e);
                            });
                        }
                        Some(TMessage::RaftConfChangeV2(cc))=>{
                            if self.is_leader(){
                                self.raw_node.propose_conf_change(vec![], cc).unwrap_or_else(|e|{
                                    error!(self.logger,"propose conf change v2 {}",e);
                                });
                            }
                        }
                        None=>{
                            warn!(self.logger, "network to consensus closed");
                        }
                        _=>{
                            error!(self.logger,"unsupported tmsg from network")
                        }
                    }
                }
                _interval=ticker.next()=>{
                    self.raw_node.tick();
                }
                state_machine_tmsg=self.from_state_machine.recv()=>{
                    match state_machine_tmsg{
                        Some(TMessage::AppResponse(resp))=>{
                            if self.is_leader()&&self.callbacks.get(&resp.id).is_some(){
                                let cb=self.callbacks.remove(&resp.id).unwrap();
                                cb.send(TMessage::AppResponse(resp)).unwrap_or_else(|data|{
                                    error!(self.logger,"data will never be received {:?}",data)
                                })
                            }
                        }
                        None=>{
                            warn!(self.logger, "state machine to consensus closed");
                        }
                        _=> {
                            error!(self.logger,"unsupported tmsg from state machine")
                        }
                    }
                }
            };
            self.on_ready().await;
        }
    }

    async fn handle_messages(&self, msgs: Vec<Message>) {
        for msg in msgs {
            self.to_network
                .send(TMessage::RaftMessage(msg))
                .await
                .unwrap_or_else(|e| {
                    error!(self.logger, "send raft message to network {}", e);
                });
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
            if let EntryType::EntryConfChangeV2 = entry.get_entry_type() {
                // For conf change messages, make them effective.
                let mut cc = ConfChangeV2::default();
                cc.merge_from_bytes(&entry.data).unwrap();
                let cs = self.raw_node.apply_conf_change(&cc).unwrap();
                store.wl().set_conf_state(cs);
            } else {
                let req = utils::convert_propose_data_to_req(entry.data);
                info!(self.logger,"will apply {:?}",req);
                self.to_state_machine
                    .send(TMessage::AppRequest(req))
                    .await
                    .unwrap_or_else(|e| {
                        error!(self.logger, "send request to state machine {}", e);
                    });
            }
        }
    }

    async fn on_ready(&mut self) {
        if !self.raw_node.has_ready() {
            return;
        }

        // The Raft is ready, we can do something now.
        let mut ready = self.raw_node.ready();

        let store = self.raw_node.raft.raft_log.store.clone();

        // 1.   Check whether messages is empty or not. 
        //      If not, it means that the node will send messages to other nodes.
        if !ready.messages().is_empty() {
            self.handle_messages(ready.take_messages()).await;
        }

        // 2.   Check whether snapshot is empty or not.
        //      If not empty, it means that the Raft node has received a Raft snapshot from the leader 
        //      and we must apply the snapshot.
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

        // 3.   Check whether committed_entires is empty or not.
        //      If not, it means that there are some newly committed log entries which you must apply to the state machine. 
        //      Of course, after applying, you need to update the applied index and resume apply later.
        self.handle_committed_entries(&store, ready.take_committed_entries())
            .await;

        // 4.   Check whether entries is empty or not.
        //      If not empty, it means that there are newly added entries but have not been committed yet, we must append the entries to the Raft log.
        if let Err(e) = store.wl().append(ready.entries()) {
            error!(
                self.logger,
                "persist raft log fail: {:?}, need to retry or panic", e
            );
            return;
        }

        // 5.   Check whether hs is empty or not.
        //      If not empty, it means that the HardState of the node has changed. 
        //      For example, the node may vote for a new leader, or the commit index has been increased.
        //      We must persist the changed HardState.
        if let Some(hs) = ready.hs() {
            store.wl().set_hardstate(hs.clone());
        }

        // 6.   Check whether persisted_messages is empty or not.
        //      If not, it means that the node will send messages to other nodes after persisting hardstate, entries and snapshot.
        if !ready.persisted_messages().is_empty() {
            self.handle_messages(ready.take_persisted_messages()).await;
        }

        // 7.   Call advance to notify that the previous work is completed.
        //      Get the return value LightReady and handle its messages and committed_entries like step 1 and step 3 does. 
        //      Then call advance_apply to advance the applied index inside.
        let mut light_rd = self.raw_node.advance(ready);
        // Update commit index.
        if let Some(commit) = light_rd.commit_index() {
            store.wl().mut_hard_state().set_commit(commit);
        }
        self.handle_messages(light_rd.take_messages()).await;
        self.handle_committed_entries(&store, light_rd.take_committed_entries())
            .await;
        self.raw_node.advance_apply();
    }

    fn is_leader(&self) -> bool {
        self.raw_node.raft.state == StateRole::Leader
    }

    fn example_config(id: u64) -> Config {
        Config {
            id,
            election_tick: 50,
            heartbeat_tick: 20,
            ..Default::default()
        }
    }
}
