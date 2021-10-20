use std::{collections::HashMap, sync::{Arc, Mutex}};

use slog::Logger;
use tokio::sync::mpsc::channel;

use crate::{Consensus, Network, StateMachine};

pub struct DkvrrServer {
    id: u64,
    logger: Logger,
}
impl DkvrrServer {
    pub fn new(id: u64, logger: Logger) -> Self {
        DkvrrServer { id, logger }
    }
    pub fn run(&self) {
        let id = self.id;

        let (consensus_to_network_tx, consensus_to_network_rx) = channel(1024);
        let (network_to_consensus_tx, network_to_consensus_rx) = channel(1024);

        let (consensus_to_state_machine_tx, consensus_to_state_machine_rx) = channel(1024);
        let (state_machine_to_consensus_tx, state_machine_to_consensus_rx) = channel(1024);
        let kv=Arc::new(Mutex::new(HashMap::new()));
        let kv_clone=kv.clone();
        let consensus_logger = self.logger.new(slog::o!("module"=>"consensus"));
        tokio::spawn(async move {
            let mut consensus = Consensus::new(
                id,
                kv_clone,
                consensus_logger,
                consensus_to_network_tx,
                consensus_to_state_machine_tx,
                network_to_consensus_rx,
                state_machine_to_consensus_rx,
            );
            consensus.run().await;
        });

        let network_logger = self.logger.new(slog::o!("module"=>"network"));
        tokio::spawn(async move {
            let mut network = Network::new(
                id,
                network_logger,
                network_to_consensus_tx,
                consensus_to_network_rx,
            );
            network.run().await;
        });

        let state_machine_logger = self.logger.new(slog::o!("module"=>"state machine"));
        tokio::spawn(async move {
            let mut state_machine = StateMachine::new(
                state_machine_logger,
                kv,
                state_machine_to_consensus_tx,
                consensus_to_state_machine_rx,
            );
            state_machine.run().await;
        });
    }
}
