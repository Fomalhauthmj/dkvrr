use std::{collections::HashMap, thread};

use dkvrr::{consensus, network, state_machine};
use raft::eraftpb::ConfState;
use slog::{o, Drain, Logger};
use structopt::StructOpt;
use tonic::transport::Endpoint;
#[derive(StructOpt, Debug)]
#[structopt(name = "basic")]
struct Opt {
    #[structopt(short, long)]
    id: u64,
    #[structopt(short, long)]
    serve_addr: String,
}
#[tokio::main]
async fn main() {
    let mut rpc_endpoints: HashMap<u64, Endpoint> = HashMap::new();
    rpc_endpoints.insert(1, "http://127.0.0.1:10086".parse().unwrap());
    rpc_endpoints.insert(2, "http://127.0.0.1:10087".parse().unwrap());

    let opt = Opt::from_args();
    let logger = create_root_logger(opt.id);
    let serve_addr = opt.serve_addr.parse().unwrap();
    let handle = tokio::runtime::Handle::current();
    let conf_state = ConfState::from((vec![1, 2], vec![]));

    let (consensus_to_network_tx, consensus_to_network_rx) = tokio::sync::mpsc::channel(1024);
    let (network_to_consensus_tx, network_to_consensus_rx) = tokio::sync::mpsc::channel(1024);

    let (consensus_to_state_machine_tx, consensus_to_state_machine_rx) =
        tokio::sync::mpsc::channel(1024);
    let (state_machine_to_consensus_tx, state_machine_to_consensus_rx) =
        tokio::sync::mpsc::channel(1024);

    let consensus_logger = logger.new(o!("module"=>"consensus"));
    let _consensus_handle = tokio::spawn(async move {
        let mut consensus = consensus::Consensus::new(
            opt.id,
            conf_state,
            consensus_logger,
            consensus_to_network_tx,
            consensus_to_state_machine_tx,
        );
        consensus
            .start(network_to_consensus_rx, state_machine_to_consensus_rx)
            .await;
    });
    let network_logger = logger.new(o!("module"=>"network"));
    let _network_handle = tokio::spawn(async move {
        let network = network::Network::new(
            handle.clone(),
            network_logger,
            serve_addr,
            rpc_endpoints,
            network_to_consensus_tx,
        );
        network.start(consensus_to_network_rx, handle.clone()).await;
    });
    let state_machine_logger = logger.new(o!("module"=>"state machine"));
    let _state_machine_handle = tokio::spawn(async move {
        let mut state_machine =
            state_machine::StateMachine::new(state_machine_logger, state_machine_to_consensus_tx);
        state_machine.start(consensus_to_state_machine_rx).await;
    });
    thread::park();
}
pub fn create_root_logger(id: u64) -> Logger {
    let decorator = slog_term::TermDecorator::new().build();
    let drain = slog_term::FullFormat::new(decorator).build().fuse();
    let drain = slog_async::Async::new(drain)
        .chan_size(4096)
        .overflow_strategy(slog_async::OverflowStrategy::Block)
        .build()
        .fuse();
    slog::Logger::root(drain, o!("tag" => format!("[{}]",id)))
}
