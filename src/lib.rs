pub mod app {
    tonic::include_proto!("app");
}
mod client;
mod common;
mod consensus;
mod error;
mod network;
mod server;
mod state_machine;
mod storage;
mod utils;
pub use client::DkvrrClient;
pub use common::{Callback, TMessage, TReceiver, TSender};
pub use consensus::Consensus;
pub use storage::{MemStorage,RocksDBStorage};
pub use error::{DkvrrError, Result};
pub use network::{get_rpc_endpoint, Network};
pub use server::DkvrrServer;
pub use state_machine::StateMachine;
