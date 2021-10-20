use crate::{TMessage, TReceiver, TSender};

use self::rpc::AppMsgService;
use self::tasks::{send_to, serve};
use slog::{error, warn, Logger};
use std::net::SocketAddr;
use std::str::FromStr;
use tonic::transport::Endpoint;
mod rpc;
mod tasks;
pub struct Network {
    logger: Logger,
    from_consensus: TReceiver,
}
impl Network {
    pub fn new(id: u64, logger: Logger, to_consensus: TSender, from_consensus: TReceiver) -> Self {
        let rpc_service_logger = logger.new(slog::o!("from"=>"rpc service"));
        let serve_logger = logger.new(slog::o!("from"=>"rpc serve"));
        let service = AppMsgService::new(rpc_service_logger, to_consensus);
        tokio::spawn(serve(serve_logger, get_serve_addr(id), service));
        Network {
            logger,
            from_consensus,
        }
    }
    pub async fn run(&mut self) {
        loop {
            match self.from_consensus.recv().await {
                Some(tmsg) => match tmsg {
                    TMessage::RaftMessage(msg) => {
                        let endpoint = get_rpc_endpoint(msg.to);
                        let logger = self
                            .logger
                            .new(slog::o!("send to"=>format!("{:?}",&endpoint)));
                        tokio::spawn(send_to(logger, endpoint, msg));
                    }
                    _ => {
                        error!(self.logger, "unsupported tmsg from consensus");
                    }
                },
                None => {
                    warn!(self.logger, "consensus to network closed");
                    break;
                }
            };
        }
    }
}
pub fn get_rpc_endpoint(id: u64) -> Endpoint {
    let addr = format!("http://127.0.0.1:{}", 4000 + id);
    Endpoint::from_str(&addr).unwrap()
}
fn get_serve_addr(id: u64) -> SocketAddr {
    let addr = format!("127.0.0.1:{}", 4000 + id);
    SocketAddr::from_str(&addr).unwrap()
}
