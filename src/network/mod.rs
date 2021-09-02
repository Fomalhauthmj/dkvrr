use self::rpc::AppMsgService;
use self::tasks::{send_to, serve};
use crate::types::{TMessage, TReceiver, TRequest, TSender};
use slog::{o, trace, Logger};
use std::{collections::HashMap, net::SocketAddr};
use tokio::runtime::Handle;
use tonic::transport::Endpoint;
mod rpc;
mod tasks;
pub struct Network {
    logger: Logger,
    rpc_endpoints: HashMap<u64, Endpoint>,
}
impl Network {
    pub fn new(
        handle: Handle,
        logger: Logger,
        serve_addr: SocketAddr,
        rpc_endpoints: HashMap<u64, Endpoint>,
        network_to_consensus_tx: TSender,
    ) -> Self {
        let serve_logger = logger.new(o!("from"=>"rpc service"));
        let service = AppMsgService::new(serve_logger, network_to_consensus_tx);
        handle.spawn(serve(logger.clone(), serve_addr, service));
        Network {
            logger,
            rpc_endpoints,
        }
    }
    pub async fn start(&self, mut consensus_to_network_rx: TReceiver, handle: Handle) {
        loop {
            match consensus_to_network_rx.recv().await {
                Some(tmsg) => match tmsg {
                    TMessage::Request(TRequest::RaftMessage(msg)) => {
                        //TODO check endpoint existence
                        let endpoint = self.rpc_endpoints.get(&msg.to).unwrap();
                        handle.spawn(send_to(
                            self.logger.new(o!("from"=>"send to")),
                            endpoint.clone(),
                            TMessage::Request(TRequest::RaftMessage(msg)),
                        ));
                    }
                    _ => {
                        unimplemented!()
                    }
                },
                None => {
                    trace!(self.logger, "consensus to network closed");
                    break;
                }
            };
        }
    }
    // TODO need add/remove endpoints helper functions
}
