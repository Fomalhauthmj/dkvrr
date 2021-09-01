use self::rpc::AppMsgService;
use self::tasks::{send_to, serve};
use crate::types::{TMessage, TRequest, TSender};
use raft::eraftpb::Message;
use slog::{o, Logger};
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
        handle: &Handle,
        logger: Logger,
        serve_addr: SocketAddr,
        rpc_endpoints: HashMap<u64, Endpoint>,
        network_to_core: TSender,
    ) -> Self {
        let serve_logger = logger.new(o!("from"=>"rpc service"));
        let service = AppMsgService::new(serve_logger, network_to_core);
        handle.spawn(serve(logger.clone(), serve_addr, service));
        Network {
            logger,
            rpc_endpoints,
        }
    }
    // TODO need add/remove endpoints helper functions
    pub fn handle_raft_messages(&self, handle: &Handle, msgs: Vec<Message>) {
        for msg in msgs {
            //TODO check endpoint existence
            let endpoint = self.rpc_endpoints.get(&msg.to).unwrap();
            handle.spawn(send_to(
                self.logger.new(o!("from"=>"send to")),
                endpoint.clone(),
                TMessage::Request(TRequest::RaftMessage(msg)),
            ));
        }
    }
}
