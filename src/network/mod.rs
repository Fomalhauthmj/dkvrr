use self::rpc::AppMsgService;
use crate::app::{
    app_message_client::AppMessageClient, app_message_server::AppMessageServer, RaftMessageResponse,
};
use raft::eraftpb::Message;
use slog::{debug, error, Logger};
use std::{collections::HashMap, net::SocketAddr};
use tokio::{
    runtime::Handle,
    sync::{mpsc, oneshot},
};
use tonic::{client, transport::{Endpoint, Server}};
mod rpc;

pub type NetworkSender = mpsc::Sender<NetworkMessage>;
pub type NetworkReceiver = mpsc::Receiver<NetworkMessage>;
pub type NetworkCallBack = oneshot::Sender<NetworkMessage>;

#[derive(Debug)]
pub enum NetworkMessage {
    RaftRequest(Message),
    RaftRequestWithCallBack(Message, NetworkCallBack),
    RaftResponse(RaftMessageResponse),
    AppRequest,
}

pub struct Network {
    id: u64,
    logger: Logger,
    serve_addr: SocketAddr,
    rpc_endpoints: HashMap<u64, Endpoint>,
    pub network_to_server: NetworkSender,
}
impl Network {
    pub fn new(
        id: u64,
        logger: Logger,
        serve_addr: SocketAddr,
        rpc_endpoints: HashMap<u64, Endpoint>,
        network_to_server: NetworkSender,
    ) -> Self {
        Network {
            id,
            logger,
            serve_addr,
            rpc_endpoints,
            network_to_server,
        }
    }
    pub fn start(&self, handle: &Handle) {
        let service = AppMsgService::new(self.network_to_server.clone(), self.logger.clone());
        handle.spawn(listen_on(self.serve_addr, service));
        debug!(self.logger, "start finish");
    }
    pub async fn handle_messages(&self,msgs:Vec<Message>){
        for msg in msgs{
            self.send_to(msg.to, NetworkMessage::RaftRequest(msg))
            .await;
        }
    }
    pub async fn send_to(&self, id: u64, msg: NetworkMessage) {
        debug!(self.logger, "will send message {:?}",msg);
        let addr = self.rpc_endpoints.get(&id).unwrap().clone();
        match AppMessageClient::connect(addr).await {
            Ok(mut client) => {
                debug!(self.logger, "connect success");
                match msg {
                    NetworkMessage::RaftRequest(req) => match client.send_raft_message(req).await {
                        Ok(resp) => {
                            debug!(self.logger, "send success");
                            self.network_to_server
                                .send(NetworkMessage::RaftResponse(resp.into_inner()))
                                .await;
                        }
                        Err(e) => {
                            error!(self.logger, "send error {}", e);
                        }
                    },
                    _ => {
                        unimplemented!();
                    }
                };
            }
            Err(e) => {
                error!(self.logger, "connect error {}", e);
            }
        }
    }
}
pub async fn listen_on(addr: SocketAddr, service: AppMsgService) {
    Server::builder()
        .concurrency_limit_per_connection(32)
        .add_service(AppMessageServer::new(service))
        .serve(addr)
        .await
        .unwrap();
}
