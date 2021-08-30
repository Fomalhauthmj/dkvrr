use self::rpc::AppMsgService;
use crate::app::{
    app_message_client::AppMessageClient, app_message_server::AppMessageServer, RaftMessageResponse,
};
use raft::eraftpb::Message;
use std::{collections::HashMap,net::SocketAddr};
use tokio::{
    runtime::Handle,
    sync::{mpsc, oneshot},
};
use tonic::transport::Server;
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
    raft_listen_addrs: HashMap<u64, SocketAddr>,
    pub network_to_server: NetworkSender,
}
impl Network {
    pub fn new(
        id: u64,
        raft_listen_addrs: HashMap<u64, SocketAddr>,
        network_to_server: NetworkSender,
    ) -> Self {
        Network {
            id,
            raft_listen_addrs,
            network_to_server,
        }
    }
    pub fn start(&self, handle: &Handle) {
        let service = AppMsgService::new(self.network_to_server.clone());
        let addr = self.raft_listen_addrs.get(&self.id).unwrap();
        handle.spawn(listen_on(addr.clone(), service));
    }
    pub async fn send_to(&self, id: u64, msg: NetworkMessage){
        let addr = self.raft_listen_addrs.get(&id).unwrap();
        let mut client = AppMessageClient::connect(addr.to_string()).await.unwrap();
        match msg {
            NetworkMessage::RaftRequest(req) => {
                let resp = client.send_raft_message(req).await.unwrap();
                self.network_to_server
                    .send(NetworkMessage::RaftResponse(resp.into_inner()))
                    .await.unwrap();
            }
            _ => {
                unimplemented!();
            }
        };
    }
}
pub async fn listen_on(addr: SocketAddr, service: AppMsgService) {
    Server::builder()
        .add_service(AppMessageServer::new(service))
        .serve(addr)
        .await.unwrap();
}
