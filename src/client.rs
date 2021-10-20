use tonic::transport::{Channel, Endpoint};

use crate::app::{app_message_client::AppMessageClient, AppCmd, AppRequest};
use crate::error::Result;
use raft::prelude::*;
pub struct DkvrrClient {
    client: AppMessageClient<Channel>,
}
impl DkvrrClient {
    pub async fn connect(addr: Endpoint) -> Result<Self> {
        let client = AppMessageClient::connect(addr)
            .await
            .expect("connect error");
        Ok(DkvrrClient { client })
    }
    // TODO 考虑request id的处理
    pub async fn get(&mut self, key: String) {
        let mut req = AppRequest::default();
        req.set_cmd(AppCmd::Get);
        req.key = key;
        let resp = self.client.app(req).await;
        println!("{:?}", resp);
    }

    pub async fn set(&mut self, key: String, value: String) {
        let mut req = AppRequest::default();
        req.set_cmd(AppCmd::Set);
        req.key = key;
        req.value = value;
        let resp = self.client.app(req).await;
        println!("{:?}", resp);
    }

    pub async fn remove(&mut self, key: String) {
        let mut req = AppRequest::default();
        req.set_cmd(AppCmd::Remove);
        req.key = key;
        let resp = self.client.app(req).await;
        println!("{:?}", resp);
    }
    pub async fn add_node(&mut self, id: u64) {
        let mut cc = ConfChangeV2::default();
        let mut single = ConfChangeSingle::default();
        single.set_node_id(id);
        single.set_change_type(ConfChangeType::AddNode);
        cc.set_changes(vec![single]);
        let resp = self.client.raft_conf_change_v2(cc).await;
        println!("{:?}", resp);
    }
    pub async fn remove_node(&mut self, id: u64) {
        let mut cc = ConfChangeV2::default();
        let mut single = ConfChangeSingle::default();
        single.set_node_id(id);
        single.set_change_type(ConfChangeType::RemoveNode);
        cc.set_changes(vec![single]);
        let resp = self.client.raft_conf_change_v2(cc).await;
        println!("{:?}", resp);
    }
}
