use crate::app::app_message_server::AppMessage;
use crate::network::{NetworkMessage, NetworkSender};
use tokio::sync::oneshot::channel;
#[derive(Debug)]
pub struct AppMsgService {
    sender: NetworkSender,
}
impl AppMsgService {
    pub fn new(sender: NetworkSender) -> Self {
        AppMsgService { sender }
    }
}
#[tonic::async_trait]
impl AppMessage for AppMsgService {
    async fn send_raft_message(
        &self,
        request: tonic::Request<raft::eraftpb::Message>,
    ) -> Result<tonic::Response<crate::app::RaftMessageResponse>, tonic::Status> {
        let req = request.into_inner();
        let (tx, mut rx) = channel();
        match self
            .sender
            .send(NetworkMessage::RaftRequestWithCallBack(req, tx))
            .await
        {
            Ok(_) => loop {
                match rx.try_recv() {
                    Ok(msg) => {
                        if let NetworkMessage::RaftResponse(resp) = msg {
                            return Ok(tonic::Response::new(resp));
                        }
                    }
                    _ => {
                        todo!()
                    }
                }
            },
            Err(_e) => {
                todo!()
            }
        };
    }
}
