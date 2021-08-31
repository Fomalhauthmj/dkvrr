use crate::app::app_message_server::AppMessage;
use crate::network::{NetworkMessage, NetworkSender};
use slog::{Logger, debug, error, info};
use tokio::sync::oneshot::channel;
#[derive(Debug)]
pub struct AppMsgService {
    sender: NetworkSender,
    logger: Logger,
}
impl AppMsgService {
    pub fn new(sender: NetworkSender, logger: Logger) -> Self {
        AppMsgService { sender, logger }
    }
}
#[tonic::async_trait]
impl AppMessage for AppMsgService {
    async fn send_raft_message(
        &self,
        request: tonic::Request<raft::eraftpb::Message>,
    ) -> Result<tonic::Response<crate::app::RaftMessageResponse>, tonic::Status> {
        debug!(self.logger,"recv message from remote {:?}",request);
        let req = request.into_inner();
        let (tx, rx) = channel();
        match self
            .sender
            .send(NetworkMessage::RaftRequestWithCallBack(req, tx))
            .await
        {
            Ok(_) =>{
                info!(self.logger,"success send to server ");
                match rx.await {
                    Ok(msg) => {
                        debug!(self.logger,"server has processd message");
                        if let NetworkMessage::RaftResponse(resp) = msg {
                            return Ok(tonic::Response::new(resp));
                        }
                    }
                    Err(e)=> {
                        debug!(self.logger,"{}",e);
                        return Err(tonic::Status::new(tonic::Code::Unimplemented, "sender error"));
                    }
                }
            },
            Err(e) => {
                error!(self.logger,"send error {}",e);
                return Err(tonic::Status::new(tonic::Code::Unimplemented, "sender error"));
            }
        };
        return Err(tonic::Status::new(tonic::Code::Unimplemented, "sender error"));
    }
}
