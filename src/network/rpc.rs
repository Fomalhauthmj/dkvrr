use crate::app::app_message_server::AppMessage;
use crate::{TMessage, TSender};
use slog::{error, Logger};
use tokio::sync::oneshot;

#[derive(Debug)]
pub struct AppMsgService {
    logger: Logger,
    sender: TSender,
}
impl AppMsgService {
    pub fn new(logger: Logger, sender: TSender) -> Self {
        AppMsgService { logger, sender }
    }
}
#[tonic::async_trait]
impl AppMessage for AppMsgService {
    async fn raft_message(
        &self,
        request: tonic::Request<raft::eraftpb::Message>,
    ) -> Result<tonic::Response<crate::app::RaftMessageMockResponse>, tonic::Status> {
        let msg = request.into_inner();
        let tmsg = TMessage::RaftMessage(msg);
        match self.sender.send(tmsg).await {
            Ok(_) => {
                return Ok(tonic::Response::new(
                    crate::app::RaftMessageMockResponse::default(),
                ));
            }
            Err(e) => {
                error!(self.logger, "send raft message to consensus {}", e);
                return Err(tonic::Status::new(
                    tonic::Code::Internal,
                    "send raft message to consensus",
                ));
            }
        };
    }

    async fn raft_conf_change_v2(
        &self,
        request: tonic::Request<raft::eraftpb::ConfChangeV2>,
    ) -> Result<tonic::Response<crate::app::RaftConfChangeV2MockResponse>, tonic::Status> {
        let msg = request.into_inner();
        let tmsg = TMessage::RaftConfChangeV2(msg);
        match self.sender.send(tmsg).await {
            Ok(_) => {
                return Ok(tonic::Response::new(
                    crate::app::RaftConfChangeV2MockResponse::default(),
                ));
            }
            Err(e) => {
                error!(self.logger, "send raft conf change v2 to consensus {}", e);
                return Err(tonic::Status::new(
                    tonic::Code::Internal,
                    "send raft conf change v2 to consensus",
                ));
            }
        };
    }

    async fn app(
        &self,
        request: tonic::Request<crate::app::AppRequest>,
    ) -> Result<tonic::Response<crate::app::AppResponse>, tonic::Status> {
        let msg = request.into_inner();
        let (tx, rx) = oneshot::channel();
        let tmsg = TMessage::AppRequestWithCallback(msg, tx);
        match self.sender.send(tmsg).await {
            Ok(_) => match rx.await {
                Ok(tmsg) => {
                    if let TMessage::AppResponse(resp) = tmsg {
                        return Ok(tonic::Response::new(resp));
                    }
                    return Err(tonic::Status::new(
                        tonic::Code::Internal,
                        "app response parse",
                    ));
                }
                Err(e) => {
                    error!(self.logger, "recv app response from consensus {}", e);
                    return Err(tonic::Status::new(
                        tonic::Code::Internal,
                        "recv app response from consensus",
                    ));
                }
            },
            Err(e) => {
                error!(self.logger, "send app request to consensus {}", e);
                return Err(tonic::Status::new(
                    tonic::Code::Internal,
                    "send app request to consensus",
                ));
            }
        };
    }
}
