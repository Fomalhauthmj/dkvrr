use crate::app::app_message_server::AppMessage;
use crate::types::{TMessage, TRequest, TResponse, TSender};
use slog::{error, trace, Logger};
use tokio::sync::oneshot::channel;

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
        trace!(self.logger, "recv raft message {:?}", request);
        let msg = request.into_inner();
        let tmsg = TMessage::Request(TRequest::RaftMessage(msg));
        match self.sender.send(tmsg).await {
            Ok(_) => {
                return Ok(tonic::Response::new(
                    crate::app::RaftMessageMockResponse::default(),
                ));
            }
            Err(e) => {
                error!(self.logger, "send tmsg to core {}", e);
                return Err(tonic::Status::new(
                    tonic::Code::Internal,
                    "send tmsg to core error",
                ));
            }
        };
    }

    async fn raft_conf_change_v2(
        &self,
        request: tonic::Request<raft::eraftpb::ConfChangeV2>,
    ) -> Result<tonic::Response<crate::app::RaftConfChangeV2MockResponse>, tonic::Status> {
        trace!(self.logger, "recv raft conf change v2 {:?}", request);
        let msg = request.into_inner();
        let tmsg = TMessage::Request(TRequest::RaftConfChangeV2(msg));
        match self.sender.send(tmsg).await {
            Ok(_) => {
                return Ok(tonic::Response::new(
                    crate::app::RaftConfChangeV2MockResponse::default(),
                ));
            }
            Err(e) => {
                error!(self.logger, "send tmsg to core {}", e);
                return Err(tonic::Status::new(
                    tonic::Code::Internal,
                    "send tmsg to core error",
                ));
            }
        };
    }

    async fn app(
        &self,
        request: tonic::Request<crate::app::AppRequest>,
    ) -> Result<tonic::Response<crate::app::AppResponse>, tonic::Status> {
        trace!(self.logger, "recv raft message {:?}", request);
        let msg = request.into_inner();
        let (tx, rx) = channel::<TMessage>();
        let tmsg = TMessage::RequestWithCallback(TRequest::App(msg), tx);
        match self.sender.send(tmsg).await {
            Ok(_) => match rx.await {
                Ok(tmsg) => {
                    if let TMessage::Response(TResponse::App(resp)) = tmsg {
                        return Ok(tonic::Response::new(resp));
                    }
                    return Err(tonic::Status::new(
                        tonic::Code::Internal,
                        "unmatch response from core error",
                    ));
                }
                Err(e) => {
                    error!(self.logger, "recv callback tmsg from core {}", e);
                    return Err(tonic::Status::new(
                        tonic::Code::Internal,
                        "recv callback tmsg from core error",
                    ));
                }
            },
            Err(e) => {
                error!(self.logger, "send tmsg to core {}", e);
                return Err(tonic::Status::new(
                    tonic::Code::Internal,
                    "send tmsg to core error",
                ));
            }
        };
    }
}
