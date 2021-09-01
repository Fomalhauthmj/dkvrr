use tokio::sync::{mpsc, oneshot};

use crate::app;

pub enum TRequest{
    RaftMessage(raft::eraftpb::Message),
    RaftConfChangeV2(raft::eraftpb::ConfChangeV2),
    App(app::AppRequest),
}
pub enum TResponse{
    RaftMessage(app::RaftMessageMockResponse),
    RaftConfChangeV2(app::RaftConfChangeV2MockResponse),
    App(app::AppResponse),
}
pub type Callback=oneshot::Sender<TMessage>;
pub type TSender=mpsc::Sender<TMessage>;
pub type TReceiver=mpsc::Receiver<TMessage>;
pub enum TMessage{
    Request(TRequest),
    Response(TResponse),
    RequestWithCallback(TRequest,Callback),
}