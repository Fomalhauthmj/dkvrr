use crate::app;
use raft::prelude::*;
use tokio::sync::{mpsc, oneshot};
pub type Callback = oneshot::Sender<TMessage>;
pub type TSender = mpsc::Sender<TMessage>;
pub type TReceiver = mpsc::Receiver<TMessage>;
#[derive(Debug)]
pub enum TMessage {
    RaftMessage(Message),
    RaftConfChangeV2(ConfChangeV2),
    AppRequest(app::AppRequest),
    AppRequestWithCallback(app::AppRequest, Callback),
    AppResponse(app::AppResponse),
}
