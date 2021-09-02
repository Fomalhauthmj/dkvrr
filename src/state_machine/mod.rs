use std::collections::HashMap;

use slog::{error, trace, Logger};

use crate::app::*;
use crate::types::{TMessage, TReceiver, TRequest, TResponse, TSender};
pub struct StateMachine {
    logger: Logger,
    inner: HashMap<String, String>,
    state_machine_to_consensus_tx: TSender,
}
impl StateMachine {
    pub fn new(logger: Logger, state_machine_to_consensus_tx: TSender) -> Self {
        StateMachine {
            logger,
            inner: HashMap::new(),
            state_machine_to_consensus_tx,
        }
    }
    pub async fn start(&mut self, mut consensus_to_state_machine_rx: TReceiver) {
        loop {
            match consensus_to_state_machine_rx.recv().await {
                Some(TMessage::Request(TRequest::App(req))) => {
                    let resp = self.apply(req);
                    let resp = TMessage::Response(TResponse::App(resp));
                    match self.state_machine_to_consensus_tx.send(resp).await {
                        Ok(_) => trace!(self.logger, "send apply resp to consensus success"),
                        Err(e) => error!(self.logger, "send apply resp to consensus error {}", e),
                    }
                }
                None => error!(self.logger, "consensus_to_state_machine_rx closed"),
                _ => error!(self.logger, "unsupported request"),
            }
        }
    }
    fn apply(&mut self, req: AppRequest) -> AppResponse {
        let mut resp = AppResponse::default();
        resp.id = req.id;
        match req.cmd() {
            AppCmd::Put => {
                match self.inner.insert(req.key, req.value) {
                    Some(s) => resp.value = s,
                    None => resp.value = "None".to_string(),
                }
                resp.success = true;
            }
            AppCmd::Get => match self.inner.get(&req.key) {
                Some(s) => {
                    resp.value = s.to_string();
                    resp.success = true;
                }
                None => {
                    resp.reason = "key not exist".to_string();
                    resp.success = false;
                }
            },
            AppCmd::Delete => match self.inner.remove(&req.key) {
                Some(s) => {
                    resp.value = s.to_string();
                    resp.success = true;
                }
                None => {
                    resp.reason = "key not exist".to_string();
                    resp.success = false;
                }
            },
        }
        resp
    }
}
