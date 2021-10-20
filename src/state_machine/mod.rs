use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use slog::{error, warn, Logger};

use crate::app::*;
use crate::{TMessage, TReceiver, TSender};
pub struct StateMachine {
    logger: Logger,
    inner: Arc<Mutex<HashMap<String, String>>>,
    to_consensus: TSender,
    from_consensus: TReceiver,
}
impl StateMachine {
    pub fn new(
        logger: Logger,
        inner: Arc<Mutex<HashMap<String, String>>>,
        to_consensus: TSender,
        from_consensus: TReceiver,
    ) -> Self {
        StateMachine {
            logger,
            inner,
            to_consensus,
            from_consensus,
        }
    }
    pub async fn run(&mut self) {
        loop {
            match self.from_consensus.recv().await {
                Some(TMessage::AppRequest(req)) => {
                    let resp = TMessage::AppResponse(self.apply(req));
                    self.to_consensus.send(resp).await.unwrap_or_else(|e| {
                        error!(self.logger, "send app response to consensus {}", e);
                    });
                }
                None => {
                    warn!(self.logger, "consensus to state machine closed");
                    break;
                }
                _ => error!(self.logger, "unsupported tmsg from consensus"),
            }
        }
    }
    fn apply(&mut self, req: AppRequest) -> AppResponse {
        let mut resp = AppResponse {
            id: req.id,
            ..Default::default()
        };
        let mut inner = self.inner.lock().unwrap();
        match req.cmd() {
            AppCmd::Set => {
                inner.insert(req.key, req.value);
                resp.success = true;
            }
            AppCmd::Get => match inner.get(&req.key) {
                Some(s) => {
                    resp.value = s.clone();
                    resp.success = true;
                }
                None => {
                    resp.reason = "Key not exist".to_string();
                    resp.success = false;
                }
            },
            AppCmd::Remove => match inner.remove(&req.key) {
                Some(s) => {
                    resp.value = s;
                    resp.success = true;
                }
                None => {
                    resp.reason = "Key not exist".to_string();
                    resp.success = false;
                }
            },
        }
        resp
    }
}
