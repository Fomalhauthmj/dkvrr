use crate::app::{app_message_client::AppMessageClient, app_message_server::AppMessageServer};
use crate::network::rpc::AppMsgService;
use crate::types::{TMessage, TRequest, TResponse};
use slog::{debug, error, trace, Logger};
use std::net::SocketAddr;
use tonic::transport::{Endpoint, Server};

pub async fn serve(logger: Logger, addr: SocketAddr, service: AppMsgService) {
    trace!(logger, "will start serve");
    match Server::builder()
        .concurrency_limit_per_connection(32)
        .add_service(AppMessageServer::new(service))
        .serve(addr)
        .await
    {
        Ok(_) => debug!(logger, "serve started"),
        Err(e) => error!(logger, "serve error {}", e),
    }
    trace!(logger, "serve finished");
}
pub async fn send_to(logger: Logger, addr: Endpoint, msg: TMessage) {
    let remote = addr.clone();
    let client = AppMessageClient::connect(addr).await;
    if client.is_err() {
        error!(
            logger,
            "client connect to {:?} error {}",
            remote,
            client.unwrap_err()
        );
        return;
    };
    let mut client = client.unwrap();
    trace!(logger, "client connect to {:?} success", remote);
    match msg {
        TMessage::RequestWithCallback(treq, cb) => match treq {
            TRequest::App(req) => match client.app(req).await {
                Ok(rep) => cb
                    .send(TMessage::Response(TResponse::App(rep.into_inner())))
                    .unwrap_or(error!(logger, "callback error")),
                Err(e) => error!(logger, "send error {}", e),
            },
            _ => unimplemented!(),
        },
        TMessage::Request(treq) => match treq {
            TRequest::RaftConfChangeV2(req) => match client.raft_conf_change_v2(req).await {
                Ok(_) => trace!(logger, "send raft conf change susscess"),
                Err(e) => error!(logger, "send error {}", e),
            },
            TRequest::RaftMessage(req) => match client.raft_message(req).await {
                Ok(_) => trace!(logger, "send raft message susscess"),
                Err(e) => error!(logger, "send error {}", e),
            },
            _ => unimplemented!(),
        },
        _ => unimplemented!(),
    };
}
