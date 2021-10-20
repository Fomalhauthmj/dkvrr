use crate::app::{app_message_client::AppMessageClient, app_message_server::AppMessageServer};
use crate::network::rpc::AppMsgService;
use raft::prelude::*;
use slog::{error, Logger};
use std::net::SocketAddr;
use tonic::transport::{Endpoint, Server};
pub async fn serve(logger: Logger, addr: SocketAddr, service: AppMsgService) {
    Server::builder()
        .concurrency_limit_per_connection(32)
        .add_service(AppMessageServer::new(service))
        .serve(addr)
        .await
        .unwrap_or_else(|e| {
            error!(logger, "serve {}", e);
        });
}
pub async fn send_to(logger: Logger, addr: Endpoint, msg: Message) {
    match AppMessageClient::connect(addr.clone()).await {
        Ok(mut c) => {
            let _ = c.raft_message(msg).await;
        }
        Err(e) => {
            error!(logger, "client connect to {:?} {}", addr, e);
        }
    }
}
