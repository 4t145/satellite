use std::{
    net::{Ipv4Addr, SocketAddr, SocketAddrV4},
    time::Duration,
};

use satellite::transport::{
    cluster::{discovery::redis::RedisDiscoveryBackend, connection::local::connect_locals},
    node::{Node, NodeConfig},
};
use tracing::level_filters::LevelFilter;

#[tokio::main]
pub async fn main() {
    tracing_subscriber::fmt()
        .with_max_level(LevelFilter::TRACE)
        .init();
    let sd = RedisDiscoveryBackend {
        redis_url: "redis://localhost:6379"
            .parse()
            .expect("invalid redis path"),
        path: "satellite:node".to_string(),
        poll_rate: Duration::from_secs(5),
    };
    let host = SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::LOCALHOST, 80));
    let node1 = Node::new(
        NodeConfig {
            host,
            ..Default::default()
        },
        sd.clone(),
    )
    .await;
    let node2 = Node::new(
        NodeConfig {
            host,
            ..Default::default()
        },
        sd,
    )
    .await;
    connect_locals(node1, node2).await;
    satellite::server::axum::Axum { socket_addr: host }
        .serve()
        .await;
}
