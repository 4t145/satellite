use std::{net::SocketAddr, sync::Arc};

use crate::transport::{
    cluster::connection::axum::AxumConnection,
    endpoint::{websocket::WebsocketConnection, EpAddr},
    node::{get_local_node, Node, NodeAddr, RemoteNode},
};

use axum::{
    body::{self},
    extract::{
        ws::{WebSocket, WebSocketUpgrade},
        ConnectInfo, Path,
    },
    http::{Response, StatusCode},
    response::IntoResponse,
    routing::get,
    Router,
};
use tracing::{instrument, info};

async fn cluster(
    ws: WebSocketUpgrade,
    // user_agent: Option<TypedHeader<headers::UserAgent>>,
    ConnectInfo(addr): ConnectInfo<SocketAddr>,
    Path(id): Path<String>,
    Path(peer_id): Path<String>,
) -> impl IntoResponse {
    if let Some(node) = get_local_node(&id) {
        let remote_node = RemoteNode {
            id: peer_id,
            socket_addr: addr,
        };
        ws.on_upgrade(move |socket| handle_socket_cluster(socket, remote_node, node))
    } else {
        Response::builder()
            .status(StatusCode::NOT_FOUND)
            .body(body::boxed(body::Empty::new()))
            .unwrap()
    }
}

async fn user(
    ws: WebSocketUpgrade,
    ConnectInfo(addr): ConnectInfo<SocketAddr>,
    Path(id): Path<String>,
) -> impl IntoResponse {
    if let Some(node) = get_local_node(&id) {
        ws.on_upgrade(move |socket| handle_socket_user(socket, addr, node))
    } else {
        Response::builder()
            .status(StatusCode::NOT_FOUND)
            .body(body::boxed(body::Empty::new()))
            .unwrap()
    }
}

async fn handle_socket_user(socket: WebSocket, peer: SocketAddr, node: Arc<Node>) {
    let ep = EpAddr::new("user", peer.to_string());
    let backend = WebsocketConnection {
        source: EpAddr::new("user", peer.to_string()),
        peer_addr: peer,
        websocket: socket,
        buffer_size: node.config.ep_buffer_size,
    };
    node.connect_ep(ep, backend).await
}

#[instrument]
async fn handle_socket_cluster(socket: WebSocket, peer: RemoteNode, node: Arc<Node>) {
    info!("new node connecting from axum");
    let peer = Arc::new(peer);
    let backend = AxumConnection {
        socket,
        peer: peer.clone(),
        cluster_outbound_buffer_size: node.config.cluster_outbound_buffer_size,
    };
    let addr = NodeAddr::Remote(peer);
    node.connect_node(addr, backend).await;
}

pub struct Axum {
    pub socket_addr: SocketAddr,
}

impl Axum {
    pub async fn serve(self) {
        let app = Router::new()
            .route("/cluster/:id/:peerid", get(cluster))
            .route("/user/:id", get(user));

        axum::Server::bind(&self.socket_addr)
            .serve(app.into_make_service())
            .await
            .unwrap();
    }
}
