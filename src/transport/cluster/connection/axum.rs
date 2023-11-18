use std::{
    collections::{HashMap, HashSet},
    net::SocketAddr,
    sync::{Arc, Weak},
};

use futures::{SinkExt, StreamExt};
use tokio::sync::{mpsc, RwLock};

use crate::transport::cluster::{
    get_local_node,
    message::{ClusterMessage, ClusterMessagePayload, Hello},
    Node, NodeAddr, RemoteNode,
};

use super::{ClusterConnection, ClusterConnectionBackend};

use axum::{
    body::{self, Body, HttpBody},
    extract::{
        ws::{CloseFrame, Message, WebSocket, WebSocketUpgrade},
        ConnectInfo, Path,
    },
    http::{Response, StatusCode},
    response::IntoResponse,
    routing::get,
    Router,
};

async fn ws_handler(
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
        ws.on_upgrade(move |socket| handle_socket(socket, remote_node, node))
    } else {
        Response::builder()
            .status(StatusCode::NOT_FOUND)
            .body(body::boxed(body::Empty::new()))
            .unwrap()
    }
}

async fn handle_socket(socket: WebSocket, peer: RemoteNode, node: Arc<Node>) {
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
    socket_addr: SocketAddr,
}

impl Axum {
    pub async fn serve(self) {
        let app = Router::new().route("/ws/:id/:peerid", get(ws_handler));

        axum::Server::bind(&self.socket_addr)
            .serve(app.into_make_service())
            .await
            .unwrap();
    }
}

// for close code: https://developer.mozilla.org/en-US/docs/Web/API/CloseEvent/code
const CLOSE_CODE_PRIVATE: u16 = 4000;
// const CLOSE_CODE_INVALID_BINCODE: u16 = 4001;
// const CLOSE_CODE_EXPECT_HELLO: u16 = 4002;
pub(crate) struct AxumConnection {
    pub socket: WebSocket,
    pub peer: Arc<RemoteNode>,
    pub cluster_outbound_buffer_size: usize,
}
impl ClusterConnectionBackend for AxumConnection {
    fn spawn(
        self,
        cluster_message_inbound: tokio::sync::mpsc::Sender<(
            crate::transport::cluster::NodeAddr,
            crate::transport::cluster::message::ClusterMessage,
        )>,
    ) -> super::ClusterConnection {
        let (mut tx, mut rx) = self.socket.split();
        // let cluster_response_signal: Arc<Mutex<HashMap<Uuid, oneshot::Sender<ClusterMessagePayload>>>> = Default::default();
        let (cluster_message_outbound_tx, mut cluster_message_outbound_rx) =
            mpsc::channel::<ClusterMessage>(self.cluster_outbound_buffer_size);

        // wait hello
        let task = async move {
            let addr = NodeAddr::Remote(self.peer);
            let inbound_task = tokio::spawn(async move {
                while let Some(recv) = rx.next().await {
                    match recv {
                        Ok(Message::Binary(data)) => {
                            if let Ok(message) = bincode::deserialize(&data) {
                                let _ = cluster_message_inbound.send((addr.clone(), message)).await;
                            }
                        }
                        Ok(Message::Close(cf)) => {
                            if let Some(cf) = cf {
                                if cf.code >= CLOSE_CODE_PRIVATE {
                                    // warn protocal error
                                }
                            }
                            let _ = cluster_message_inbound
                                .send((addr.clone(), ClusterMessagePayload::Close.wrap()))
                                .await;
                            break;
                        }
                        Ok(_) => {}
                        Err(_e) => {
                            break;
                        }
                    }
                }
            });
            let outbound_task = tokio::spawn(async move {
                while let Some(v) = cluster_message_outbound_rx.recv().await {
                    if let Ok(bin) = bincode::serialize(&v) {
                        let _ = tx.send(Message::Binary(bin)).await;
                    }
                }
            });
            tokio::select! {
                _ = inbound_task => {}
                _ = outbound_task => {}
            }
        };
        tokio::spawn(task);
        ClusterConnection {
            cluster_message_outbound_tx,
            cluster_response_signal: Default::default(),
        }
    }
}
