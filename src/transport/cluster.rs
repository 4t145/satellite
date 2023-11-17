pub mod connection;
pub mod message;
use std::{
    collections::HashMap,
    sync::{Arc, Weak},
    time::Duration,
};

use tokio::sync::{mpsc, oneshot, Mutex, RwLock};

use url::Url;
use uuid::Uuid;

use self::{
    connection::{ClusterConnection, ClusterConnectionBackend},
    message::{ClusterMessage, ClusterMessagePayload},
};

use super::{
    endpoint::{BoxedEndpoint, EndPoint, EndPointAddr},
    endpoint::{EpConnection, EpConnectionBackend},
    EpData, EpMessage, ConnectionError, ConnectionResult,
};
// #[derive(Debug)]
// pub struct Router {
//     table:
// }

pub type NodeId = String;
#[derive(Debug, Clone, Hash, PartialEq, Eq)]
pub enum NodeAddr {
    Local(Arc<Node>),
    Remote(Arc<RemoteNode>),
}

#[derive(Debug, Clone, Hash, PartialEq, Eq)]
pub struct RemoteNode {
    id: String,
    url: Url,
}

#[derive(Debug, Default)]
pub struct NodeConfig {
    id: String,
    ep_buffer_size: usize,
    cluster_message_timeout: Duration,
    cluster_buffer_size: usize,
}
#[derive(Debug)]
pub struct Node {
    id: String,
    config: NodeConfig,
    id_addr_map: RwLock<HashMap<String, NodeAddr>>,
    router: RwLock<HashMap<EndPointAddr, NodeAddr>>,
    ep_message_tx: mpsc::Sender<(EndPointAddr, EpMessage)>,
    ep_connections: RwLock<HashMap<EndPointAddr, EpConnection>>,
    cluster_message_tx: mpsc::Sender<(NodeAddr, ClusterMessage)>,
    cluster_connections: RwLock<HashMap<NodeAddr, ClusterConnection>>,
    cluster_message_waiters: Mutex<HashMap<Uuid, oneshot::Sender<ClusterMessagePayload>>>,
    self_weak_ref: Weak<Node>,
}

impl std::hash::Hash for Node {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.id.hash(state)
    }
}
impl PartialEq for Node {
    fn eq(&self, other: &Self) -> bool {
        self.id == other.id
    }
}
impl Eq for Node {}

impl Node {
    pub fn arc(&self) -> Arc<Self> {
        self.self_weak_ref
            .upgrade()
            .expect("self reference not pointed to self")
    }
    pub async fn spawn(id: impl Into<String>, config: NodeConfig) -> Arc<Self> {
        let id = id.into();
        let (ep_message_tx, mut ep_message_rx) = mpsc::channel(config.ep_buffer_size);
        let (cluster_message_tx, mut cluster_message_rx) =
            mpsc::channel(config.cluster_buffer_size);
        let node = Arc::new_cyclic(|this| Node {
            id,
            id_addr_map: Default::default(),
            router: Default::default(),
            ep_connections: Default::default(),
            cluster_message_waiters: Default::default(),
            ep_message_tx,
            cluster_message_tx,
            self_weak_ref: this.clone(),
            config,
            cluster_connections: Default::default(),
        });
        // handle ep messages
        {
            let node = node.clone();
            let handle_ep_message_task = async move {
                while let Some((addr, message)) = ep_message_rx.recv().await {
                    let node = node.clone();
                    tokio::spawn(async move {
                        node.handle_ep_message(addr, message).await;
                    });
                }
            };
            tokio::spawn(handle_ep_message_task);
        }
        // handle cluster messages
        {
            let node = node.clone();
            let handle_cluster_message_task = async move {
                while let Some((source, message)) = cluster_message_rx.recv().await {
                    let node = node.clone();
                    tokio::spawn(async move {
                        node.handle_cluster_message(&source, message).await;
                    });
                }
            };
            tokio::spawn(handle_cluster_message_task);
        }
        node
    }

    pub async fn connect_ep<B: EpConnectionBackend>(&self, addr: EndPointAddr, backend: B) {
        let ep_message_tx = self.ep_message_tx.clone();
        self.ep_connections
            .write()
            .await
            .insert(addr, backend.spawn(ep_message_tx));
    }

    pub async fn connect_node<C: ClusterConnectionBackend>(&self, addr: NodeAddr, backend: C) {
        backend.spawn(self.cluster_message_tx.clone());
    }

    pub fn as_local_node_addr(&self) -> NodeAddr {
        NodeAddr::Local(
            self.self_weak_ref
                .upgrade()
                .expect("self reference not pointed to self"),
        )
    }

    async fn handle_cluster_message(&self, source: &NodeAddr, message: ClusterMessage) {
        let (uuid, message) = message.unwrap();
        if let Some(waiter) = self.cluster_message_waiters.lock().await.remove(&uuid) {
            let _ = waiter.send(message);
            return;
        }
        match message {
            ClusterMessagePayload::ForwardMessage { from, data } => {
                let response = self.handle_ep_data(from, data).await;
                let _ = self.response_cluster_message(
                    uuid,
                    source.clone(),
                    ClusterMessagePayload::ForwardResponse { response },
                );
            }
            ClusterMessagePayload::Ping => {
                self.response_cluster_message(
                    uuid,
                    source.clone(),
                    ClusterMessagePayload::Pong {
                        node_id: self.config.id.clone(),
                    },
                )
                .await;
            }
            ClusterMessagePayload::FindEp { ep } => {
                let hit = self.router.read().await.get(&ep).is_some();
                self.response_cluster_message(
                    uuid,
                    source.clone(),
                    ClusterMessagePayload::FindEpResponse {
                        ep: hit.then_some(ep),
                    },
                )
                .await;
            }
            _ => {}
        }
    }

    pub async fn response_cluster_message(
        &self,
        uuid: Uuid,
        to: NodeAddr,
        payload: ClusterMessagePayload,
    )  {
        let result = if let Some(conn) = self.cluster_connections.read().await.get(&to) {
            conn.send_message(payload.response(uuid)).await
        } else {
            Err(ConnectionError::Unreachable)
        };
        // handle result
    }

    pub async fn send_cluster_message(&self, to: NodeAddr, payload: ClusterMessagePayload) -> ConnectionResult<Uuid> {
        if let Some(conn) = self.cluster_connections.read().await.get(&to) {
            let message = payload.wrap();
            let id = message.id();
            conn.send_message(message).await?;
            Ok(id)
        } else {
            Err(ConnectionError::Unreachable)
        }
    }

    // if timeout, return none
    pub async fn wait_cluster_response(
        &self,
        id: Uuid,
        timeout: Option<Duration>,
    ) -> oneshot::Receiver<ClusterMessagePayload> {
        let (tx, rx) = oneshot::channel();
        self.cluster_message_waiters.lock().await.insert(id, tx);
        let weak = self.self_weak_ref.clone();
        if let Some(timeout) = timeout {
            tokio::spawn(async move {
                tokio::time::sleep(timeout).await;
                if let Some(this) = weak.upgrade() {
                    if let Some(tx) = this.cluster_message_waiters.lock().await.remove(&id) {
                        drop(tx)
                    }
                }
            });
        }
        rx
    }

    pub async fn find_ep(&self, ep: &EndPointAddr) -> Option<NodeAddr> {
        if let Some(ep) = self.router.read().await.get(ep) {
            Some(ep.clone())
        } else {
            let mut waiters = Vec::new();
            let rg = self.cluster_connections.read().await;
            for (addr, conn) in rg.iter() {
                if !conn.is_alive() {
                    let this = self.arc();
                    let addr = addr.clone();
                    tokio::spawn(async move {
                        this.cluster_connections.write().await.remove(&addr);
                    });
                    continue;
                }
                let message: ClusterMessage =
                    ClusterMessagePayload::FindEp { ep: ep.clone() }.wrap();
                let id = message.id();
                let wait = self
                    .wait_cluster_response(id, Some(self.config.cluster_message_timeout))
                    .await;
                if self
                    .cluster_message_tx
                    .send((addr.clone(), message))
                    .await
                    .is_err()
                {
                    continue;
                };
                let addr = addr.clone();
                waiters.push(Box::pin(async move {
                    let message = wait.await;
                    if let Ok(ClusterMessagePayload::FindEpResponse { ep: Some(_) }) = message {
                        Some(addr)
                    } else {
                        None
                    }
                }));
            }
            drop(rg);
            loop {
                let (result, _, remain) = futures::future::select_all(waiters).await;
                if remain.is_empty() {
                    break None;
                } else if let Some(addr) = result {
                    {
                        let this = self.arc();
                        let ep = ep.clone();
                        let addr = addr.clone();
                        tokio::spawn(async move {
                            this.router.write().await.insert(ep, addr);
                        });
                    }
                    break Some(addr);
                } else {
                    waiters = remain;
                    continue;
                }
            }
        }
    }

    pub async fn response_ep(&self, id: &str, to: &EndPointAddr, response: ConnectionResult) {
        if let Some(c) = self.ep_connections.read().await.get(to) {
            c.response(id, response)
        } else if let Some(node) = self.find_ep(to).await {
            let _ = self
                .cluster_message_tx
                .send((
                    node.clone(),
                    ClusterMessagePayload::ForwardResponse {
                        response,
                        // to: to.clone(),
                        // id: id.to_string(),
                    }
                    .wrap(),
                ))
                .await;
        } else {
            // addr is unreachable
            // no need to response
        }
    }

    pub async fn handle_ep_message(&self, ep_addr: EndPointAddr, message: EpMessage) -> ConnectionResult {
        todo!("handle_ep_message")
    }
    pub async fn handle_ep_data(&self, ep_addr: EndPointAddr, message: EpData) -> ConnectionResult {
        todo!("handle_ep_data")
    }

    pub async fn send_ep_message(
        &self,
        id: &str,
        from: &EndPointAddr,
        to: &EndPointAddr,
        payload: &[u8],
    ) -> ConnectionResult {
        if let Some(c) = self.ep_connections.read().await.get(to) {
            c.send(id, to, payload)
        } else if let Some(node) = self.find_ep(to).await {
            let message = ClusterMessagePayload::ForwardMessage {
                from: from.clone(),
                data: super::EpData {
                    from: from.clone(),
                    id: id.to_string(),
                    to: to.clone(),
                    payload: payload.to_vec(),
                },
            }
            .wrap();
            let id = message.id();
            let rx = self
                .wait_cluster_response(id, Some(self.config.cluster_message_timeout))
                .await;
            if self.cluster_message_tx.send((node, message)).await.is_err() {
                return ConnectionResult::Err(ConnectionError::Unreachable);
            }
            let Ok(resp) = rx.await else {
                return ConnectionResult::Err(ConnectionError::Overload);
            };
            let ClusterMessagePayload::ForwardResponse { response, .. } = resp else {
                return ConnectionResult::Err(ConnectionError::ProtocolError);
            };
            return response;
        } else {
            ConnectionResult::Err(ConnectionError::Unreachable)
        }
    }
}
