pub mod connection;
pub mod discovery;
pub mod message;

use std::{
    collections::{HashMap, HashSet},
    net::{Ipv4Addr, SocketAddr, SocketAddrV4},
    sync::{Arc, OnceLock, Weak},
    time::Duration,
};

use futures::StreamExt;
use tokio::sync::{mpsc, oneshot, Mutex, RwLock};

use tokio_util::time::DelayQueue;
use url::Url;
use uuid::Uuid;

use self::{
    connection::{
        local::LocalConnection, tungstenite::TungsteniteClientConnection, ClusterConnection,
        ClusterConnectionBackend,
    },
    discovery::{ServiceDiscovery, ServiceDiscoveryBackend},
    message::{ClusterMessage, ClusterMessagePayload},
};

use super::{
    endpoint::{BoxedEndpoint, EndPoint, EpAddr},
    endpoint::{EpConnection, EpConnectionBackend},
    ConnectionError, ConnectionResult, EpData, EpMessage,
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

impl NodeAddr {
    pub fn get_id(&self) -> &str {
        match self {
            NodeAddr::Local(node) => &node.config.id,
            NodeAddr::Remote(node) => &node.id,
        }
    }
}

#[derive(Debug, Clone, Hash, PartialEq, Eq)]
pub struct RemoteNode {
    id: String,
    socket_addr: SocketAddr,
}

#[derive(Debug)]
pub struct NodeConfig {
    id: String,
    host: SocketAddr,
    ep_buffer_size: usize,
    cluster_message_timeout: Duration,
    cluster_inbound_buffer_size: usize,
    cluster_outbound_buffer_size: usize,
    hb_interval: Duration,
    update_remote_interval: Duration,
}
impl NodeConfig {
    pub fn as_remote_node(&self) -> RemoteNode {
        RemoteNode {
            id: self.id.clone(),
            socket_addr: self.host,
        }
    }
}

impl Default for NodeConfig {
    fn default() -> Self {
        Self {
            id: uuid::Uuid::new_v4().to_string(),
            host: SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::LOCALHOST, 80)),
            ep_buffer_size: 1024,
            cluster_message_timeout: Duration::from_secs(5),
            cluster_inbound_buffer_size: 1024,
            cluster_outbound_buffer_size: 1024,
            hb_interval: Duration::from_secs(5),
            update_remote_interval: Duration::from_secs(5),
        }
    }
}

static LOCAL_NODES: OnceLock<std::sync::RwLock<HashMap<String, Weak<Node>>>> = OnceLock::new();
fn local_nodes() -> &'static std::sync::RwLock<HashMap<String, Weak<Node>>> {
    LOCAL_NODES.get_or_init(|| std::sync::RwLock::new(HashMap::new()))
}
fn add_local_node(node: Arc<Node>) {
    let id = node.id();
    local_nodes()
        .write()
        .expect("local_nodes lock poisoned")
        .insert(id, Arc::downgrade(&node));
}
fn remove_local_node(id: String) {
    local_nodes()
        .write()
        .expect("local_nodes lock poisoned")
        .remove(&id);
}
pub(crate) fn get_local_node(id: &str) -> Option<Arc<Node>> {
    local_nodes()
        .read()
        .expect("local_nodes lock poisoned")
        .get(id)
        .and_then(|weak| weak.upgrade())
}
#[derive(Debug)]
pub struct Node {
    config: NodeConfig,
    router: RwLock<HashMap<EpAddr, NodeAddr>>,
    service_discovery: ServiceDiscovery,
    ep_message_tx: mpsc::Sender<(EpAddr, EpMessage)>,
    ep_connections: RwLock<HashMap<EpAddr, EpConnection>>,
    hb_tx: mpsc::UnboundedSender<EpAddr>,
    cluster_message_tx: mpsc::Sender<(NodeAddr, ClusterMessage)>,
    cluster_connections: RwLock<HashMap<NodeAddr, ClusterConnection>>,
    self_weak_ref: Weak<Node>,
}

impl Drop for Node {
    fn drop(&mut self) {
        remove_local_node(self.id())
    }
}

impl std::hash::Hash for Node {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.config.id.hash(state)
    }
}
impl PartialEq for Node {
    fn eq(&self, other: &Self) -> bool {
        self.config.id == other.config.id
    }
}
impl Eq for Node {}

impl Node {
    pub fn id(&self) -> String {
        self.config.id.clone()
    }
    pub fn arc(&self) -> Arc<Self> {
        self.self_weak_ref
            .upgrade()
            .expect("self reference not pointed to self")
    }
    pub async fn new(
        config: NodeConfig,
        service_discovery: impl ServiceDiscoveryBackend,
    ) -> Arc<Self> {
        let (ep_message_tx, mut ep_message_rx) = mpsc::channel(config.ep_buffer_size);
        let (cluster_message_tx, mut cluster_message_rx) =
            mpsc::channel(config.cluster_inbound_buffer_size);
        let (hb_tx, mut hb_rx) = mpsc::unbounded_channel::<EpAddr>();
        let service_discovery = service_discovery.spawn();
        let node = Arc::new_cyclic(|this| Node {
            router: Default::default(),
            ep_connections: Default::default(),
            hb_tx,
            ep_message_tx,
            cluster_message_tx,
            self_weak_ref: this.clone(),
            config,
            service_discovery,
            cluster_connections: Default::default(),
        });
        add_local_node(node.clone());
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
        // connection expire
        {
            let node = node.clone();
            let task = async move {
                let mut expqueue = DelayQueue::<EpAddr>::new();
                loop {
                    tokio::select! {
                        Some(expire) = expqueue.next() => {
                            let addr = expire.into_inner();
                            node.disconnect_ep(&addr).await;
                        }
                        addr = hb_rx.recv() => {
                            if let Some(addr) = addr {
                                expqueue.insert(addr, 3 * node.config.hb_interval);
                            } else {
                                break
                            }
                        }
                    }
                }
            };
            tokio::spawn(task);
        }
        // service discovery
        {
            {
                let node = node.clone();
                let task = async move {
                    let mut interval = tokio::time::interval(Duration::from_secs(5));
                    loop {
                        node.service_discovery.report(&node).await;
                        interval.tick().await;
                    }
                };
                tokio::spawn(task);
            }
            {
                let node = node.clone();
                let task = async move {
                    let mut interval = tokio::time::interval(node.config.update_remote_interval);
                    loop {
                        let node_list = node.service_discovery.node_list().await;
                        let connected = node
                            .cluster_connections
                            .read()
                            .await
                            .keys()
                            .map(|addr| {
                                addr.get_id().to_string()
                            })
                            .collect::<HashSet<_>>();
                        
                        for remote in node_list.into_iter() {
                            if remote.id == node.config.id {
                                continue;
                            } else if !connected.contains(&remote.id) {
                                // node is a local node
                                if let Some(local) = get_local_node(&remote.id) {
                                    node.connect_node(
                                        NodeAddr::Local(local.clone()),
                                        LocalConnection { local },
                                    )
                                    .await;
                                } else {
                                    let peer = Arc::new(remote);
                                    node.connect_node(
                                        NodeAddr::Remote(peer.clone()),
                                        TungsteniteClientConnection {
                                            peer,
                                            source: node.clone(),
                                        },
                                    )
                                    .await
                                }
                            }
                        }
                        interval.tick().await;
                    }
                };
                tokio::spawn(task);
            }
        }
        node
    }

    pub async fn connect_ep<B: EpConnectionBackend>(&self, addr: EpAddr, backend: B) {
        let ep_message_tx = self.ep_message_tx.clone();
        self.ep_connections
            .write()
            .await
            .insert(addr, backend.spawn(ep_message_tx));
    }

    pub async fn disconnect_ep(&self, addr: &EpAddr) {
        self.ep_connections.write().await.remove(addr);
        self.router.write().await.remove(addr);
        for (_, conn) in self.cluster_connections.read().await.iter() {
            // broadcast logout
        }
    }

    pub async fn connect_node<C: ClusterConnectionBackend>(&self, addr: NodeAddr, backend: C) {
        self.cluster_connections
            .write()
            .await
            .insert(addr, backend.spawn(self.cluster_message_tx.clone()));
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
        match message {
            ClusterMessagePayload::ForwardMessage { from, data } => {
                self.handle_ep_data(from, data).await;
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
            _ => {
                // no need to handle
            }
        }
    }

    pub async fn response_cluster_message(
        &self,
        uuid: Uuid,
        to: NodeAddr,
        payload: ClusterMessagePayload,
    ) {
        let result = if let Some(conn) = self.cluster_connections.read().await.get(&to) {
            conn.send_message(payload.response(uuid)).await
        } else {
            Err(ConnectionError::Unreachable)
        };
        // handle result
    }

    pub async fn send_cluster_message(
        &self,
        to: &NodeAddr,
        payload: ClusterMessagePayload,
    ) -> ConnectionResult<Uuid> {
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
    pub async fn send_cluster_message_and_wait_response(
        &self,
        to: &NodeAddr,
        payload: ClusterMessagePayload,
    ) -> ConnectionResult<ClusterMessagePayload> {
        if let Some(conn) = self.cluster_connections.read().await.get(&to) {
            let message = payload.wrap();
            conn.send_message_and_wait_response(message, self.config.cluster_message_timeout)
                .await
        } else {
            Err(ConnectionError::Unreachable)
        }
    }

    pub async fn find_ep(&self, ep: &EpAddr) -> Option<NodeAddr> {
        if let Some(ep) = self.router.read().await.get(ep) {
            Some(ep.clone())
        } else {
            let mut waiters = Vec::new();
            let rg = self.cluster_connections.read().await;
            for (addr, conn) in rg.iter() {
                let addr = addr.clone();
                if !conn.is_alive() {
                    let this = self.arc();
                    let addr = addr.clone();
                    tokio::spawn(async move {
                        this.cluster_connections.write().await.remove(&addr);
                    });
                    continue;
                }
                let addr = addr.clone();
                let arc = self.arc();
                waiters.push(Box::pin(async move {
                    let message = arc
                        .send_cluster_message_and_wait_response(
                            &addr,
                            ClusterMessagePayload::FindEp { ep: ep.clone() },
                        )
                        .await;
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

    pub async fn response_ep(&self, id: String, to: EpAddr, response: ConnectionResult) {
        if let Some(c) = self.ep_connections.read().await.get(&to) {
            c.response(id, response)
        } else if let Some(node) = self.find_ep(&to).await {
            let _ = self
                .cluster_message_tx
                .send((
                    node.clone(),
                    ClusterMessagePayload::ForwardResponse { response }.wrap(),
                ))
                .await;
        } else {
            // parse

            // addr is unreachable
            // no need to response
        }
    }

    pub async fn handle_ep_message(&self, from: EpAddr, message: EpMessage) {
        match message {
            EpMessage::Data(data) => {
                let _ = self.handle_ep_data(from, data).await;
            }
            EpMessage::Heartbeat => {
                let _ = self.handle_ep_hb(from).await;
            }
        }
    }
    pub async fn handle_ep_data(&self, from: EpAddr, data: EpData) {
        let id = data.id.clone();
        let response = self.send_ep_message(from.clone(), data).await;
        self.response_ep(id, from, response).await;
    }
    pub async fn handle_ep_hb(&self, from: EpAddr) {
        let _ = self.hb_tx.send(from);
    }
    pub async fn send_ep_message(&self, from: EpAddr, data: EpData) -> ConnectionResult {
        if let Some(c) = self.ep_connections.read().await.get(&data.peer) {
            c.send(data.id, from, data.payload)
        } else if let Some(node) = self.find_ep(&data.peer).await {
            let payload = ClusterMessagePayload::ForwardMessage { from, data };
            let resp = self
                .send_cluster_message_and_wait_response(&node, payload)
                .await?;
            let ClusterMessagePayload::ForwardResponse { response, .. } = resp else {
                return ConnectionResult::Err(ConnectionError::ProtocolError);
            };
            return response;
        } else {
            ConnectionResult::Err(ConnectionError::Unreachable)
        }
    }
}
