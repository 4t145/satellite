use std::{
    collections::{HashMap, HashSet},
    net::{Ipv4Addr, SocketAddr, SocketAddrV4},
    sync::{Arc, OnceLock, Weak},
    time::Duration,
};

use futures::StreamExt;
use tokio::sync::{mpsc, RwLock};

use tokio_util::time::DelayQueue;
use tracing::{instrument, info};

use super::cluster::{
    connection::{
        local::connect_locals, tungstenite::TungsteniteClientConnection, ClusterConnection,
    },
    discovery::{ServiceDiscovery, ServiceDiscoveryBackend},
    message::ClusterMessage,
};

use super::{
    endpoint::EpConnection,
    endpoint::{message::EpMsgE2H, EpAddr},
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
    pub id: String,
    pub socket_addr: SocketAddr,
}

#[derive(Debug)]
pub struct NodeConfig {
    pub id: String,
    pub host: SocketAddr,
    pub ep_buffer_size: usize,
    pub cluster_message_timeout: Duration,
    pub cluster_inbound_buffer_size: usize,
    pub cluster_outbound_buffer_size: usize,
    pub hb_interval: Duration,
    pub update_remote_interval: Duration,
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

pub(super) static LOCAL_NODES: OnceLock<std::sync::RwLock<HashMap<String, Weak<Node>>>> =
    OnceLock::new();
pub(super) fn local_nodes() -> &'static std::sync::RwLock<HashMap<String, Weak<Node>>> {
    LOCAL_NODES.get_or_init(|| std::sync::RwLock::new(HashMap::new()))
}
pub(super) fn add_local_node(node: Arc<Node>) {
    let id = node.id();
    local_nodes()
        .write()
        .expect("local_nodes lock poisoned")
        .insert(id, Arc::downgrade(&node));
}
pub(super) fn remove_local_node(id: String) {
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
    pub config: NodeConfig,
    pub(super) router: RwLock<HashMap<EpAddr, NodeAddr>>,
    pub(super) service_discovery: ServiceDiscovery,
    pub(super) ep_message_tx: mpsc::Sender<(EpAddr, EpMsgE2H)>,
    pub(super) ep_connections: RwLock<HashMap<EpAddr, EpConnection>>,
    pub(super) hb_tx: mpsc::UnboundedSender<EpAddr>,
    pub(super) cluster_message_tx: mpsc::Sender<(NodeAddr, ClusterMessage)>,
    pub(super) cluster_connections: RwLock<HashMap<NodeAddr, ClusterConnection>>,
    pub(super) self_weak_ref: Weak<Node>,
}

impl Drop for Node {
    fn drop(&mut self) {
        remove_local_node(self.id());
        tracing::info!("node {id} dropped", id = self.id());
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
    #[instrument]
    pub async fn new(
        config: NodeConfig,
        service_discovery: impl ServiceDiscoveryBackend,
    ) -> Arc<Self> {
        info!("creating new node with config: {config:?}");
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
                            .map(|addr| addr.get_id().to_string())
                            .collect::<HashSet<_>>();

                        for remote in node_list.into_iter() {
                            if remote.id == node.config.id {
                                continue;
                            } else if !connected.contains(&remote.id) {
                                // node is a local node
                                if let Some(peer) = get_local_node(&remote.id) {
                                    connect_locals(node.clone(), peer).await;
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
}
