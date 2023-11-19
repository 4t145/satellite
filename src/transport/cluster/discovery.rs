use std::{collections::HashSet, fmt::Debug};

use tokio::sync::{mpsc, watch};

use crate::transport::node::RemoteNode;

use super::Node;

pub mod redis;

pub trait ServiceDiscoveryBackend: Debug {
    fn spawn(self) -> ServiceDiscovery;
}
#[derive(Debug)]
pub struct ServiceDiscovery {
    node_list: watch::Receiver<HashSet<RemoteNode>>,
    reporter: mpsc::Sender<RemoteNode>,
}

impl ServiceDiscovery {
    pub async fn node_list(&self) -> HashSet<RemoteNode> {
        self.node_list.borrow().clone()
    }

    pub async fn report(&self, node: &Node) {
        let remote_node = node.config.as_remote_node();
        let _ = self.reporter.send(remote_node).await;
    }
}
