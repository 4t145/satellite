use std::collections::HashMap;

use tokio::sync::mpsc;

use crate::transport::ConnectionResult;

use super::{NodeAddr, message::ClusterMessage, RemoteNode};
pub trait ClusterConnectionBackend {
    fn spawn(self, cluster_message_tx: mpsc::Sender<(NodeAddr, ClusterMessage)>) -> ClusterConnection;
}
#[derive(Debug)]
pub struct ClusterConnection {
    // pub cluster_message_tx: mpsc::Sender<ClusterMessage>,
}

impl ClusterConnection {
    pub fn is_alive(&self) -> bool {
        todo!()
    }
    pub async fn send_message(&self, message: ClusterMessage) -> ConnectionResult {
        todo!()
    }
}

pub struct ClusterClient {
    pub remotes: HashMap<NodeAddr, mpsc::Sender<ClusterMessage>>
}