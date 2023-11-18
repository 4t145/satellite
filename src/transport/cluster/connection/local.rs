use std::sync::Arc;

use crate::transport::cluster::Node;

use super::ClusterConnectionBackend;

pub struct LocalConnection {
    pub local: Arc<Node>
}

impl ClusterConnectionBackend for LocalConnection {
    fn spawn(
        self,
        cluster_message_inbound: tokio::sync::mpsc::Sender<(crate::transport::cluster::NodeAddr, crate::transport::cluster::message::ClusterMessage)>,
    ) -> super::ClusterConnection {
        todo!()
    }
}