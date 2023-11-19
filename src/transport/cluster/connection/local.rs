use std::sync::Arc;

use tokio::sync::mpsc;

use crate::transport::cluster::{message::ClusterMessage, Node, NodeAddr};

use super::{ClusterConnection, ClusterConnectionBackend};
#[derive(Debug)]
pub struct HalfLocalConnection {
    pub peer: Arc<Node>,
}

pub async fn connect_locals(a: Arc<Node>, b: Arc<Node>) {
    let addr_a = NodeAddr::Local(a.clone());
    let addr_b = NodeAddr::Local(b.clone());
    a.connect_node(addr_b, HalfLocalConnection { peer: b.clone() })
        .await;
    b.connect_node(addr_a, HalfLocalConnection { peer: a.clone() })
        .await;
}
impl ClusterConnectionBackend for HalfLocalConnection {
    fn spawn(
        self,
        _cluster_message_inbound: tokio::sync::mpsc::Sender<(
            crate::transport::cluster::NodeAddr,
            crate::transport::cluster::message::ClusterMessage,
        )>,
    ) -> super::ClusterConnection {
        // let cluster_response_signal: Arc<Mutex<HashMap<Uuid, oneshot::Sender<ClusterMessagePayload>>>> = Default::default();
        let (cluster_message_outbound_tx, mut cluster_message_outbound_rx) =
            mpsc::channel::<ClusterMessage>(self.peer.config.cluster_outbound_buffer_size);

        // wait hello
        let task = async move {
            let addr = NodeAddr::Local(self.peer.clone());
            let local = self.peer.clone();
            let outbound_task = tokio::spawn(async move {
                while let Some(msg) = cluster_message_outbound_rx.recv().await {
                    if local
                        .cluster_message_tx
                        .send((addr.clone(), msg))
                        .await
                        .is_err()
                    {
                        break;
                    }
                }
            });
            outbound_task.await
        };
        tokio::spawn(task);
        ClusterConnection {
            cluster_message_outbound_tx,
            cluster_response_signal: Default::default(),
        }
    }
}
