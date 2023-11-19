use std::sync::Arc;

use futures::{SinkExt, StreamExt};
use tokio::sync::mpsc;
use tokio_tungstenite::tungstenite::{protocol::frame::coding::CloseCode, Message};
use url::Url;

use crate::transport::{
    cluster::{
        message::{ClusterMessage, ClusterMessagePayload},
        Node, NodeAddr,
    },
    node::RemoteNode,
};

use super::{ClusterConnection, ClusterConnectionBackend};

#[derive(Debug)]
pub struct TungsteniteClientConnection {
    pub peer: Arc<RemoteNode>,
    pub source: Arc<Node>,
}

impl ClusterConnectionBackend for TungsteniteClientConnection {
    fn spawn(
        self,
        cluster_message_inbound: tokio::sync::mpsc::Sender<(
            crate::transport::cluster::NodeAddr,
            crate::transport::cluster::message::ClusterMessage,
        )>,
    ) -> super::ClusterConnection {
        let peer_id = self.peer.id.clone();
        let source_id = self.source.id();
        let url = Url::parse(&format!(
            "ws://{}/cluster/{peer_id}/{source_id}",
            self.peer.socket_addr
        ))
        .unwrap();
        // let cluster_response_signal: Arc<Mutex<HashMap<Uuid, oneshot::Sender<ClusterMessagePayload>>>> = Default::default();
        let (cluster_message_outbound_tx, mut cluster_message_outbound_rx) =
            mpsc::channel::<ClusterMessage>(self.source.config.cluster_outbound_buffer_size);
        tokio::spawn(async move {
            let Ok((stream, _)) = tokio_tungstenite::connect_async(url).await else {
                return;
            };
            let (mut tx, mut rx) = stream.split();
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
                                if matches!(cf.code, CloseCode::Library(_)) {
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
            let _ = inbound_task.await;
            outbound_task.abort();
        });
        ClusterConnection {
            cluster_message_outbound_tx,
            cluster_response_signal: Default::default(),
        }
    }
}
