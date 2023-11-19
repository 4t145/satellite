use std::sync::Arc;

use axum::extract::ws::{Message, WebSocket};
use futures::{SinkExt, StreamExt};
use tokio::sync::mpsc;

use crate::transport::{
    cluster::{
        message::{ClusterMessage, ClusterMessagePayload},
        NodeAddr,
    },
    node::RemoteNode,
};

use super::{ClusterConnection, ClusterConnectionBackend};

// for close code: https://developer.mozilla.org/en-US/docs/Web/API/CloseEvent/code
const CLOSE_CODE_PRIVATE: u16 = 4000;
// const CLOSE_CODE_INVALID_BINCODE: u16 = 4001;
// const CLOSE_CODE_EXPECT_HELLO: u16 = 4002;
#[derive(Debug)]
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
            let _ = inbound_task.await;
            outbound_task.abort();
        };
        tokio::spawn(task);
        ClusterConnection {
            cluster_message_outbound_tx,
            cluster_response_signal: Default::default(),
        }
    }
}
