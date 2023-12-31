pub mod axum;
pub mod local;
pub mod tungstenite;

use std::{collections::HashMap, fmt::Debug, sync::Arc, time::Duration};

use tokio::sync::{mpsc, oneshot, Mutex};
use uuid::Uuid;

use crate::transport::{ConnectionError, ConnectionResult};

use super::{
    message::{ClusterMessage, ClusterMessagePayload},
    NodeAddr,
};
pub trait ClusterConnectionBackend: Debug {
    fn spawn(
        self,
        cluster_message_inbound: mpsc::Sender<(NodeAddr, ClusterMessage)>,
    ) -> ClusterConnection;
}
#[derive(Debug)]
pub struct ClusterConnection {
    // pub cluster_message_tx: mpsc::Sender<ClusterMessage>,
    cluster_message_outbound_tx: mpsc::Sender<ClusterMessage>,
    cluster_response_signal: Arc<Mutex<HashMap<Uuid, oneshot::Sender<ClusterMessagePayload>>>>,
}

impl ClusterConnection {
    pub fn is_alive(&self) -> bool {
        todo!()
    }
    pub async fn send_message(&self, message: ClusterMessage) -> ConnectionResult {
        self.cluster_message_outbound_tx
            .send(message)
            .await
            .map_err(|_| ConnectionError::Unreachable)
    }
    pub fn send_message_anyway(&self, message: ClusterMessage) {
        let tx = self.cluster_message_outbound_tx.clone();
        tokio::spawn(async move { tx.send(message).await });
    }
    pub async fn send_message_and_wait_response(
        &self,
        message: ClusterMessage,
        timeout: Duration,
    ) -> ConnectionResult<ClusterMessagePayload> {
        let (tx, rx) = oneshot::channel::<ClusterMessagePayload>();
        let id = message.id();
        self.cluster_response_signal.lock().await.insert(id, tx);
        let cluster_response_signal = self.cluster_response_signal.clone();
        tokio::spawn(async move {
            tokio::time::sleep(timeout).await;
            cluster_response_signal.lock().await.remove(&id);
        });
        self.send_message(message).await?;
        rx.await.map_err(|_| ConnectionError::Overload)
    }
}

pub struct ClusterClient {
    pub remotes: HashMap<NodeAddr, mpsc::Sender<ClusterMessage>>,
}
