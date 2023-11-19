pub mod connection;
pub mod discovery;
pub mod message;

use tracing::instrument;
use uuid::Uuid;

use self::{
    connection::ClusterConnectionBackend,
    message::{ClusterMessage, ClusterMessagePayload},
};

use super::{
    node::{Node, NodeAddr},
    ConnectionError, ConnectionResult,
};

impl Node {
    #[instrument]
    pub async fn connect_node<C: ClusterConnectionBackend>(&self, addr: NodeAddr, backend: C) {
        tracing::info!("connecting node");
        self.cluster_connections
            .write()
            .await
            .insert(addr, backend.spawn(self.cluster_message_tx.clone()));
        tracing::info!("connected node");
    }

    pub fn as_local_node_addr(&self) -> NodeAddr {
        NodeAddr::Local(
            self.self_weak_ref
                .upgrade()
                .expect("self reference not pointed to self"),
        )
    }

    pub(super) async fn handle_cluster_message(&self, source: &NodeAddr, message: ClusterMessage) {
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

    pub(super) async fn response_cluster_message(
        &self,
        uuid: Uuid,
        to: NodeAddr,
        payload: ClusterMessagePayload,
    ) {
        let _result = if let Some(conn) = self.cluster_connections.read().await.get(&to) {
            conn.send_message(payload.response(uuid)).await
        } else {
            Err(ConnectionError::Unreachable)
        };
        // handle result
    }

    pub(super) async fn send_cluster_message(
        &self,
        to: &NodeAddr,
        payload: ClusterMessagePayload,
    ) -> ConnectionResult<Uuid> {
        if let Some(conn) = self.cluster_connections.read().await.get(to) {
            let message = payload.wrap();
            let id = message.id();
            conn.send_message(message).await?;
            Ok(id)
        } else {
            Err(ConnectionError::Unreachable)
        }
    }

    // if timeout, return none
    pub(super) async fn send_cluster_message_and_wait_response(
        &self,
        to: &NodeAddr,
        payload: ClusterMessagePayload,
    ) -> ConnectionResult<ClusterMessagePayload> {
        if let Some(conn) = self.cluster_connections.read().await.get(to) {
            let message = payload.wrap();
            conn.send_message_and_wait_response(message, self.config.cluster_message_timeout)
                .await
        } else {
            Err(ConnectionError::Unreachable)
        }
    }
}
