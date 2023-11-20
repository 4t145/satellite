pub mod channel;
pub mod message;
pub mod websocket;
use tokio::sync::mpsc;
use tracing::instrument;

use self::message::{EpData, EpMsgE2H};

use super::cluster::message::ClusterMessagePayload;
use super::node::{Node, NodeAddr};
use super::ConnectionError;
use super::ConnectionResult;
use serde::{Deserialize, Serialize};
use std::borrow::Cow;

use std::fmt::Debug;
use std::sync::Arc;
#[derive(Debug, Hash, PartialEq, Eq, Clone)]
pub struct EpAddr {
    pub protocol: Cow<'static, str>,
    pub address: Arc<str>,
}

impl Serialize for EpAddr {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(&format!("{}://{}", self.protocol, self.address))
    }
}

impl<'a> Deserialize<'a> for EpAddr {
    fn deserialize<D: serde::Deserializer<'a>>(deserializer: D) -> Result<Self, D::Error> {
        let s = String::deserialize(deserializer)?;
        let Some((p, str)) = s.split_once("://")  else {

        };
        Ok(Self {
            protocol: Cow::Owned(protocol.to_owned()),
            address: Arc::from(address),
        })
    }
}

impl EpAddr {
    pub const WEBSOCKET: &'static str = "ws";
    pub fn new(protocol: &'static str, address: impl Into<String>) -> Self {
        Self {
            protocol: Cow::Borrowed(protocol),
            address: address.into(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct EpConnection {
    pub remote_addr: EpAddr,
    pub data_tx: mpsc::Sender<EpData>,
    pub response_tx: mpsc::Sender<(String, ConnectionResult)>,
    // pub ep_message_rx: Mutex<mpsc::Receiver<EpMessage>>,
}

impl EpConnection {
    pub fn remote_addr(&self) -> &EpAddr {
        &self.remote_addr
    }

    pub fn send(&self, message_id: String, from: EpAddr, payload: Vec<u8>) -> ConnectionResult {
        let message = EpData {
            id: message_id,
            peer: from,
            payload: payload.to_vec(),
        };
        match self.data_tx.try_send(message) {
            Ok(_) => Ok(()),
            Err(e) => match e {
                mpsc::error::TrySendError::Full(_) => Err(ConnectionError::Overload),
                mpsc::error::TrySendError::Closed(_) => Err(ConnectionError::Unreachable),
            },
        }
    }

    pub fn response(&self, message_id: String, result: ConnectionResult) {
        let _ = self.response_tx.try_send((message_id, result));
    }
}

pub trait EpConnectionBackend: Debug {
    fn spawn(self, ep_message_tx: mpsc::Sender<(EpAddr, EpMsgE2H)>) -> EpConnection;
}

impl Node {
    #[instrument]
    pub async fn connect_ep<B: EpConnectionBackend>(&self, addr: EpAddr, backend: B) {
        tracing::trace!("connecting endpoint");
        let ep_message_tx = self.ep_message_tx.clone();
        self.ep_connections
            .write()
            .await
            .insert(addr.clone(), backend.spawn(ep_message_tx));
        let message = ClusterMessagePayload::EpLogin { ep: addr.clone() }.wrap();
        for (_, conn) in self.cluster_connections.read().await.iter() {
            conn.send_message_anyway(message.clone());
        }
        tracing::trace!("connected endpoint");
    }

    #[instrument]
    pub async fn disconnect_ep(&self, addr: &EpAddr) {
        tracing::trace!("disconnect endpoint");
        self.ep_connections.write().await.remove(addr);
        self.router.write().await.remove(addr);
        let message = ClusterMessagePayload::EpLogout { ep: addr.clone() }.wrap();
        for (_, conn) in self.cluster_connections.read().await.iter() {
            conn.send_message_anyway(message.clone());
        }
    }

    pub(self) async fn find_ep(&self, ep: &EpAddr) -> Option<NodeAddr> {
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

    pub async fn handle_ep_message(&self, from: EpAddr, message: EpMsgE2H) {
        match message {
            EpMsgE2H::Data(data) => {
                let _ = self.handle_ep_data(from, data).await;
            }
            EpMsgE2H::Heartbeat => {
                let _ = self.handle_ep_hb(from).await;
            }
            EpMsgE2H::Close => {
                let _ = self.disconnect_ep(&from).await;
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
        let to = data.peer.clone();
        // protocol handler
        let protoco = to.protocol;
        if let Some(protocol) = self.protocol_handlers.get(&protoco.as_ref()) {
            protocol.handle_message(to.address, self.arc()).await?;
        } else {
            // no protocol handler
            return ConnectionResult::Err(ConnectionError::ProtocolNotSupported);
        }
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
