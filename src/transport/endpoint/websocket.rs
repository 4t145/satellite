use std::net::SocketAddr;

use axum::extract::ws::{Message, WebSocket};
use futures::{SinkExt, StreamExt};
use tokio::sync::mpsc;

use crate::transport::{
    endpoint::{message::EpMsgH2E, EpAddr},
    ConnectionResult,
};

use super::{
    message::{EpData, EpMsgE2H},
    EpConnection, EpConnectionBackend,
};
#[derive(Debug)]
pub struct WebsocketConnection {
    pub source: EpAddr,
    pub peer_addr: SocketAddr,
    pub websocket: WebSocket,
    pub buffer_size: usize,
}

impl EpConnectionBackend for WebsocketConnection {
    fn spawn(self, ep_message_tx: mpsc::Sender<(EpAddr, EpMsgE2H)>) -> EpConnection {
        let (mut tx, mut rx) = self.websocket.split();
        // let cluster_response_signal: Arc<Mutex<HashMap<Uuid, oneshot::Sender<ClusterMessagePayload>>>> = Default::default();
        let (response_tx, mut response_rx) =
            mpsc::channel::<(String, ConnectionResult)>(self.buffer_size);
        let (data_tx, mut data_rx) = mpsc::channel::<EpData>(self.buffer_size);

        // wait hello
        let source = self.source.clone();
        let task = async move {
            let inbound_task = tokio::spawn(async move {
                while let Some(recv) = rx.next().await {
                    match recv {
                        Ok(Message::Text(json)) => {
                            if let Ok(message) = serde_json::from_str::<EpMsgE2H>(&json) {
                                let _ = ep_message_tx.send((source.clone(), message)).await;
                            }
                        }
                        Ok(Message::Binary(data)) => {
                            if let Ok(message) = bincode::deserialize(&data) {
                                let _ = ep_message_tx.send((source.clone(), message)).await;
                            }
                        }
                        Ok(Message::Close(_cf)) => {
                            let _ = ep_message_tx.send((source.clone(), EpMsgE2H::Close)).await;

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
                loop {
                    let message = tokio::select! {
                        Some((message_id, result)) = response_rx.recv() => {
                            Message::Text(serde_json::to_string(&EpMsgH2E::Response{id: message_id, result}).expect("Serialize EpMsgH2E shouldn't fail"))
                        }
                        Some(data) = data_rx.recv() => {
                            Message::Text(serde_json::to_string(&EpMsgH2E::Data(data)).expect("Serialize EpMsgH2E shouldn't fail"))
                        }
                    };
                    let _result = tx.send(message).await;
                }
            });
            let _ = inbound_task.await;
            outbound_task.abort();
        };
        tokio::spawn(task);
        EpConnection {
            remote_addr: self.source,
            data_tx,
            response_tx,
        }
    }
}
