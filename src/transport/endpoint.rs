pub mod channel;
pub mod websocket;

use tokio::sync::mpsc;

use super::cluster::NodeAddr;
use super::{EpData, ConnectionError};
use super::{EpMessage, ConnectionResult};
use serde::{Deserialize, Serialize};
use std::future::Future;
use std::sync::Mutex;
use std::task::{Context, Poll};
use std::{fmt::Debug, pin::Pin};

pub trait EndPoint {}

pub struct BoxedEndpoint(pub Box<dyn EndPoint>);

impl EndPoint for BoxedEndpoint {}

#[derive(Debug, Hash, PartialEq, Eq, Clone, Serialize, Deserialize)]
pub struct EpAddr {
    pub protocol: String,
    pub address: String,
}

#[derive(Debug)]
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

    // pub fn poll_next_message(&self, cx: &mut Context<'_>) -> Poll<Option<EpMessage>> {
    //     if let Ok(mut lock) = self.ep_message_rx.lock() {
    //         lock.poll_recv(cx)
    //     } else {
    //         Poll::Pending
    //     }
    // }
}

pub trait EpConnectionBackend {
    fn spawn(self, ep_message_tx: mpsc::Sender<(EpAddr, EpMessage)>) -> EpConnection;
}
