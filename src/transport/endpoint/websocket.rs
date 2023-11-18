use std::net::SocketAddr;

use tokio::sync::mpsc;

use crate::transport::{cluster::NodeAddr, endpoint::EpAddr, EpMessage};

use super::EpConnectionBackend;

pub struct WebsocketConnection {
    pub source: EpAddr,
    pub peer_addr: SocketAddr,
}

impl EpConnectionBackend for WebsocketConnection {
    fn spawn(
        self,
        ep_message_tx: mpsc::Sender<(EpAddr, EpMessage)>,
    ) -> super::EpConnection {
        todo!()
    }
}
