use std::net::SocketAddr;

use tokio::sync::mpsc;

use crate::transport::{cluster::NodeAddr, endpoint::EndPointAddr, EpMessage};

use super::EpConnectionBackend;

pub struct WebsocketConnection {
    pub source: EndPointAddr,
    pub peer_addr: SocketAddr,
}

impl EpConnectionBackend for WebsocketConnection {
    fn spawn(
        self,
        ep_message_tx: mpsc::Sender<(EndPointAddr, EpMessage)>,
    ) -> super::EpConnection {
        todo!()
    }
}
