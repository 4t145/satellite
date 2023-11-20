use crate::transport::endpoint::{EpConnectionBackend, EpAddr};

pub struct Subject {
    pub topic: String,
}
impl Subject {
    pub const PROTOCOL: &'static str = "subject";
}

// impl EpConnectionBackend for Subject {
//     fn spawn(self, ep_message_tx: tokio::sync::mpsc::Sender<(crate::transport::endpoint::EpAddr, crate::transport::endpoint::message::EpMsgE2H)>) -> crate::transport::endpoint::EpConnection {
//         let addr = EpAddr::new(Self::PROTOCOL, self.topic);

//     }
// }