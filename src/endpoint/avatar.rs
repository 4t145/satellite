use std::collections::{HashMap, HashSet};

use tokio::sync::RwLock;

use crate::transport::endpoint::{EpAddr, EpConnection, EpConnectionBackend};

pub type AvatarId = String;
pub struct AvatarManager {
    pub backend_table: RwLock<HashMap<AvatarId, EpAddr>>,
}
impl AvatarManager {
    pub async fn add_record(&self, id: AvatarId, backend: EpAddr) {
        self.backend_table.write().await.insert(id, backend);
    }
    pub async fn delete_record(&self, id: &str) {
        self.backend_table.write().await.remove(id);
    }
    pub async fn get_record(&self, id: &str) -> Option<EpAddr> {
        self.backend_table.read().await.get(id).cloned()
    }
    pub async fn dedup<'iter, I: Iterator<Item = &'iter AvatarId>>(
        &self,
        ids: I,
    ) -> HashSet<AvatarId> {
        let rg = self.backend_table.read().await;
        let mut set = HashSet::new();
        for id in ids {
            if let Some(value) = rg.get(id) {
                if value.protocol.as_ref() == Avatar::PROTOCOL {
                    // TODO
                    // temeporary don't support avatar behind a avatar
                    continue;
                } else {
                    set.insert(id.to_owned());
                }
            };
        }
        set
    }
}

#[derive(Debug)]
/// avatar should be light
pub struct Avatar {
    pub owner: EpConnection,
    pub id: AvatarId,
}

impl Avatar {
    pub const PROTOCOL: &'static str = "avatar";
}

impl EpConnectionBackend for Avatar {
    fn spawn(
        self,
        _ep_message_tx: tokio::sync::mpsc::Sender<(
            crate::transport::endpoint::EpAddr,
            crate::transport::endpoint::message::EpMsgE2H,
        )>,
    ) -> EpConnection {
        let new_addr = EpAddr::new(Self::PROTOCOL, self.id);
        EpConnection {
            remote_addr: new_addr,
            ..self.owner
        }
    }
}
