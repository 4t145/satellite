use serde::{de::DeserializeOwned, Deserialize, Serialize};
use uuid::Uuid;

use crate::transport::{endpoint::EndPointAddr, EpMessage, ConnectionResult, EpData};

pub trait ClusterRequest {
    type Response;
}
#[derive(Debug, Serialize, Deserialize)]
pub enum ClusterMessagePayload {
    ForwardMessage {
        from: EndPointAddr,
        data: EpData,
    },
    ForwardResponse {
        // id: String,
        // to: EndPointAddr,
        response: ConnectionResult,
    },
    Ping,
    Pong {
        node_id: String,
    },
    FindEp {
        ep: EndPointAddr,
    },
    FindEpResponse {
        ep: Option<EndPointAddr>,
    },
}

impl ClusterMessagePayload {
    pub fn wrap(self) -> ClusterMessage {
        ClusterMessage::wrap(self)
    }

    pub fn response(self, uuid: Uuid) -> ClusterMessage {
        ClusterMessage::with_id(self, uuid)
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ClusterMessage {
    id: Uuid,
    payload: Vec<u8>,
}

impl ClusterMessage {
    pub fn id(&self) -> Uuid {
        self.id.clone()
    }
    pub fn wrap<T: Serialize>(data: T) -> Self {
        Self {
            id: Uuid::new_v4(),
            payload: bincode::serialize(&data).unwrap(),
        }
    }
    pub fn with_id<T: Serialize>(data: T, id: Uuid) -> Self {
        Self {
            id,
            payload: bincode::serialize(&data).unwrap(),
        }
    }
    /// Unwraps the message, returning the id and the payload
    /// # Panics
    /// Panics if the payload cannot be deserialized as T
    pub fn unwrap<T: DeserializeOwned>(self) -> (Uuid, T) {
        (self.id, bincode::deserialize(&self.payload).unwrap())
    }
}
