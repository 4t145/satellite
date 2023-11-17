use serde::{Deserialize, Serialize};
use serde_json::Value;

use self::endpoint::EndPointAddr;

pub mod cluster;
pub mod endpoint;
#[derive(Debug, Serialize, Deserialize)]

pub struct EpData {
    pub id: String,
    pub from: EndPointAddr,
    pub to: EndPointAddr,
    pub payload: Vec<u8>,
}
#[derive(Debug, Serialize, Deserialize)]
pub enum EpMessage {
    Heartbeat,
    Data(EpData),
}

#[derive(Debug, Serialize, Deserialize)]
pub enum ConnectionError {
    Unreachable,
    Overload,
    ProtocolError,
}

pub type ConnectionResult<T = ()> = Result<T, ConnectionError>;
