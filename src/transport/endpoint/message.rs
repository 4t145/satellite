use serde::{Deserialize, Serialize};

use crate::transport::ConnectionResult;

use super::EpAddr;

#[derive(Debug, Serialize, Deserialize)]

pub struct EpData {
    pub id: String,
    pub peer: EpAddr,
    pub payload: Vec<u8>,
}
#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum EpMsgE2H {
    Heartbeat,
    Data(EpData),
    Close,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum EpMsgH2E {
    Response {
        id: String,
        result: ConnectionResult,
    },
    Data(EpData),
}
