use serde::Serialize;
use serde_json::Value;

use self::endpoint::EndPointAddr;

pub mod endpoint;
pub mod connection;
pub mod node;

pub struct DataMessage {
    pub id: String,
    pub to: EndPointAddr,
    pub payload: Vec<u8>,
}

pub enum Message {
    Heartbeat,
    Data(DataMessage),
}

#[derive(Debug, Serialize)]
pub enum SendError {
    Unreachable
}


pub type SendResult = Result<(), SendError>;