use serde::{Deserialize, Serialize};

pub mod cluster;
pub mod endpoint;
pub mod node;
#[derive(Debug, Serialize, Deserialize)]
pub enum ConnectionError {
    Unreachable,
    Overload,
    ProtocolError,
}

pub type ConnectionResult<T = ()> = Result<T, ConnectionError>;
