use serde::{Deserialize, Serialize};

pub mod cluster;
pub mod endpoint;
pub mod node;
pub mod protocol;
#[derive(Debug, Serialize, Deserialize)]
pub enum ConnectionError {
    Unreachable,
    Overload,
    ProtocolError,
    ProtocolNotSupported,
}

pub type ConnectionResult<T = ()> = Result<T, ConnectionError>;
