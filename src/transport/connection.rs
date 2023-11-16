use std::fmt::Debug;

use super::{endpoint::{EndPoint, EndPointAddr}, Message, SendResult};




pub trait ConnectionHubside: Debug {
    fn source(&self) -> &EndPointAddr;
    fn send(&self, message_id: &str, from: &EndPointAddr, payload: &[u8]) -> SendResult;
    fn response(&self, message_id: &str, result: SendResult);
    fn next_message(&mut self) -> Message;
}


