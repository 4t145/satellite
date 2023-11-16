use std::{
    collections::HashMap,
    sync::{Arc, Weak},
};

use url::Url;

use super::{
    connection::ConnectionHubside,
    endpoint::{BoxedEndpoint, EndPoint, EndPointAddr},
    Message, SendResult, SendError,
};
// #[derive(Debug)]
// pub struct Router {
//     table:
// }

pub type NodeId = String;
#[derive(Debug, Clone)]
pub enum NodeAddr {
    This,
    Local(Weak<Node>),
    Remote(Arc<RemoteConnection>),
}

impl NodeAddr {
    pub fn is_this(&self) -> bool {
        match self {
            NodeAddr::This => true,
            _ => false,
        }
    }
}

#[derive(Debug)]
pub struct RemoteConnection {
    id: String,
    url: Url,
}

#[derive(Debug)]
pub struct Node {
    id: String,
    nodes: HashMap<String, NodeAddr>,
    router: HashMap<EndPointAddr, NodeAddr>,
    connections: HashMap<EndPointAddr, Box<dyn ConnectionHubside>>,
    weak_ref: Weak<Node>,
}

pub enum ClusterMessage {
    ForwardMessage {
        id: String,
        from: EndPointAddr,
        message: Message,
    },
    ForwardResponse {
        id: String,
        to: EndPointAddr,
        response: SendResult,
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

impl Node {
    pub fn as_local_node_addr(&self) -> NodeAddr {
        NodeAddr::Local(self.weak_ref.clone())
    }

    pub fn handle_cluster_message(&self, source: &NodeAddr, message: ClusterMessage) {
        match message {
            ClusterMessage::ForwardMessage { id, from, message } => {

            }
            ClusterMessage::ForwardResponse { id, to, response } => {
                self.response(&id, &to, response);
            }
            ClusterMessage::Ping => self.send_cluster_message(
                source,
                ClusterMessage::Pong {
                    node_id: self.id.clone(),
                },
            ),
            ClusterMessage::Pong { node_id } => {}
            ClusterMessage::FindEp { ep } => todo!(),
            ClusterMessage::FindEpResponse { ep } => todo!(),
        }
    }

    pub fn send_cluster_message(&self, node: &NodeAddr, message: ClusterMessage) {
        match node {
            NodeAddr::This => {
                // should not send to self
            }
            NodeAddr::Local(node) => {
                if let Some(node) = node.upgrade() {
                    node.handle_cluster_message(&self.as_local_node_addr(), message);
                }
            }
            NodeAddr::Remote(node) => {
                todo!("send to remote")
            }
        }
    }

    pub async fn find_ep(&self, ep: &EndPointAddr) -> Option<NodeAddr> {
        if let Some(ep) = self.router.get(ep) {
            Some(ep.clone())
        } else {
            let mut remotes = Vec::new();
            for (node_id, node_addr) in self.nodes.iter() {
                match node_addr {
                    NodeAddr::Local(node) => {
                        if let Some(node) = node.upgrade() {
                            if node.connections.get(ep).is_some() {
                                return Some(node_addr.clone());
                            }
                        }
                    }
                    NodeAddr::Remote(node) => {
                        remotes.push(node.clone());
                    }
                    _ => {}
                };
            }

            todo!("join all remotes find")

            // b
        }
    }

    pub async fn response(&self, id: &str, to: &EndPointAddr, response: SendResult) {
        if let Some(c) = self.connections.get(to) {
            c.response(id, response)
        } else if let Some(node) = self.find_ep(to).await {
            self.send_cluster_message(
                &node,
                ClusterMessage::ForwardResponse {
                    response,
                    id: id.to_string(),
                    to: to.clone(),
                },
            );
        } else {
            // do nothing
        }
    }

    pub async fn send(&self, id: &str, from: &EndPointAddr, to: &EndPointAddr, payload: &[u8]) -> SendResult {
        if let Some(c) = self.connections.get(to) {
            let result = c.send(id, to, payload);
            result
        } else if let Some(node) = self.find_ep(to).await {
            self.send_cluster_message(
                &node,
                ClusterMessage::ForwardMessage {
                    id: id.to_string(),
                    from: from.clone(),
                    message: Message::Data(super::DataMessage { id: id.to_string(), to: to.clone(), payload: payload.to_vec() })
                },
            );
            todo!("wait for response")
        } else {
            SendResult::Err(SendError::Unreachable)
        }
    }
}
