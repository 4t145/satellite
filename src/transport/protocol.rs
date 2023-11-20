use std::{pin::Pin, sync::Arc, fmt::Debug};

use futures::Future;

use super::{node::Node, ConnectionResult};

pub trait ProtocolHandler: Send + Sync + 'static + Debug {
    fn handle_message(
        &self,
        addr: String,
        host: Arc<Node>,
    ) -> Pin<Box<dyn Future<Output = ConnectionResult> + Send>>;
}
