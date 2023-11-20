use super::{ServiceDiscoveryBackend, ServiceDiscovery};

#[derive(Debug)]
pub struct StandaloneDiscoveryBackend;

impl ServiceDiscoveryBackend for StandaloneDiscoveryBackend {
    fn spawn(self) -> ServiceDiscovery {
        ServiceDiscovery {
            node_list: tokio::sync::watch::channel(Default::default()).1,
            reporter: tokio::sync::mpsc::channel(1).0,
        }
    }
}