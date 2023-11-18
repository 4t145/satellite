use std::{
    collections::{HashMap, HashSet},
    net::SocketAddr,
    time::Duration,
};

use crate::transport::cluster::RemoteNode;
use redis::AsyncCommands;
use tokio::sync::{mpsc, watch};
use url::Url;

use super::{ServiceDiscovery, ServiceDiscoveryBackend};

pub struct RedisDiscoveryBackend {
    redis_url: Url,
    path: String,
    poll_rate: Duration,
}

impl ServiceDiscoveryBackend for RedisDiscoveryBackend {
    fn spawn(self) -> super::ServiceDiscovery {
        let (node_list_sender, node_list) =
            watch::channel::<HashSet<RemoteNode>>(HashSet::default());
        let (reporter_tx, mut reporter_rx) = mpsc::channel::<RemoteNode>(16);
        let url = self.redis_url.clone();
        let path = self.path.clone();
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(self.poll_rate);
            'lp: loop {
                'poll: {
                    let Ok(client) = redis::Client::open(url.to_string()) else {
                        break 'poll;
                    };
                    let Ok(mut conn) = client.get_tokio_connection().await else {
                        break 'poll;
                    };
                    let Ok(mut iter) = redis::cmd("SCAN")
                        .arg(0)
                        .arg(format!("{}:*", &path))
                        .clone()
                        .iter_async::<'_, String>(&mut conn)
                        .await
                    else {
                        break 'poll;
                    };
                    let mut key_list = Vec::new();
                    let mut set = HashSet::new();
                    while let Some(key) = iter.next_item().await {
                        key_list.push(key);
                    }
                    drop(iter);
                    for key in key_list {
                        let Ok(val) = redis::cmd("GET")
                            .arg(key.as_str())
                            .query_async::<_, String>(&mut conn)
                            .await
                        else {
                            continue;
                        };
                        let Ok(socket_addr) = val.parse::<SocketAddr>() else {
                            continue;
                        };
                        let id = key
                            .trim_start_matches(&path)
                            .trim_start_matches(':')
                            .to_string();
                        set.insert(RemoteNode { id, socket_addr });
                    }
                    if node_list_sender.send(set).is_err() {
                        break 'lp;
                    }
                }
                interval.tick().await;
            }
        });

        tokio::spawn(async move {
            while let Some(node) = reporter_rx.recv().await {
                'report: {
                    let Ok(client) = redis::Client::open(self.redis_url.clone()) else {
                        break 'report;
                    };
                    let Ok(mut conn) = client.get_tokio_connection().await else {
                        break 'report;
                    };
                    let key = format!("{}:{}", self.path, node.id);
                    let val = node.socket_addr.to_string();
                    let _result = redis::cmd("SET")
                        .arg(key)
                        .arg(val)
                        .arg("EX")
                        .arg(30)
                        .query_async::<_, ()>(&mut conn).await;
                }
            }
        });
        ServiceDiscovery {
            node_list,
            reporter: reporter_tx,
        }
    }
}
