//! Nameserver routing table (R1 scaffold).
//!
//! The real implementation will speak the RocketMQ Remoting protocol and
//! support broker registration / heartbeat / route lookup. This module keeps
//! the route model testable without a network.

use mq_common::{MqError, Result};
use std::collections::HashMap;
use std::sync::RwLock;

pub mod server;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BrokerRoute {
    pub cluster: String,
    pub broker_name: String,
    pub broker_id: u64,
    pub addr: String,
}

#[derive(Default)]
pub struct RouteTable {
    brokers: RwLock<HashMap<String, BrokerRoute>>,
}

impl RouteTable {
    pub fn register(&self, route: BrokerRoute) -> Result<()> {
        if route.addr.is_empty() {
            return Err(MqError::Invalid("broker address is empty".into()));
        }
        let key = format!("{}:{}", route.broker_name, route.broker_id);
        self.brokers.write().unwrap().insert(key, route);
        Ok(())
    }

    pub fn unregister(&self, broker_name: &str, broker_id: u64) {
        let key = format!("{broker_name}:{broker_id}");
        self.brokers.write().unwrap().remove(&key);
    }

    pub fn list(&self) -> Vec<BrokerRoute> {
        self.brokers.read().unwrap().values().cloned().collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn register_and_remove() {
        let t = RouteTable::default();
        t.register(BrokerRoute {
            cluster: "DefaultCluster".into(),
            broker_name: "broker-a".into(),
            broker_id: 0,
            addr: "127.0.0.1:10911".into(),
        })
        .unwrap();
        assert_eq!(t.list().len(), 1);
        t.unregister("broker-a", 0);
        assert!(t.list().is_empty());
    }
}
