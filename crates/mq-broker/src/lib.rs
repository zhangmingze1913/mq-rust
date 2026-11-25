//! Broker scaffold. R1 wires this to the Remoting server and store.

use mq_common::{BrokerConfig, BrokerRole};

pub mod acl;
pub mod admin;
pub mod audit;
pub mod connections;
pub mod filter;
pub mod hmac;
pub mod inbox;
pub mod metrics;
pub mod server;
pub mod store;
pub mod tc;
pub mod tc_outbox;
pub mod trace;

#[derive(Debug, Clone)]
pub struct Broker {
    pub config: BrokerConfig,
}

impl Broker {
    pub fn new(config: BrokerConfig) -> Self {
        Self { config }
    }

    pub fn describe(&self) -> String {
        format!(
            "broker={} cluster={} id={} role={:?} listen={}",
            self.config.name,
            self.config.cluster,
            self.config.broker_id,
            self.config.role,
            self.config.listen_port
        )
    }

    pub fn is_master(&self) -> bool {
        !matches!(self.config.role, BrokerRole::Slave)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_broker_is_master() {
        let b = Broker::new(BrokerConfig::default());
        assert!(b.is_master());
        assert!(b.describe().contains("broker-a"));
    }
}
