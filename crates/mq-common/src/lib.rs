//! Common types, configuration and errors shared by all MQ-rust crates.
//!
//! Keep this crate free of Tokio / network / storage dependencies so it can be
//! used by clients, servers and tooling alike.

use bytes::Bytes;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::time::{SystemTime, UNIX_EPOCH};

pub type MsgId = [u8; 16];

/// A message as it is handed to the store / broker.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Message {
    pub topic: String,
    pub queue_id: i32,
    pub body: Bytes,
    pub properties: BTreeMap<String, String>,
    pub tags: Option<String>,
    pub keys: Option<String>,
    pub flag: i32,
    pub born_timestamp: i64,
    pub born_host: String,
    pub store_timestamp: i64,
    pub store_host: String,
    pub reconsume_times: i32,
    pub prepared_transaction_offset: u64,
    pub sys_flag: i32,
}

impl Message {
    pub fn new(topic: impl Into<String>, body: impl Into<Bytes>) -> Self {
        let now = now_ms();
        Self {
            topic: topic.into(),
            queue_id: 0,
            body: body.into(),
            properties: BTreeMap::new(),
            tags: None,
            keys: None,
            flag: 0,
            born_timestamp: now,
            born_host: String::new(),
            store_timestamp: now,
            store_host: String::new(),
            reconsume_times: 0,
            prepared_transaction_offset: 0,
            sys_flag: 0,
        }
    }

    /// Approximate encoded size; the real CommitLog adds a fixed header.
    pub fn encoded_len(&self) -> usize {
        self.body.len()
            + self.topic.len()
            + self
                .properties
                .iter()
                .map(|(k, v)| k.len() + v.len() + 2)
                .sum::<usize>()
            + 128
    }

    pub fn with_queue(mut self, queue_id: i32) -> Self {
        self.queue_id = queue_id;
        self
    }

    pub fn with_property(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.properties.insert(key.into(), value.into());
        self
    }
}

/// Topic metadata. RocketMQ calls the read/write queue counts `readQueueNums`
/// and `writeQueueNums`; keep those names in the wire/config layer.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct TopicConfig {
    pub name: String,
    pub read_queue_nums: i32,
    pub write_queue_nums: i32,
    pub perm: u32,
    pub order: bool,
}

impl TopicConfig {
    pub fn new(name: impl Into<String>, queues: i32) -> Self {
        Self {
            name: name.into(),
            read_queue_nums: queues,
            write_queue_nums: queues,
            perm: 6, // read + write
            order: false,
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct BrokerConfig {
    pub name: String,
    pub cluster: String,
    pub broker_id: u64,
    pub listen_port: u16,
    pub namesrv: Vec<String>,
    pub role: BrokerRole,
    pub store_root: String,
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub enum BrokerRole {
    AsyncMaster,
    SyncMaster,
    Slave,
}

impl Default for BrokerConfig {
    fn default() -> Self {
        Self {
            name: "broker-a".into(),
            cluster: "DefaultCluster".into(),
            broker_id: 0,
            listen_port: 10911,
            namesrv: vec!["127.0.0.1:9876".into()],
            role: BrokerRole::AsyncMaster,
            store_root: "./data".into(),
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum MqError {
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
    #[error("protocol error: {0}")]
    Protocol(String),
    #[error("store error: {0}")]
    Store(String),
    #[error("not found: {0}")]
    NotFound(String),
    #[error("invalid argument: {0}")]
    Invalid(String),
    #[error("configuration error: {0}")]
    Config(String),
}

pub type Result<T> = std::result::Result<T, MqError>;

pub fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn message_encoded_len_includes_body_and_topic() {
        let msg = Message::new("orders", vec![0u8; 1024]);
        assert!(msg.encoded_len() >= 1024 + "orders".len());
    }

    #[test]
    fn topic_defaults_are_read_write() {
        let t = TopicConfig::new("orders", 8);
        assert_eq!(t.read_queue_nums, 8);
        assert_eq!(t.write_queue_nums, 8);
        assert_eq!(t.perm, 6);
    }
}
