//! RocketMQ route data (`TopicRouteData` / `QueueData` / `BrokerData`).
//!
//! Field names must stay in camelCase: the Java client deserializes this JSON
//! with fastjson directly into `org.apache.rocketmq.common.protocol.route.*`.

use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct QueueData {
    #[serde(rename = "brokerName")]
    pub broker_name: String,
    #[serde(rename = "readQueueNums")]
    pub read_queue_nums: i32,
    #[serde(rename = "writeQueueNums")]
    pub write_queue_nums: i32,
    pub perm: i32,
    #[serde(rename = "topicSysFlag")]
    pub topic_sys_flag: i32,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct BrokerData {
    pub cluster: String,
    #[serde(rename = "brokerName")]
    pub broker_name: String,
    /// JSON object keys are broker ids as strings (`"0"`).
    #[serde(rename = "brokerAddrs")]
    pub broker_addrs: BTreeMap<String, String>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct TopicRouteData {
    #[serde(rename = "orderTopicConf", default)]
    pub order_topic_conf: Option<String>,
    #[serde(rename = "queueDatas", default)]
    pub queue_datas: Vec<QueueData>,
    #[serde(rename = "brokerDatas", default)]
    pub broker_datas: Vec<BrokerData>,
    #[serde(rename = "filterServerTable", default)]
    pub filter_server_table: BTreeMap<String, Vec<String>>,
}

impl TopicRouteData {
    pub fn single_broker(
        cluster: impl Into<String>,
        broker_name: impl Into<String>,
        broker_addr: impl Into<String>,
        queues: i32,
    ) -> Self {
        let broker_name = broker_name.into();
        let mut addrs = BTreeMap::new();
        addrs.insert("0".to_string(), broker_addr.into());
        Self {
            order_topic_conf: None,
            queue_datas: vec![QueueData {
                broker_name: broker_name.clone(),
                read_queue_nums: queues,
                write_queue_nums: queues,
                perm: 6,
                topic_sys_flag: 0,
            }],
            broker_datas: vec![BrokerData {
                cluster: cluster.into(),
                broker_name,
                broker_addrs: addrs,
            }],
            filter_server_table: BTreeMap::new(),
        }
    }

    pub fn to_json(&self) -> mq_common::Result<Vec<u8>> {
        serde_json::to_vec(self).map_err(|e| mq_common::MqError::Protocol(e.to_string()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn route_json_uses_camel_case() {
        let route =
            TopicRouteData::single_broker("DefaultCluster", "broker-a", "127.0.0.1:10921", 8);
        let json = String::from_utf8(route.to_json().unwrap()).unwrap();
        assert!(json.contains("\"queueDatas\""));
        assert!(json.contains("\"brokerDatas\""));
        assert!(json.contains("\"brokerAddrs\""));
        assert!(json.contains("\"readQueueNums\":8"));
        assert!(json.contains("\"0\":\"127.0.0.1:10921\""));
    }
}
