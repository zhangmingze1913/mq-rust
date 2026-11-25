//! Typed views over RocketMQ Remoting `extFields`.
//!
//! RocketMQ sends request headers as a flat map of strings inside the JSON
//! header. V1 uses human-readable names; V2 (`SEND_MESSAGE_V2`, code 310) uses
//! single-letter aliases to save bytes. Both are supported here.

use mq_common::{MqError, Result};
use std::collections::BTreeMap;

type Ext<'a> = Option<&'a BTreeMap<String, String>>;

fn get<'a>(ext: Ext<'a>, key: &str) -> Result<&'a str> {
    ext.and_then(|m| m.get(key))
        .map(String::as_str)
        .ok_or_else(|| MqError::Protocol(format!("missing ext field {key}")))
}

fn opt<'a>(ext: Ext<'a>, key: &str) -> Option<&'a str> {
    ext.and_then(|m| m.get(key)).map(String::as_str)
}

fn get_i32(ext: Ext<'_>, key: &str) -> Result<i32> {
    get(ext, key)?
        .parse()
        .map_err(|e| MqError::Protocol(format!("bad i32 {key}: {e}")))
}

fn get_i64(ext: Ext<'_>, key: &str) -> Result<i64> {
    get(ext, key)?
        .parse()
        .map_err(|e| MqError::Protocol(format!("bad i64 {key}: {e}")))
}

fn opt_i32(ext: Ext<'_>, key: &str) -> Result<Option<i32>> {
    match opt(ext, key) {
        Some(v) if !v.is_empty() => v
            .parse()
            .map(Some)
            .map_err(|e| MqError::Protocol(format!("bad i32 {key}: {e}"))),
        _ => Ok(None),
    }
}

fn get_bool(ext: Ext<'_>, key: &str) -> bool {
    matches!(opt(ext, key), Some("true") | Some("1"))
}

#[derive(Clone, Debug)]
pub struct SendMessageRequest {
    pub producer_group: String,
    pub topic: String,
    pub default_topic: String,
    pub default_topic_queue_nums: i32,
    pub queue_id: i32,
    pub sys_flag: i32,
    pub born_timestamp: i64,
    pub flag: i32,
    pub properties: String,
    pub reconsume_times: i32,
    pub unit_mode: bool,
    pub batch: bool,
    pub max_reconsume_times: Option<i32>,
    pub broker_name: Option<String>,
}

impl SendMessageRequest {
    /// `v2 = true` for code 310; `false` for code 10.
    pub fn from_ext(ext: Ext<'_>, v2: bool) -> Result<Self> {
        if v2 {
            Ok(Self {
                producer_group: get(ext, "a")?.to_string(),
                topic: get(ext, "b")?.to_string(),
                default_topic: opt(ext, "c").unwrap_or_default().to_string(),
                default_topic_queue_nums: opt_i32(ext, "d")?.unwrap_or(0),
                queue_id: get_i32(ext, "e")?,
                sys_flag: get_i32(ext, "f")?,
                born_timestamp: get_i64(ext, "g")?,
                flag: get_i32(ext, "h")?,
                properties: opt(ext, "i").unwrap_or_default().to_string(),
                reconsume_times: opt_i32(ext, "j")?.unwrap_or(0),
                unit_mode: get_bool(ext, "k"),
                batch: get_bool(ext, "m"),
                max_reconsume_times: opt_i32(ext, "l")?,
                broker_name: opt(ext, "n").map(str::to_string),
            })
        } else {
            Ok(Self {
                producer_group: get(ext, "producerGroup")?.to_string(),
                topic: get(ext, "topic")?.to_string(),
                default_topic: opt(ext, "defaultTopic").unwrap_or_default().to_string(),
                default_topic_queue_nums: opt_i32(ext, "defaultTopicQueueNums")?.unwrap_or(0),
                queue_id: get_i32(ext, "queueId")?,
                sys_flag: get_i32(ext, "sysFlag")?,
                born_timestamp: get_i64(ext, "bornTimestamp")?,
                flag: get_i32(ext, "flag")?,
                properties: opt(ext, "properties").unwrap_or_default().to_string(),
                reconsume_times: opt_i32(ext, "reconsumeTimes")?.unwrap_or(0),
                unit_mode: get_bool(ext, "unitMode"),
                batch: get_bool(ext, "batch"),
                max_reconsume_times: opt_i32(ext, "maxReconsumeTimes")?,
                broker_name: opt(ext, "bname").map(str::to_string),
            })
        }
    }
}

#[derive(Clone, Debug)]
pub struct SendMessageResponse {
    pub msg_id: String,
    pub queue_id: i32,
    pub queue_offset: i64,
    pub transaction_id: Option<String>,
}

impl SendMessageResponse {
    pub fn apply(&self, cmd: &mut super::RemotingCommand) {
        cmd.put_ext("msgId", self.msg_id.clone());
        cmd.put_ext("queueId", self.queue_id.to_string());
        cmd.put_ext("queueOffset", self.queue_offset.to_string());
        if let Some(tx) = &self.transaction_id {
            cmd.put_ext("transactionId", tx.clone());
        }
    }
}

#[derive(Clone, Debug)]
pub struct PullMessageRequest {
    pub consumer_group: String,
    pub topic: String,
    pub queue_id: i32,
    pub queue_offset: i64,
    pub max_msg_nums: i32,
    pub sys_flag: i32,
    pub commit_offset: i64,
    pub suspend_timeout_millis: i64,
    pub subscription: Option<String>,
    pub sub_version: i64,
    pub expression_type: Option<String>,
}

impl PullMessageRequest {
    pub fn from_ext(ext: Ext<'_>) -> Result<Self> {
        Ok(Self {
            consumer_group: get(ext, "consumerGroup")?.to_string(),
            topic: get(ext, "topic")?.to_string(),
            queue_id: get_i32(ext, "queueId")?,
            queue_offset: get_i64(ext, "queueOffset")?,
            max_msg_nums: get_i32(ext, "maxMsgNums")?,
            sys_flag: get_i32(ext, "sysFlag")?,
            commit_offset: get_i64(ext, "commitOffset").unwrap_or(0),
            suspend_timeout_millis: get_i64(ext, "suspendTimeoutMillis").unwrap_or(0),
            subscription: opt(ext, "subscription").map(str::to_string),
            sub_version: get_i64(ext, "subVersion").unwrap_or(0),
            expression_type: opt(ext, "expressionType").map(str::to_string),
        })
    }
}

#[derive(Clone, Debug)]
pub struct PullMessageResponse {
    pub suggest_which_broker_id: i64,
    pub next_begin_offset: i64,
    pub min_offset: i64,
    pub max_offset: i64,
}

impl PullMessageResponse {
    pub fn apply(&self, cmd: &mut super::RemotingCommand) {
        cmd.put_ext(
            "suggestWhichBrokerId",
            self.suggest_which_broker_id.to_string(),
        );
        cmd.put_ext("nextBeginOffset", self.next_begin_offset.to_string());
        cmd.put_ext("minOffset", self.min_offset.to_string());
        cmd.put_ext("maxOffset", self.max_offset.to_string());
    }
}

#[derive(Clone, Debug)]
pub struct QueryConsumerOffsetRequest {
    pub consumer_group: String,
    pub topic: String,
    pub queue_id: i32,
}

impl QueryConsumerOffsetRequest {
    pub fn from_ext(ext: Ext<'_>) -> Result<Self> {
        Ok(Self {
            consumer_group: get(ext, "consumerGroup")?.to_string(),
            topic: get(ext, "topic")?.to_string(),
            queue_id: get_i32(ext, "queueId")?,
        })
    }
}

#[derive(Clone, Debug)]
pub struct GetMaxOffsetRequest {
    pub topic: String,
    pub queue_id: i32,
}

impl GetMaxOffsetRequest {
    pub fn from_ext(ext: Ext<'_>) -> Result<Self> {
        Ok(Self {
            topic: get(ext, "topic")?.to_string(),
            queue_id: get_i32(ext, "queueId")?,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn v2_map() -> BTreeMap<String, String> {
        [
            ("a", "producer-group"),
            ("b", "orders"),
            ("c", "TBW102"),
            ("d", "8"),
            ("e", "3"),
            ("f", "0"),
            ("g", "1700000000000"),
            ("h", "0"),
            ("i", "KEYS\u{1}order-1\u{2}"),
            ("j", "0"),
            ("k", "false"),
            ("m", "false"),
        ]
        .into_iter()
        .map(|(k, v)| (k.to_string(), v.to_string()))
        .collect()
    }

    #[test]
    fn parses_send_message_v2() {
        let m = v2_map();
        let req = SendMessageRequest::from_ext(Some(&m), true).unwrap();
        assert_eq!(req.topic, "orders");
        assert_eq!(req.queue_id, 3);
        assert_eq!(req.properties, "KEYS\u{1}order-1\u{2}");
    }

    #[test]
    fn parses_send_message_v1() {
        let m: BTreeMap<String, String> = [
            ("producerGroup", "g"),
            ("topic", "orders"),
            ("queueId", "1"),
            ("sysFlag", "0"),
            ("bornTimestamp", "1"),
            ("flag", "0"),
        ]
        .into_iter()
        .map(|(k, v)| (k.to_string(), v.to_string()))
        .collect();
        let req = SendMessageRequest::from_ext(Some(&m), false).unwrap();
        assert_eq!(req.topic, "orders");
        assert_eq!(req.queue_id, 1);
    }
}
