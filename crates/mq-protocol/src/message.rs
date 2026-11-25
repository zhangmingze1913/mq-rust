//! RocketMQ message wire encoding used by `PULL_MESSAGE` responses.
//!
//! Layout follows `MessageDecoder.encode()` from RocketMQ 4.9.x. All integers
//! are big-endian; the properties string uses `\u{1}` between key and value and
//! `\u{2}` between pairs.

use bytes::Bytes;
use serde::{Deserialize, Serialize};
use std::sync::atomic::{AtomicU64, Ordering};

pub const MESSAGE_MAGIC_CODE: i32 = -626843481;
pub const NAME_VALUE_SEPARATOR: char = '\u{1}';
pub const PROPERTY_SEPARATOR: char = '\u{2}';

static MSG_ID_COUNTER: AtomicU64 = AtomicU64::new(1);

pub fn new_msg_id() -> String {
    let now = mq_common::now_ms() as u64;
    let seq = MSG_ID_COUNTER.fetch_add(1, Ordering::Relaxed);
    format!("{now:016X}{seq:016X}")
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct StoredMessage {
    pub msg_id: String,
    pub topic: String,
    pub queue_id: i32,
    pub queue_offset: i64,
    pub physical_offset: i64,
    pub sys_flag: i32,
    pub flag: i32,
    pub born_timestamp: i64,
    pub born_host: [u8; 4],
    pub born_port: u16,
    pub store_timestamp: i64,
    pub store_host: [u8; 4],
    pub store_port: u16,
    pub reconsume_times: i32,
    pub prepared_transaction_offset: i64,
    pub body: Bytes,
    pub properties: String,
}

impl StoredMessage {
    /// Decode one RocketMQ message from `bytes`, returning the message and the
    /// number of bytes consumed. Used by `SEND_BATCH_MESSAGE` bodies.
    pub fn decode(bytes: &[u8]) -> mq_common::Result<(Self, usize)> {
        use mq_common::MqError;
        if bytes.len() < 4 {
            return Err(MqError::Protocol("message too short".into()));
        }
        let total_size = i32::from_be_bytes(bytes[0..4].try_into().unwrap());
        if total_size < 60 || total_size as usize > bytes.len() {
            return Err(MqError::Protocol(format!(
                "bad message total_size: {total_size}"
            )));
        }
        let mut cur = 0usize;
        let mut take = |n: usize| -> mq_common::Result<&[u8]> {
            if cur + n > total_size as usize {
                return Err(MqError::Protocol("truncated message".into()));
            }
            let s = &bytes[cur..cur + n];
            cur += n;
            Ok(s)
        };
        let _total = i32::from_be_bytes(take(4)?.try_into().unwrap());
        let _magic = i32::from_be_bytes(take(4)?.try_into().unwrap());
        let _crc = i32::from_be_bytes(take(4)?.try_into().unwrap());
        let queue_id = i32::from_be_bytes(take(4)?.try_into().unwrap());
        let flag = i32::from_be_bytes(take(4)?.try_into().unwrap());
        let queue_offset = i64::from_be_bytes(take(8)?.try_into().unwrap());
        let physical_offset = i64::from_be_bytes(take(8)?.try_into().unwrap());
        let sys_flag = i32::from_be_bytes(take(4)?.try_into().unwrap());
        let born_timestamp = i64::from_be_bytes(take(8)?.try_into().unwrap());
        let born_host: [u8; 4] = take(4)?.try_into().unwrap();
        let born_port = i32::from_be_bytes(take(4)?.try_into().unwrap()) as u16;
        let store_timestamp = i64::from_be_bytes(take(8)?.try_into().unwrap());
        let store_host: [u8; 4] = take(4)?.try_into().unwrap();
        let store_port = i32::from_be_bytes(take(4)?.try_into().unwrap()) as u16;
        let reconsume_times = i32::from_be_bytes(take(4)?.try_into().unwrap());
        let prepared_transaction_offset = i64::from_be_bytes(take(8)?.try_into().unwrap());
        let body_len = i32::from_be_bytes(take(4)?.try_into().unwrap());
        if body_len < 0 {
            return Err(MqError::Protocol("negative body length".into()));
        }
        let body = Bytes::copy_from_slice(take(body_len as usize)?);
        let topic_len = take(1)?[0] as usize;
        let topic = String::from_utf8_lossy(take(topic_len)?).into_owned();
        let props_len = i16::from_be_bytes(take(2)?.try_into().unwrap());
        if props_len < 0 {
            return Err(MqError::Protocol("negative properties length".into()));
        }
        let properties = String::from_utf8_lossy(take(props_len as usize)?).into_owned();
        let msg_id = property_value(&properties, "UNIQ_KEY").unwrap_or_else(new_msg_id);
        Ok((
            Self {
                msg_id,
                topic,
                queue_id,
                queue_offset,
                physical_offset,
                sys_flag,
                flag,
                born_timestamp,
                born_host,
                born_port,
                store_timestamp,
                store_host,
                store_port,
                reconsume_times,
                prepared_transaction_offset,
                body,
                properties,
            },
            total_size as usize,
        ))
    }

    pub fn encode(&self) -> Vec<u8> {
        let properties = ensure_uniq_key(&self.properties, &self.msg_id);
        let topic = self.topic.as_bytes();
        let props = properties.as_bytes();
        let body_crc = crc32fast::hash(&self.body);
        let total_size = 4  // total size
            + 4  // magic
            + 4  // body crc
            + 4  // queue id
            + 4  // flag
            + 8  // queue offset
            + 8  // physical offset
            + 4  // sys flag
            + 8  // born timestamp
            + 8  // born host + port
            + 8  // store timestamp
            + 8  // store host + port
            + 4  // reconsume times
            + 8  // prepared transaction offset
            + 4  // body length
            + self.body.len()
            + 1  // topic length
            + topic.len()
            + 2  // properties length
            + props.len();

        let mut out = Vec::with_capacity(total_size);
        out.extend_from_slice(&(total_size as i32).to_be_bytes());
        out.extend_from_slice(&MESSAGE_MAGIC_CODE.to_be_bytes());
        out.extend_from_slice(&body_crc.to_be_bytes());
        out.extend_from_slice(&self.queue_id.to_be_bytes());
        out.extend_from_slice(&self.flag.to_be_bytes());
        out.extend_from_slice(&self.queue_offset.to_be_bytes());
        out.extend_from_slice(&self.physical_offset.to_be_bytes());
        out.extend_from_slice(&self.sys_flag.to_be_bytes());
        out.extend_from_slice(&self.born_timestamp.to_be_bytes());
        out.extend_from_slice(&self.born_host);
        out.extend_from_slice(&(self.born_port as i32).to_be_bytes());
        out.extend_from_slice(&self.store_timestamp.to_be_bytes());
        out.extend_from_slice(&self.store_host);
        out.extend_from_slice(&(self.store_port as i32).to_be_bytes());
        out.extend_from_slice(&self.reconsume_times.to_be_bytes());
        out.extend_from_slice(&self.prepared_transaction_offset.to_be_bytes());
        out.extend_from_slice(&(self.body.len() as i32).to_be_bytes());
        out.extend_from_slice(&self.body);
        out.push(topic.len() as u8);
        out.extend_from_slice(topic);
        out.extend_from_slice(&(props.len() as i16).to_be_bytes());
        out.extend_from_slice(props);
        debug_assert_eq!(out.len(), total_size);
        out
    }
}

pub fn ensure_uniq_key(properties: &str, msg_id: &str) -> String {
    if properties.contains("UNIQ_KEY\u{1}") {
        return properties.to_string();
    }
    let mut out = properties.to_string();
    if !out.is_empty() && !out.ends_with(PROPERTY_SEPARATOR) {
        out.push(PROPERTY_SEPARATOR);
    }
    out.push_str("UNIQ_KEY");
    out.push(NAME_VALUE_SEPARATOR);
    out.push_str(msg_id);
    out.push(PROPERTY_SEPARATOR);
    out
}

pub fn property_value(properties: &str, key: &str) -> Option<String> {
    properties.split(PROPERTY_SEPARATOR).find_map(|kv| {
        let mut it = kv.splitn(2, NAME_VALUE_SEPARATOR);
        if it.next() == Some(key) {
            it.next().map(str::to_string)
        } else {
            None
        }
    })
}

#[derive(Clone, Debug)]
pub struct BatchMessage {
    pub flag: i32,
    pub body: Bytes,
    pub properties: String,
}

/// Decode one message from a `SEND_BATCH_MESSAGE` body.
///
/// Batch entries use a compact format (no topic/queue/hosts): totalSize, magic,
/// bodyCRC, flag, bodyLen, body, propertiesLen, properties.
pub fn decode_batch_message(bytes: &[u8]) -> mq_common::Result<(BatchMessage, usize)> {
    use mq_common::MqError;
    if bytes.len() < 22 {
        return Err(MqError::Protocol("batch message too short".into()));
    }
    let total_size = i32::from_be_bytes(bytes[0..4].try_into().unwrap());
    if total_size < 22 || total_size as usize > bytes.len() {
        return Err(MqError::Protocol(format!(
            "bad batch message total_size: {total_size}"
        )));
    }
    let mut cur = 4usize;
    let mut take = |n: usize| -> mq_common::Result<&[u8]> {
        if cur + n > total_size as usize {
            return Err(MqError::Protocol("truncated batch message".into()));
        }
        let s = &bytes[cur..cur + n];
        cur += n;
        Ok(s)
    };
    let _magic = i32::from_be_bytes(take(4)?.try_into().unwrap());
    let _crc = i32::from_be_bytes(take(4)?.try_into().unwrap());
    let flag = i32::from_be_bytes(take(4)?.try_into().unwrap());
    let body_len = i32::from_be_bytes(take(4)?.try_into().unwrap());
    if body_len < 0 {
        return Err(MqError::Protocol("negative batch body length".into()));
    }
    let body = Bytes::copy_from_slice(take(body_len as usize)?);
    let props_len = i16::from_be_bytes(take(2)?.try_into().unwrap());
    if props_len < 0 {
        return Err(MqError::Protocol("negative batch properties length".into()));
    }
    let properties = String::from_utf8_lossy(take(props_len as usize)?).into_owned();
    Ok((
        BatchMessage {
            flag,
            body,
            properties,
        },
        total_size as usize,
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample() -> StoredMessage {
        StoredMessage {
            msg_id: "00112233445566778899AABBCCDDEEFF".into(),
            topic: "orders".into(),
            queue_id: 2,
            queue_offset: 7,
            physical_offset: 1024,
            sys_flag: 0,
            flag: 0,
            born_timestamp: 1,
            born_host: [127, 0, 0, 1],
            born_port: 1234,
            store_timestamp: 2,
            store_host: [127, 0, 0, 1],
            store_port: 10921,
            reconsume_times: 0,
            prepared_transaction_offset: 0,
            body: Bytes::from_static(b"hello"),
            properties: "KEYS\u{1}order-1\u{2}".into(),
        }
    }

    #[test]
    fn encoded_size_matches_header() {
        let m = sample();
        let bytes = m.encode();
        let total = i32::from_be_bytes(bytes[0..4].try_into().unwrap());
        assert_eq!(total as usize, bytes.len());
    }

    #[test]
    fn uniq_key_is_added_when_missing() {
        let props = ensure_uniq_key("KEYS\u{1}order-1\u{2}", "ABC");
        assert!(props.contains("UNIQ_KEY\u{1}ABC\u{2}"));
    }

    #[test]
    fn uniq_key_is_not_duplicated() {
        let props = ensure_uniq_key("UNIQ_KEY\u{1}XYZ\u{2}", "ABC");
        assert!(!props.contains("ABC"));
    }

    #[test]
    fn decode_roundtrip_single_message() {
        let m = sample();
        let encoded = m.encode();
        let (decoded, used) = StoredMessage::decode(&encoded).unwrap();
        assert_eq!(used, encoded.len());
        assert_eq!(decoded.topic, "orders");
        assert_eq!(decoded.queue_id, 2);
        assert_eq!(decoded.queue_offset, 7);
        assert_eq!(&decoded.body[..], b"hello");
    }

    #[test]
    fn decode_two_message_batch() {
        let mut batch = sample().encode();
        batch.extend_from_slice(&sample().encode());
        let (a, n1) = StoredMessage::decode(&batch).unwrap();
        let (b, n2) = StoredMessage::decode(&batch[n1..]).unwrap();
        assert_eq!(a.topic, b.topic);
        assert_eq!(n1 + n2, batch.len());
    }

    #[test]
    fn decode_compact_batch_message() {
        let body = b"hello";
        let props = b"UNIQ_KEY\x01id-1\x02";
        let total = 22 + body.len() + props.len();
        let mut bytes = Vec::new();
        bytes.extend_from_slice(&(total as i32).to_be_bytes());
        bytes.extend_from_slice(&0i32.to_be_bytes());
        bytes.extend_from_slice(&0i32.to_be_bytes());
        bytes.extend_from_slice(&0i32.to_be_bytes());
        bytes.extend_from_slice(&(body.len() as i32).to_be_bytes());
        bytes.extend_from_slice(body);
        bytes.extend_from_slice(&(props.len() as i16).to_be_bytes());
        bytes.extend_from_slice(props);
        let (msg, used) = decode_batch_message(&bytes).unwrap();
        assert_eq!(used, bytes.len());
        assert_eq!(&msg.body[..], body);
        assert_eq!(msg.flag, 0);
        assert!(msg.properties.contains("UNIQ_KEY"));
    }
}
