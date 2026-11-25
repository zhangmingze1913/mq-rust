//! RocketMQ Remoting wire protocol.
//!
//! Frame layout (all integers big-endian):
//!
//! ```text
//! +----------------+----------------+----------------+----------------+
//! | total_len i32  | header_len i32 | header JSON    | body           |
//! +----------------+----------------+----------------+----------------+
//! total_len = 4 + header_len + body_len   (it does NOT include its own 4 bytes)
//! ```
//!
//! This matches RocketMQ 4.9.x `RemotingCommand.encodeHeader()`: the length
//! field covers the header-length field, the header and the body. Netty's
//! `LengthFieldBasedFrameDecoder` adds the 4-byte length field back when it
//! slices a frame.

use bytes::{Buf, BufMut, Bytes, BytesMut};
use mq_common::{MqError, Result};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

pub mod header;
pub mod message;
pub mod route;

pub const MIN_FRAME_LEN: usize = 4;
pub const MAX_FRAME_LEN: usize = 64 * 1024 * 1024;

pub const FLAG_RESPONSE: i32 = 1;
pub const FLAG_ONEWAY: i32 = 2;

/// A decoded Remoting frame: opaque header + opaque body.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RemotingFrame {
    pub header: Bytes,
    pub body: Bytes,
    /// 0 = JSON, 1 = RocketMQ binary (`FastCodesHeader`).
    pub serialize_type: u8,
}

pub fn encode_frame(header: &[u8], body: &[u8]) -> BytesMut {
    encode_frame_with_type(header, body, 0)
}

pub fn encode_frame_with_type(header: &[u8], body: &[u8], serialize_type: u8) -> BytesMut {
    let header_len = header.len() as i32;
    // The length field covers: header-length field (4) + header + body.
    let total_len = 4 + header_len + body.len() as i32;
    let mut out = BytesMut::with_capacity(4 + total_len as usize);
    out.put_i32(total_len);
    out.put_i32((((serialize_type as i32) & 0xFF) << 24) | (header_len & 0x00FF_FFFF));
    out.put_slice(header);
    out.put_slice(body);
    out
}

/// Try to decode one frame from `src`.
///
/// Returns `Ok(None)` when more bytes are needed. On success the consumed bytes
/// are removed from `src`, allowing a connection task to loop over a buffer.
pub fn decode_frame(src: &mut BytesMut) -> Result<Option<RemotingFrame>> {
    if src.len() < 4 {
        return Ok(None);
    }
    let total_len = i32::from_be_bytes([src[0], src[1], src[2], src[3]]);
    if total_len < MIN_FRAME_LEN as i32 || total_len as usize > MAX_FRAME_LEN {
        return Err(MqError::Protocol(format!(
            "invalid remoting frame length: {total_len}"
        )));
    }
    let total_len = total_len as usize;
    // `total_len` excludes its own 4-byte field.
    if src.len() < 4 + total_len {
        return Ok(None);
    }

    let mut frame = src.split_to(4 + total_len);
    frame.advance(4);
    let raw_header_len = frame.get_i32();
    let serialize_type = ((raw_header_len >> 24) & 0xFF) as u8;
    let header_len = raw_header_len & 0x00FF_FFFF;
    if serialize_type > 1 {
        return Err(MqError::Protocol(format!(
            "unsupported remoting serialize type: {serialize_type}"
        )));
    }
    if header_len < 0 || header_len as usize > total_len - 4 {
        return Err(MqError::Protocol(format!(
            "invalid remoting header length: {header_len}"
        )));
    }
    let header_len = header_len as usize;
    let header = frame.split_to(header_len).freeze();
    let body = frame.freeze();
    Ok(Some(RemotingFrame {
        header,
        body,
        serialize_type,
    }))
}

/// The JSON header of a Remoting command.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct RemotingCommand {
    pub code: i32,
    #[serde(default = "default_language")]
    pub language: String,
    #[serde(default)]
    pub version: i32,
    #[serde(default)]
    pub opaque: i32,
    #[serde(default)]
    pub flag: i32,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub remark: Option<String>,
    #[serde(rename = "extFields", default, skip_serializing_if = "Option::is_none")]
    pub ext_fields: Option<BTreeMap<String, String>>,
    #[serde(skip)]
    pub body: Bytes,
    #[serde(skip)]
    pub serialize_type: u8,
}

fn default_language() -> String {
    "RUST".to_string()
}

impl RemotingCommand {
    pub fn request(code: i32, opaque: i32) -> Self {
        Self {
            code,
            language: default_language(),
            version: 0,
            opaque,
            flag: 0,
            remark: None,
            ext_fields: Some(BTreeMap::new()),
            body: Bytes::new(),
            serialize_type: 0,
        }
    }

    pub fn response(code: i32, opaque: i32) -> Self {
        Self {
            code,
            language: default_language(),
            version: 0,
            opaque,
            flag: FLAG_RESPONSE,
            remark: None,
            ext_fields: Some(BTreeMap::new()),
            body: Bytes::new(),
            serialize_type: 0,
        }
    }

    pub fn with_body(mut self, body: impl Into<Bytes>) -> Self {
        self.body = body.into();
        self
    }

    pub fn with_remark(mut self, remark: impl Into<String>) -> Self {
        self.remark = Some(remark.into());
        self
    }

    pub fn put_ext(&mut self, key: impl Into<String>, value: impl Into<String>) {
        self.ext_fields
            .get_or_insert_with(BTreeMap::new)
            .insert(key.into(), value.into());
    }

    pub fn ext(&self, key: &str) -> Option<&str> {
        self.ext_fields
            .as_ref()
            .and_then(|m| m.get(key))
            .map(String::as_str)
    }

    pub fn is_response(&self) -> bool {
        self.flag & FLAG_RESPONSE != 0
    }

    pub fn is_oneway(&self) -> bool {
        self.flag & FLAG_ONEWAY != 0
    }

    pub fn encode(&self) -> Result<BytesMut> {
        let header = if self.serialize_type == 1 {
            encode_binary_header(self)?
        } else {
            serde_json::to_vec(self)
                .map_err(|e| MqError::Protocol(format!("encode remoting header: {e}")))?
        };
        Ok(encode_frame_with_type(
            &header,
            &self.body,
            self.serialize_type,
        ))
    }

    pub fn decode(frame: RemotingFrame) -> Result<Self> {
        let mut cmd = if frame.serialize_type == 1 {
            decode_binary_header(&frame.header)?
        } else {
            serde_json::from_slice(&frame.header)
                .map_err(|e| MqError::Protocol(format!("decode remoting header: {e}")))?
        };
        cmd.body = frame.body;
        cmd.serialize_type = frame.serialize_type;
        Ok(cmd)
    }
}

fn language_code(language: &str) -> u8 {
    match language {
        "CPP" => 1,
        "DOTNET" => 2,
        "PYTHON" => 3,
        "DELPHI" => 4,
        "ERLANG" => 5,
        "RUBY" => 6,
        "OTHER" => 7,
        "HTTP" => 8,
        "GO" => 9,
        "PHP" => 10,
        "OMS" => 11,
        "RUST" => 12,
        _ => 0,
    }
}

fn language_name(code: u8) -> String {
    match code {
        1 => "CPP",
        2 => "DOTNET",
        3 => "PYTHON",
        4 => "DELPHI",
        5 => "ERLANG",
        6 => "RUBY",
        7 => "OTHER",
        8 => "HTTP",
        9 => "GO",
        10 => "PHP",
        11 => "OMS",
        12 => "RUST",
        _ => "JAVA",
    }
    .to_string()
}

fn encode_binary_header(cmd: &RemotingCommand) -> Result<Vec<u8>> {
    let mut out = Vec::with_capacity(64);
    out.extend_from_slice(&(cmd.code as i16).to_be_bytes());
    out.push(language_code(&cmd.language));
    out.extend_from_slice(&(cmd.version as i16).to_be_bytes());
    out.extend_from_slice(&cmd.opaque.to_be_bytes());
    out.extend_from_slice(&cmd.flag.to_be_bytes());
    let remark = cmd.remark.as_deref().unwrap_or("");
    out.extend_from_slice(&(remark.len() as i32).to_be_bytes());
    out.extend_from_slice(remark.as_bytes());

    let mut ext = Vec::new();
    if let Some(fields) = &cmd.ext_fields {
        for (k, v) in fields {
            let kb = k.as_bytes();
            let vb = v.as_bytes();
            if kb.len() > u16::MAX as usize || vb.len() > i32::MAX as usize {
                return Err(MqError::Protocol("ext field too large".into()));
            }
            ext.extend_from_slice(&(kb.len() as u16).to_be_bytes());
            ext.extend_from_slice(kb);
            ext.extend_from_slice(&(vb.len() as i32).to_be_bytes());
            ext.extend_from_slice(vb);
        }
    }
    out.extend_from_slice(&(ext.len() as i32).to_be_bytes());
    out.extend_from_slice(&ext);
    Ok(out)
}

fn decode_binary_header(header: &[u8]) -> Result<RemotingCommand> {
    let mut cur = 0usize;
    let take = |cur: &mut usize, n: usize| -> Result<&[u8]> {
        if *cur + n > header.len() {
            return Err(MqError::Protocol("truncated binary remoting header".into()));
        }
        let s = &header[*cur..*cur + n];
        *cur += n;
        Ok(s)
    };
    let code = i16::from_be_bytes(take(&mut cur, 2)?.try_into().unwrap()) as i32;
    let language = language_name(take(&mut cur, 1)?[0]);
    let version = i16::from_be_bytes(take(&mut cur, 2)?.try_into().unwrap()) as i32;
    let opaque = i32::from_be_bytes(take(&mut cur, 4)?.try_into().unwrap());
    let flag = i32::from_be_bytes(take(&mut cur, 4)?.try_into().unwrap());
    let remark_len = i32::from_be_bytes(take(&mut cur, 4)?.try_into().unwrap());
    if remark_len < 0 {
        return Err(MqError::Protocol("negative remark length".into()));
    }
    let remark = if remark_len > 0 {
        Some(String::from_utf8_lossy(take(&mut cur, remark_len as usize)?).into_owned())
    } else {
        None
    };
    let ext_len = i32::from_be_bytes(take(&mut cur, 4)?.try_into().unwrap());
    if ext_len < 0 {
        return Err(MqError::Protocol("negative extFields length".into()));
    }
    let mut ext_fields = BTreeMap::new();
    let end = cur + ext_len as usize;
    if end > header.len() {
        return Err(MqError::Protocol("extFields exceeds header".into()));
    }
    while cur < end {
        let klen = u16::from_be_bytes(take(&mut cur, 2)?.try_into().unwrap()) as usize;
        let key = String::from_utf8_lossy(take(&mut cur, klen)?).into_owned();
        let vlen = i32::from_be_bytes(take(&mut cur, 4)?.try_into().unwrap());
        if vlen < 0 {
            return Err(MqError::Protocol("negative ext field value length".into()));
        }
        let value = String::from_utf8_lossy(take(&mut cur, vlen as usize)?).into_owned();
        ext_fields.insert(key, value);
    }
    Ok(RemotingCommand {
        code,
        language,
        version,
        opaque,
        flag,
        remark,
        ext_fields: Some(ext_fields),
        body: Bytes::new(),
        serialize_type: 1,
    })
}

/// Request codes copied from RocketMQ 4.9.x `RequestCode`.
pub mod request_code {
    pub const SEND_MESSAGE: i32 = 10;
    pub const SEND_MESSAGE_V2: i32 = 310;
    pub const SEND_BATCH_MESSAGE: i32 = 320;
    pub const PULL_MESSAGE: i32 = 11;
    pub const QUERY_CONSUMER_OFFSET: i32 = 14;
    pub const UPDATE_CONSUMER_OFFSET: i32 = 15;
    pub const GET_MAX_OFFSET: i32 = 30;
    pub const GET_MIN_OFFSET: i32 = 31;
    pub const GET_BROKER_RUNTIME_INFO: i32 = 28;
    pub const SEARCH_OFFSET_BY_TIMESTAMP: i32 = 32;
    pub const HEART_BEAT: i32 = 34;
    pub const UNREGISTER_CLIENT: i32 = 35;
    pub const CONSUMER_SEND_MSG_BACK: i32 = 36;
    pub const END_TRANSACTION: i32 = 37;
    pub const GET_CONSUMER_LIST_BY_GROUP: i32 = 38;
    pub const CHECK_TRANSACTION_STATE: i32 = 39;
    pub const LOCK_BATCH_MQ: i32 = 41;
    pub const UNLOCK_BATCH_MQ: i32 = 42;
    pub const CHECK_CLIENT_CONFIG: i32 = 46;
    pub const GET_ALL_TOPIC_LIST_FROM_NAMESERVER: i32 = 206;
    pub const GET_CONSUME_STATS: i32 = 208;
    pub const GET_ROUTEINFO_BY_TOPIC: i32 = 105;
    pub const GET_BROKER_CLUSTER_INFO: i32 = 106;
    pub const REGISTER_BROKER: i32 = 103;
    pub const UNREGISTER_BROKER: i32 = 104;
}

/// Response codes copied from RocketMQ 4.9.x `ResponseCode`.
pub mod response_code {
    pub const SUCCESS: i32 = 0;
    pub const SYSTEM_ERROR: i32 = 1;
    pub const SYSTEM_BUSY: i32 = 2;
    pub const REQUEST_CODE_NOT_SUPPORTED: i32 = 3;
    pub const FLUSH_DISK_TIMEOUT: i32 = 10;
    pub const SLAVE_NOT_AVAILABLE: i32 = 11;
    pub const FLUSH_SLAVE_TIMEOUT: i32 = 12;
    pub const MESSAGE_ILLEGAL: i32 = 13;
    pub const SERVICE_NOT_AVAILABLE: i32 = 14;
    pub const NO_PERMISSION: i32 = 16;
    pub const TOPIC_NOT_EXIST: i32 = 17;
    pub const PULL_NOT_FOUND: i32 = 19;
    pub const PULL_RETRY_IMMEDIATELY: i32 = 20;
    pub const PULL_OFFSET_MOVED: i32 = 21;
    pub const TRANSACTION_SHOULD_COMMIT: i32 = 200;
    pub const TRANSACTION_SHOULD_ROLLBACK: i32 = 201;
    pub const TRANSACTION_STATE_UNKNOW: i32 = 202;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn frame_roundtrip() {
        let header = br#"{"code":10}"#;
        let body = b"hello";
        let mut buf = encode_frame(header, body);
        let frame = decode_frame(&mut buf).unwrap().unwrap();
        assert_eq!(&frame.header[..], header);
        assert_eq!(&frame.body[..], body);
        assert!(buf.is_empty());
    }

    #[test]
    fn frame_length_excludes_its_own_field() {
        let header = b"{}";
        let body = b"body";
        let buf = encode_frame(header, body);
        // 4 (length field) + header_len(4) + 2 + 4 = 14 bytes on the wire;
        // the length value itself is 10.
        assert_eq!(buf.len(), 14);
        assert_eq!(i32::from_be_bytes([buf[0], buf[1], buf[2], buf[3]]), 10);
    }

    #[test]
    fn partial_frame_returns_none() {
        let mut buf = encode_frame(b"{}", b"body");
        buf.truncate(5);
        assert!(decode_frame(&mut buf).unwrap().is_none());
    }

    #[test]
    fn invalid_length_is_rejected() {
        let mut buf = BytesMut::from(&b"\x00\x00\x00\x01"[..]);
        assert!(decode_frame(&mut buf).is_err());
    }

    #[test]
    fn command_roundtrip_keeps_ext_fields() {
        let mut cmd = RemotingCommand::request(request_code::SEND_MESSAGE, 42);
        cmd.put_ext("topic", "orders");
        cmd.body = Bytes::from_static(b"payload");
        let encoded = cmd.encode().unwrap();
        let mut buf = encoded;
        let frame = decode_frame(&mut buf).unwrap().unwrap();
        let decoded = RemotingCommand::decode(frame).unwrap();
        assert_eq!(decoded.code, request_code::SEND_MESSAGE);
        assert_eq!(decoded.opaque, 42);
        assert_eq!(decoded.ext("topic"), Some("orders"));
        assert_eq!(&decoded.body[..], b"payload");
    }

    #[test]
    fn response_flag_is_set() {
        let cmd = RemotingCommand::response(response_code::SUCCESS, 1);
        assert!(cmd.is_response());
        assert!(!cmd.is_oneway());
    }

    #[test]
    fn decode_java_4_9_8_golden_frame() {
        // Produced by tools/java/FrameGolden.java against RocketMQ 4.9.8:
        // SEND_MESSAGE (10), opaque=42, language=JAVA, body=[1,2,3].
        const HEX: &str = "000000660000005f7b22636f6465223a31302c22666c6167223a302c226c616e6775616765223a224a415641222c226f7061717565223a34322c2273657269616c697a655479706543757272656e74525043223a224a534f4e222c2276657273696f6e223a307d010203";
        let bytes: Vec<u8> = (0..HEX.len())
            .step_by(2)
            .map(|i| u8::from_str_radix(&HEX[i..i + 2], 16).unwrap())
            .collect();
        let mut buf = BytesMut::from(&bytes[..]);
        let frame = decode_frame(&mut buf).unwrap().unwrap();
        assert!(buf.is_empty());
        let cmd = RemotingCommand::decode(frame).unwrap();
        assert_eq!(cmd.code, request_code::SEND_MESSAGE);
        assert_eq!(cmd.opaque, 42);
        assert_eq!(cmd.language, "JAVA");
        assert_eq!(&cmd.body[..], &[1, 2, 3]);
    }

    #[test]
    fn binary_header_roundtrip() {
        let mut cmd = RemotingCommand::request(request_code::SEND_MESSAGE_V2, 7);
        cmd.serialize_type = 1;
        cmd.put_ext("b", "orders");
        cmd.body = Bytes::from_static(b"body");
        let encoded = cmd.encode().unwrap();
        let mut buf = encoded;
        let frame = decode_frame(&mut buf).unwrap().unwrap();
        assert_eq!(frame.serialize_type, 1);
        let decoded = RemotingCommand::decode(frame).unwrap();
        assert_eq!(decoded.code, request_code::SEND_MESSAGE_V2);
        assert_eq!(decoded.opaque, 7);
        assert_eq!(decoded.ext("b"), Some("orders"));
        assert_eq!(&decoded.body[..], b"body");
    }
}
