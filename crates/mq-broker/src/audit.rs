//! Append-only JSONL audit log for transaction events.
//!
//! This is intentionally simple: one line per event, flushed immediately. The
//! production version will rotate/compress and optionally mirror to WORM storage.

use serde_json::{json, Value};
use std::fs::{File, OpenOptions};
use std::io::Write;
use std::path::Path;
use std::sync::Mutex;

pub struct AuditLog {
    file: Mutex<File>,
}

impl AuditLog {
    pub fn open(path: impl AsRef<Path>) -> std::io::Result<Self> {
        let path = path.as_ref();
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let file = OpenOptions::new().create(true).append(true).open(path)?;
        Ok(Self {
            file: Mutex::new(file),
        })
    }

    pub fn write(&self, event: &str, fields: Value) {
        let mut rec = fields;
        if let Some(obj) = rec.as_object_mut() {
            obj.insert("event".into(), json!(event));
            obj.insert("ts_ms".into(), json!(mq_common::now_ms()));
        }
        let Ok(line) = serde_json::to_vec(&rec) else {
            return;
        };
        if let Ok(mut file) = self.file.lock() {
            let _ = file.write_all(&line);
            let _ = file.write_all(b"\n");
            let _ = file.flush();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn writes_jsonl_event() {
        let path = std::env::temp_dir().join(format!(
            "mq-rust-audit-{}-{}.log",
            std::process::id(),
            mq_common::now_ms()
        ));
        let _ = std::fs::remove_file(&path);
        {
            let audit = AuditLog::open(&path).unwrap();
            audit.write("tx.commit", json!({"tx_id": "tx-1"}));
        }
        let text = std::fs::read_to_string(&path).unwrap();
        assert!(text.contains("\"event\":\"tx.commit\""));
        assert!(text.contains("\"tx_id\":\"tx-1\""));
        let _ = std::fs::remove_file(&path);
    }
}
