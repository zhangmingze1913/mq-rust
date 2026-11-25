//! Bounded in-memory message trace with optional JSONL persistence.

use serde::{Deserialize, Serialize};
use std::collections::VecDeque;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::Mutex;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TraceEvent {
    pub event: String,
    pub msg_id: String,
    pub topic: String,
    pub queue_id: i32,
    pub queue_offset: i64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub group: Option<String>,
    pub timestamp: i64,
}

pub struct TraceLog {
    capacity: usize,
    events: Mutex<VecDeque<TraceEvent>>,
    path: Option<PathBuf>,
}

impl TraceLog {
    pub fn new(capacity: usize) -> Self {
        Self {
            capacity: capacity.max(1),
            events: Mutex::new(VecDeque::with_capacity(capacity.max(1))),
            path: None,
        }
    }

    /// Open a trace log with JSONL persistence; keeps at most `capacity`
    /// events in memory and appends every new event to `path`.
    pub fn open(capacity: usize, path: impl AsRef<Path>) -> std::io::Result<Self> {
        let path = path.as_ref().to_path_buf();
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let mut events = VecDeque::with_capacity(capacity.max(1));
        if let Ok(text) = std::fs::read_to_string(&path) {
            for line in text.lines() {
                if let Ok(event) = serde_json::from_str::<TraceEvent>(line) {
                    events.push_back(event);
                    if events.len() > capacity.max(1) {
                        events.pop_front();
                    }
                }
            }
        }
        Ok(Self {
            capacity: capacity.max(1),
            events: Mutex::new(events),
            path: Some(path),
        })
    }

    pub fn record(
        &self,
        event: impl Into<String>,
        topic: &str,
        queue_id: i32,
        queue_offset: i64,
        msg_id: &str,
        group: Option<&str>,
    ) {
        let event = TraceEvent {
            event: event.into(),
            msg_id: msg_id.to_string(),
            topic: topic.to_string(),
            queue_id,
            queue_offset,
            group: group.map(str::to_string),
            timestamp: mq_common::now_ms(),
        };
        if let Some(path) = &self.path {
            if let Ok(mut file) = std::fs::OpenOptions::new()
                .create(true)
                .append(true)
                .open(path)
            {
                if let Ok(line) = serde_json::to_string(&event) {
                    let _ = writeln!(file, "{line}");
                }
            }
        }
        let mut events = self.events.lock().unwrap();
        if events.len() >= self.capacity {
            events.pop_front();
        }
        events.push_back(event);
    }

    pub fn by_msg_id(&self, msg_id: &str) -> Vec<TraceEvent> {
        self.events
            .lock()
            .unwrap()
            .iter()
            .filter(|event| event.msg_id == msg_id)
            .cloned()
            .collect()
    }

    pub fn recent(&self, topic: Option<&str>, limit: usize) -> Vec<TraceEvent> {
        let events = self.events.lock().unwrap();
        let mut out: Vec<TraceEvent> = events
            .iter()
            .filter(|event| topic.is_none_or(|topic| event.topic == topic))
            .cloned()
            .collect();
        if out.len() > limit {
            out.drain(0..out.len() - limit);
        }
        out
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn trace_keeps_latest_and_filters() {
        let trace = TraceLog::new(2);
        trace.record("send", "t", 0, 0, "m1", None);
        trace.record("pull", "t", 0, 0, "m1", Some("g"));
        trace.record("send", "t", 0, 1, "m2", None);
        assert_eq!(trace.by_msg_id("m1").len(), 1);
        assert_eq!(trace.by_msg_id("m1")[0].event, "pull");
        assert_eq!(trace.by_msg_id("m2").len(), 1);
        assert_eq!(trace.recent(Some("t"), 10).len(), 2);
        assert_eq!(trace.recent(Some("other"), 10).len(), 0);
    }

    #[test]
    fn trace_persists_and_reloads() {
        let path = std::env::temp_dir().join(format!(
            "mq-rust-trace-{}-{}.jsonl",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_millis()
        ));
        let _ = std::fs::remove_file(&path);
        {
            let trace = TraceLog::open(10, &path).unwrap();
            trace.record("send", "t", 0, 0, "m1", None);
            trace.record("pull", "t", 0, 0, "m1", Some("g"));
        }
        let reopened = TraceLog::open(10, &path).unwrap();
        let events = reopened.by_msg_id("m1");
        assert_eq!(events.len(), 2);
        assert_eq!(events[0].event, "send");
        assert_eq!(events[1].event, "pull");
        let _ = std::fs::remove_file(&path);
    }
}
