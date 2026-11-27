//! Bounded in-memory message trace with optional rotated JSONL persistence.

use serde::{Deserialize, Serialize};
use std::collections::VecDeque;
use std::fs::{File, OpenOptions};
use std::io::{BufWriter, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::mpsc::SyncSender;
use std::sync::Mutex;
use std::thread::JoinHandle;

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

fn rotated_path(path: &Path) -> PathBuf {
    path.with_file_name(format!(
        "{}.1",
        path.file_name().unwrap_or_default().to_string_lossy()
    ))
}

struct TraceWriter {
    file: BufWriter<File>,
    bytes: u64,
    writes: u64,
}

impl TraceWriter {
    fn open(path: &Path) -> std::io::Result<Self> {
        let file = OpenOptions::new().create(true).append(true).open(path)?;
        let bytes = file.metadata()?.len();
        Ok(Self {
            file: BufWriter::with_capacity(64 * 1024, file),
            bytes,
            writes: 0,
        })
    }

    fn flush(&mut self) {
        let _ = self.file.flush();
    }
}

struct WriterHandle {
    tx: Option<SyncSender<Vec<u8>>>,
    join: Option<JoinHandle<()>>,
}

impl WriterHandle {
    fn spawn(path: PathBuf, max_bytes: u64) -> Self {
        let (tx, rx) = std::sync::mpsc::sync_channel::<Vec<u8>>(65536);
        let join = std::thread::spawn(move || {
            let mut writer = TraceWriter::open(&path).ok();
            while let Ok(line) = rx.recv() {
                let Some(w) = writer.as_mut() else { continue };
                if w.file.write_all(&line).is_err() || w.file.write_all(b"\n").is_err() {
                    continue;
                }
                w.bytes += line.len() as u64 + 1;
                w.writes += 1;
                if w.writes % 64 == 0 {
                    w.flush();
                }
                if w.bytes >= max_bytes {
                    if let Some(mut old) = writer.take() {
                        old.flush();
                    }
                    let rotated = rotated_path(&path);
                    let _ = std::fs::remove_file(&rotated);
                    let _ = std::fs::rename(&path, rotated);
                    writer = TraceWriter::open(&path).ok();
                }
            }
            if let Some(mut w) = writer.take() {
                w.flush();
            }
        });
        Self {
            tx: Some(tx),
            join: Some(join),
        }
    }
}

impl Drop for WriterHandle {
    fn drop(&mut self) {
        self.tx.take();
        if let Some(join) = self.join.take() {
            let _ = join.join();
        }
    }
}

pub struct TraceLog {
    capacity: usize,
    events: Mutex<VecDeque<TraceEvent>>,
    writer: Option<WriterHandle>,
    sample: u32,
    seq: AtomicU64,
    enabled: bool,
}

impl TraceLog {
    pub fn new(capacity: usize) -> Self {
        Self {
            capacity: capacity.max(1),
            events: Mutex::new(VecDeque::with_capacity(capacity.max(1))),
            writer: None,
            sample: 1,
            seq: AtomicU64::new(0),
            enabled: true,
        }
    }

    /// A trace log that records nothing; used for throughput benchmarks.
    pub fn disabled() -> Self {
        Self {
            capacity: 1,
            events: Mutex::new(VecDeque::with_capacity(1)),
            writer: None,
            sample: 1,
            seq: AtomicU64::new(0),
            enabled: false,
        }
    }

    /// Open a trace log with JSONL persistence; keeps at most `capacity`
    /// events in memory and appends every new event to `path`.
    pub fn open(capacity: usize, path: impl AsRef<Path>) -> std::io::Result<Self> {
        Self::open_with_limit(capacity, path, 64 * 1024 * 1024)
    }

    /// Like [`TraceLog::open`] but rotates the file once it exceeds `max_bytes`.
    pub fn open_with_limit(
        capacity: usize,
        path: impl AsRef<Path>,
        max_bytes: u64,
    ) -> std::io::Result<Self> {
        Self::open_with_sample(capacity, path, max_bytes, 1)
    }

    /// Persist only every `sample`-th event; every event stays in memory.
    pub fn open_with_sample(
        capacity: usize,
        path: impl AsRef<Path>,
        max_bytes: u64,
        sample: u32,
    ) -> std::io::Result<Self> {
        let path = path.as_ref().to_path_buf();
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let mut events = VecDeque::with_capacity(capacity.max(1));
        for candidate in [rotated_path(&path), path.clone()] {
            let Ok(text) = std::fs::read_to_string(&candidate) else {
                continue;
            };
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
            writer: Some(WriterHandle::spawn(path, max_bytes.max(1))),
            sample: sample.max(1),
            seq: AtomicU64::new(0),
            enabled: true,
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
        if !self.enabled {
            return;
        }
        if self.writer.is_some() && self.sample > 1 {
            let seq = self.seq.fetch_add(1, Ordering::Relaxed);
            let sampled = seq
                .checked_rem(u64::from(self.sample))
                .is_some_and(|rem| rem == 0);
            if !sampled {
                return;
            }
        }
        let event = TraceEvent {
            event: event.into(),
            msg_id: msg_id.to_string(),
            topic: topic.to_string(),
            queue_id,
            queue_offset,
            group: group.map(str::to_string),
            timestamp: mq_common::now_ms(),
        };
        if let Some(writer) = &self.writer {
            if let Ok(line) = serde_json::to_string(&event) {
                if let Some(tx) = &writer.tx {
                    let _ = tx.try_send(line.into_bytes());
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
        self.page(topic, None, None, 0, limit).1
    }

    pub fn page(
        &self,
        topic: Option<&str>,
        from: Option<i64>,
        to: Option<i64>,
        offset: usize,
        limit: usize,
    ) -> (usize, Vec<TraceEvent>) {
        let events = self.events.lock().unwrap();
        let mut filtered: Vec<TraceEvent> = events
            .iter()
            .filter(|event| topic.is_none_or(|topic| event.topic == topic))
            .filter(|event| from.is_none_or(|from| event.timestamp >= from))
            .filter(|event| to.is_none_or(|to| event.timestamp <= to))
            .cloned()
            .collect();
        filtered.reverse();
        let total = filtered.len();
        let page = filtered.into_iter().skip(offset).take(limit).collect();
        (total, page)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn trace_page_filters_and_offsets() {
        let trace = TraceLog::new(10);
        for index in 0..5 {
            trace.record("send", "t", 0, index, &format!("m{index}"), None);
        }
        let (total, page) = trace.page(Some("t"), None, None, 1, 2);
        assert_eq!(total, 5);
        assert_eq!(page.len(), 2);
        assert_eq!(page[0].msg_id, "m3");
        assert_eq!(page[1].msg_id, "m2");
        let now = mq_common::now_ms();
        let (total, page) = trace.page(None, Some(now + 1_000), None, 0, 10);
        assert_eq!(total, 0);
        assert!(page.is_empty());
    }

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
    fn trace_rotates_file() {
        let path = std::env::temp_dir().join(format!(
            "mq-rust-trace-rot-{}-{}.jsonl",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_millis()
        ));
        let rotated = rotated_path(&path);
        let _ = std::fs::remove_file(&path);
        let _ = std::fs::remove_file(&rotated);
        {
            let trace = TraceLog::open_with_limit(10, &path, 1).unwrap();
            trace.record("send", "t", 0, 0, "m1", None);
        }
        assert!(rotated.exists());
        let reopened = TraceLog::open_with_limit(10, &path, 1).unwrap();
        assert_eq!(reopened.by_msg_id("m1").len(), 1);
        let _ = std::fs::remove_file(&path);
        let _ = std::fs::remove_file(&rotated);
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
