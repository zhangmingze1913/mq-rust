//! Persistent consumer inbox for application-level idempotency.
//!
//! This is a helper for exactly-once *processing*: a consumer asks
//! `POST /inbox` with `group + msg_id` and only processes when
//! `duplicate=false`. The broker does not call it automatically.

use std::collections::HashSet;
use std::fs::{File, OpenOptions};
use std::io::{BufRead, BufReader, Write};
use std::path::Path;
use std::sync::{Mutex, RwLock};

pub struct Inbox {
    file: Mutex<File>,
    seen: RwLock<HashSet<(String, String)>>,
}

impl Inbox {
    pub fn open(path: impl AsRef<Path>) -> std::io::Result<Self> {
        let path = path.as_ref();
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let file = OpenOptions::new()
            .create(true)
            .read(true)
            .append(true)
            .open(path)?;
        let mut seen = HashSet::new();
        for line in BufReader::new(file.try_clone()?)
            .lines()
            .map_while(Result::ok)
        {
            if let Some((group, msg_id)) = line.split_once('\t') {
                seen.insert((group.to_string(), msg_id.to_string()));
            }
        }
        Ok(Self {
            file: Mutex::new(file),
            seen: RwLock::new(seen),
        })
    }

    /// Returns `true` when the key is new (process it), `false` on duplicate.
    pub fn check_and_mark(&self, group: &str, msg_id: &str) -> std::io::Result<bool> {
        let key = (group.to_string(), msg_id.to_string());
        if self.seen.read().unwrap().contains(&key) {
            return Ok(false);
        }
        let mut seen = self.seen.write().unwrap();
        if !seen.insert(key) {
            return Ok(false);
        }
        let mut file = self.file.lock().unwrap();
        writeln!(file, "{group}\t{msg_id}")?;
        file.flush()?;
        Ok(true)
    }
}

#[cfg(test)]
mod tests {
    use super::Inbox;

    #[test]
    fn inbox_dedups_and_survives_reopen() {
        let path = std::env::temp_dir().join(format!(
            "mq-rust-inbox-{}-{}.log",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_millis()
        ));
        let _ = std::fs::remove_file(&path);
        {
            let inbox = Inbox::open(&path).unwrap();
            assert!(inbox.check_and_mark("g1", "m1").unwrap());
            assert!(!inbox.check_and_mark("g1", "m1").unwrap());
            assert!(inbox.check_and_mark("g2", "m1").unwrap());
        }
        let inbox = Inbox::open(&path).unwrap();
        assert!(!inbox.check_and_mark("g1", "m1").unwrap());
        let _ = std::fs::remove_file(&path);
    }
}
