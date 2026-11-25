//! Best-effort TC outbox with an append-only local journal.
//!
//! Broker writes local Half state immediately, then queues prepare/commit/
//! rollback for TC with retry/backoff. Pending operations are appended to
//! `tc-outbox.log`; successful ones get a `D` tombstone. On Broker restart,
//! pending operations are replayed.
//!
//! This is not yet a full transactional outbox (no compaction/fsync policy),
//! but it survives a Broker process restart.

use crate::tc::TcClient;
use std::collections::HashSet;
use std::fs::{File, OpenOptions};
use std::io::{BufRead, BufReader, Write};
use std::path::Path;
use std::sync::{Arc, Mutex};
use tokio::sync::mpsc;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum TcOp {
    Prepare,
    Commit,
    Rollback,
}

impl TcOp {
    fn as_str(self) -> &'static str {
        match self {
            TcOp::Prepare => "prepare",
            TcOp::Commit => "commit",
            TcOp::Rollback => "rollback",
        }
    }

    fn parse(s: &str) -> Option<Self> {
        match s {
            "prepare" => Some(TcOp::Prepare),
            "commit" => Some(TcOp::Commit),
            "rollback" => Some(TcOp::Rollback),
            _ => None,
        }
    }
}

struct Journal {
    file: Mutex<File>,
}

impl Journal {
    fn append(&self, kind: char, op: TcOp, tx_id: &str) {
        if let Ok(mut file) = self.file.lock() {
            let _ = writeln!(file, "{kind}\t{}\t{tx_id}", op.as_str());
            let _ = file.flush();
        }
    }
}

pub struct TcOutbox {
    tx: mpsc::UnboundedSender<(TcOp, String)>,
    journal: Option<Arc<Journal>>,
}

impl TcOutbox {
    pub fn start(client: Arc<TcClient>, path: Option<&Path>) -> Self {
        let (tx, mut rx) = mpsc::unbounded_channel::<(TcOp, String)>();
        let journal = path.and_then(|path| {
            if let Some(parent) = path.parent() {
                let _ = std::fs::create_dir_all(parent);
            }
            let file = OpenOptions::new()
                .create(true)
                .read(true)
                .append(true)
                .open(path)
                .ok();
            file.map(|file| {
                Arc::new(Journal {
                    file: Mutex::new(file),
                })
            })
        });

        if let (Some(_), Some(path)) = (&journal, path) {
            let _ = replay(path, &tx);
            tracing::info!(?path, "tc outbox journal loaded");
        }

        let worker_journal = journal.clone();
        tokio::spawn(async move {
            while let Some((op, tx_id)) = rx.recv().await {
                loop {
                    let ok = match op {
                        TcOp::Prepare => client.prepare(&tx_id).await,
                        TcOp::Commit => client.commit(&tx_id).await,
                        TcOp::Rollback => client.rollback(&tx_id).await,
                    };
                    if ok {
                        if let Some(journal) = &worker_journal {
                            journal.append('D', op, &tx_id);
                        }
                        break;
                    }
                    tokio::time::sleep(std::time::Duration::from_millis(200)).await;
                }
            }
        });

        Self { tx, journal }
    }

    pub fn enqueue(&self, op: TcOp, tx_id: &str) {
        if let Some(journal) = &self.journal {
            journal.append('P', op, tx_id);
        }
        let _ = self.tx.send((op, tx_id.to_string()));
    }
}

fn replay(path: &Path, tx: &mpsc::UnboundedSender<(TcOp, String)>) -> std::io::Result<()> {
    let file = File::open(path)?;
    let mut done: HashSet<(String, String)> = HashSet::new();
    let mut pending: Vec<(TcOp, String)> = Vec::new();
    for line in BufReader::new(file).lines().map_while(Result::ok) {
        let mut parts = line.splitn(3, '\t');
        let kind = parts.next().unwrap_or_default();
        let op = parts.next().unwrap_or_default();
        let tx_id = parts.next().unwrap_or_default();
        if tx_id.is_empty() {
            continue;
        }
        if kind == "D" {
            done.insert((op.to_string(), tx_id.to_string()));
        } else if let Some(op) = TcOp::parse(op) {
            if !done.contains(&(op.as_str().to_string(), tx_id.to_string())) {
                pending.push((op, tx_id.to_string()));
            }
        }
    }
    for (op, tx_id) in pending {
        let _ = tx.send((op, tx_id));
    }
    Ok(())
}
