//! File-backed Raft log storage for TC.
//!
//! State is a single JSON file rewritten atomically after each mutation. This
//! is intentionally simple: TC logs are small. A production implementation
//! would use segmented WAL + mmap.

use openraft::entry::RaftEntry;
use openraft::storage::IOFlushed;
use openraft::storage::LogState;
use openraft::storage::RaftLogReader;
use openraft::storage::RaftLogStorage;
use openraft::type_config::alias::EntryOf;
use openraft::type_config::alias::LogIdOf;
use openraft::type_config::alias::VoteOf;
use openraft::OptionalSend;
use openraft_memstore::TypeConfig;
use serde::Deserialize;
use serde::Serialize;
use std::collections::BTreeMap;
use std::io;
use std::path::Path;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::sync::RwLock;

#[derive(Serialize, Deserialize, Default)]
struct LogFile {
    last_purged: Option<LogIdOf<TypeConfig>>,
    committed: Option<LogIdOf<TypeConfig>>,
    vote: Option<VoteOf<TypeConfig>>,
    entries: BTreeMap<u64, String>,
}

pub struct PersistentLogStore {
    path: PathBuf,
    inner: Arc<RwLock<LogFile>>,
}

impl PersistentLogStore {
    pub fn open(path: impl AsRef<Path>) -> io::Result<Self> {
        let path = path.as_ref().to_path_buf();
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let data = match std::fs::read(&path) {
            Ok(bytes) => serde_json::from_slice(&bytes).unwrap_or_default(),
            Err(_) => LogFile::default(),
        };
        Ok(Self {
            path,
            inner: Arc::new(RwLock::new(data)),
        })
    }

    fn persist(&self, data: &LogFile) -> io::Result<()> {
        let bytes = serde_json::to_vec(data)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e.to_string()))?;
        let tmp = self.path.with_extension("tmp");
        std::fs::write(&tmp, bytes)?;
        std::fs::rename(tmp, &self.path)?;
        Ok(())
    }
}

#[derive(Clone)]
pub struct LogReader {
    inner: Arc<RwLock<LogFile>>,
}

impl RaftLogReader<TypeConfig> for LogReader {
    async fn try_get_log_entries<RB>(
        &mut self,
        range: RB,
    ) -> Result<Vec<EntryOf<TypeConfig>>, io::Error>
    where
        RB: std::ops::RangeBounds<u64> + Clone + std::fmt::Debug + OptionalSend,
    {
        let data = self.inner.read().await;
        data.entries
            .range(range)
            .map(|(_, s)| {
                serde_json::from_str(s)
                    .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e.to_string()))
            })
            .collect()
    }

    async fn read_vote(&mut self) -> Result<Option<VoteOf<TypeConfig>>, io::Error> {
        Ok(self.inner.read().await.vote)
    }
}

impl RaftLogStorage<TypeConfig> for PersistentLogStore {
    type LogReader = LogReader;

    async fn get_log_state(&mut self) -> Result<LogState<TypeConfig>, io::Error> {
        let data = self.inner.read().await;
        let last = match data.entries.values().next_back() {
            Some(s) => {
                let entry: EntryOf<TypeConfig> = serde_json::from_str(s)
                    .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e.to_string()))?;
                Some(entry.log_id())
            }
            None => data.last_purged,
        };
        Ok(LogState {
            last_purged_log_id: data.last_purged,
            last_log_id: last,
        })
    }

    async fn get_log_reader(&mut self) -> Self::LogReader {
        LogReader {
            inner: Arc::clone(&self.inner),
        }
    }

    async fn save_vote(&mut self, vote: &VoteOf<TypeConfig>) -> Result<(), io::Error> {
        let mut data = self.inner.write().await;
        data.vote = Some(*vote);
        self.persist(&data)
    }

    async fn save_committed(
        &mut self,
        committed: Option<LogIdOf<TypeConfig>>,
    ) -> Result<(), io::Error> {
        let mut data = self.inner.write().await;
        data.committed = committed;
        self.persist(&data)
    }

    async fn read_committed(&mut self) -> Result<Option<LogIdOf<TypeConfig>>, io::Error> {
        Ok(self.inner.read().await.committed)
    }

    async fn append<I>(
        &mut self,
        entries: I,
        callback: IOFlushed<TypeConfig>,
    ) -> Result<(), io::Error>
    where
        I: IntoIterator<Item = EntryOf<TypeConfig>> + OptionalSend,
    {
        let mut data = self.inner.write().await;
        for entry in entries {
            let text = serde_json::to_string(&entry)
                .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e.to_string()))?;
            data.entries.insert(entry.index(), text);
        }
        self.persist(&data)?;
        callback.io_completed(Ok(()));
        Ok(())
    }

    async fn truncate_after(
        &mut self,
        last_log_id: Option<LogIdOf<TypeConfig>>,
    ) -> Result<(), io::Error> {
        let start = last_log_id.map(|id| id.index() + 1).unwrap_or(0);
        let mut data = self.inner.write().await;
        let keys: Vec<u64> = data.entries.range(start..).map(|(k, _)| *k).collect();
        for key in keys {
            data.entries.remove(&key);
        }
        self.persist(&data)
    }

    async fn purge(&mut self, log_id: LogIdOf<TypeConfig>) -> Result<(), io::Error> {
        let mut data = self.inner.write().await;
        let keys: Vec<u64> = data
            .entries
            .range(..=log_id.index())
            .map(|(k, _)| *k)
            .collect();
        for key in keys {
            data.entries.remove(&key);
        }
        data.last_purged = Some(log_id);
        self.persist(&data)
    }
}
