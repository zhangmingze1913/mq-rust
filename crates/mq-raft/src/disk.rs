//! Minimal durable Raft log store.
//!
//! The file is append-only JSONL. On startup records are replayed into an
//! in-memory `MemLogStore`. Compaction is not implemented yet.

use std::fmt::Debug;
use std::io::{self, Write};
use std::ops::RangeBounds;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use openraft::alias::EntryOf;
use openraft::alias::LogIdOf;
use openraft::alias::VoteOf;
use openraft::entry::RaftEntry;
use openraft::storage::IOFlushed;
use openraft::storage::LogState;
use openraft::storage::RaftLogReader;
use openraft::storage::RaftLogStorage;
use openraft::OptionalSend;
use openraft_memstore::MemLogStore;
use openraft_memstore::TypeConfig;
use serde::Deserialize;
use serde::Serialize;

#[derive(Default, Serialize, Deserialize)]
struct DiskState {
    vote: Option<VoteOf<TypeConfig>>,
    purged: Option<LogIdOf<TypeConfig>>,
    entries: Vec<EntryOf<TypeConfig>>,
}

#[derive(Serialize, Deserialize)]
#[serde(tag = "op")]
enum DiskRecord {
    #[serde(rename = "v")]
    Vote { vote: VoteOf<TypeConfig> },
    #[serde(rename = "a")]
    Append { entry: EntryOf<TypeConfig> },
    #[serde(rename = "t")]
    Truncate { last: Option<LogIdOf<TypeConfig>> },
    #[serde(rename = "p")]
    Purge { id: LogIdOf<TypeConfig> },
}

fn apply_record(state: &mut DiskState, record: DiskRecord) {
    match record {
        DiskRecord::Vote { vote } => state.vote = Some(vote),
        DiskRecord::Append { entry } => {
            state.entries.retain(|e| e.index() != entry.index());
            state.entries.push(entry);
            state.entries.sort_by_key(|e| e.index());
        }
        DiskRecord::Truncate { last } => {
            let keep = last.map(|id| id.index()).unwrap_or(0);
            state.entries.retain(|e| e.index() <= keep);
        }
        DiskRecord::Purge { id } => {
            state.purged = Some(id);
            state.entries.retain(|e| e.index() > id.index());
        }
    }
}

#[derive(Clone)]
pub struct DiskLogStore {
    inner: Arc<MemLogStore>,
    state: Arc<Mutex<DiskState>>,
    path: Arc<PathBuf>,
}

impl DiskLogStore {
    pub async fn open(path: impl AsRef<Path>) -> io::Result<Self> {
        let path = path.as_ref().to_path_buf();
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let mut state = DiskState::default();
        if let Ok(text) = std::fs::read_to_string(&path) {
            for line in text.lines() {
                if line.trim().is_empty() {
                    continue;
                }
                if let Ok(record) = serde_json::from_str::<DiskRecord>(line) {
                    apply_record(&mut state, record);
                }
            }
        }
        let inner = Arc::new(MemLogStore::new(Default::default()));
        if let Some(vote) = &state.vote {
            RaftLogStorage::save_vote(&mut Arc::clone(&inner), vote).await?;
        }
        if !state.entries.is_empty() {
            RaftLogStorage::append(
                &mut Arc::clone(&inner),
                state.entries.clone(),
                IOFlushed::noop(),
            )
            .await?;
        }
        if let Some(purged) = state.purged {
            RaftLogStorage::purge(&mut Arc::clone(&inner), purged).await?;
        }
        Ok(Self {
            inner,
            state: Arc::new(Mutex::new(state)),
            path: Arc::new(path),
        })
    }

    fn write_record(&self, record: &DiskRecord) {
        let Ok(mut file) = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&*self.path)
        else {
            return;
        };
        if serde_json::to_writer(&mut file, record).is_ok() {
            let _ = file.write_all(b"\n");
            let _ = file.flush();
        }
    }

    /// Rewrite the JSONL file from the in-memory state (compaction).
    fn rewrite(&self) {
        let state = self.state.lock().unwrap();
        let tmp = self.path.with_extension("compact");
        let Ok(mut file) = std::fs::File::create(&tmp) else {
            return;
        };
        let mut write = |record: &DiskRecord| {
            if serde_json::to_writer(&mut file, record).is_ok() {
                let _ = file.write_all(b"\n");
            }
        };
        if let Some(vote) = state.vote {
            write(&DiskRecord::Vote { vote });
        }
        for entry in &state.entries {
            write(&DiskRecord::Append {
                entry: entry.clone(),
            });
        }
        if let Some(id) = state.purged {
            write(&DiskRecord::Purge { id });
        }
        let _ = file.flush();
        drop(file);
        let _ = std::fs::rename(&tmp, &*self.path);
    }
}

impl RaftLogReader<TypeConfig> for DiskLogStore {
    async fn try_get_log_entries<RB>(
        &mut self,
        range: RB,
    ) -> Result<Vec<EntryOf<TypeConfig>>, io::Error>
    where
        RB: RangeBounds<u64> + Clone + Debug + OptionalSend,
    {
        RaftLogReader::try_get_log_entries(&mut Arc::clone(&self.inner), range).await
    }

    async fn read_vote(&mut self) -> Result<Option<VoteOf<TypeConfig>>, io::Error> {
        Ok(self.state.lock().unwrap().vote)
    }
}

impl RaftLogStorage<TypeConfig> for DiskLogStore {
    type LogReader = Self;

    async fn get_log_state(&mut self) -> Result<LogState<TypeConfig>, io::Error> {
        RaftLogStorage::get_log_state(&mut Arc::clone(&self.inner)).await
    }

    async fn get_log_reader(&mut self) -> Self::LogReader {
        self.clone()
    }

    async fn save_vote(&mut self, vote: &VoteOf<TypeConfig>) -> Result<(), io::Error> {
        self.state.lock().unwrap().vote = Some(*vote);
        self.write_record(&DiskRecord::Vote { vote: *vote });
        RaftLogStorage::save_vote(&mut Arc::clone(&self.inner), vote).await
    }

    async fn save_committed(
        &mut self,
        committed: Option<LogIdOf<TypeConfig>>,
    ) -> Result<(), io::Error> {
        RaftLogStorage::save_committed(&mut Arc::clone(&self.inner), committed).await
    }

    async fn read_committed(&mut self) -> Result<Option<LogIdOf<TypeConfig>>, io::Error> {
        RaftLogStorage::read_committed(&mut Arc::clone(&self.inner)).await
    }

    async fn append<I>(
        &mut self,
        entries: I,
        callback: IOFlushed<TypeConfig>,
    ) -> Result<(), io::Error>
    where
        I: IntoIterator<Item = EntryOf<TypeConfig>> + OptionalSend,
    {
        let entries: Vec<_> = entries.into_iter().collect();
        {
            let mut state = self.state.lock().unwrap();
            for entry in &entries {
                state.entries.retain(|e| e.index() != entry.index());
                state.entries.push(entry.clone());
            }
            state.entries.sort_by_key(|e| e.index());
        }
        for entry in &entries {
            self.write_record(&DiskRecord::Append {
                entry: entry.clone(),
            });
        }
        RaftLogStorage::append(&mut Arc::clone(&self.inner), entries, callback).await
    }

    async fn truncate_after(
        &mut self,
        last_log_id: Option<LogIdOf<TypeConfig>>,
    ) -> Result<(), io::Error> {
        {
            let mut state = self.state.lock().unwrap();
            let keep = last_log_id.map(|id| id.index()).unwrap_or(0);
            state.entries.retain(|e| e.index() <= keep);
        }
        self.rewrite();
        RaftLogStorage::truncate_after(&mut Arc::clone(&self.inner), last_log_id).await
    }

    async fn purge(&mut self, log_id: LogIdOf<TypeConfig>) -> Result<(), io::Error> {
        {
            let mut state = self.state.lock().unwrap();
            state.purged = Some(log_id);
            state.entries.retain(|e| e.index() > log_id.index());
        }
        self.rewrite();
        RaftLogStorage::purge(&mut Arc::clone(&self.inner), log_id).await
    }
}
