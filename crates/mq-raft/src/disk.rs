//! Minimal durable Raft log store.
//!
//! It keeps a full in-memory snapshot of the log (entries + vote + purged id)
//! and rewrites a JSON file after each mutation. This is intentionally simple;
//! production should switch to a segmented log.

use std::fmt::Debug;
use std::io;
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

#[derive(Clone)]
pub struct DiskLogStore {
    inner: Arc<MemLogStore>,
    state: Arc<Mutex<DiskState>>,
    path: Arc<PathBuf>,
}

impl DiskLogStore {
    pub async fn open(path: impl AsRef<Path>) -> io::Result<Self> {
        let path = path.as_ref().to_path_buf();
        let state = match std::fs::read(&path) {
            Ok(bytes) => serde_json::from_slice(&bytes)
                .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e.to_string()))?,
            Err(err) if err.kind() == io::ErrorKind::NotFound => DiskState::default(),
            Err(err) => return Err(err),
        };
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

    fn persist(&self) {
        let state = self.state.lock().unwrap();
        let Ok(bytes) = serde_json::to_vec(&*state) else {
            return;
        };
        let tmp = self.path.with_extension("tmp");
        if std::fs::write(&tmp, bytes).is_ok() {
            let _ = std::fs::rename(&tmp, &*self.path);
        }
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
        self.persist();
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
        self.persist();
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
        self.persist();
        RaftLogStorage::truncate_after(&mut Arc::clone(&self.inner), last_log_id).await
    }

    async fn purge(&mut self, log_id: LogIdOf<TypeConfig>) -> Result<(), io::Error> {
        {
            let mut state = self.state.lock().unwrap();
            state.purged = Some(log_id);
            state.entries.retain(|e| e.index() > log_id.index());
        }
        self.persist();
        RaftLogStorage::purge(&mut Arc::clone(&self.inner), log_id).await
    }
}
