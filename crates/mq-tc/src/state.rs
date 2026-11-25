//! Application state machine for TC transaction statuses.
//!
//! Unlike `openraft_memstore`, this state machine exposes the applied status map
//! to the TC service, so followers can answer status queries and a newly elected
//! leader has the committed state.

use futures::TryStreamExt;
use openraft::entry::EntryPayload;
use openraft::storage::EntryResponder;
use openraft::storage::RaftSnapshotBuilder;
use openraft::storage::RaftStateMachine;
use openraft::type_config::alias::LogIdOf;
use openraft::type_config::alias::SnapshotMetaOf;
use openraft::type_config::alias::SnapshotOf;
use openraft::type_config::alias::StoredMembershipOf;
use openraft::OptionalSend;
use openraft_memstore::ClientResponse;
use openraft_memstore::TypeConfig;
use serde::Deserialize;
use serde::Serialize;
use std::collections::HashMap;
use std::fs::{File, OpenOptions};
use std::io;
use std::io::{BufRead, BufReader, Cursor, Write};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use tokio::sync::RwLock;

pub type StatusMap = Arc<RwLock<HashMap<String, String>>>;
type CurrentSnapshot = Arc<RwLock<Option<SnapshotOf<TypeConfig, Cursor<Vec<u8>>>>>>;

#[derive(Serialize, Deserialize)]
struct SnapshotFile {
    last_log_id: Option<LogIdOf<TypeConfig>>,
    last_membership: StoredMembershipOf<TypeConfig>,
    status: HashMap<String, String>,
}

#[derive(Default, Clone)]
pub struct StatusSM {
    status: StatusMap,
    applied: Arc<RwLock<Option<LogIdOf<TypeConfig>>>>,
    membership: Arc<RwLock<StoredMembershipOf<TypeConfig>>>,
    wal: Option<Arc<Mutex<File>>>,
    current: CurrentSnapshot,
    snapshot_path: Option<PathBuf>,
}

impl StatusSM {
    pub fn new() -> (Self, StatusMap) {
        let status = Arc::new(RwLock::new(HashMap::new()));
        (
            Self {
                status: Arc::clone(&status),
                applied: Arc::new(RwLock::new(None)),
                membership: Arc::new(RwLock::new(StoredMembershipOf::<TypeConfig>::default())),
                wal: None,
                current: Arc::new(RwLock::new(None)),
                snapshot_path: None,
            },
            status,
        )
    }

    /// Build a state machine backed by a status WAL. Existing entries are
    /// loaded into the shared status map before Raft starts.
    pub fn new_with_wal(path: &Path) -> io::Result<(Self, StatusMap)> {
        Self::new_with_wal_and_snapshot(path, None)
    }

    /// Same as [`Self::new_with_wal`], plus a persisted Raft snapshot.
    pub fn new_with_wal_and_snapshot(
        path: &Path,
        snapshot_path: Option<&Path>,
    ) -> io::Result<(Self, StatusMap)> {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let file = OpenOptions::new()
            .create(true)
            .read(true)
            .append(true)
            .open(path)?;
        let mut restored = HashMap::new();
        for line in BufReader::new(file.try_clone()?)
            .lines()
            .map_while(Result::ok)
        {
            if let Some((tx, status)) = line.split_once('\t') {
                restored.insert(tx.to_string(), status.to_string());
            }
        }
        let mut applied = None;
        let mut membership = StoredMembershipOf::<TypeConfig>::default();
        let mut current = None;
        if let Some(snapshot_path) = snapshot_path {
            if let Ok(bytes) = std::fs::read(snapshot_path) {
                if let Ok(snapshot) = serde_json::from_slice::<SnapshotFile>(&bytes) {
                    restored = snapshot.status;
                    applied = snapshot.last_log_id;
                    membership = snapshot.last_membership;
                    let body = serde_json::to_vec(&restored).unwrap_or_default();
                    current = Some(SnapshotOf::<TypeConfig, Cursor<Vec<u8>>> {
                        meta: SnapshotMetaOf::<TypeConfig> {
                            last_log_id: applied,
                            last_membership: membership.clone(),
                        },
                        snapshot: Cursor::new(body),
                    });
                }
            }
        }
        let status = Arc::new(RwLock::new(restored));
        Ok((
            Self {
                status: Arc::clone(&status),
                applied: Arc::new(RwLock::new(applied)),
                membership: Arc::new(RwLock::new(membership)),
                wal: Some(Arc::new(Mutex::new(file))),
                current: Arc::new(RwLock::new(current)),
                snapshot_path: snapshot_path.map(Path::to_path_buf),
            },
            status,
        ))
    }
}

impl RaftStateMachine<TypeConfig> for StatusSM {
    type SnapshotData = Cursor<Vec<u8>>;
    type SnapshotBuilder = StatusSM;

    async fn applied_state(
        &mut self,
    ) -> Result<(Option<LogIdOf<TypeConfig>>, StoredMembershipOf<TypeConfig>), io::Error> {
        Ok((
            *self.applied.read().await,
            self.membership.read().await.clone(),
        ))
    }

    async fn apply<Strm>(&mut self, mut entries: Strm) -> Result<(), io::Error>
    where
        Strm: futures::Stream<Item = Result<EntryResponder<TypeConfig>, io::Error>>
            + Unpin
            + OptionalSend,
    {
        while let Some((entry, responder)) = entries.try_next().await? {
            *self.applied.write().await = Some(entry.log_id);
            let response = match entry.payload {
                EntryPayload::Blank => ClientResponse(None),
                EntryPayload::Normal(ref data) => {
                    let previous = self
                        .status
                        .write()
                        .await
                        .insert(data.client.clone(), data.status.clone());
                    if let Some(wal) = &self.wal {
                        if let Ok(mut file) = wal.lock() {
                            let _ = writeln!(file, "{}\t{}", data.client, data.status);
                            let _ = file.flush();
                        }
                    }
                    ClientResponse(previous)
                }
                EntryPayload::Membership(ref mem) => {
                    *self.membership.write().await =
                        StoredMembershipOf::<TypeConfig>::new(Some(entry.log_id), mem.clone());
                    ClientResponse(None)
                }
            };
            if let Some(responder) = responder {
                responder.send(response);
            }
        }
        Ok(())
    }

    async fn get_snapshot_builder(&mut self) -> Self::SnapshotBuilder {
        self.clone()
    }

    async fn install_snapshot(
        &mut self,
        meta: &SnapshotMetaOf<TypeConfig>,
        snapshot: Self::SnapshotData,
    ) -> Result<(), io::Error> {
        let restored: HashMap<String, String> = serde_json::from_slice(&snapshot.into_inner())
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e.to_string()))?;
        *self.status.write().await = restored;
        *self.applied.write().await = meta.last_log_id;
        *self.membership.write().await = meta.last_membership.clone();
        if let Some(path) = &self.snapshot_path {
            let file = SnapshotFile {
                last_log_id: meta.last_log_id,
                last_membership: meta.last_membership.clone(),
                status: self.status.read().await.clone(),
            };
            if let Ok(bytes) = serde_json::to_vec(&file) {
                let tmp = path.with_extension("tmp");
                if std::fs::write(&tmp, bytes).is_ok() {
                    let _ = std::fs::rename(tmp, path);
                }
            }
        }
        Ok(())
    }

    async fn get_current_snapshot(
        &mut self,
    ) -> Result<Option<SnapshotOf<TypeConfig, Self::SnapshotData>>, io::Error> {
        Ok(self.current.read().await.clone())
    }
}

impl RaftSnapshotBuilder<TypeConfig> for StatusSM {
    type SnapshotData = Cursor<Vec<u8>>;

    async fn build_snapshot(
        &mut self,
    ) -> Result<SnapshotOf<TypeConfig, Self::SnapshotData>, io::Error> {
        let status = self.status.read().await.clone();
        let body = serde_json::to_vec(&status)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e.to_string()))?;
        let meta = SnapshotMetaOf::<TypeConfig> {
            last_log_id: *self.applied.read().await,
            last_membership: self.membership.read().await.clone(),
        };
        let snapshot = SnapshotOf::<TypeConfig, Self::SnapshotData> {
            meta: meta.clone(),
            snapshot: Cursor::new(body.clone()),
        };
        *self.current.write().await = Some(snapshot.clone());
        if let Some(path) = &self.snapshot_path {
            let file = SnapshotFile {
                last_log_id: meta.last_log_id,
                last_membership: meta.last_membership.clone(),
                status,
            };
            if let Ok(bytes) = serde_json::to_vec(&file) {
                let tmp = path.with_extension("tmp");
                if std::fs::write(&tmp, bytes).is_ok() {
                    let _ = std::fs::rename(tmp, path);
                }
            }
        }
        Ok(snapshot)
    }
}
