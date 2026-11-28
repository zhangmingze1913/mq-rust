//! Raft-backed message log (prototype for docs/25).
//!
//! The command payload is encoded into `openraft_memstore::ClientRequest.status`;
//! the state machine decodes it into [`MessageCommand`] and appends to an
//! in-memory log. Broker persistence integration is a later step.

use futures::TryStreamExt;
use openraft::entry::EntryPayload;
use openraft::entry::RaftEntry;
use openraft::errors::RPCError;
use openraft::network::RPCOption;
use openraft::raft::AppendEntriesRequest;
use openraft::raft::AppendEntriesResponse;
use openraft::raft::AppendResult;
use openraft::raft::SnapshotResponse;
use openraft::raft::VoteRequest;
use openraft::raft::VoteResponse;
use openraft::storage::EntryResponder;
use openraft::storage::RaftSnapshotBuilder;
use openraft::storage::RaftStateMachine;
use openraft::type_config::alias::LogIdOf;
use openraft::type_config::alias::SnapshotMetaOf;
use openraft::type_config::alias::SnapshotOf;
use openraft::type_config::alias::StoredMembershipOf;
use openraft::type_config::alias::VoteOf;
use openraft::OptionalSend;
use openraft::RaftNetworkFactory;
use openraft::RaftNetworkV2;
use openraft_memstore::ClientRequest;
use openraft_memstore::ClientResponse;
use openraft_memstore::MemLogStore;
use openraft_memstore::TypeConfig;
use serde::Deserialize;
use serde::Serialize;
use std::collections::HashMap;
use std::io;
use std::io::Cursor;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::RwLock;

pub mod disk;
pub use disk::DiskLogStore;

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct MessageCommand {
    pub topic: String,
    pub queue_id: i32,
    pub msg_id: String,
    pub body: Vec<u8>,
    pub properties: String,
    #[serde(default)]
    pub deliver_at_ms: Option<u64>,
    #[serde(default)]
    pub tx_id: Option<String>,
    #[serde(default)]
    pub tx_kind: Option<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(untagged)]
pub enum RaftCommand {
    Batch { messages: Vec<MessageCommand> },
    Single(MessageCommand),
}

pub type Messages = Arc<RwLock<Vec<MessageCommand>>>;
pub type Raft = Arc<openraft::Raft<TypeConfig, MessageSM>>;
pub type Applier = Arc<dyn Fn(MessageCommand) + Send + Sync>;
pub type BatchApplier = Arc<dyn Fn(Vec<MessageCommand>) + Send + Sync>;
type CurrentSnapshot = Arc<RwLock<Option<SnapshotOf<TypeConfig, Cursor<Vec<u8>>>>>>;

#[derive(Clone)]
pub struct MessageSM {
    messages: Messages,
    applier: Option<Applier>,
    batch_applier: Option<BatchApplier>,
    applied: Arc<RwLock<Option<LogIdOf<TypeConfig>>>>,
    membership: Arc<RwLock<StoredMembershipOf<TypeConfig>>>,
    current: CurrentSnapshot,
    snapshot_path: Option<Arc<PathBuf>>,
    delta_path: Option<Arc<PathBuf>>,
    meta_path: Option<Arc<PathBuf>>,
    snapshot_base_count: Arc<std::sync::atomic::AtomicUsize>,
    applied_path: Option<Arc<PathBuf>>,
    applied_persist_ms: Arc<std::sync::atomic::AtomicU64>,
}

#[derive(Clone, Serialize, Deserialize)]
struct SnapshotMetaFile {
    applied: Option<LogIdOf<TypeConfig>>,
    membership: StoredMembershipOf<TypeConfig>,
    base_count: usize,
    total: usize,
}

#[derive(Serialize, Deserialize)]
struct SnapshotFile {
    messages: Vec<MessageCommand>,
    #[serde(default)]
    total_messages: usize,
    applied: Option<LogIdOf<TypeConfig>>,
    membership: StoredMembershipOf<TypeConfig>,
}

const SNAPSHOT_TAIL_MESSAGES: usize = 1000;

impl Default for MessageSM {
    fn default() -> Self {
        Self {
            messages: Arc::new(RwLock::new(Vec::new())),
            applier: None,
            batch_applier: None,
            applied: Arc::new(RwLock::new(None)),
            membership: Arc::new(RwLock::new(StoredMembershipOf::<TypeConfig>::default())),
            current: Arc::new(RwLock::new(None)),
            snapshot_path: None,
            delta_path: None,
            meta_path: None,
            snapshot_base_count: Arc::new(std::sync::atomic::AtomicUsize::new(0)),
            applied_path: None,
            applied_persist_ms: Arc::new(std::sync::atomic::AtomicU64::new(0)),
        }
    }
}

impl MessageSM {
    pub fn new() -> (Self, Messages) {
        let messages = Arc::new(RwLock::new(Vec::new()));
        (
            Self {
                messages: Arc::clone(&messages),
                ..Default::default()
            },
            messages,
        )
    }

    pub fn new_with_applier(applier: Applier) -> (Self, Messages) {
        let messages = Arc::new(RwLock::new(Vec::new()));
        (
            Self {
                messages: Arc::clone(&messages),
                applier: Some(applier),
                ..Default::default()
            },
            messages,
        )
    }

    pub fn new_with_applier_and_snapshot(
        applier: Applier,
        snapshot_path: impl Into<PathBuf>,
    ) -> (Self, Messages) {
        let path = snapshot_path.into();
        let applied_path = path.with_extension("applied");
        let delta_path = path.with_extension("snapshot.delta");
        let meta_path = path.with_extension("snapshot.meta");
        let loaded = std::fs::read(&path)
            .ok()
            .and_then(|bytes| serde_json::from_slice::<SnapshotFile>(&bytes).ok());
        let meta_loaded = std::fs::read(&meta_path)
            .ok()
            .and_then(|bytes| serde_json::from_slice::<SnapshotMetaFile>(&bytes).ok());
        let delta_loaded: Vec<MessageCommand> = if meta_loaded.is_some() {
            std::fs::read(&delta_path)
                .ok()
                .and_then(|bytes| serde_json::from_slice::<Vec<MessageCommand>>(&bytes).ok())
                .unwrap_or_default()
        } else {
            Vec::new()
        };
        let applied_loaded = if loaded.is_some() {
            std::fs::read(&applied_path)
                .ok()
                .and_then(|bytes| {
                    serde_json::from_slice::<Option<LogIdOf<TypeConfig>>>(&bytes).ok()
                })
                .flatten()
        } else {
            None
        };
        let (messages, applied_value, membership, base_count) = match loaded {
            Some(snapshot) => {
                let SnapshotFile {
                    messages: mut base_messages,
                    total_messages: base_count,
                    applied: base_applied,
                    membership: base_membership,
                } = snapshot;
                let is_full_base = base_messages.len() == base_count;
                if is_full_base {
                    base_messages.extend(delta_loaded);
                }
                let meta = meta_loaded.clone();
                (
                    Arc::new(RwLock::new(base_messages)),
                    meta.as_ref()
                        .and_then(|m| m.applied)
                        .or(applied_loaded)
                        .or(base_applied),
                    Arc::new(RwLock::new(
                        meta.map(|m| m.membership).unwrap_or(base_membership),
                    )),
                    base_count,
                )
            }
            None => (
                Arc::new(RwLock::new(Vec::new())),
                applied_loaded,
                Arc::new(RwLock::new(StoredMembershipOf::<TypeConfig>::default())),
                0,
            ),
        };
        let applied = Arc::new(RwLock::new(applied_value));
        (
            Self {
                messages: Arc::clone(&messages),
                applier: Some(applier),
                batch_applier: None,
                applied,
                membership,
                current: Arc::new(RwLock::new(None)),
                snapshot_path: Some(Arc::new(path)),
                delta_path: Some(Arc::new(delta_path)),
                meta_path: Some(Arc::new(meta_path)),
                snapshot_base_count: Arc::new(std::sync::atomic::AtomicUsize::new(base_count)),
                applied_path: Some(Arc::new(applied_path)),
                applied_persist_ms: Arc::new(std::sync::atomic::AtomicU64::new(0)),
            },
            messages,
        )
    }

    pub fn new_with_appliers_and_snapshot(
        applier: Applier,
        batch_applier: BatchApplier,
        snapshot_path: impl Into<PathBuf>,
    ) -> (Self, Messages) {
        let (mut sm, messages) = Self::new_with_applier_and_snapshot(applier, snapshot_path);
        sm.batch_applier = Some(batch_applier);
        (sm, messages)
    }

    fn persist_applied(&self, log_id: Option<LogIdOf<TypeConfig>>, force: bool) {
        let Some(path) = &self.applied_path else {
            return;
        };
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_millis() as u64)
            .unwrap_or(0);
        let last = self
            .applied_persist_ms
            .load(std::sync::atomic::Ordering::Relaxed);
        if !force && log_id.is_some() && now.saturating_sub(last) < 500 {
            return;
        }
        self.applied_persist_ms
            .store(now, std::sync::atomic::Ordering::Relaxed);
        let Ok(bytes) = serde_json::to_vec(&log_id) else {
            return;
        };
        let tmp = path.with_extension("applied.tmp");
        if std::fs::write(&tmp, bytes).is_ok() {
            let _ = std::fs::rename(&tmp, &**path);
        }
    }

    fn persist_snapshot(
        &self,
        messages: &[MessageCommand],
        total_messages: usize,
        applied: Option<LogIdOf<TypeConfig>>,
        membership: &StoredMembershipOf<TypeConfig>,
    ) {
        let Some(path) = &self.snapshot_path else {
            return;
        };
        let file = SnapshotFile {
            messages: messages.to_vec(),
            total_messages,
            applied,
            membership: membership.clone(),
        };
        let Ok(bytes) = serde_json::to_vec(&file) else {
            return;
        };
        let tmp = path.with_extension("tmp");
        if std::fs::write(&tmp, bytes).is_ok() {
            let _ = std::fs::rename(&tmp, &**path);
        }
    }

    fn persist_delta(
        &self,
        delta: &[MessageCommand],
        applied: Option<LogIdOf<TypeConfig>>,
        membership: &StoredMembershipOf<TypeConfig>,
        base_count: usize,
        total: usize,
    ) {
        let (Some(delta_path), Some(meta_path)) = (&self.delta_path, &self.meta_path) else {
            return;
        };
        let Ok(delta_bytes) = serde_json::to_vec(delta) else {
            return;
        };
        let meta = SnapshotMetaFile {
            applied,
            membership: membership.clone(),
            base_count,
            total,
        };
        let Ok(meta_bytes) = serde_json::to_vec(&meta) else {
            return;
        };
        if std::fs::write(&**delta_path, delta_bytes).is_ok() {
            let _ = std::fs::write(&**meta_path, meta_bytes);
        }
    }

    fn clear_delta(&self) {
        if let Some(path) = &self.delta_path {
            let _ = std::fs::remove_file(&**path);
        }
        if let Some(path) = &self.meta_path {
            let _ = std::fs::remove_file(&**path);
        }
    }
}

impl RaftStateMachine<TypeConfig> for MessageSM {
    type SnapshotData = Cursor<Vec<u8>>;
    type SnapshotBuilder = Self;

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
            self.persist_applied(Some(entry.log_id), false);
            let response = match entry.payload {
                EntryPayload::Blank => ClientResponse(None),
                EntryPayload::Normal(ref data) => {
                    if let Ok(cmd) = serde_json::from_str::<RaftCommand>(&data.status) {
                        match cmd {
                            RaftCommand::Batch { messages } => {
                                if let Some(batch_applier) = &self.batch_applier {
                                    batch_applier(messages.clone());
                                } else {
                                    for message in &messages {
                                        if let Some(applier) = &self.applier {
                                            applier(message.clone());
                                        }
                                    }
                                }
                                self.messages.write().await.extend(messages);
                            }
                            RaftCommand::Single(message) => {
                                if let Some(applier) = &self.applier {
                                    applier(message.clone());
                                }
                                self.messages.write().await.push(message);
                            }
                        }
                    }
                    ClientResponse(None)
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
        let restored: Vec<MessageCommand> = serde_json::from_slice(&snapshot.into_inner())
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e.to_string()))?;
        *self.messages.write().await = restored.clone();
        *self.applied.write().await = meta.last_log_id;
        self.persist_applied(meta.last_log_id, true);
        *self.membership.write().await = meta.last_membership.clone();
        self.persist_snapshot(
            &restored,
            restored.len(),
            meta.last_log_id,
            &meta.last_membership,
        );
        self.snapshot_base_count
            .store(restored.len(), std::sync::atomic::Ordering::Relaxed);
        self.clear_delta();
        Ok(())
    }

    async fn get_current_snapshot(
        &mut self,
    ) -> Result<Option<SnapshotOf<TypeConfig, Self::SnapshotData>>, io::Error> {
        Ok(self.current.read().await.clone())
    }
}

impl RaftSnapshotBuilder<TypeConfig> for MessageSM {
    type SnapshotData = Cursor<Vec<u8>>;

    async fn build_snapshot(
        &mut self,
    ) -> Result<SnapshotOf<TypeConfig, Self::SnapshotData>, io::Error> {
        let all = self.messages.read().await;
        let total = all.len();
        let full = std::env::var("MQ_SNAPSHOT_FULL").as_deref() == Ok("1");
        let messages: Vec<_> = if full {
            all.clone()
        } else {
            all.iter()
                .rev()
                .take(SNAPSHOT_TAIL_MESSAGES)
                .rev()
                .cloned()
                .collect()
        };
        let body = serde_json::to_vec(&messages)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e.to_string()))?;
        let meta = SnapshotMetaOf::<TypeConfig> {
            last_log_id: *self.applied.read().await,
            last_membership: self.membership.read().await.clone(),
        };
        let snapshot = SnapshotOf::<TypeConfig, Self::SnapshotData> {
            meta,
            snapshot: Cursor::new(body),
        };
        *self.current.write().await = Some(snapshot.clone());
        let applied = *self.applied.read().await;
        let membership = self.membership.read().await.clone();
        if full {
            let base = self
                .snapshot_base_count
                .load(std::sync::atomic::Ordering::Relaxed);
            let should_rebase = base == 0 || total < base || total - base >= 1000;
            if should_rebase {
                self.persist_snapshot(&all, total, applied, &membership);
                self.snapshot_base_count
                    .store(total, std::sync::atomic::Ordering::Relaxed);
                self.clear_delta();
            } else if total > base {
                let delta = all[base..].to_vec();
                self.persist_delta(&delta, applied, &membership, base, total);
            }
        } else {
            self.persist_snapshot(&messages, total, applied, &membership);
            self.snapshot_base_count
                .store(total, std::sync::atomic::Ordering::Relaxed);
            self.clear_delta();
        }
        Ok(snapshot)
    }
}

#[derive(Serialize, Deserialize)]
enum RpcRequest {
    Append(AppendEntriesRequest<TypeConfig>),
    Vote(VoteRequest<TypeConfig>),
    Forward(ClientRequest),
}

#[derive(Serialize, Deserialize)]
enum RpcResponse {
    Append(AppendEntriesResponse<TypeConfig>),
    Vote(VoteResponse<TypeConfig>),
    Forward(Result<(), String>),
}

const RPC_MAGIC: &[u8; 4] = b"MQB1";

fn encode_rpc<T: Serialize>(value: &T) -> Result<Vec<u8>, String> {
    let body = bincode::serialize(value).map_err(|e| e.to_string())?;
    let mut out = Vec::with_capacity(body.len() + 4);
    out.extend_from_slice(RPC_MAGIC);
    out.extend_from_slice(&body);
    Ok(out)
}

fn decode_rpc<T: serde::de::DeserializeOwned>(bytes: &[u8]) -> Result<T, String> {
    if bytes.starts_with(RPC_MAGIC) {
        bincode::deserialize(&bytes[4..]).map_err(|e| e.to_string())
    } else {
        serde_json::from_slice(bytes).map_err(|e| e.to_string())
    }
}

#[derive(Clone)]
struct NetworkFactory {
    peers: Arc<std::sync::RwLock<HashMap<u64, String>>>,
}

struct NetworkClient {
    addr: String,
    conn: tokio::sync::Mutex<Option<TcpStream>>,
}

impl RaftNetworkFactory<TypeConfig> for NetworkFactory {
    type Network = NetworkClient;

    async fn new_client(&mut self, target: u64, _node: &()) -> Self::Network {
        NetworkClient {
            addr: self
                .peers
                .read()
                .unwrap()
                .get(&target)
                .cloned()
                .unwrap_or_default(),
            conn: tokio::sync::Mutex::new(None),
        }
    }
}

impl RaftNetworkV2<TypeConfig> for NetworkClient {
    type SnapshotData = Cursor<Vec<u8>>;

    async fn append_entries(
        &mut self,
        rpc: AppendEntriesRequest<TypeConfig>,
        _option: RPCOption,
    ) -> Result<AppendResult<TypeConfig>, RPCError<TypeConfig>> {
        let prev = rpc.prev_log_id;
        let last = rpc.entries.last().map(|e| e.log_id()).or(rpc.prev_log_id);
        let resp = self
            .call(&RpcRequest::Append(rpc))
            .await
            .map_err(unreachable)?;
        match resp {
            RpcResponse::Append(resp) => Ok(resp.into_append_result(prev, last)),
            _ => Err(unreachable("unexpected append response")),
        }
    }

    async fn vote(
        &mut self,
        rpc: VoteRequest<TypeConfig>,
        _option: RPCOption,
    ) -> Result<VoteResponse<TypeConfig>, RPCError<TypeConfig>> {
        let resp = self
            .call(&RpcRequest::Vote(rpc))
            .await
            .map_err(unreachable)?;
        match resp {
            RpcResponse::Vote(resp) => Ok(resp),
            _ => Err(unreachable("unexpected vote response")),
        }
    }

    async fn full_snapshot(
        &mut self,
        _vote: VoteOf<TypeConfig>,
        _snapshot: SnapshotOf<TypeConfig, Self::SnapshotData>,
        _cancel: impl std::future::Future<Output = openraft::errors::ReplicationClosed>
            + OptionalSend
            + 'static,
        _option: RPCOption,
    ) -> Result<SnapshotResponse<TypeConfig>, openraft::errors::StreamingError<TypeConfig>> {
        Err(openraft::errors::StreamingError::Unreachable(
            openraft::errors::Unreachable::from_string("snapshot transfer not enabled"),
        ))
    }
}

impl NetworkClient {
    async fn call(&self, req: &RpcRequest) -> Result<RpcResponse, String> {
        let payload = encode_rpc(req)?;
        let mut guard = self.conn.lock().await;
        if guard.is_none() {
            let stream = TcpStream::connect(&self.addr)
                .await
                .map_err(|e| format!("connect {}: {e}", self.addr))?;
            *guard = Some(stream);
        }
        let first = exchange(guard.as_mut().unwrap(), &payload).await;
        if first.is_ok() {
            return first;
        }
        *guard = None;
        let mut stream = TcpStream::connect(&self.addr)
            .await
            .map_err(|e| format!("connect {}: {e}", self.addr))?;
        let result = exchange(&mut stream, &payload).await;
        *guard = Some(stream);
        result
    }
}

async fn exchange(stream: &mut TcpStream, payload: &[u8]) -> Result<RpcResponse, String> {
    stream
        .write_all(&(payload.len() as u32).to_be_bytes())
        .await
        .map_err(|e| e.to_string())?;
    stream.write_all(payload).await.map_err(|e| e.to_string())?;
    let mut len = [0u8; 4];
    stream
        .read_exact(&mut len)
        .await
        .map_err(|e| e.to_string())?;
    let mut buf = vec![0u8; u32::from_be_bytes(len) as usize];
    stream
        .read_exact(&mut buf)
        .await
        .map_err(|e| e.to_string())?;
    decode_rpc(&buf).map_err(|e| e.to_string())
}

fn unreachable(msg: impl ToString) -> RPCError<TypeConfig> {
    RPCError::Unreachable(openraft::errors::Unreachable::from_string(msg.to_string()))
}

async fn serve(listener: TcpListener, raft: Raft) {
    loop {
        let Ok((stream, _)) = listener.accept().await else {
            return;
        };
        let raft = Arc::clone(&raft);
        tokio::spawn(async move {
            let _ = handle(stream, raft).await;
        });
    }
}

async fn handle(mut stream: TcpStream, raft: Raft) -> std::io::Result<()> {
    loop {
        let mut len = [0u8; 4];
        match stream.read_exact(&mut len).await {
            Ok(_) => {}
            Err(err) if err.kind() == std::io::ErrorKind::UnexpectedEof => return Ok(()),
            Err(err) => return Err(err),
        }
        let mut buf = vec![0u8; u32::from_be_bytes(len) as usize];
        stream.read_exact(&mut buf).await?;
        let req: RpcRequest = decode_rpc(&buf)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
        let resp = match req {
            RpcRequest::Append(req) => match raft.append_entries(req).await {
                Ok(result) => RpcResponse::Append(AppendEntriesResponse::from(result)),
                Err(e) => return Err(std::io::Error::other(e.to_string())),
            },
            RpcRequest::Vote(req) => match raft.vote(req).await {
                Ok(v) => RpcResponse::Vote(v),
                Err(e) => return Err(std::io::Error::other(e.to_string())),
            },
            RpcRequest::Forward(req) => RpcResponse::Forward(
                raft.client_write(req)
                    .await
                    .map(|_| ())
                    .map_err(|e| e.to_string()),
            ),
        };
        let payload = encode_rpc(&resp)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
        stream
            .write_all(&(payload.len() as u32).to_be_bytes())
            .await?;
        stream.write_all(&payload).await?;
    }
}

pub struct MessageRaft {
    node_id: u64,
    raft: Raft,
    messages: Messages,
    peers: Arc<std::sync::RwLock<HashMap<u64, String>>>,
}

impl MessageRaft {
    pub async fn start(
        node_id: u64,
        listen: &str,
        peers: Vec<(u64, String)>,
    ) -> Result<Self, Box<dyn std::error::Error>> {
        let (sm, messages) = MessageSM::new();
        Self::start_with_sm(node_id, listen, peers, sm, messages, true, None).await
    }

    pub async fn start_with_applier(
        node_id: u64,
        listen: &str,
        peers: Vec<(u64, String)>,
        applier: Applier,
        initialize: bool,
    ) -> Result<Self, Box<dyn std::error::Error>> {
        let (sm, messages) = MessageSM::new_with_applier(applier);
        Self::start_with_sm(node_id, listen, peers, sm, messages, initialize, None).await
    }

    pub async fn start_persistent(
        node_id: u64,
        listen: &str,
        peers: Vec<(u64, String)>,
        applier: Applier,
        initialize: bool,
        log_path: impl AsRef<std::path::Path>,
    ) -> Result<Self, Box<dyn std::error::Error>> {
        let (sm, messages) = MessageSM::new_with_applier(applier);
        Self::start_with_sm(
            node_id,
            listen,
            peers,
            sm,
            messages,
            initialize,
            Some(log_path.as_ref().to_path_buf()),
        )
        .await
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn start_with_appliers_snapshot(
        node_id: u64,
        listen: &str,
        peers: Vec<(u64, String)>,
        applier: Applier,
        batch_applier: BatchApplier,
        initialize: bool,
        log_path: impl AsRef<std::path::Path>,
        snapshot_path: impl Into<PathBuf>,
    ) -> Result<Self, Box<dyn std::error::Error>> {
        let (sm, messages) =
            MessageSM::new_with_appliers_and_snapshot(applier, batch_applier, snapshot_path);
        Self::start_with_sm(
            node_id,
            listen,
            peers,
            sm,
            messages,
            initialize,
            Some(log_path.as_ref().to_path_buf()),
        )
        .await
    }

    pub async fn start_with_applier_snapshot(
        node_id: u64,
        listen: &str,
        peers: Vec<(u64, String)>,
        applier: Applier,
        initialize: bool,
        log_path: impl AsRef<std::path::Path>,
        snapshot_path: impl Into<PathBuf>,
    ) -> Result<Self, Box<dyn std::error::Error>> {
        let (sm, messages) = MessageSM::new_with_applier_and_snapshot(applier, snapshot_path);
        Self::start_with_sm(
            node_id,
            listen,
            peers,
            sm,
            messages,
            initialize,
            Some(log_path.as_ref().to_path_buf()),
        )
        .await
    }

    async fn start_with_sm(
        node_id: u64,
        listen: &str,
        peers: Vec<(u64, String)>,
        sm: MessageSM,
        messages: Messages,
        initialize: bool,
        log_path: Option<std::path::PathBuf>,
    ) -> Result<Self, Box<dyn std::error::Error>> {
        let listener = TcpListener::bind(listen).await?;
        let peer_map = Arc::new(std::sync::RwLock::new(
            peers.iter().cloned().collect::<HashMap<u64, String>>(),
        ));
        let config = Arc::new(
            openraft::Config {
                cluster_name: "mq-rust-raft".into(),
                heartbeat_interval: 200,
                election_timeout_min: 600,
                election_timeout_max: 1200,
                snapshot_policy: openraft::SnapshotPolicy::LogsSinceLast(50000),
                max_in_snapshot_log_to_keep: 1000,
                purge_batch_size: 16,
                ..Default::default()
            }
            .validate()?,
        );
        let raft = if let Some(path) = log_path {
            let log_store = DiskLogStore::open(path).await?;
            Arc::new(
                openraft::Raft::new(
                    node_id,
                    config,
                    NetworkFactory {
                        peers: Arc::clone(&peer_map),
                    },
                    log_store,
                    sm,
                )
                .await?,
            )
        } else {
            let log_store = Arc::new(MemLogStore::new(Default::default()));
            Arc::new(
                openraft::Raft::new(
                    node_id,
                    config,
                    NetworkFactory {
                        peers: Arc::clone(&peer_map),
                    },
                    log_store,
                    sm,
                )
                .await?,
            )
        };
        tokio::spawn(serve(listener, Arc::clone(&raft)));
        let mut members = std::collections::BTreeMap::new();
        for (id, _) in &peers {
            members.insert(*id, ());
        }
        if initialize {
            let _ = raft.initialize(members).await;
        }
        Ok(Self {
            node_id,
            raft,
            messages,
            peers: peer_map,
        })
    }

    pub fn is_leader(&self) -> bool {
        self.raft.is_leader()
    }

    pub async fn snapshot_now(&self) -> Result<(), String> {
        self.raft
            .trigger()
            .snapshot()
            .await
            .map_err(|e| e.to_string())
    }

    pub async fn add_node(&self, node_id: u64, addr: String) -> Result<(), String> {
        self.peers.write().unwrap().insert(node_id, addr);
        self.raft
            .add_learner(node_id, (), true)
            .await
            .map_err(|e| e.to_string())?;
        let mut voters: std::collections::BTreeSet<u64> =
            self.peers.read().unwrap().keys().copied().collect();
        voters.insert(node_id);
        self.raft
            .change_membership(voters, false)
            .await
            .map_err(|e| e.to_string())?;
        Ok(())
    }

    pub async fn wait_leader(&self, timeout: Duration) -> Option<u64> {
        let deadline = tokio::time::Instant::now() + timeout;
        while tokio::time::Instant::now() < deadline {
            if self.raft.is_leader() {
                return Some(self.node_id);
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        None
    }

    pub async fn write(&self, cmd: MessageCommand) -> Result<(), String> {
        let req = ClientRequest {
            client: cmd.msg_id.clone(),
            serial: 1,
            status: serde_json::to_string(&RaftCommand::Single(cmd)).map_err(|e| e.to_string())?,
        };
        self.client_write_or_forward(req).await
    }

    pub async fn write_batch(&self, messages: Vec<MessageCommand>) -> Result<(), String> {
        let client = messages
            .first()
            .map(|m| m.msg_id.clone())
            .unwrap_or_else(|| "batch".into());
        let req = ClientRequest {
            client,
            serial: 1,
            status: serde_json::to_string(&RaftCommand::Batch { messages })
                .map_err(|e| e.to_string())?,
        };
        self.client_write_or_forward(req).await
    }

    async fn client_write_or_forward(&self, req: ClientRequest) -> Result<(), String> {
        let result = self.raft.client_write(req.clone()).await;
        match result {
            Ok(_) => Ok(()),
            Err(err) => {
                if let openraft::error::RaftError::APIError(
                    openraft::error::ClientWriteError::ForwardToLeader(forward),
                ) = &err
                {
                    if let Some(leader) = forward.leader_id {
                        return self.forward_write(leader, req).await;
                    }
                }
                Err(err.to_string())
            }
        }
    }

    async fn forward_write(&self, leader: u64, req: ClientRequest) -> Result<(), String> {
        let addr = self
            .peers
            .read()
            .unwrap()
            .get(&leader)
            .cloned()
            .ok_or_else(|| format!("missing leader addr {leader}"))?;
        let client = NetworkClient {
            addr,
            conn: tokio::sync::Mutex::new(None),
        };
        match client.call(&RpcRequest::Forward(req)).await? {
            RpcResponse::Forward(result) => result,
            _ => Err("unexpected forward response".into()),
        }
    }

    pub async fn messages(&self) -> Vec<MessageCommand> {
        self.messages.read().await.clone()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    static SNAPSHOT_ENV_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());

    async fn free_port() -> u16 {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        listener.local_addr().unwrap().port()
    }

    #[allow(clippy::await_holding_lock)]
    #[tokio::test]
    async fn snapshot_full_mode_keeps_all_messages() {
        let _env_lock = SNAPSHOT_ENV_LOCK.lock().unwrap();
        let path =
            std::env::temp_dir().join(format!("mq-raft-snapshot-full-{}.json", std::process::id()));
        let _ = std::fs::remove_file(&path);
        std::env::set_var("MQ_SNAPSHOT_FULL", "1");
        let applier: Applier = Arc::new(|_| {});
        let (mut sm, messages) = MessageSM::new_with_applier_and_snapshot(applier.clone(), &path);
        {
            let mut guard = messages.write().await;
            for index in 0..1500 {
                guard.push(MessageCommand {
                    topic: "snap".into(),
                    queue_id: 0,
                    msg_id: format!("f{index}"),
                    body: b"x".to_vec(),
                    properties: String::new(),
                    deliver_at_ms: None,
                    tx_id: None,
                    tx_kind: None,
                });
            }
        }
        sm.build_snapshot().await.unwrap();
        drop(sm);
        let (_sm2, messages2) = MessageSM::new_with_applier_and_snapshot(applier, &path);
        assert_eq!(messages2.read().await.len(), 1500);
        std::env::remove_var("MQ_SNAPSHOT_FULL");
        let _ = std::fs::remove_file(&path);
    }

    #[allow(clippy::await_holding_lock)]
    #[tokio::test]
    async fn snapshot_full_delta_merges_on_reopen() {
        let _env_lock = SNAPSHOT_ENV_LOCK.lock().unwrap();
        std::env::set_var("MQ_SNAPSHOT_FULL", "1");
        let path = std::env::temp_dir().join(format!(
            "mq-raft-snapshot-delta-{}.json",
            std::process::id()
        ));
        for suffix in ["", ".snapshot.delta", ".snapshot.meta"] {
            let _ = std::fs::remove_file(format!("{}{}", path.display(), suffix));
        }
        let applier: Applier = Arc::new(|_| {});
        let (mut sm, messages) = MessageSM::new_with_applier_and_snapshot(applier.clone(), &path);
        {
            let mut guard = messages.write().await;
            for index in 0..1500 {
                guard.push(MessageCommand {
                    topic: "snapd".into(),
                    queue_id: 0,
                    msg_id: format!("b{index}"),
                    body: b"x".to_vec(),
                    properties: String::new(),
                    deliver_at_ms: None,
                    tx_id: None,
                    tx_kind: None,
                });
            }
        }
        sm.build_snapshot().await.unwrap();
        {
            let mut guard = messages.write().await;
            for index in 0..50 {
                guard.push(MessageCommand {
                    topic: "snapd".into(),
                    queue_id: 0,
                    msg_id: format!("d{index}"),
                    body: b"y".to_vec(),
                    properties: String::new(),
                    deliver_at_ms: None,
                    tx_id: None,
                    tx_kind: None,
                });
            }
        }
        sm.build_snapshot().await.unwrap();
        assert!(path.with_extension("snapshot.delta").exists());
        drop(sm);
        let (_sm2, messages2) = MessageSM::new_with_applier_and_snapshot(applier, &path);
        assert_eq!(messages2.read().await.len(), 1550);
        std::env::remove_var("MQ_SNAPSHOT_FULL");
        for suffix in ["", ".snapshot.delta", ".snapshot.meta"] {
            let _ = std::fs::remove_file(format!("{}{}", path.display(), suffix));
        }
    }

    #[allow(clippy::await_holding_lock)]
    #[tokio::test]
    async fn snapshot_tail_is_bounded() {
        let _env_lock = SNAPSHOT_ENV_LOCK.lock().unwrap();
        let path =
            std::env::temp_dir().join(format!("mq-raft-snapshot-tail-{}.json", std::process::id()));
        let _ = std::fs::remove_file(&path);
        let applier: Applier = Arc::new(|_| {});
        let (mut sm, messages) = MessageSM::new_with_applier_and_snapshot(applier.clone(), &path);
        {
            let mut guard = messages.write().await;
            for index in 0..1500 {
                guard.push(MessageCommand {
                    topic: "snap".into(),
                    queue_id: 0,
                    msg_id: format!("s{index}"),
                    body: b"x".to_vec(),
                    properties: String::new(),
                    deliver_at_ms: None,
                    tx_id: None,
                    tx_kind: None,
                });
            }
        }
        sm.build_snapshot().await.unwrap();
        drop(sm);
        let (_sm2, messages2) = MessageSM::new_with_applier_and_snapshot(applier, &path);
        let restored = messages2.read().await;
        assert_eq!(restored.len(), SNAPSHOT_TAIL_MESSAGES);
        assert_eq!(restored.last().unwrap().msg_id, "s1499");
        let _ = std::fs::remove_file(&path);
    }

    #[tokio::test]
    async fn snapshot_survives_reopen() {
        let path =
            std::env::temp_dir().join(format!("mq-raft-snapshot-{}.json", std::process::id()));
        let _ = std::fs::remove_file(&path);
        let applier: Applier = Arc::new(|_| {});
        let (mut sm, messages) = MessageSM::new_with_applier_and_snapshot(applier.clone(), &path);
        let cmd = MessageCommand {
            topic: "snap".into(),
            queue_id: 0,
            msg_id: "s1".into(),
            body: b"x".to_vec(),
            properties: String::new(),
            deliver_at_ms: None,
            tx_id: None,
            tx_kind: None,
        };
        messages.write().await.push(cmd.clone());
        sm.build_snapshot().await.unwrap();
        drop(sm);
        let (_sm2, messages2) = MessageSM::new_with_applier_and_snapshot(applier, &path);
        assert_eq!(*messages2.read().await, vec![cmd]);
        let _ = std::fs::remove_file(&path);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn three_nodes_replicate_message_log() {
        let mut ports = Vec::new();
        for _ in 0..3 {
            ports.push(free_port().await);
        }
        let peers: Vec<(u64, String)> = ports
            .iter()
            .enumerate()
            .map(|(i, p)| ((i + 1) as u64, format!("127.0.0.1:{p}")))
            .collect();
        let mut nodes = Vec::new();
        for (i, port) in ports.iter().enumerate() {
            nodes.push(
                MessageRaft::start((i + 1) as u64, &format!("127.0.0.1:{port}"), peers.clone())
                    .await
                    .unwrap(),
            );
        }
        let mut leader = None;
        for _ in 0..100 {
            for node in &nodes {
                if let Some(id) = node.wait_leader(Duration::from_millis(50)).await {
                    leader = Some(id);
                    break;
                }
            }
            if leader.is_some() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        let _leader = leader.expect("leader elected");
        let cmd = MessageCommand {
            topic: "raft-test".into(),
            queue_id: 0,
            msg_id: "m1".into(),
            body: b"hello".to_vec(),
            properties: String::new(),
            deliver_at_ms: None,
            tx_id: None,
            tx_kind: None,
        };
        let mut wrote = false;
        for _ in 0..100 {
            let current = nodes.iter().position(|node| node.is_leader());
            if let Some(index) = current {
                if nodes[index].write(cmd.clone()).await.is_ok() {
                    wrote = true;
                    break;
                }
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        assert!(wrote, "leader accepted write");
        tokio::time::sleep(Duration::from_millis(400)).await;
        for node in &nodes {
            assert_eq!(node.messages().await, vec![cmd.clone()]);
        }
    }
}
