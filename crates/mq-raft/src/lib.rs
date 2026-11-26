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
        let loaded = std::fs::read(&path)
            .ok()
            .and_then(|bytes| serde_json::from_slice::<SnapshotFile>(&bytes).ok());
        let (messages, applied, membership) = match loaded {
            Some(snapshot) => (
                Arc::new(RwLock::new(snapshot.messages)),
                Arc::new(RwLock::new(snapshot.applied)),
                Arc::new(RwLock::new(snapshot.membership)),
            ),
            None => (
                Arc::new(RwLock::new(Vec::new())),
                Arc::new(RwLock::new(None)),
                Arc::new(RwLock::new(StoredMembershipOf::<TypeConfig>::default())),
            ),
        };
        (
            Self {
                messages: Arc::clone(&messages),
                applier: Some(applier),
                batch_applier: None,
                applied,
                membership,
                current: Arc::new(RwLock::new(None)),
                snapshot_path: Some(Arc::new(path)),
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
        *self.membership.write().await = meta.last_membership.clone();
        self.persist_snapshot(
            &restored,
            restored.len(),
            meta.last_log_id,
            &meta.last_membership,
        );
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
        let messages: Vec<_> = all
            .iter()
            .rev()
            .take(SNAPSHOT_TAIL_MESSAGES)
            .rev()
            .cloned()
            .collect();
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
        self.persist_snapshot(&messages, total, applied, &membership);
        Ok(snapshot)
    }
}

#[derive(Serialize, Deserialize)]
enum RpcRequest {
    Append(AppendEntriesRequest<TypeConfig>),
    Vote(VoteRequest<TypeConfig>),
}

#[derive(Serialize, Deserialize)]
enum RpcResponse {
    Append(AppendEntriesResponse<TypeConfig>),
    Vote(VoteResponse<TypeConfig>),
}

#[derive(Clone)]
struct NetworkFactory {
    peers: HashMap<u64, String>,
}

struct NetworkClient {
    addr: String,
}

impl RaftNetworkFactory<TypeConfig> for NetworkFactory {
    type Network = NetworkClient;

    async fn new_client(&mut self, target: u64, _node: &()) -> Self::Network {
        NetworkClient {
            addr: self.peers.get(&target).cloned().unwrap_or_default(),
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
        let mut stream = TcpStream::connect(&self.addr)
            .await
            .map_err(|e| format!("connect {}: {e}", self.addr))?;
        let payload = serde_json::to_vec(req).map_err(|e| e.to_string())?;
        stream
            .write_all(&(payload.len() as u32).to_be_bytes())
            .await
            .map_err(|e| e.to_string())?;
        stream
            .write_all(&payload)
            .await
            .map_err(|e| e.to_string())?;
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
        serde_json::from_slice(&buf).map_err(|e| e.to_string())
    }
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
    let mut len = [0u8; 4];
    stream.read_exact(&mut len).await?;
    let mut buf = vec![0u8; u32::from_be_bytes(len) as usize];
    stream.read_exact(&mut buf).await?;
    let req: RpcRequest = serde_json::from_slice(&buf)
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
    };
    let payload = serde_json::to_vec(&resp)
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
    stream
        .write_all(&(payload.len() as u32).to_be_bytes())
        .await?;
    stream.write_all(&payload).await?;
    Ok(())
}

pub struct MessageRaft {
    node_id: u64,
    raft: Raft,
    messages: Messages,
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
        let peer_map: HashMap<u64, String> = peers.iter().cloned().collect();
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
                    NetworkFactory { peers: peer_map },
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
                    NetworkFactory { peers: peer_map },
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
        })
    }

    pub fn is_leader(&self) -> bool {
        self.raft.is_leader()
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
        self.raft
            .client_write(req)
            .await
            .map_err(|e| e.to_string())?;
        Ok(())
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
        self.raft
            .client_write(req)
            .await
            .map_err(|e| e.to_string())?;
        Ok(())
    }

    pub async fn messages(&self) -> Vec<MessageCommand> {
        self.messages.read().await.clone()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    async fn free_port() -> u16 {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        listener.local_addr().unwrap().port()
    }

    #[tokio::test]
    async fn snapshot_tail_is_bounded() {
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
        let leader = leader.expect("leader elected");
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
        nodes[(leader - 1) as usize]
            .write(cmd.clone())
            .await
            .unwrap();
        tokio::time::sleep(Duration::from_millis(400)).await;
        for node in &nodes {
            assert_eq!(node.messages().await, vec![cmd.clone()]);
        }
    }
}
