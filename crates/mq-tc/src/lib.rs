//! MQ-rust Transaction Coordinator (TC) 锟?Raft-backed state machine skeleton.
//!
//! This first version runs a **single Raft node** using openraft's in-memory
//! store. The API is intentionally small: prepare / commit / rollback / status.
//! The state machine value is a JSON status string inside `ClientRequest.status`.
//!
//! Multi-node networking and persistent log storage are the next step.

pub mod log;
pub mod state;
pub mod tcp;

use openraft::errors::RPCError;
use openraft::network::RPCOption;
use openraft::raft::AppendEntriesRequest;
use openraft::raft::AppendResult;
use openraft::raft::SnapshotResponse;
use openraft::raft::VoteRequest;
use openraft::raft::VoteResponse;
use openraft::Config;
use openraft::OptionalSend;
use openraft::RaftNetworkFactory;
use openraft::RaftNetworkV2;
use openraft_memstore::BlockConfig;
use openraft_memstore::ClientRequest;
use openraft_memstore::MemLogStore;
use openraft_memstore::TypeConfig;
use std::collections::BTreeMap;
use std::collections::HashMap;
use std::fs::File;
use std::fs::OpenOptions;
use std::io::BufRead;
use std::io::BufReader;
use std::io::Cursor;
use std::io::Write;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::OnceLock;
use tokio::sync::RwLock;

fn peer_registry() -> Arc<RwLock<HashMap<NodeId, TcRaft>>> {
    static PEERS: OnceLock<Arc<RwLock<HashMap<NodeId, TcRaft>>>> = OnceLock::new();
    Arc::clone(PEERS.get_or_init(|| Arc::new(RwLock::new(HashMap::new()))))
}

pub type NodeId = u64;
pub type TcRaft = Arc<openraft::Raft<TypeConfig, state::StatusSM>>;

#[derive(Clone)]
struct NetworkFactory {
    peers: Arc<RwLock<HashMap<NodeId, TcRaft>>>,
}

struct NetworkClient {
    target: NodeId,
    peers: Arc<RwLock<HashMap<NodeId, TcRaft>>>,
}

impl RaftNetworkFactory<TypeConfig> for NetworkFactory {
    type Network = NetworkClient;

    async fn new_client(&mut self, target: NodeId, _node: &()) -> Self::Network {
        NetworkClient {
            target,
            peers: Arc::clone(&self.peers),
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
        let peer = self
            .peers
            .read()
            .await
            .get(&self.target)
            .cloned()
            .ok_or_else(|| {
                RPCError::Unreachable(openraft::errors::Unreachable::from_string(format!(
                    "tc node {} not started",
                    self.target
                )))
            })?;
        peer.append_entries(rpc).await.map_err(|e| {
            RPCError::Unreachable(openraft::errors::Unreachable::from_string(e.to_string()))
        })
    }

    async fn vote(
        &mut self,
        rpc: VoteRequest<TypeConfig>,
        _option: RPCOption,
    ) -> Result<VoteResponse<TypeConfig>, RPCError<TypeConfig>> {
        let peer = self
            .peers
            .read()
            .await
            .get(&self.target)
            .cloned()
            .ok_or_else(|| {
                RPCError::Unreachable(openraft::errors::Unreachable::from_string(format!(
                    "tc node {} not started",
                    self.target
                )))
            })?;
        peer.vote(rpc).await.map_err(|e| {
            RPCError::Unreachable(openraft::errors::Unreachable::from_string(e.to_string()))
        })
    }

    async fn full_snapshot(
        &mut self,
        _vote: openraft::type_config::alias::VoteOf<TypeConfig>,
        _snapshot: openraft::type_config::alias::SnapshotOf<TypeConfig, Self::SnapshotData>,
        _cancel: impl std::future::Future<Output = openraft::errors::ReplicationClosed>
            + OptionalSend
            + 'static,
        _option: RPCOption,
    ) -> Result<SnapshotResponse<TypeConfig>, openraft::errors::StreamingError<TypeConfig>> {
        unimplemented!("TC snapshot transfer is not enabled yet")
    }
}

/// Transaction coordinator backed by a single openraft node.
pub struct Tc {
    node_id: NodeId,
    raft: TcRaft,
    status: Arc<RwLock<HashMap<String, String>>>,
    wal: Option<Arc<Mutex<File>>>,
}

impl Tc {
    /// Start a single-node Raft group with node id 1.
    pub async fn start() -> Result<Self, Box<dyn std::error::Error>> {
        Self::start_cluster(1, vec![1]).await
    }

    /// Start a single-node TC with a status WAL that survives process restart.
    pub async fn start_with_wal(
        path: impl AsRef<std::path::Path>,
    ) -> Result<Self, Box<dyn std::error::Error>> {
        Self::start_cluster_wal(1, vec![1], Some(path.as_ref())).await
    }

    /// Start one node of a Raft group. `members` must list all node ids.
    ///
    /// Nodes run in the same process and talk through an in-process registry;
    /// this is the test harness for multi-node election/replication. A TCP
    /// transport can replace `NetworkFactory` without changing `Tc`.
    pub async fn start_cluster(
        node_id: NodeId,
        members: Vec<NodeId>,
    ) -> Result<Self, Box<dyn std::error::Error>> {
        Self::start_cluster_wal(node_id, members, None).await
    }

    /// Start a TC node with TCP Raft transport.
    ///
    /// `members` maps node id -> `host:port` for every node, including self.
    pub async fn start_tcp(
        node_id: NodeId,
        listen: &str,
        members: Vec<(NodeId, String)>,
    ) -> Result<Self, Box<dyn std::error::Error>> {
        Self::start_tcp_wal(node_id, listen, members, None).await
    }

    pub async fn start_tcp_wal(
        node_id: NodeId,
        listen: &str,
        members: Vec<(NodeId, String)>,
        wal_path: Option<&std::path::Path>,
    ) -> Result<Self, Box<dyn std::error::Error>> {
        let listener = tokio::net::TcpListener::bind(listen).await?;
        let peers: HashMap<NodeId, String> = members.iter().cloned().collect();
        let config = Arc::new(
            Config {
                cluster_name: "mq-rust-tc".to_string(),
                max_in_snapshot_log_to_keep: 2,
                heartbeat_interval: 500,
                election_timeout_min: 1500,
                election_timeout_max: 3000,
                ..Default::default()
            }
            .validate()?,
        );
        let log_store = Arc::new(MemLogStore::new(BlockConfig::default()));
        let (sm, status_map) = match wal_path {
            Some(path) => state::StatusSM::new_with_wal(path)?,
            None => state::StatusSM::new(),
        };
        let state_machine = sm;
        let raft = Arc::new(
            openraft::Raft::new(
                node_id,
                config,
                tcp::NetworkFactory::new(peers),
                Arc::clone(&log_store),
                state_machine,
            )
            .await?,
        );
        tokio::spawn(tcp::serve(listener, Arc::clone(&raft)));

        let mut member_map = BTreeMap::new();
        for (id, _) in members {
            member_map.insert(id, ());
        }
        let _ = raft.initialize(member_map).await;

        let tc = Self {
            node_id,
            raft,
            status: status_map,
            wal: None,
        };
        tc.wait_for_leader().await;
        Ok(tc)
    }

    /// Start a TCP node with persistent Raft log + status WAL.
    pub async fn start_tcp_persistent(
        node_id: NodeId,
        listen: &str,
        members: Vec<(NodeId, String)>,
        data_dir: &std::path::Path,
    ) -> Result<Self, Box<dyn std::error::Error>> {
        std::fs::create_dir_all(data_dir)?;
        let listener = tokio::net::TcpListener::bind(listen).await?;
        let peers: HashMap<NodeId, String> = members.iter().cloned().collect();
        let config = Arc::new(
            Config {
                cluster_name: "mq-rust-tc".to_string(),
                max_in_snapshot_log_to_keep: 2,
                heartbeat_interval: 500,
                election_timeout_min: 1500,
                election_timeout_max: 3000,
                ..Default::default()
            }
            .validate()?,
        );
        let log_store =
            log::PersistentLogStore::open(data_dir.join(format!("node-{node_id}.raft.json")))?;
        let (sm, status_map) = state::StatusSM::new_with_wal_and_snapshot(
            &data_dir.join(format!("node-{node_id}.status.wal")),
            Some(&data_dir.join(format!("node-{node_id}.snapshot.json"))),
        )?;
        let state_machine = sm;
        let raft = Arc::new(
            openraft::Raft::new(
                node_id,
                config,
                tcp::NetworkFactory::new(peers),
                log_store,
                state_machine,
            )
            .await?,
        );
        tokio::spawn(tcp::serve(listener, Arc::clone(&raft)));

        let mut member_map = BTreeMap::new();
        for (id, _) in members {
            member_map.insert(id, ());
        }
        let _ = raft.initialize(member_map).await;

        let tc = Self {
            node_id,
            raft,
            status: status_map,
            wal: None,
        };
        tc.wait_for_leader().await;
        Ok(tc)
    }

    pub async fn start_cluster_wal(
        node_id: NodeId,
        members: Vec<NodeId>,
        wal_path: Option<&std::path::Path>,
    ) -> Result<Self, Box<dyn std::error::Error>> {
        let mut restored: HashMap<String, String> = HashMap::new();
        let wal = if let Some(path) = wal_path {
            if let Some(parent) = path.parent() {
                std::fs::create_dir_all(parent)?;
            }
            let file = OpenOptions::new()
                .create(true)
                .read(true)
                .append(true)
                .open(path)?;
            let reader = BufReader::new(file.try_clone()?);
            for line in reader.lines().map_while(Result::ok) {
                if let Some((tx, status)) = line.split_once('\t') {
                    restored.insert(tx.to_string(), status.to_string());
                }
            }
            Some(Arc::new(Mutex::new(file)))
        } else {
            None
        };

        let peers = peer_registry();
        let config = Arc::new(
            Config {
                cluster_name: "mq-rust-tc".to_string(),
                max_in_snapshot_log_to_keep: 2,
                heartbeat_interval: 500,
                election_timeout_min: 1500,
                election_timeout_max: 3000,
                ..Default::default()
            }
            .validate()?,
        );
        let log_store = Arc::new(MemLogStore::new(BlockConfig::default()));
        let (sm, status_map) = state::StatusSM::new();
        *status_map.write().await = restored;
        let state_machine = sm;
        let raft = Arc::new(
            openraft::Raft::new(
                node_id,
                config,
                NetworkFactory {
                    peers: Arc::clone(&peers),
                },
                Arc::clone(&log_store),
                state_machine,
            )
            .await?,
        );
        peers.write().await.insert(node_id, Arc::clone(&raft));

        let mut member_map = BTreeMap::new();
        for id in members {
            member_map.insert(id, ());
        }
        // Ignore "already initialized" when several nodes call this.
        let _ = raft.initialize(member_map).await;

        let tc = Self {
            node_id,
            raft,
            status: status_map,
            wal,
        };
        tc.wait_for_leader().await;
        Ok(tc)
    }

    async fn wait_for_leader(&self) {
        for _ in 0..50 {
            if self.raft.current_leader().await.is_some() {
                return;
            }
            tokio::time::sleep(std::time::Duration::from_millis(200)).await;
        }
    }

    async fn write_status(&self, tx_id: &str, status: &str) -> Result<(), String> {
        let req = ClientRequest {
            client: tx_id.to_string(),
            serial: 1,
            status: status.to_string(),
        };
        self.raft
            .client_write(req)
            .await
            .map_err(|e| e.to_string())?;
        self.status
            .write()
            .await
            .insert(tx_id.to_string(), status.to_string());
        if let Some(wal) = &self.wal {
            if let Ok(mut file) = wal.lock() {
                let _ = writeln!(file, "{tx_id}\t{status}");
                let _ = file.flush();
            }
        }
        Ok(())
    }

    pub async fn prepare(&self, tx_id: &str) -> Result<(), String> {
        self.write_status(tx_id, "prepared").await
    }

    pub async fn commit(&self, tx_id: &str) -> Result<(), String> {
        self.write_status(tx_id, "committed").await
    }

    pub async fn rollback(&self, tx_id: &str) -> Result<(), String> {
        self.write_status(tx_id, "rolled_back").await
    }

    pub async fn status(&self, tx_id: &str) -> Option<String> {
        self.status.read().await.get(tx_id).cloned()
    }

    pub fn node_id(&self) -> NodeId {
        self.node_id
    }

    pub fn is_leader(&self) -> bool {
        self.raft.is_leader()
    }

    pub async fn current_leader(&self) -> Option<NodeId> {
        self.raft.current_leader().await
    }

    pub fn raft(&self) -> &TcRaft {
        &self.raft
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn single_node_prepare_commit() {
        let tc = Tc::start_cluster(10, vec![10]).await.unwrap();
        tc.prepare("tx-1").await.unwrap();
        assert_eq!(tc.status("tx-1").await.as_deref(), Some("prepared"));
        tc.commit("tx-1").await.unwrap();
        assert_eq!(tc.status("tx-1").await.as_deref(), Some("committed"));
    }

    #[tokio::test]
    async fn status_wal_survives_restart() {
        let path = std::env::temp_dir().join(format!(
            "mq-rust-tc-wal-{}-{}.log",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_millis()
        ));
        let _ = std::fs::remove_file(&path);
        {
            let tc = Tc::start_cluster_wal(30, vec![30], Some(&path))
                .await
                .unwrap();
            tc.prepare("tx-wal").await.unwrap();
            tc.commit("tx-wal").await.unwrap();
        }
        let tc = Tc::start_cluster_wal(31, vec![31], Some(&path))
            .await
            .unwrap();
        assert_eq!(tc.status("tx-wal").await.as_deref(), Some("committed"));
        let _ = std::fs::remove_file(&path);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn snapshot_contains_committed_status() {
        let dir = std::env::temp_dir().join(format!(
            "mq-rust-tc-snap-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_millis()
        ));
        let _ = std::fs::remove_dir_all(&dir);
        let members = vec![(1u64, "127.0.0.1:19931".to_string())];
        let tc = Tc::start_tcp_persistent(1, "127.0.0.1:19931", members, &dir)
            .await
            .unwrap();
        for i in 0..10 {
            let id = format!("tx-snap-{i}");
            tc.prepare(&id).await.unwrap();
            tc.commit(&id).await.unwrap();
        }
        tc.raft.trigger().snapshot().await.unwrap();
        tokio::time::sleep(std::time::Duration::from_millis(400)).await;
        let snapshot = tc.raft.get_snapshot().await.unwrap().expect("snapshot");
        let data: HashMap<String, String> =
            serde_json::from_slice(&snapshot.snapshot.into_inner()).unwrap();
        assert_eq!(data.get("tx-snap-0").map(String::as_str), Some("committed"));
        for i in 0..2 {
            let id = format!("tx-after-{i}");
            tc.prepare(&id).await.unwrap();
            tc.commit(&id).await.unwrap();
        }
        let log_file = std::fs::read(dir.join("node-1.raft.json")).unwrap();
        let log: serde_json::Value = serde_json::from_slice(&log_file).unwrap();
        assert!(
            log["entries"].as_object().unwrap().len() < 12,
            "log was not compacted: {}",
            log["entries"].as_object().unwrap().len()
        );
        drop(tc);

        // Snapshot file must restore the status after restart.
        let members = vec![(1u64, "127.0.0.1:19932".to_string())];
        let tc = Tc::start_tcp_persistent(1, "127.0.0.1:19932", members, &dir)
            .await
            .unwrap();
        assert_eq!(tc.status("tx-snap-0").await.as_deref(), Some("committed"));
        assert_eq!(tc.status("tx-after-1").await.as_deref(), Some("committed"));
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn three_node_persistent_restart() {
        let dir = std::env::temp_dir().join(format!(
            "mq-rust-tc-3log-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_millis()
        ));
        let _ = std::fs::remove_dir_all(&dir);
        let d1 = dir.join("1");
        let d2 = dir.join("2");
        let d3 = dir.join("3");
        let members = vec![
            (1u64, "127.0.0.1:19911".to_string()),
            (2u64, "127.0.0.1:19912".to_string()),
            (3u64, "127.0.0.1:19913".to_string()),
        ];
        let (a, b, c) = tokio::join!(
            Tc::start_tcp_persistent(1, "127.0.0.1:19911", members.clone(), &d1),
            Tc::start_tcp_persistent(2, "127.0.0.1:19912", members.clone(), &d2),
            Tc::start_tcp_persistent(3, "127.0.0.1:19913", members.clone(), &d3),
        );
        let a = a.unwrap();
        let b = b.unwrap();
        let c = c.unwrap();
        let mut leader = None;
        for tc in [&a, &b, &c] {
            if tc.is_leader() {
                leader = Some(tc);
                break;
            }
        }
        let leader = leader.expect("no leader");
        leader.prepare("tx-3log").await.unwrap();
        leader.commit("tx-3log").await.unwrap();
        tokio::time::sleep(std::time::Duration::from_millis(300)).await;

        let members2 = vec![
            (1u64, "127.0.0.1:19921".to_string()),
            (2u64, "127.0.0.1:19922".to_string()),
            (3u64, "127.0.0.1:19923".to_string()),
        ];
        let (a, b, c) = tokio::join!(
            Tc::start_tcp_persistent(1, "127.0.0.1:19921", members2.clone(), &d1),
            Tc::start_tcp_persistent(2, "127.0.0.1:19922", members2.clone(), &d2),
            Tc::start_tcp_persistent(3, "127.0.0.1:19923", members2, &d3),
        );
        let a = a.unwrap();
        let b = b.unwrap();
        let c = c.unwrap();
        let mut leader = None;
        for tc in [&a, &b, &c] {
            if tc.is_leader() {
                leader = Some(tc);
                break;
            }
        }
        let leader = leader.expect("no leader after restart");
        assert_eq!(leader.status("tx-3log").await.as_deref(), Some("committed"));
        leader.prepare("tx-3log-2").await.unwrap();
        leader.commit("tx-3log-2").await.unwrap();
        assert_eq!(
            leader.status("tx-3log-2").await.as_deref(),
            Some("committed")
        );
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn persistent_log_survives_restart() {
        let dir = std::env::temp_dir().join(format!(
            "mq-rust-tc-log-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_millis()
        ));
        let _ = std::fs::remove_dir_all(&dir);
        let members = vec![(1u64, "127.0.0.1:19901".to_string())];
        let tc = Tc::start_tcp_persistent(1, "127.0.0.1:19901", members.clone(), &dir)
            .await
            .unwrap();
        tc.prepare("tx-log").await.unwrap();
        tc.commit("tx-log").await.unwrap();
        drop(tc);

        let members2 = vec![(1u64, "127.0.0.1:19902".to_string())];
        let tc = Tc::start_tcp_persistent(1, "127.0.0.1:19902", members2, &dir)
            .await
            .unwrap();
        assert_eq!(tc.status("tx-log").await.as_deref(), Some("committed"));
        tc.prepare("tx-log-2").await.unwrap();
        tc.commit("tx-log-2").await.unwrap();
        assert_eq!(tc.status("tx-log-2").await.as_deref(), Some("committed"));
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn tcp_wal_survives_restart() {
        let dir = std::env::temp_dir().join(format!(
            "mq-rust-tc-tcpwal-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_millis()
        ));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let wal = |id: u64| dir.join(format!("node-{id}.wal"));
        let w1 = wal(1);
        let w2 = wal(2);
        let w3 = wal(3);
        let members = vec![
            (1u64, "127.0.0.1:19701".to_string()),
            (2u64, "127.0.0.1:19702".to_string()),
            (3u64, "127.0.0.1:19703".to_string()),
        ];
        let (a, b, c) = tokio::join!(
            Tc::start_tcp_wal(1, "127.0.0.1:19701", members.clone(), Some(&w1)),
            Tc::start_tcp_wal(2, "127.0.0.1:19702", members.clone(), Some(&w2)),
            Tc::start_tcp_wal(3, "127.0.0.1:19703", members.clone(), Some(&w3)),
        );
        let a = a.unwrap();
        let b = b.unwrap();
        let c = c.unwrap();
        let mut leader = None;
        for tc in [&a, &b, &c] {
            if tc.raft.is_leader() {
                leader = Some(tc);
                break;
            }
        }
        let leader = leader.expect("no leader");
        leader.prepare("tx-tcp-wal").await.unwrap();
        leader.commit("tx-tcp-wal").await.unwrap();
        tokio::time::sleep(std::time::Duration::from_millis(300)).await;

        // Restart on new ports; the WALs carry the applied status.
        let members2 = vec![
            (1u64, "127.0.0.1:19801".to_string()),
            (2u64, "127.0.0.1:19802".to_string()),
            (3u64, "127.0.0.1:19803".to_string()),
        ];
        let (a, b, c) = tokio::join!(
            Tc::start_tcp_wal(1, "127.0.0.1:19801", members2.clone(), Some(&w1)),
            Tc::start_tcp_wal(2, "127.0.0.1:19802", members2.clone(), Some(&w2)),
            Tc::start_tcp_wal(3, "127.0.0.1:19803", members2, Some(&w3)),
        );
        let a = a.unwrap();
        let b = b.unwrap();
        let c = c.unwrap();
        let mut found = false;
        for tc in [&a, &b, &c] {
            if tc.status("tx-tcp-wal").await.as_deref() == Some("committed") {
                found = true;
                break;
            }
        }
        assert!(found, "status not restored from TCP WAL");
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn tcp_three_nodes_elect_and_replicate() {
        let members = vec![
            (1u64, "127.0.0.1:19201".to_string()),
            (2u64, "127.0.0.1:19202".to_string()),
            (3u64, "127.0.0.1:19203".to_string()),
        ];
        let (a, b, c) = tokio::join!(
            Tc::start_tcp(1, "127.0.0.1:19201", members.clone()),
            Tc::start_tcp(2, "127.0.0.1:19202", members.clone()),
            Tc::start_tcp(3, "127.0.0.1:19203", members),
        );
        let a = a.unwrap();
        let b = b.unwrap();
        let c = c.unwrap();
        let mut leader = None;
        for tc in [&a, &b, &c] {
            if tc.raft.is_leader() {
                leader = Some(tc);
                break;
            }
        }
        let leader = leader.expect("no TCP leader");
        leader.prepare("tx-tcp").await.unwrap();
        leader.commit("tx-tcp").await.unwrap();
        for _ in 0..20 {
            let a_s = a.status("tx-tcp").await;
            let b_s = b.status("tx-tcp").await;
            let c_s = c.status("tx-tcp").await;
            if a_s.as_deref() == Some("committed")
                && b_s.as_deref() == Some("committed")
                && c_s.as_deref() == Some("committed")
            {
                return;
            }
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        }
        for tc in [&a, &b, &c] {
            assert_eq!(tc.status("tx-tcp").await.as_deref(), Some("committed"));
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn three_nodes_elect_and_replicate() {
        let (a, b, c) = tokio::join!(
            Tc::start_cluster(1, vec![1, 2, 3]),
            Tc::start_cluster(2, vec![1, 2, 3]),
            Tc::start_cluster(3, vec![1, 2, 3]),
        );
        let a = a.unwrap();
        let b = b.unwrap();
        let c = c.unwrap();

        let mut leader = None;
        for tc in [&a, &b, &c] {
            if tc.raft.is_leader() {
                leader = Some(tc);
                break;
            }
        }
        let leader = leader.expect("no leader elected");
        leader.prepare("tx-3node").await.unwrap();
        leader.commit("tx-3node").await.unwrap();
        assert_eq!(
            leader.status("tx-3node").await.as_deref(),
            Some("committed")
        );
    }
}
