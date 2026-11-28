//! In-memory topic/queue store for the R1 compatibility milestone.
//!
//! The production store is `mq-store` (CommitLog + ConsumeQueue). R1 keeps the
//! protocol surface honest first: Java SDK -> broker -> pull -> Java SDK. The
//! `StoredMessage` shape is already the one the CommitLog will persist.

use mq_common::{MqError, Result};
use mq_protocol::message::StoredMessage;
use mq_store::{CommitLog, ConsumeQueue, CqEntry};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicI64, AtomicU64, AtomicUsize, Ordering};
use std::sync::mpsc::{self, Receiver, Sender, SyncSender};
use std::sync::{Arc, Mutex, RwLock};
use std::time::{Duration, Instant};
use tokio::sync::Notify;

#[derive(Clone, Debug)]
pub struct QueueState {
    pub messages: Vec<StoredMessage>,
    pub base_offset: i64,
}

#[derive(Debug)]
pub struct TopicState {
    pub queues: Vec<RwLock<QueueState>>,
}

#[derive(Clone, Debug, Hash, PartialEq, Eq)]
struct OffsetKey {
    group: String,
    topic: String,
    queue_id: i32,
}

type DedupMap = HashMap<(String, String), (i32, i64)>;

#[derive(Clone, Debug)]
struct HalfEntry {
    msg: StoredMessage,
    created_at_ms: i64,
    check_count: u32,
    last_check_ms: i64,
}

#[derive(Clone, Debug, Serialize)]
pub struct HalfStatus {
    pub tx_id: String,
    pub topic: String,
    pub created_at_ms: i64,
    pub check_count: u32,
    pub age_ms: i64,
}

#[derive(Serialize, Deserialize)]
struct HalfEvent {
    msg: StoredMessage,
    state: String,
    created_at_ms: i64,
}

#[derive(Serialize, Deserialize)]
struct DelayEvent {
    msg: StoredMessage,
    due_at_ms: i64,
    state: String,
}

struct WriteJob {
    msg: StoredMessage,
    reply: SyncSender<Result<StoredMessage>>,
}

enum WriteCmd {
    Job(WriteJob),
    Batch {
        msgs: Vec<StoredMessage>,
        reply: SyncSender<Vec<Result<StoredMessage>>>,
    },
    Sync(SyncSender<Result<()>>),
    Rebuild {
        msgs: Vec<StoredMessage>,
        reply: SyncSender<Result<Vec<StoredMessage>>>,
    },
}

struct CommitWriter {
    tx: Sender<WriteCmd>,
}

impl CommitWriter {
    fn submit(&self, msg: StoredMessage) -> Result<StoredMessage> {
        let (reply, rx) = mpsc::sync_channel(0);
        self.tx
            .send(WriteCmd::Job(WriteJob { msg, reply }))
            .map_err(|_| MqError::Store("commit writer stopped".into()))?;
        rx.recv()
            .map_err(|_| MqError::Store("commit writer reply lost".into()))?
    }

    fn sync(&self) -> Result<()> {
        let (reply, rx) = mpsc::sync_channel(0);
        self.tx
            .send(WriteCmd::Sync(reply))
            .map_err(|_| MqError::Store("commit writer stopped".into()))?;
        rx.recv()
            .map_err(|_| MqError::Store("commit writer reply lost".into()))?
    }

    fn rebuild(&self, msgs: Vec<StoredMessage>) -> Result<Vec<StoredMessage>> {
        let (reply, rx) = mpsc::sync_channel(0);
        self.tx
            .send(WriteCmd::Rebuild { msgs, reply })
            .map_err(|_| MqError::Store("commit writer stopped".into()))?;
        rx.recv()
            .map_err(|_| MqError::Store("commit writer rebuild reply lost".into()))?
    }

    fn submit_batch(&self, msgs: Vec<StoredMessage>) -> Result<Vec<StoredMessage>> {
        let (reply, rx) = mpsc::sync_channel(0);
        self.tx
            .send(WriteCmd::Batch { msgs, reply })
            .map_err(|_| MqError::Store("commit writer stopped".into()))?;
        let results = rx
            .recv()
            .map_err(|_| MqError::Store("commit writer batch reply lost".into()))?;
        let mut out = Vec::with_capacity(results.len());
        for result in results {
            out.push(result?);
        }
        Ok(out)
    }
}

fn run_commit_writer(mut log: CommitLog, rx: Receiver<WriteCmd>) {
    loop {
        match rx.recv() {
            Ok(WriteCmd::Job(first)) => {
                let mut jobs = vec![first];
                while jobs.len() < 64 {
                    match rx.try_recv() {
                        Ok(WriteCmd::Job(job)) => jobs.push(job),
                        Ok(WriteCmd::Batch { msgs, reply }) => {
                            let _ = reply.send(append_msgs_batch(&mut log, msgs));
                        }
                        Ok(WriteCmd::Sync(reply)) => {
                            let _ = reply.send(log.sync());
                        }
                        Ok(WriteCmd::Rebuild { msgs, reply }) => {
                            let _ = reply.send(rebuild_log(&mut log, msgs));
                        }
                        Err(_) => break,
                    }
                }
                for job in jobs {
                    let mut msg = job.msg;
                    msg.physical_offset = log.len() as i64;
                    let payload = msg.encode();
                    match log.append(&payload) {
                        Ok(offset) => {
                            msg.physical_offset = offset.physical_offset as i64;
                            let _ = job.reply.send(Ok(msg));
                        }
                        Err(err) => {
                            let _ = job
                                .reply
                                .send(Err(MqError::Store(format!("append batch: {err}"))));
                        }
                    }
                }
            }
            Ok(WriteCmd::Batch { msgs, reply }) => {
                let _ = reply.send(append_msgs_batch(&mut log, msgs));
            }
            Ok(WriteCmd::Sync(reply)) => {
                let _ = reply.send(log.sync());
            }
            Ok(WriteCmd::Rebuild { msgs, reply }) => {
                let _ = reply.send(rebuild_log(&mut log, msgs));
            }
            Err(_) => break,
        }
    }
}

fn append_msgs_batch(log: &mut CommitLog, msgs: Vec<StoredMessage>) -> Vec<Result<StoredMessage>> {
    let payloads: Vec<Vec<u8>> = msgs.iter().map(|msg| msg.encode()).collect();
    match log.append_batch(&payloads) {
        Ok(offsets) => msgs
            .into_iter()
            .zip(offsets)
            .map(|(mut msg, offset)| {
                msg.physical_offset = offset.physical_offset as i64;
                Ok(msg)
            })
            .collect(),
        Err(err) => msgs
            .into_iter()
            .map(|_| Err(MqError::Store(format!("append batch: {err}"))))
            .collect(),
    }
}

fn rebuild_log(log: &mut CommitLog, msgs: Vec<StoredMessage>) -> Result<Vec<StoredMessage>> {
    log.truncate()?;
    let mut out = Vec::with_capacity(msgs.len());
    for mut msg in msgs {
        let payload = msg.encode();
        let offset = log.append(&payload)?;
        msg.physical_offset = offset.physical_offset as i64;
        out.push(msg);
    }
    Ok(out)
}

pub enum ReplicaCmd {
    Add(std::net::TcpStream),
    Frame(Vec<u8>),
}

pub struct BrokerStore {
    topics: RwLock<HashMap<String, Arc<RwLock<TopicState>>>>,
    offsets: RwLock<HashMap<OffsetKey, i64>>,
    offset_log: Option<Mutex<CommitLog>>,
    queue_nums: i32,
    next_physical: AtomicI64,
    append_count: AtomicI64,
    retained_dropped: AtomicU64,
    logs: Vec<Mutex<CommitLog>>,
    writer: Option<CommitWriter>,
    shards: usize,
    half_log: Option<Mutex<CommitLog>>,
    retry_log: Option<Mutex<CommitLog>>,
    delay_log: Option<Mutex<CommitLog>>,
    consumers: RwLock<HashMap<String, HashMap<String, Instant>>>,
    consumer_addrs: RwLock<HashMap<String, String>>,
    producers: RwLock<HashMap<String, HashMap<String, Instant>>>,
    producer_addrs: RwLock<HashMap<String, String>>,
    extra_groups: RwLock<std::collections::HashSet<String>>,
    half: RwLock<HashMap<String, HalfEntry>>,
    dedup: Vec<Mutex<DedupMap>>,
    retry_counts: RwLock<HashMap<String, i32>>,
    msg_index: RwLock<HashMap<String, (String, i32, i64)>>,
    index_log: Option<Mutex<CommitLog>>,
    subscriptions: RwLock<HashMap<(String, String), (String, String)>>,
    delayed: RwLock<HashMap<String, (StoredMessage, i64)>>,
    cq: RwLock<HashMap<(String, i32), Mutex<ConsumeQueue>>>,
    cq_root: Option<PathBuf>,
    cq_segment_entries: AtomicU64,
    replicas: Mutex<Vec<std::net::TcpStream>>,
    replica_tx: Mutex<Option<Sender<ReplicaCmd>>>,
    replica_acked: Arc<AtomicI64>,
    replica_ack_events: Arc<AtomicI64>,
    replica_active: Arc<AtomicBool>,
    sync_replicate: AtomicBool,
    replica_timeout_ms: AtomicU64,
    replica_quorum: AtomicUsize,
    notify: Notify,
}

impl BrokerStore {
    pub fn new(queue_nums: i32) -> Self {
        Self {
            topics: RwLock::new(HashMap::new()),
            offsets: RwLock::new(HashMap::new()),
            offset_log: None,
            queue_nums: queue_nums.max(1),
            next_physical: AtomicI64::new(0),
            append_count: AtomicI64::new(0),
            retained_dropped: AtomicU64::new(0),
            logs: Vec::new(),
            writer: None,
            shards: 1,
            half_log: None,
            retry_log: None,
            delay_log: None,
            consumers: RwLock::new(HashMap::new()),
            consumer_addrs: RwLock::new(HashMap::new()),
            producers: RwLock::new(HashMap::new()),
            producer_addrs: RwLock::new(HashMap::new()),
            extra_groups: RwLock::new(std::collections::HashSet::new()),
            half: RwLock::new(HashMap::new()),
            dedup: (0..16).map(|_| Mutex::new(HashMap::new())).collect(),
            retry_counts: RwLock::new(HashMap::new()),
            msg_index: RwLock::new(HashMap::new()),
            index_log: None,
            subscriptions: RwLock::new(HashMap::new()),
            delayed: RwLock::new(HashMap::new()),
            cq: RwLock::new(HashMap::new()),
            cq_root: None,
            cq_segment_entries: AtomicU64::new(100_000),
            replicas: Mutex::new(Vec::new()),
            replica_tx: Mutex::new(None),
            replica_acked: Arc::new(AtomicI64::new(0)),
            replica_ack_events: Arc::new(AtomicI64::new(0)),
            replica_active: Arc::new(AtomicBool::new(false)),
            sync_replicate: AtomicBool::new(false),
            replica_timeout_ms: AtomicU64::new(1000),
            replica_quorum: AtomicUsize::new(1),
            notify: Notify::new(),
        }
    }

    /// Open a persistent store and rebuild the topic/queue view from the
    /// sharded CommitLogs. Default to 4 shards.
    pub fn open(root: impl AsRef<Path>, queue_nums: i32) -> Result<Self> {
        Self::open_sharded(root, queue_nums, 4)
    }

    /// Open `shards` CommitLogs, sharded by `queue_id % shards`.
    pub fn open_sharded(root: impl AsRef<Path>, queue_nums: i32, shards: usize) -> Result<Self> {
        Self::open_sharded_with_group(root, queue_nums, shards, false)
    }

    /// Open with an optional single-writer group-commit thread.
    pub fn open_sharded_with_group(
        root: impl AsRef<Path>,
        queue_nums: i32,
        shards: usize,
        group_commit: bool,
    ) -> Result<Self> {
        Self::open_sharded_with_group_segment(
            root,
            queue_nums,
            shards,
            group_commit,
            256 * 1024 * 1024,
        )
    }

    pub fn open_sharded_with_group_segment(
        root: impl AsRef<Path>,
        queue_nums: i32,
        shards: usize,
        group_commit: bool,
        segment_bytes: u64,
    ) -> Result<Self> {
        let root = root.as_ref();
        std::fs::create_dir_all(root)?;
        let cq_root = Some(root.to_path_buf());
        let shards = shards.max(1);
        let mut records = Vec::new();
        let mut logs = Vec::with_capacity(shards);
        let mut writer = None;
        if shards == 1 && group_commit {
            let mut log = CommitLog::open_with_segment(root.join("commitlog-0"), segment_bytes)?;
            records.extend(log.scan()?);
            let (tx, rx) = mpsc::channel();
            std::thread::spawn(move || run_commit_writer(log, rx));
            writer = Some(CommitWriter { tx });
        } else {
            for i in 0..shards {
                let mut log = CommitLog::open_with_segment(
                    root.join(format!("commitlog-{i}")),
                    segment_bytes,
                )?;
                records.extend(log.scan()?);
                logs.push(Mutex::new(log));
            }
        }
        let mut half_log = CommitLog::open_with_segment(root.join("half"), segment_bytes)?;
        let half_records = half_log.scan()?;
        let mut retry_log = CommitLog::open_with_segment(root.join("retry"), segment_bytes)?;
        let retry_records = retry_log.scan()?;
        let mut delay_log = CommitLog::open_with_segment(root.join("delay"), segment_bytes)?;
        let delay_records = delay_log.scan()?;
        let mut index_log = CommitLog::open_with_segment(root.join("index"), segment_bytes)?;
        let index_records = index_log.scan()?;
        let mut offset_log = CommitLog::open_with_segment(root.join("offsets"), segment_bytes)?;
        let offset_records = offset_log.scan()?;
        let store = Self {
            topics: RwLock::new(HashMap::new()),
            offsets: RwLock::new(HashMap::new()),
            offset_log: Some(Mutex::new(offset_log)),
            queue_nums: queue_nums.max(1),
            next_physical: AtomicI64::new(0),
            append_count: AtomicI64::new(0),
            retained_dropped: AtomicU64::new(0),
            logs,
            writer,
            shards,
            half_log: Some(Mutex::new(half_log)),
            retry_log: Some(Mutex::new(retry_log)),
            delay_log: Some(Mutex::new(delay_log)),
            consumers: RwLock::new(HashMap::new()),
            consumer_addrs: RwLock::new(HashMap::new()),
            producers: RwLock::new(HashMap::new()),
            producer_addrs: RwLock::new(HashMap::new()),
            extra_groups: RwLock::new(std::collections::HashSet::new()),
            half: RwLock::new(HashMap::new()),
            dedup: (0..16).map(|_| Mutex::new(HashMap::new())).collect(),
            retry_counts: RwLock::new(HashMap::new()),
            msg_index: RwLock::new(HashMap::new()),
            index_log: Some(Mutex::new(index_log)),
            subscriptions: RwLock::new(HashMap::new()),
            delayed: RwLock::new(HashMap::new()),
            cq: RwLock::new(HashMap::new()),
            cq_root,
            cq_segment_entries: AtomicU64::new(100_000),
            replicas: Mutex::new(Vec::new()),
            replica_tx: Mutex::new(None),
            replica_acked: Arc::new(AtomicI64::new(0)),
            replica_ack_events: Arc::new(AtomicI64::new(0)),
            replica_active: Arc::new(AtomicBool::new(false)),
            sync_replicate: AtomicBool::new(false),
            replica_timeout_ms: AtomicU64::new(1000),
            replica_quorum: AtomicUsize::new(1),
            notify: Notify::new(),
        };
        let mut index = std::collections::HashMap::new();
        for payload in index_records {
            if let Ok((id, topic, queue, offset)) =
                serde_json::from_slice::<(String, String, i32, i64)>(&payload)
            {
                index.insert(id, (topic, queue, offset));
            }
        }
        *store.msg_index.write().unwrap() = index;
        for payload in offset_records {
            if let Ok((group, topic, queue_id, offset)) =
                serde_json::from_slice::<(String, String, i32, i64)>(&payload)
            {
                store.offsets.write().unwrap().insert(
                    OffsetKey {
                        group,
                        topic,
                        queue_id,
                    },
                    offset,
                );
            }
        }
        store.compact_metadata()?;
        for payload in retry_records {
            if let Ok((origin, count)) = serde_json::from_slice::<(String, i32)>(&payload) {
                store.retry_counts.write().unwrap().insert(origin, count);
            }
        }
        for payload in delay_records {
            if let Ok(event) = serde_json::from_slice::<DelayEvent>(&payload) {
                let mut delayed = store.delayed.write().unwrap();
                if event.state == "scheduled" {
                    delayed.insert(event.msg.msg_id.clone(), (event.msg, event.due_at_ms));
                } else {
                    delayed.remove(&event.msg.msg_id);
                }
            }
        }
        for payload in half_records {
            let Ok(event) = serde_json::from_slice::<HalfEvent>(&payload) else {
                continue;
            };
            match event.state.as_str() {
                "prepared" => {
                    store.half.write().unwrap().insert(
                        event.msg.msg_id.clone(),
                        HalfEntry {
                            msg: event.msg,
                            created_at_ms: event.created_at_ms,
                            check_count: 0,
                            last_check_ms: 0,
                        },
                    );
                }
                "committed" | "rolled_back" => {
                    store.half.write().unwrap().remove(&event.msg.msg_id);
                }
                _ => {}
            }
        }
        for payload in records {
            let decoded = StoredMessage::decode(&payload)
                .map(|(msg, _)| msg)
                .or_else(|_| serde_json::from_slice::<StoredMessage>(&payload));
            match decoded {
                Ok(msg) => {
                    store.append_internal(msg, false)?;
                }
                Err(err) => {
                    tracing::warn!(error = %err, "skip corrupt broker store record");
                }
            }
        }
        store.load_retention();
        store.load_admin_meta();
        store.repair_cq()?;
        if std::env::var("MQ_COMPACT_COMMITLOG").as_deref() == Ok("1") {
            store.compact_commitlogs_exact()?;
            store.repair_cq_force()?;
        }
        Ok(store)
    }

    pub fn compact_now(&self) -> Result<()> {
        self.compact_commitlogs_exact()?;
        self.repair_cq_force()
    }

    fn repair_cq_force(&self) -> Result<()> {
        if let Some(root) = &self.cq_root {
            let _ = std::fs::remove_dir_all(root.join("cq"));
        }
        self.cq.write().unwrap().clear();
        self.repair_cq()
    }

    fn compact_commitlogs_exact(&self) -> Result<()> {
        if self.logs.is_empty() && self.writer.is_none() {
            tracing::warn!("exact commitlog compaction needs a storage log");
            return Ok(());
        }
        let root = self
            .cq_root
            .clone()
            .ok_or_else(|| MqError::Store("missing store root".into()))?;
        let mut per_shard: Vec<Vec<StoredMessage>> = vec![Vec::new(); self.shards];
        let topics: Vec<_> = self
            .topics
            .read()
            .unwrap()
            .iter()
            .map(|(k, v)| (k.clone(), Arc::clone(v)))
            .collect();
        for (_, state) in &topics {
            let state = state.read().unwrap();
            for (queue_id, queue) in state.queues.iter().enumerate() {
                for msg in &queue.read().unwrap().messages {
                    let shard = (queue_id as i32).rem_euclid(self.shards as i32) as usize;
                    per_shard[shard].push(msg.clone());
                }
            }
        }
        let mut offsets = std::collections::HashMap::new();
        if let Some(writer) = &self.writer {
            let mut messages: Vec<StoredMessage> = per_shard.into_iter().flatten().collect();
            messages.sort_by_key(|msg| msg.physical_offset);
            for msg in writer.rebuild(messages)? {
                offsets.insert((msg.topic.clone(), msg.msg_id.clone()), msg.physical_offset);
            }
        } else {
            for (shard, mut messages) in per_shard.into_iter().enumerate() {
                messages.sort_by_key(|msg| msg.physical_offset);
                let path = root.join(format!("commitlog-{shard}"));
                let tmp = root.join(format!("commitlog-{shard}.compact"));
                let _ = std::fs::remove_file(&tmp);
                let _ = std::fs::remove_file(tmp.with_extension("compact.1"));
                let mut log = CommitLog::open_with_segment(&tmp, 256 * 1024 * 1024)?;
                for msg in &messages {
                    let payload = msg.encode();
                    let appended = log.append(&payload)?;
                    offsets.insert(
                        (msg.topic.clone(), msg.msg_id.clone()),
                        appended.physical_offset as i64,
                    );
                }
                drop(log);
                let prefix = format!("commitlog-{shard}");
                for entry in std::fs::read_dir(&root)? {
                    let entry = entry?;
                    let name = entry.file_name().to_string_lossy().to_string();
                    if name == prefix
                        || (name.starts_with(&format!("{prefix}."))
                            && !name.starts_with(&format!("{prefix}.compact")))
                    {
                        let _ = std::fs::remove_file(entry.path());
                    }
                }
                std::fs::rename(&tmp, &path)?;
                let reopened = CommitLog::open_with_segment(&path, 256 * 1024 * 1024)?;
                *self.logs[shard].lock().unwrap() = reopened;
            }
        }
        for (_, state) in &topics {
            let state = state.read().unwrap();
            for queue in &state.queues {
                let mut queue = queue.write().unwrap();
                for msg in &mut queue.messages {
                    if let Some(offset) = offsets.get(&(msg.topic.clone(), msg.msg_id.clone())) {
                        msg.physical_offset = *offset;
                    }
                }
            }
        }
        Ok(())
    }

    pub fn queue_nums(&self) -> i32 {
        self.queue_nums
    }

    /// Drop the oldest messages so each queue holds at most `max_messages`.
    /// Returns the number of dropped messages.
    pub fn retain(&self, max_messages: usize, max_bytes: u64, max_age_ms: i64) -> Result<usize> {
        if max_messages == 0 && max_bytes == 0 && max_age_ms == 0 {
            return Ok(0);
        }
        let topics: Vec<_> = self
            .topics
            .read()
            .unwrap()
            .iter()
            .map(|(name, topic)| (name.clone(), Arc::clone(topic)))
            .collect();
        let mut dropped = 0usize;
        let mut needs_repair = false;
        for (name, topic) in topics {
            let topic = topic.read().unwrap();
            for (queue_id, queue) in topic.queues.iter().enumerate() {
                let mut queue = queue.write().unwrap();
                let len = queue.messages.len();
                let mut drop = 0usize;
                if max_messages > 0 && len > max_messages {
                    drop = len - max_messages;
                }
                if max_bytes > 0 {
                    let mut total: u64 = queue.messages.iter().map(|m| m.body.len() as u64).sum();
                    let mut by_bytes = 0usize;
                    while total > max_bytes && by_bytes < len {
                        total -= queue.messages[by_bytes].body.len() as u64;
                        by_bytes += 1;
                    }
                    drop = drop.max(by_bytes);
                }
                if max_age_ms > 0 {
                    let cutoff = mq_common::now_ms() - max_age_ms;
                    let mut by_age = 0usize;
                    while by_age < len && queue.messages[by_age].born_timestamp < cutoff {
                        by_age += 1;
                    }
                    drop = drop.max(by_age);
                }
                if drop > 0 {
                    queue.messages.drain(0..drop);
                    queue.base_offset += drop as i64;
                    dropped += drop;
                    match self.drop_cq_prefix(&name, queue_id as i32, len, drop) {
                        Ok(true) => {}
                        _ => needs_repair = true,
                    }
                }
            }
        }
        if dropped > 0 {
            self.retained_dropped
                .fetch_add(dropped as u64, Ordering::Relaxed);
            if needs_repair {
                self.repair_cq()?;
            }
            self.persist_retention();
            let freed = self.trim_commitlogs();
            tracing::info!(dropped, freed, "retained messages dropped");
        }
        Ok(dropped)
    }

    fn drop_cq_prefix(
        &self,
        topic: &str,
        queue_id: i32,
        old_len: usize,
        drop: usize,
    ) -> Result<bool> {
        let Some(path) = self.cq_path(topic, queue_id) else {
            return Ok(false);
        };
        let segment = self.cq_segment_entries.load(Ordering::Relaxed).max(1);
        let key = (topic.to_string(), queue_id);
        let mut map = self.cq.write().unwrap();
        if !map.contains_key(&key) {
            match ConsumeQueue::open_with_segment(&path, segment) {
                Ok(cq) => {
                    map.insert(key.clone(), Mutex::new(cq));
                }
                Err(_) => return Ok(false),
            }
        }
        let handle = map.get(&key).unwrap();
        let mut cq = handle.lock().unwrap();
        if cq.len() as usize != old_len {
            return Ok(false);
        }
        cq.drop_prefix(drop as u64)?;
        Ok(true)
    }

    fn retention_path(&self) -> Option<PathBuf> {
        self.cq_root
            .as_ref()
            .map(|root| root.join("retention.json"))
    }

    fn persist_retention(&self) {
        let Some(path) = self.retention_path() else {
            return;
        };
        let topics: Vec<_> = self
            .topics
            .read()
            .unwrap()
            .iter()
            .map(|(name, topic)| (name.clone(), Arc::clone(topic)))
            .collect();
        let mut entries = Vec::new();
        for (name, topic) in topics {
            let topic = topic.read().unwrap();
            for (queue_id, queue) in topic.queues.iter().enumerate() {
                let base = queue.read().unwrap().base_offset;
                entries.push((name.clone(), queue_id as i32, base));
            }
        }
        if let Ok(payload) = serde_json::to_vec(&entries) {
            let _ = std::fs::write(path, payload);
        }
    }

    fn load_retention(&self) {
        let Some(path) = self.retention_path() else {
            return;
        };
        let Ok(text) = std::fs::read(&path) else {
            return;
        };
        let Ok(entries) = serde_json::from_slice::<Vec<(String, i32, i64)>>(&text) else {
            return;
        };
        for (topic, queue_id, base) in entries {
            if let Some(topic_lock) = self.topics.read().unwrap().get(&topic).cloned() {
                let topic_guard = topic_lock.read().unwrap();
                let Some(queue_lock) = topic_guard.queues.get(queue_id as usize) else {
                    continue;
                };
                let mut updated = Vec::new();
                {
                    let mut queue = queue_lock.write().unwrap();
                    let old_base = queue.base_offset;
                    queue.base_offset = base;
                    if base > old_base {
                        let delta = base - old_base;
                        for msg in &mut queue.messages {
                            msg.queue_offset += delta;
                            updated.push((msg.msg_id.clone(), msg.queue_offset));
                        }
                    }
                }
                if !updated.is_empty() {
                    let mut index = self.msg_index.write().unwrap();
                    for (msg_id, offset) in updated {
                        index.insert(msg_id, (topic.clone(), queue_id, offset));
                    }
                }
            }
        }
    }

    pub fn retained_dropped(&self) -> u64 {
        self.retained_dropped.load(Ordering::Relaxed)
    }

    pub fn trim_commitlogs(&self) -> u64 {
        let shards = self.logs.len();
        if shards == 0 {
            return 0;
        }
        let mut mins = vec![u64::MAX; shards];
        let topics: Vec<_> = self.topics.read().unwrap().values().cloned().collect();
        for topic in topics {
            let topic = topic.read().unwrap();
            for (queue_id, queue) in topic.queues.iter().enumerate() {
                let queue = queue.read().unwrap();
                if let Some(msg) = queue.messages.first() {
                    if msg.physical_offset > 0 {
                        let shard = (queue_id as i32).rem_euclid(shards as i32) as usize;
                        mins[shard] = mins[shard].min(msg.physical_offset as u64);
                    }
                }
            }
        }
        let mut freed = 0u64;
        for (shard, cutoff) in mins.into_iter().enumerate() {
            if cutoff == u64::MAX {
                continue;
            }
            let mut log = self.logs[shard].lock().unwrap();
            match log.trim_prefix(cutoff) {
                Ok(bytes) => freed += bytes,
                Err(err) => tracing::warn!(shard, %err, "commitlog trim failed"),
            }
        }
        freed
    }

    pub fn commitlog_bytes(&self) -> u64 {
        self.logs
            .iter()
            .map(|log| log.lock().unwrap().disk_bytes())
            .sum()
    }

    /// Per-topic (name, messages, body bytes).
    pub fn topic_stats(&self) -> Vec<(String, usize, u64)> {
        let topics: Vec<_> = self
            .topics
            .read()
            .unwrap()
            .iter()
            .map(|(k, v)| (k.clone(), Arc::clone(v)))
            .collect();
        topics
            .into_iter()
            .map(|(name, topic)| {
                let topic = topic.read().unwrap();
                let mut messages = 0usize;
                let mut bytes = 0u64;
                for queue in &topic.queues {
                    let queue = queue.read().unwrap();
                    messages += queue.messages.len();
                    bytes += queue
                        .messages
                        .iter()
                        .map(|m| m.body.len() as u64)
                        .sum::<u64>();
                }
                (name, messages, bytes)
            })
            .collect()
    }

    /// (topics, queues, messages, body_bytes)
    pub fn stats(&self) -> (usize, usize, usize, u64) {
        let topics: Vec<_> = self.topics.read().unwrap().values().cloned().collect();
        let mut queues = 0usize;
        let mut messages = 0usize;
        let mut bytes = 0u64;
        for topic in topics {
            let topic = topic.read().unwrap();
            queues += topic.queues.len();
            for queue in &topic.queues {
                let queue = queue.read().unwrap();
                messages += queue.messages.len();
                bytes += queue
                    .messages
                    .iter()
                    .map(|m| m.body.len() as u64)
                    .sum::<u64>();
            }
        }
        (self.topics.read().unwrap().len(), queues, messages, bytes)
    }

    fn repair_cq(&self) -> Result<()> {
        let Some(root) = &self.cq_root else {
            return Ok(());
        };
        let topics: Vec<_> = self
            .topics
            .read()
            .unwrap()
            .iter()
            .map(|(k, v)| (k.clone(), Arc::clone(v)))
            .collect();
        for (topic, state) in topics {
            let state = state.read().unwrap();
            let topic_dir = topic.replace(['/', '\\', ':'], "_");
            for (queue_id, queue) in state.queues.iter().enumerate() {
                let messages = queue.read().unwrap().messages.clone();
                if messages.is_empty() {
                    continue;
                }
                let path = root
                    .join("cq")
                    .join(&topic_dir)
                    .join(format!("{queue_id}.cq"));
                let mut cq = ConsumeQueue::open_with_segment(
                    &path,
                    self.cq_segment_entries.load(Ordering::Relaxed),
                )?;
                if cq.len() as usize == messages.len() {
                    continue;
                }
                tracing::warn!(
                    topic = %topic,
                    queue_id,
                    cq_entries = cq.len(),
                    messages = messages.len(),
                    "repair consume queue"
                );
                cq.truncate()?;
                for msg in messages {
                    let entry = CqEntry {
                        commitlog_offset: msg.physical_offset,
                        msg_size: msg.encode().len() as i32,
                        tag_hash: 0,
                    };
                    cq.append(entry)?;
                }
            }
        }
        Ok(())
    }

    pub fn compact_metadata(&self) -> Result<()> {
        if let Some(log) = &self.index_log {
            let mut log = log.lock().unwrap();
            log.truncate()?;
            let entries: Vec<_> = self
                .msg_index
                .read()
                .unwrap()
                .iter()
                .map(|(id, (topic, queue, offset))| (id.clone(), topic.clone(), *queue, *offset))
                .collect();
            for entry in entries {
                if let Ok(payload) = serde_json::to_vec(&entry) {
                    log.append(&payload)?;
                }
            }
        }
        if let Some(log) = &self.offset_log {
            let mut log = log.lock().unwrap();
            log.truncate()?;
            let entries: Vec<_> = self
                .offsets
                .read()
                .unwrap()
                .iter()
                .map(|(key, offset)| (key.group.clone(), key.topic.clone(), key.queue_id, *offset))
                .collect();
            for entry in entries {
                if let Ok(payload) = serde_json::to_vec(&entry) {
                    log.append(&payload)?;
                }
            }
        }
        Ok(())
    }

    pub fn append(&self, msg: StoredMessage) -> Result<StoredMessage> {
        self.append_internal(msg, true)
    }

    /// Persist a batch in one writer job, then register each message.
    pub fn append_batch(&self, msgs: Vec<StoredMessage>) -> Result<Vec<StoredMessage>> {
        if msgs.is_empty() {
            return Ok(Vec::new());
        }
        let (persisted, encoded_lens) = if let Some(writer) = &self.writer {
            (writer.submit_batch(msgs)?, None)
        } else {
            let shard = msgs[0].queue_id.rem_euclid(self.shards as i32) as usize;
            let mut out = Vec::with_capacity(msgs.len());
            let mut lens = Vec::with_capacity(msgs.len());
            if let Some(log) = self.logs.get(shard) {
                let mut log = log.lock().unwrap();
                let payloads: Vec<Vec<u8>> = msgs.iter().map(|msg| msg.encode()).collect();
                lens.extend(payloads.iter().map(|payload| payload.len()));
                let offsets = log.append_batch(&payloads)?;
                for (mut msg, offset) in msgs.into_iter().zip(offsets) {
                    msg.physical_offset = offset.physical_offset as i64;
                    out.push(msg);
                }
            }
            (out, Some(lens))
        };
        let mut out = Vec::with_capacity(persisted.len());
        for (index, msg) in persisted.into_iter().enumerate() {
            let len = encoded_lens
                .as_ref()
                .and_then(|lens| lens.get(index).copied())
                .unwrap_or_else(|| msg.encode().len());
            let physical = msg.physical_offset;
            out.push(self.register_message(msg, Some(physical), true, len)?);
        }
        Ok(out)
    }

    fn append_internal(&self, mut msg: StoredMessage, persist: bool) -> Result<StoredMessage> {
        let shard = dedup_shard(&msg.topic, &msg.msg_id);
        let dedup_hit = {
            let dedup = self.dedup[shard].lock().unwrap();
            dedup.get(&(msg.topic.clone(), msg.msg_id.clone())).copied()
        };
        if let Some((queue_id, queue_offset)) = dedup_hit {
            let topic_lock = self.topics.read().unwrap().get(&msg.topic).cloned();
            if let Some(topic) = topic_lock {
                let topic = topic.read().unwrap();
                if let Some(existing) = topic.queues.get(queue_id as usize).and_then(|queue| {
                    queue
                        .read()
                        .unwrap()
                        .messages
                        .get(queue_offset as usize)
                        .cloned()
                }) {
                    return Ok(existing);
                }
            }
        }
        let mut persisted_offset = None;
        let mut encoded_len = 0usize;
        if persist {
            if let Some(writer) = &self.writer {
                msg = writer.submit(msg)?;
                persisted_offset = Some(msg.physical_offset);
            } else {
                let shard = msg.queue_id.rem_euclid(self.shards as i32) as usize;
                if let Some(log) = self.logs.get(shard) {
                    let mut log = log.lock().unwrap();
                    msg.physical_offset = log.len() as i64;
                    let payload = msg.encode();
                    encoded_len = payload.len();
                    let offset = log.append(&payload)?;
                    msg.physical_offset = offset.physical_offset as i64;
                    persisted_offset = Some(msg.physical_offset);
                    self.append_count.fetch_add(1, Ordering::Relaxed);
                }
            }
        }
        self.register_message(msg, persisted_offset, persist, encoded_len)
    }

    fn register_message(
        &self,
        mut msg: StoredMessage,
        persisted_offset: Option<i64>,
        persist: bool,
        encoded_len: usize,
    ) -> Result<StoredMessage> {
        let shard = dedup_shard(&msg.topic, &msg.msg_id);
        let topic_lock = {
            let existing = self.topics.read().unwrap().get(&msg.topic).cloned();
            match existing {
                Some(topic) => topic,
                None => {
                    let mut topics = self.topics.write().unwrap();
                    Arc::clone(topics.entry(msg.topic.clone()).or_insert_with(|| {
                        Arc::new(RwLock::new(TopicState {
                            queues: (0..self.queue_nums)
                                .map(|_| {
                                    RwLock::new(QueueState {
                                        messages: Vec::new(),
                                        base_offset: 0,
                                    })
                                })
                                .collect(),
                        }))
                    }))
                }
            }
        };
        let topic = topic_lock.read().unwrap();
        if msg.queue_id < 0 || msg.queue_id >= topic.queues.len() as i32 {
            return Err(MqError::Invalid(format!(
                "queue id {} out of range for topic {}",
                msg.queue_id, msg.topic
            )));
        }
        let mut queue = topic.queues[msg.queue_id as usize].write().unwrap();
        msg.queue_offset = queue.base_offset + queue.messages.len() as i64;
        if let Some(offset) = persisted_offset {
            msg.physical_offset = offset;
        } else if persist {
            msg.physical_offset = self.next_physical.fetch_add(1, Ordering::Relaxed);
        }
        queue.messages.push(msg.clone());
        self.append_cq(&msg, encoded_len);
        drop(queue);
        drop(topic);
        self.dedup[shard].lock().unwrap().insert(
            (msg.topic.clone(), msg.msg_id.clone()),
            (msg.queue_id, msg.queue_offset),
        );
        self.msg_index.write().unwrap().insert(
            msg.msg_id.clone(),
            (msg.topic.clone(), msg.queue_id, msg.queue_offset),
        );
        if let Some(log) = &self.index_log {
            if let Ok(payload) = serde_json::to_vec(&(
                msg.msg_id.clone(),
                msg.topic.clone(),
                msg.queue_id,
                msg.queue_offset,
            )) {
                let mut log = log.lock().unwrap();
                let _ = log.append(&payload);
            }
        }
        let ack_events_before = self.replica_ack_events.load(Ordering::Relaxed);
        if self.replica_active.load(Ordering::Relaxed) {
            let replica_payload = msg.encode();
            self.broadcast_replica(&replica_payload);
        }
        if persist && self.sync_replicate.load(Ordering::Relaxed) {
            let deadline = Instant::now()
                + Duration::from_millis(self.replica_timeout_ms.load(Ordering::Relaxed));
            let quorum = self.replica_quorum.load(Ordering::Relaxed).max(1) as i64;
            while self.replica_ack_events.load(Ordering::Relaxed) < ack_events_before + quorum {
                if Instant::now() >= deadline {
                    return Err(MqError::Store("replica ack timeout".into()));
                }
                std::thread::sleep(Duration::from_millis(1));
            }
        }
        self.notify.notify_waiters();
        Ok(msg)
    }

    pub fn message_by_id(&self, msg_id: &str) -> Option<StoredMessage> {
        let (topic, queue_id, queue_offset) =
            self.msg_index.read().unwrap().get(msg_id).cloned()?;
        let topic = self.topics.read().unwrap().get(&topic).cloned()?;
        let topic = topic.read().unwrap();
        let queue = topic.queues.get(queue_id as usize)?;
        let queue = queue.read().unwrap();
        queue
            .messages
            .get((queue_offset - queue.base_offset) as usize)
            .cloned()
    }

    fn cq_path(&self, topic: &str, queue_id: i32) -> Option<PathBuf> {
        let root = self.cq_root.as_ref()?;
        let topic_dir = topic.replace(['/', '\\', ':'], "_");
        Some(
            root.join("cq")
                .join(topic_dir)
                .join(format!("{queue_id}.cq")),
        )
    }

    pub fn set_replica_writer(&self, tx: Sender<ReplicaCmd>) {
        *self.replica_tx.lock().unwrap() = Some(tx);
    }

    pub fn add_replica(&self, mut stream: std::net::TcpStream) {
        self.replica_active.store(true, Ordering::Relaxed);
        let _ = stream.set_nodelay(true);
        let _ = stream.set_write_timeout(Some(Duration::from_millis(200)));
        self.replay_to(&mut stream);
        if let Ok(mut reader) = stream.try_clone() {
            let acked = Arc::clone(&self.replica_acked);
            let ack_events = Arc::clone(&self.replica_ack_events);
            std::thread::spawn(move || {
                use std::io::Read;
                loop {
                    let mut buf = [0u8; 8];
                    if reader.read_exact(&mut buf).is_err() {
                        break;
                    }
                    acked.store(u64::from_be_bytes(buf) as i64, Ordering::Relaxed);
                    ack_events.fetch_add(1, Ordering::Relaxed);
                }
            });
        }
        if let Some(tx) = self.replica_tx.lock().unwrap().as_ref() {
            let _ = tx.send(ReplicaCmd::Add(stream));
            return;
        }
        self.replicas.lock().unwrap().push(stream);
    }

    pub fn set_cq_segment_entries(&self, entries: u64) {
        self.cq_segment_entries
            .store(entries.max(1), Ordering::Relaxed);
    }

    pub fn set_sync_replicate(&self, enabled: bool, timeout_ms: u64) {
        self.sync_replicate.store(enabled, Ordering::Relaxed);
        self.replica_timeout_ms
            .store(timeout_ms.max(1), Ordering::Relaxed);
    }

    pub fn set_replica_quorum(&self, quorum: usize) {
        self.replica_quorum.store(quorum.max(1), Ordering::Relaxed);
    }

    pub fn replica_acked(&self) -> i64 {
        self.replica_acked.load(Ordering::Relaxed)
    }

    fn replay_to(&self, stream: &mut std::net::TcpStream) {
        use std::io::Write;
        let topics: Vec<_> = self.topics.read().unwrap().values().cloned().collect();
        for topic in topics {
            let topic = topic.read().unwrap();
            for queue in &topic.queues {
                let messages = queue.read().unwrap().messages.clone();
                for msg in messages {
                    let payload = msg.encode();
                    let mut frame = Vec::with_capacity(4 + payload.len());
                    frame.extend_from_slice(&(payload.len() as u32).to_be_bytes());
                    frame.extend_from_slice(&payload);
                    if stream.write_all(&frame).is_err() {
                        return;
                    }
                }
            }
        }
    }

    fn broadcast_replica(&self, payload: &[u8]) {
        if let Some(tx) = self.replica_tx.lock().unwrap().as_ref() {
            let _ = tx.send(ReplicaCmd::Frame(payload.to_vec()));
            return;
        }
        let mut replicas = self.replicas.lock().unwrap();
        if replicas.is_empty() {
            return;
        }
        let mut frame = Vec::with_capacity(4 + payload.len());
        frame.extend_from_slice(&(payload.len() as u32).to_be_bytes());
        frame.extend_from_slice(payload);
        replicas.retain_mut(|stream| {
            use std::io::Write;
            stream.write_all(&frame).is_ok()
        });
    }

    fn append_cq(&self, msg: &StoredMessage, encoded_len: usize) {
        let Some(path) = self.cq_path(&msg.topic, msg.queue_id) else {
            return;
        };
        let mut map = self.cq.write().unwrap();
        if let std::collections::hash_map::Entry::Vacant(entry) =
            map.entry((msg.topic.clone(), msg.queue_id))
        {
            match ConsumeQueue::open_with_segment(
                &path,
                self.cq_segment_entries.load(Ordering::Relaxed),
            ) {
                Ok(cq) => {
                    entry.insert(Mutex::new(cq));
                }
                Err(err) => {
                    tracing::warn!(error = %err, "open consume queue failed");
                    return;
                }
            }
        }
        let Some(cq) = map.get(&(msg.topic.clone(), msg.queue_id)) else {
            return;
        };
        let mut cq = cq.lock().unwrap();
        if cq.len() as i64 <= msg.queue_offset {
            let entry = CqEntry {
                commitlog_offset: msg.physical_offset,
                msg_size: if encoded_len == 0 {
                    msg.encode().len() as i32
                } else {
                    encoded_len as i32
                },
                tag_hash: 0,
            };
            if let Err(err) = cq.append(entry) {
                tracing::warn!(error = %err, "append consume queue failed");
            }
        }
    }

    pub fn prepare_half(&self, mut msg: StoredMessage) -> Result<StoredMessage> {
        if msg.queue_id < 0 || msg.queue_id >= self.queue_nums {
            msg.queue_id = 0;
        }
        msg.queue_offset = 0;
        msg.physical_offset = 0;
        let created_at_ms = mq_common::now_ms();
        if let Some(existing) = self.half.read().unwrap().get(&msg.msg_id) {
            return Ok(existing.msg.clone());
        }
        self.persist_half_event(&msg, "prepared", created_at_ms)?;
        self.half.write().unwrap().insert(
            msg.msg_id.clone(),
            HalfEntry {
                msg: msg.clone(),
                created_at_ms,
                check_count: 0,
                last_check_ms: 0,
            },
        );
        Ok(msg)
    }

    pub fn find_half(&self, tx_id: &str, msg_id: &str) -> Option<StoredMessage> {
        let half = self.half.read().unwrap();
        half.get(tx_id)
            .or_else(|| half.get(msg_id))
            .map(|entry| entry.msg.clone())
    }

    pub fn commit_half(&self, tx_id: &str, msg_id: &str) -> Result<Option<StoredMessage>> {
        let mut half = self.half.write().unwrap();
        let key = if half.contains_key(tx_id) {
            tx_id.to_string()
        } else if half.contains_key(msg_id) {
            msg_id.to_string()
        } else {
            return Ok(None);
        };
        let entry = half.remove(&key).unwrap();
        drop(half);
        self.persist_half_event(&entry.msg, "committed", entry.created_at_ms)?;
        self.append(entry.msg).map(Some)
    }

    pub fn pending_half_count(&self) -> usize {
        self.half.read().unwrap().len()
    }

    pub fn rollback_half(&self, tx_id: &str, msg_id: &str) -> bool {
        let mut half = self.half.write().unwrap();
        let key = if half.contains_key(tx_id) {
            tx_id.to_string()
        } else if half.contains_key(msg_id) {
            msg_id.to_string()
        } else {
            return false;
        };
        let entry = half.remove(&key).unwrap();
        drop(half);
        let _ = self.persist_half_event(&entry.msg, "rolled_back", entry.created_at_ms);
        true
    }

    /// Roll back half messages older than `timeout_ms`. Returns affected tx ids.
    pub fn expire_half(&self, timeout_ms: i64) -> Vec<String> {
        let now = mq_common::now_ms();
        let expired: Vec<String> = self
            .half
            .read()
            .unwrap()
            .iter()
            .filter(|(_, entry)| now.saturating_sub(entry.created_at_ms) >= timeout_ms)
            .map(|(id, _)| id.clone())
            .collect();
        for id in &expired {
            self.rollback_half(id, id);
        }
        expired
    }

    pub fn schedule_delay(&self, msg: StoredMessage, due_at_ms: i64) {
        self.persist_delay_event(&msg, due_at_ms, "scheduled");
        self.delayed
            .write()
            .unwrap()
            .insert(msg.msg_id.clone(), (msg, due_at_ms));
    }

    pub fn due_delayed(&self, now_ms: i64) -> Vec<StoredMessage> {
        let due: Vec<String> = self
            .delayed
            .read()
            .unwrap()
            .iter()
            .filter(|(_, (_, due))| *due <= now_ms)
            .map(|(id, _)| id.clone())
            .collect();
        let mut out = Vec::with_capacity(due.len());
        let mut delayed = self.delayed.write().unwrap();
        for id in due {
            if let Some((msg, due_at_ms)) = delayed.remove(&id) {
                self.persist_delay_event(&msg, due_at_ms, "delivered");
                out.push(msg);
            }
        }
        out
    }

    fn persist_delay_event(&self, msg: &StoredMessage, due_at_ms: i64, state: &str) {
        let Some(log) = &self.delay_log else {
            return;
        };
        let event = DelayEvent {
            msg: msg.clone(),
            due_at_ms,
            state: state.to_string(),
        };
        if let Ok(payload) = serde_json::to_vec(&event) {
            let mut log = log.lock().unwrap();
            let _ = log.append(&payload);
        }
    }

    /// Half messages due for another check-back.
    pub fn due_half(&self, min_age_ms: i64, backoff_ms: i64, max_check: u32) -> Vec<StoredMessage> {
        let now = mq_common::now_ms();
        self.half
            .read()
            .unwrap()
            .values()
            .filter(|entry| {
                entry.check_count < max_check
                    && now.saturating_sub(entry.created_at_ms) >= min_age_ms
                    && (entry.last_check_ms == 0
                        || now.saturating_sub(entry.last_check_ms) >= backoff_ms)
            })
            .map(|entry| entry.msg.clone())
            .collect()
    }

    pub fn mark_checked(&self, tx_id: &str) {
        if let Some(entry) = self.half.write().unwrap().get_mut(tx_id) {
            entry.check_count = entry.check_count.saturating_add(1);
            entry.last_check_ms = mq_common::now_ms();
        }
    }

    pub fn pending_half(&self) -> Vec<HalfStatus> {
        let now = mq_common::now_ms();
        self.half
            .read()
            .unwrap()
            .iter()
            .map(|(id, entry)| HalfStatus {
                tx_id: id.clone(),
                topic: entry.msg.topic.clone(),
                created_at_ms: entry.created_at_ms,
                check_count: entry.check_count,
                age_ms: now.saturating_sub(entry.created_at_ms),
            })
            .collect()
    }

    pub fn pending_count(&self) -> usize {
        self.half.read().unwrap().len()
    }

    /// Roll back Half messages whose check count reached `max_check`.
    pub fn expire_exhausted(&self, max_check: u32) -> Vec<String> {
        let expired: Vec<String> = self
            .half
            .read()
            .unwrap()
            .iter()
            .filter(|(_, entry)| entry.check_count >= max_check)
            .map(|(id, _)| id.clone())
            .collect();
        for id in &expired {
            self.rollback_half(id, id);
        }
        expired
    }

    fn persist_half_event(
        &self,
        msg: &StoredMessage,
        state: &str,
        created_at_ms: i64,
    ) -> Result<()> {
        let Some(log) = &self.half_log else {
            return Ok(());
        };
        let event = HalfEvent {
            msg: msg.clone(),
            state: state.to_string(),
            created_at_ms,
        };
        let payload = serde_json::to_vec(&event)
            .map_err(|e| MqError::Store(format!("encode half event: {e}")))?;
        let mut log = log.lock().unwrap();
        log.append(&payload)?;
        log.sync()?;
        Ok(())
    }

    pub fn sync(&self) -> Result<()> {
        if let Some(writer) = &self.writer {
            writer.sync()?;
        } else {
            for log in &self.logs {
                let mut log = log.lock().unwrap();
                log.sync()?;
            }
        }
        if let Some(half) = &self.half_log {
            let mut half = half.lock().unwrap();
            half.sync()?;
        }
        if let Some(retry) = &self.retry_log {
            let mut retry = retry.lock().unwrap();
            retry.sync()?;
        }
        if let Some(delay) = &self.delay_log {
            let mut delay = delay.lock().unwrap();
            delay.sync()?;
        }
        Ok(())
    }

    pub fn register_subscription(
        &self,
        group: &str,
        topic: &str,
        sub_string: &str,
        expression_type: &str,
    ) {
        self.subscriptions.write().unwrap().insert(
            (group.to_string(), topic.to_string()),
            (sub_string.to_string(), expression_type.to_string()),
        );
    }

    pub fn subscription(&self, group: &str, topic: &str) -> Option<(String, String)> {
        self.subscriptions
            .read()
            .unwrap()
            .get(&(group.to_string(), topic.to_string()))
            .cloned()
    }

    pub fn register_consumer(&self, group: &str, client_id: &str) {
        self.consumers
            .write()
            .unwrap()
            .entry(group.to_string())
            .or_default()
            .insert(client_id.to_string(), Instant::now());
    }

    pub fn register_consumer_addr(&self, group: &str, client_id: &str, addr: &str) {
        self.register_consumer(group, client_id);
        if !addr.is_empty() {
            self.consumer_addrs
                .write()
                .unwrap()
                .insert(client_id.to_string(), addr.to_string());
        }
    }

    pub fn consumer_addr(&self, client_id: &str) -> String {
        self.consumer_addrs
            .read()
            .unwrap()
            .get(client_id)
            .cloned()
            .unwrap_or_else(|| "127.0.0.1:0".to_string())
    }

    pub fn register_producer(&self, group: &str, client_id: &str, addr: &str) {
        self.producers
            .write()
            .unwrap()
            .entry(group.to_string())
            .or_default()
            .insert(client_id.to_string(), Instant::now());
        if !addr.is_empty() {
            self.producer_addrs
                .write()
                .unwrap()
                .insert(client_id.to_string(), addr.to_string());
        }
    }

    pub fn producer_clients(&self, group: &str) -> Vec<String> {
        self.producers
            .read()
            .unwrap()
            .get(group)
            .map(|ids| ids.keys().cloned().collect())
            .unwrap_or_default()
    }

    pub fn producer_addr(&self, client_id: &str) -> String {
        self.producer_addrs
            .read()
            .unwrap()
            .get(client_id)
            .cloned()
            .unwrap_or_else(|| "127.0.0.1:0".to_string())
    }

    pub fn unregister_client(&self, client_id: &str) {
        let mut groups = self.consumers.write().unwrap();
        for ids in groups.values_mut() {
            ids.remove(client_id);
        }
        groups.retain(|_, ids| !ids.is_empty());
        self.consumer_addrs.write().unwrap().remove(client_id);
        let mut producers = self.producers.write().unwrap();
        for ids in producers.values_mut() {
            ids.remove(client_id);
        }
        producers.retain(|_, ids| !ids.is_empty());
        self.producer_addrs.write().unwrap().remove(client_id);
    }

    pub fn consumer_ids(&self, group: &str) -> Vec<String> {
        let now = Instant::now();
        self.consumers
            .read()
            .unwrap()
            .get(group)
            .map(|ids| {
                ids.iter()
                    .filter(|(_, seen)| now.duration_since(**seen) < Duration::from_secs(60))
                    .map(|(id, _)| id.clone())
                    .collect()
            })
            .unwrap_or_default()
    }

    pub async fn wait_for_message(&self) {
        self.notify.notified().await;
    }

    fn read_from_cq(
        &self,
        topic: &str,
        queue_id: i32,
        offset: i64,
        max: usize,
    ) -> Option<Result<(Vec<StoredMessage>, i64, i64)>> {
        if max == 0 {
            return None;
        }
        let path = self.cq_path(topic, queue_id)?;
        let mut cq =
            ConsumeQueue::open_with_segment(&path, self.cq_segment_entries.load(Ordering::Relaxed))
                .ok()?;
        if cq.is_empty() {
            return None;
        }
        let base = self
            .topics
            .read()
            .unwrap()
            .get(topic)
            .cloned()
            .and_then(|t| {
                t.read()
                    .unwrap()
                    .queues
                    .get(queue_id as usize)
                    .map(|q| q.read().unwrap().base_offset)
            })
            .unwrap_or(0);
        let len = base + cq.len() as i64;
        let start = offset.max(base).min(len);
        let entries = cq.read((start - base) as u64, max).ok()?;
        let mut out = Vec::with_capacity(entries.len());
        // A queue is always appended to the same CommitLog shard.
        let shard = queue_id.rem_euclid(self.shards as i32) as usize;
        let log = self.logs.get(shard)?;
        let mut log = log.lock().unwrap();
        for (i, entry) in entries.into_iter().enumerate() {
            let payload = match log.read_at(entry.commitlog_offset.max(0) as u64) {
                Ok(payload) => payload,
                Err(_) => return None,
            };
            match StoredMessage::decode(&payload) {
                Ok((mut msg, _)) => {
                    msg.queue_offset = start + i as i64;
                    msg.queue_id = queue_id;
                    msg.physical_offset = entry.commitlog_offset;
                    out.push(msg);
                }
                Err(_) => return None,
            }
        }
        Some(Ok((out, start, len)))
    }

    pub fn read(
        &self,
        topic: &str,
        queue_id: i32,
        offset: i64,
        max: usize,
    ) -> Result<(Vec<StoredMessage>, i64, i64)> {
        let topic_lock = self.topics.read().unwrap().get(topic).cloned();
        let Some(t) = topic_lock else {
            return Ok((Vec::new(), 0, 0));
        };
        if let Some(result) = self.read_from_cq(topic, queue_id, offset, max) {
            return result;
        }
        let t = t.read().unwrap();
        if queue_id < 0 || queue_id >= t.queues.len() as i32 {
            return Err(MqError::Invalid(format!(
                "queue id {queue_id} out of range for topic {topic}"
            )));
        }
        let queue = t.queues[queue_id as usize].read().unwrap();
        let base = queue.base_offset;
        let len = base + queue.messages.len() as i64;
        let start = offset.max(base).min(len);
        let start_rel = (start - base) as usize;
        let end = (start_rel + max).min(queue.messages.len());
        Ok((queue.messages[start_rel..end].to_vec(), start, len))
    }

    pub fn max_offset(&self, topic: &str, queue_id: i32) -> i64 {
        let topic_lock = self.topics.read().unwrap().get(topic).cloned();
        topic_lock
            .and_then(|t| {
                t.read().unwrap().queues.get(queue_id as usize).map(|q| {
                    let q = q.read().unwrap();
                    q.base_offset + q.messages.len() as i64
                })
            })
            .unwrap_or(0)
    }

    pub fn find_by_msg_id(&self, msg_id: &str) -> Option<StoredMessage> {
        if let Some((topic, queue_id, offset)) = self.msg_index.read().unwrap().get(msg_id).cloned()
        {
            if let Some(topic) = self.topics.read().unwrap().get(&topic).cloned() {
                let topic = topic.read().unwrap();
                if let Some(queue) = topic.queues.get(queue_id as usize) {
                    let queue = queue.read().unwrap();
                    let rel = offset - queue.base_offset;
                    if rel >= 0 {
                        if let Some(msg) = queue.messages.get(rel as usize) {
                            return Some(msg.clone());
                        }
                    }
                }
            }
        }
        let topics: Vec<_> = self.topics.read().unwrap().values().cloned().collect();
        for topic in topics {
            for queue in &topic.read().unwrap().queues {
                let queue = queue.read().unwrap();
                if let Some(msg) = queue.messages.iter().find(|m| m.msg_id == msg_id) {
                    return Some(msg.clone());
                }
            }
        }
        None
    }

    /// Monotonic retry counter per original message. The Java client always
    /// sends `originMsgId` of the *original* message, so incrementing the
    /// origin's stored `reconsume_times` would loop at 1 forever.
    pub fn next_retry_times(&self, origin_msg_id: &str, base: i32) -> i32 {
        let mut counts = self.retry_counts.write().unwrap();
        let current = counts.get(origin_msg_id).copied().unwrap_or(base);
        let next = current.saturating_add(1);
        counts.insert(origin_msg_id.to_string(), next);
        if let Some(log) = &self.retry_log {
            if let Ok(payload) = serde_json::to_vec(&(origin_msg_id.to_string(), next)) {
                let mut log = log.lock().unwrap();
                let _ = log.append(&payload);
            }
        }
        next
    }

    pub fn min_offset(&self, topic: &str, queue_id: i32) -> i64 {
        self.topics
            .read()
            .unwrap()
            .get(topic)
            .cloned()
            .and_then(|t| {
                t.read()
                    .unwrap()
                    .queues
                    .get(queue_id as usize)
                    .map(|q| q.read().unwrap().base_offset)
            })
            .unwrap_or(0)
    }

    pub fn consumer_progress(&self, group: &str) -> Vec<(String, i32, i64, i64)> {
        let entries: Vec<(String, i32, i64)> = self
            .offsets
            .read()
            .unwrap()
            .iter()
            .filter(|(k, _)| k.group == group)
            .map(|(k, offset)| (k.topic.clone(), k.queue_id, *offset))
            .collect();
        let topics: HashMap<String, Arc<RwLock<TopicState>>> = self
            .topics
            .read()
            .unwrap()
            .iter()
            .map(|(k, v)| (k.clone(), Arc::clone(v)))
            .collect();
        entries
            .into_iter()
            .map(|(topic, queue_id, consumer_offset)| {
                let broker_offset = topics
                    .get(&topic)
                    .and_then(|t| {
                        t.read().unwrap().queues.get(queue_id as usize).map(|q| {
                            let q = q.read().unwrap();
                            q.base_offset + q.messages.len() as i64
                        })
                    })
                    .unwrap_or(0);
                (topic, queue_id, consumer_offset, broker_offset)
            })
            .collect()
    }

    pub fn query_offset(&self, group: &str, topic: &str, queue_id: i32) -> i64 {
        self.offsets
            .read()
            .unwrap()
            .get(&OffsetKey {
                group: group.to_string(),
                topic: topic.to_string(),
                queue_id,
            })
            .copied()
            .unwrap_or(0)
    }

    pub fn consumer_offsets(&self, group: &str) -> Vec<(String, i32, i64)> {
        self.offsets
            .read()
            .unwrap()
            .iter()
            .filter(|(key, _)| key.group == group)
            .map(|(key, offset)| (key.topic.clone(), key.queue_id, *offset))
            .collect()
    }

    pub fn consumer_clients(&self, group: &str) -> Vec<String> {
        self.consumers
            .read()
            .unwrap()
            .get(group)
            .map(|clients| clients.keys().cloned().collect())
            .unwrap_or_default()
    }

    pub fn topic_names(&self) -> Vec<String> {
        self.topics.read().unwrap().keys().cloned().collect()
    }

    pub fn subscription_groups(&self) -> Vec<String> {
        let mut groups: Vec<String> = self
            .consumers
            .read()
            .unwrap()
            .keys()
            .cloned()
            .chain(
                self.subscriptions
                    .read()
                    .unwrap()
                    .keys()
                    .map(|(g, _)| g.clone()),
            )
            .chain(self.extra_groups.read().unwrap().iter().cloned())
            .collect();
        groups.sort();
        groups.dedup();
        groups
    }

    pub fn create_subscription_group(&self, group: &str) {
        self.extra_groups.write().unwrap().insert(group.to_string());
        self.persist_admin_meta();
    }

    pub fn delete_subscription_group(&self, group: &str) {
        self.extra_groups.write().unwrap().remove(group);
        self.consumers.write().unwrap().remove(group);
        self.subscriptions
            .write()
            .unwrap()
            .retain(|(g, _), _| g != group);
        self.offsets
            .write()
            .unwrap()
            .retain(|key, _| key.group != group);
        self.persist_admin_meta();
    }

    fn admin_meta_path(&self) -> Option<PathBuf> {
        self.cq_root
            .as_ref()
            .map(|root| root.join("admin_meta.json"))
    }

    fn persist_admin_meta(&self) {
        let Some(path) = self.admin_meta_path() else {
            return;
        };
        let body = serde_json::json!({
            "topics": self.topic_names(),
            "groups": self.subscription_groups(),
        });
        if let Ok(bytes) = serde_json::to_vec(&body) {
            let _ = std::fs::write(path, bytes);
        }
    }

    pub fn load_admin_meta(&self) {
        let Some(path) = self.admin_meta_path() else {
            return;
        };
        let Ok(bytes) = std::fs::read(path) else {
            return;
        };
        let Ok(value) = serde_json::from_slice::<serde_json::Value>(&bytes) else {
            return;
        };
        if let Some(topics) = value.get("topics").and_then(|v| v.as_array()) {
            for topic in topics.iter().filter_map(|v| v.as_str()) {
                let mut topics = self.topics.write().unwrap();
                topics.entry(topic.to_string()).or_insert_with(|| {
                    Arc::new(RwLock::new(TopicState {
                        queues: (0..self.queue_nums)
                            .map(|_| {
                                RwLock::new(QueueState {
                                    messages: Vec::new(),
                                    base_offset: 0,
                                })
                            })
                            .collect(),
                    }))
                });
            }
        }
        if let Some(groups) = value.get("groups").and_then(|v| v.as_array()) {
            let mut extra = self.extra_groups.write().unwrap();
            for group in groups.iter().filter_map(|v| v.as_str()) {
                extra.insert(group.to_string());
            }
        }
    }

    pub fn create_topic(&self, topic: &str) {
        let mut topics = self.topics.write().unwrap();
        if topics.contains_key(topic) {
            return;
        }
        topics.insert(
            topic.to_string(),
            Arc::new(RwLock::new(TopicState {
                queues: (0..self.queue_nums)
                    .map(|_| {
                        RwLock::new(QueueState {
                            messages: Vec::new(),
                            base_offset: 0,
                        })
                    })
                    .collect(),
            })),
        );
        drop(topics);
        self.persist_admin_meta();
    }

    pub fn delete_topic(&self, topic: &str) {
        self.topics.write().unwrap().remove(topic);
        self.msg_index
            .write()
            .unwrap()
            .retain(|_, (t, _, _)| t != topic);
        for dedup in &self.dedup {
            dedup.lock().unwrap().retain(|(t, _), _| t != topic);
        }
        self.offsets
            .write()
            .unwrap()
            .retain(|key, _| key.topic != topic);
        self.cq.write().unwrap().retain(|(t, _), _| t != topic);
        if let Some(root) = &self.cq_root {
            let topic_dir = root.join("cq").join(topic.replace(['/', '\\', ':'], "_"));
            let _ = std::fs::remove_dir_all(topic_dir);
        }
        self.persist_admin_meta();
    }

    pub fn subscriptions_for_group(&self, group: &str) -> Vec<(String, String, String)> {
        self.subscriptions
            .read()
            .unwrap()
            .iter()
            .filter(|((g, _), _)| g == group)
            .map(|((_, topic), (sub, expr))| (topic.clone(), sub.clone(), expr.clone()))
            .collect()
    }

    pub fn update_offset(&self, group: &str, topic: &str, queue_id: i32, offset: i64) {
        let offset = offset.max(0);
        self.offsets.write().unwrap().insert(
            OffsetKey {
                group: group.to_string(),
                topic: topic.to_string(),
                queue_id,
            },
            offset,
        );
        if let Some(log) = &self.offset_log {
            if let Ok(payload) =
                serde_json::to_vec(&(group.to_string(), topic.to_string(), queue_id, offset))
            {
                let mut log = log.lock().unwrap();
                let _ = log.append(&payload);
            }
        }
    }
}

pub fn run_replica_writer(rx: Receiver<ReplicaCmd>) {
    use std::io::Write;
    let mut streams: Vec<std::net::TcpStream> = Vec::new();
    while let Ok(cmd) = rx.recv() {
        match cmd {
            ReplicaCmd::Add(stream) => streams.push(stream),
            ReplicaCmd::Frame(payload) => {
                let mut frame = Vec::with_capacity(4 + payload.len());
                frame.extend_from_slice(&(payload.len() as u32).to_be_bytes());
                frame.extend_from_slice(&payload);
                streams.retain_mut(|stream| stream.write_all(&frame).is_ok());
            }
        }
    }
}

fn dedup_shard(topic: &str, msg_id: &str) -> usize {
    use std::hash::{Hash, Hasher};
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    topic.hash(&mut hasher);
    msg_id.hash(&mut hasher);
    (hasher.finish() as usize) % 16
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;

    fn sample(topic: &str, queue_id: i32) -> StoredMessage {
        StoredMessage {
            msg_id: "00112233445566778899AABBCCDDEEFF".into(),
            topic: topic.into(),
            queue_id,
            queue_offset: 0,
            physical_offset: 0,
            sys_flag: 0,
            flag: 0,
            born_timestamp: 1,
            born_host: [127, 0, 0, 1],
            born_port: 0,
            store_timestamp: 1,
            store_host: [127, 0, 0, 1],
            store_port: 10921,
            reconsume_times: 0,
            prepared_transaction_offset: 0,
            body: Bytes::from_static(b"hello"),
            properties: String::new(),
        }
    }

    #[test]
    fn message_index_survives_reopen() {
        let dir = std::env::temp_dir().join(format!(
            "mq-rust-index-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_millis()
        ));
        let _ = std::fs::remove_dir_all(&dir);
        {
            let store = BrokerStore::open(&dir, 1).unwrap();
            let mut msg = sample("orders", 0);
            msg.msg_id = "IDX-MSG".into();
            store.append(msg).unwrap();
        }
        let store = BrokerStore::open(&dir, 1).unwrap();
        let found = store.find_by_msg_id("IDX-MSG").expect("index hit");
        assert_eq!(found.msg_id, "IDX-MSG");
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn consume_queue_persists_offsets() {
        let dir = std::env::temp_dir().join(format!(
            "mq-rust-cq-persist-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_millis()
        ));
        let _ = std::fs::remove_dir_all(&dir);
        let store = BrokerStore::open(&dir, 1).unwrap();
        let mut a = sample("orders", 0);
        a.msg_id = "CQ-A".into();
        store.append(a).unwrap();
        let mut b = sample("orders", 0);
        b.msg_id = "CQ-B".into();
        store.append(b).unwrap();
        drop(store);

        let store = BrokerStore::open(&dir, 1).unwrap();
        let mut c = sample("orders", 0);
        c.msg_id = "CQ-C".into();
        store.append(c).unwrap();
        let mut cq = ConsumeQueue::open(dir.join("cq").join("orders").join("0.cq")).unwrap();
        assert_eq!(cq.len(), 3);
        let entries = cq.read(0, 10).unwrap();
        assert_eq!(entries.len(), 3);
        let (msgs, start, len) = store.read("orders", 0, 0, 10).unwrap();
        assert_eq!((start, len, msgs.len()), (0, 3, 3));
        for (i, msg) in msgs.iter().enumerate() {
            assert_eq!(msg.queue_offset, i as i64);
        }
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn consume_queue_multi_shard_read() {
        let dir = std::env::temp_dir().join(format!(
            "mq-rust-cq-shard-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_millis()
        ));
        let _ = std::fs::remove_dir_all(&dir);
        let store = BrokerStore::open_sharded(&dir, 4, 2).unwrap();
        let mut a = sample("orders", 1);
        a.msg_id = "CQ-S1".into();
        store.append(a).unwrap();
        let mut b = sample("orders", 3);
        b.msg_id = "CQ-S3".into();
        store.append(b).unwrap();
        let (one, _, len1) = store.read("orders", 1, 0, 10).unwrap();
        let (three, _, len3) = store.read("orders", 3, 0, 10).unwrap();
        assert_eq!((len1, one.len()), (1, 1));
        assert_eq!((len3, three.len()), (1, 1));
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn consume_queue_repair_on_open() {
        let dir = std::env::temp_dir().join(format!(
            "mq-rust-cq-repair-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_millis()
        ));
        let _ = std::fs::remove_dir_all(&dir);
        {
            let store = BrokerStore::open(&dir, 1).unwrap();
            for i in 0..2 {
                let mut msg = sample("orders", 0);
                msg.msg_id = format!("REPAIR-{i}");
                store.append(msg).unwrap();
            }
        }
        let path = dir.join("cq").join("orders").join("0.cq");
        {
            let mut cq = ConsumeQueue::open(&path).unwrap();
            cq.append(CqEntry {
                commitlog_offset: 0,
                msg_size: 1,
                tag_hash: 0,
            })
            .unwrap();
            assert_eq!(cq.len(), 3);
        }
        let store = BrokerStore::open(&dir, 1).unwrap();
        let (msgs, _, len) = store.read("orders", 0, 0, 10).unwrap();
        assert_eq!(len, 2);
        assert_eq!(msgs.len(), 2);
        let cq = ConsumeQueue::open(&path).unwrap();
        assert_eq!(cq.len(), 2);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn retention_advances_base_offset() {
        let dir = std::env::temp_dir().join(format!(
            "mq-rust-retain-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_millis()
        ));
        let _ = std::fs::remove_dir_all(&dir);
        let store = BrokerStore::open(&dir, 1).unwrap();
        for i in 0..10 {
            let mut msg = sample("orders", 0);
            msg.msg_id = format!("RET-{i}");
            store.append(msg).unwrap();
        }
        assert_eq!(store.retain(5, 0, 0).unwrap(), 5);
        assert_eq!(store.retained_dropped(), 5);
        assert_eq!(store.min_offset("orders", 0), 5);
        let (msgs, start, len) = store.read("orders", 0, 0, 10).unwrap();
        assert_eq!((start, len, msgs.len()), (5, 10, 5));
        assert_eq!(msgs[0].queue_offset, 5);
        assert_eq!(msgs[4].queue_offset, 9);
        let cq_path = dir.join("cq").join("orders").join("0.cq");
        let cq = ConsumeQueue::open(cq_path).unwrap();
        assert_eq!(cq.len(), 5);
        assert!(store.find_by_msg_id("RET-0").is_none());
        assert!(store.find_by_msg_id("RET-9").is_some());
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn retention_by_bytes() {
        let dir = std::env::temp_dir().join(format!(
            "mq-rust-retain-bytes-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_millis()
        ));
        let _ = std::fs::remove_dir_all(&dir);
        let store = BrokerStore::open(&dir, 1).unwrap();
        for i in 0..10 {
            let mut msg = sample("bytes", 0);
            msg.msg_id = format!("BYTES-{i}");
            msg.body = bytes::Bytes::from_static(b"12345");
            store.append(msg).unwrap();
        }
        assert_eq!(store.retain(0, 15, 0).unwrap(), 7);
        assert_eq!(store.retained_dropped(), 7);
        let (msgs, start, len) = store.read("bytes", 0, 0, 10).unwrap();
        assert_eq!((start, len, msgs.len()), (7, 10, 3));
        assert_eq!(store.min_offset("bytes", 0), 7);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn retention_by_age() {
        let dir = std::env::temp_dir().join(format!(
            "mq-rust-retain-age-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_millis()
        ));
        let _ = std::fs::remove_dir_all(&dir);
        let store = BrokerStore::open(&dir, 1).unwrap();
        for i in 0..10 {
            let mut msg = sample("age", 0);
            msg.msg_id = format!("AGE-{i}");
            if i >= 7 {
                msg.born_timestamp = mq_common::now_ms();
            }
            store.append(msg).unwrap();
        }
        assert_eq!(store.retain(0, 0, 1000).unwrap(), 7);
        assert_eq!(store.retained_dropped(), 7);
        let (msgs, start, len) = store.read("age", 0, 0, 10).unwrap();
        assert_eq!((start, len, msgs.len()), (7, 10, 3));
        assert_eq!(store.min_offset("age", 0), 7);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn retention_drops_cq_segments_when_aligned() {
        let dir = std::env::temp_dir().join(format!(
            "mq-rust-retain-seg-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_millis()
        ));
        let _ = std::fs::remove_dir_all(&dir);
        let store = BrokerStore::open(&dir, 1).unwrap();
        store.set_cq_segment_entries(4);
        for i in 0..10 {
            let mut msg = sample("segret", 0);
            msg.msg_id = format!("SEGRET-{i}");
            store.append(msg).unwrap();
        }
        assert_eq!(store.retain(6, 0, 0).unwrap(), 4);
        let (msgs, start, len) = store.read("segret", 0, 0, 10).unwrap();
        assert_eq!((start, len, msgs.len()), (4, 10, 6));
        assert_eq!(msgs[0].queue_offset, 4);
        assert_eq!(msgs[5].queue_offset, 9);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn admin_meta_survives_reopen() {
        let dir = std::env::temp_dir().join(format!(
            "mq-rust-admin-meta-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_millis()
        ));
        let _ = std::fs::remove_dir_all(&dir);
        let store = BrokerStore::open(&dir, 1).unwrap();
        store.create_topic("EMPTY_TOPIC");
        store.create_subscription_group("empty_group");
        drop(store);
        let reopened = BrokerStore::open(&dir, 1).unwrap();
        assert!(reopened.topic_names().iter().any(|t| t == "EMPTY_TOPIC"));
        assert!(reopened
            .subscription_groups()
            .iter()
            .any(|g| g == "empty_group"));
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn reopen_preserves_physical_offsets() {
        let dir = std::env::temp_dir().join(format!(
            "mq-rust-physical-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_millis()
        ));
        let _ = std::fs::remove_dir_all(&dir);
        let store = BrokerStore::open(&dir, 1).unwrap();
        let mut expected = Vec::new();
        for i in 0..5 {
            let mut msg = sample("phys", 0);
            msg.msg_id = format!("PHYS-{i}");
            expected.push(store.append(msg).unwrap().physical_offset);
        }
        drop(store);
        let reopened = BrokerStore::open(&dir, 1).unwrap();
        let (messages, _, _) = reopened.read("phys", 0, 0, 10).unwrap();
        let offsets: Vec<_> = messages.iter().map(|m| m.physical_offset).collect();
        assert_eq!(offsets, expected);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn retention_trims_commitlog_and_persists_base() {
        let dir = std::env::temp_dir().join(format!(
            "mq-rust-retain-trim-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_millis()
        ));
        let _ = std::fs::remove_dir_all(&dir);
        let store = BrokerStore::open_sharded_with_group_segment(&dir, 1, 1, false, 64).unwrap();
        let mut physical = Vec::new();
        for i in 0..10 {
            let mut msg = sample("trim", 0);
            msg.msg_id = format!("TRIM-{i}");
            physical.push(store.append(msg).unwrap().physical_offset);
        }
        let before = store.commitlog_bytes();
        assert_eq!(store.retain(5, 0, 0).unwrap(), 5);
        assert!(store.commitlog_bytes() < before);
        drop(store);
        let reopened = BrokerStore::open_sharded_with_group_segment(&dir, 1, 1, false, 64).unwrap();
        assert_eq!(reopened.min_offset("trim", 0), 5);
        let (msgs, start, len) = reopened.read("trim", 0, 0, 10).unwrap();
        assert_eq!((start, len, msgs.len()), (5, 10, 5));
        assert_eq!(msgs[0].queue_offset, 5);
        let offsets: Vec<_> = msgs.iter().map(|m| m.physical_offset).collect();
        assert_eq!(offsets, physical[5..]);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn concurrent_appends_are_consistent() {
        let dir = std::env::temp_dir().join(format!(
            "mq-rust-concurrent-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_millis()
        ));
        let _ = std::fs::remove_dir_all(&dir);
        let store = Arc::new(BrokerStore::open(&dir, 8).unwrap());
        let mut handles = Vec::new();
        for t in 0..8i32 {
            let store = Arc::clone(&store);
            handles.push(std::thread::spawn(move || {
                for i in 0..100 {
                    let mut msg = sample("conc", t);
                    msg.msg_id = format!("T{t}-{i}");
                    store.append(msg).unwrap();
                }
            }));
        }
        for handle in handles {
            handle.join().unwrap();
        }
        assert_eq!(store.append_count.load(Ordering::Relaxed), 800);
        for q in 0..8i32 {
            let (msgs, start, len) = store.read("conc", q, 0, 1000).unwrap();
            assert_eq!((start, len, msgs.len()), (0, 100, 100));
            for (i, msg) in msgs.iter().enumerate() {
                assert_eq!(msg.queue_offset, i as i64);
            }
        }
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn append_and_read_offsets() {
        let store = BrokerStore::new(8);
        let mut a = sample("orders", 1);
        a.msg_id = "A".into();
        let mut b = sample("orders", 1);
        b.msg_id = "B".into();
        let a = store.append(a).unwrap();
        let b = store.append(b).unwrap();
        assert_eq!(a.queue_offset, 0);
        assert_eq!(b.queue_offset, 1);
        let (msgs, next, max) = store.read("orders", 1, 0, 10).unwrap();
        assert_eq!(msgs.len(), 2);
        assert_eq!(next, 0);
        assert_eq!(max, 2);
    }

    #[test]
    fn metadata_logs_compact_on_open() {
        let dir = std::env::temp_dir().join(format!(
            "mq-rust-compact-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_millis()
        ));
        let _ = std::fs::remove_dir_all(&dir);
        {
            let store = BrokerStore::open(&dir, 1).unwrap();
            for i in 0..50 {
                store.update_offset("g1", "orders", 0, i);
            }
        }
        let before = std::fs::metadata(dir.join("offsets")).unwrap().len();
        let store = BrokerStore::open(&dir, 1).unwrap();
        let after = std::fs::metadata(dir.join("offsets")).unwrap().len();
        assert!(after < before, "offsets not compacted: {before} -> {after}");
        assert_eq!(store.query_offset("g1", "orders", 0), 49);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn offsets_survive_reopen() {
        let dir = std::env::temp_dir().join(format!(
            "mq-rust-offsets-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_millis()
        ));
        let _ = std::fs::remove_dir_all(&dir);
        {
            let store = BrokerStore::open(&dir, 1).unwrap();
            store.update_offset("g1", "orders", 0, 5);
        }
        let store = BrokerStore::open(&dir, 1).unwrap();
        assert_eq!(store.query_offset("g1", "orders", 0), 5);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn offsets_are_per_group() {
        let store = BrokerStore::new(8);
        store.update_offset("g1", "orders", 0, 5);
        assert_eq!(store.query_offset("g1", "orders", 0), 5);
        assert_eq!(store.query_offset("g2", "orders", 0), 0);
    }

    #[test]
    fn exact_commitlog_compaction_keeps_survivors_readable() {
        let dir = std::env::temp_dir().join(format!(
            "mq-rust-compact-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        let store = BrokerStore::open(&dir, 8).unwrap();
        for index in 0..4 {
            let mut msg = sample("compact", 0);
            msg.msg_id = format!("c{index}");
            store.append(msg).unwrap();
        }
        store.retain(1, 0, 0).unwrap();
        store.compact_commitlogs_exact().unwrap();
        store.repair_cq_force().unwrap();
        let (msgs, _, _) = store.read("compact", 0, 0, 10).unwrap();
        assert_eq!(msgs.len(), 1);
        let path = dir.join("commitlog-0");
        assert!(std::fs::metadata(path).unwrap().len() < 4096);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn group_writer_commitlog_compaction_keeps_survivors() {
        let dir = std::env::temp_dir().join(format!(
            "mq-rust-compact-group-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        let store = BrokerStore::open_sharded_with_group(&dir, 8, 1, true).unwrap();
        for index in 0..4 {
            let mut msg = sample("compactg", 0);
            msg.msg_id = format!("g{index}");
            store.append(msg).unwrap();
        }
        store.retain(1, 0, 0).unwrap();
        store.compact_now().unwrap();
        let (msgs, _, _) = store.read("compactg", 0, 0, 10).unwrap();
        assert_eq!(msgs.len(), 1);
        let path = dir.join("commitlog-0");
        assert!(std::fs::metadata(path).unwrap().len() < 4096);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn duplicate_msg_id_is_idempotent_after_reopen() {
        let dir = std::env::temp_dir().join(format!(
            "mq-rust-reopen-dedup-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        {
            let store = BrokerStore::open(&dir, 8).unwrap();
            store.append(sample("orders", 0)).unwrap();
        }
        let store = BrokerStore::open(&dir, 8).unwrap();
        store.append(sample("orders", 0)).unwrap();
        let (msgs, _, max) = store.read("orders", 0, 0, 10).unwrap();
        assert_eq!(msgs.len(), 1);
        assert_eq!(max, 1);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn duplicate_msg_id_is_idempotent() {
        let store = BrokerStore::new(8);
        let a = store.append(sample("orders", 0)).unwrap();
        let b = store.append(sample("orders", 0)).unwrap();
        assert_eq!(a.queue_offset, b.queue_offset);
        let (msgs, _, max) = store.read("orders", 0, 0, 10).unwrap();
        assert_eq!(msgs.len(), 1);
        assert_eq!(max, 1);
    }

    #[test]
    fn consumer_registry_tracks_unregister() {
        let store = BrokerStore::new(8);
        store.register_consumer("g", "client-1");
        store.register_consumer("g", "client-2");
        assert_eq!(store.consumer_ids("g").len(), 2);
        store.unregister_client("client-1");
        assert_eq!(store.consumer_ids("g"), vec!["client-2".to_string()]);
    }

    #[test]
    fn persistent_store_reopens_and_replays_binary_records() {
        let dir = std::env::temp_dir().join(format!(
            "mq-rust-persist-{}-{}",
            std::process::id(),
            mq_common::now_ms()
        ));
        let _ = std::fs::remove_dir_all(&dir);
        {
            let store = BrokerStore::open(&dir, 8).unwrap();
            store.append(sample("orders", 1)).unwrap();
        }
        let store = BrokerStore::open(&dir, 8).unwrap();
        let (msgs, _, max) = store.read("orders", 1, 0, 10).unwrap();
        assert_eq!(msgs.len(), 1);
        assert_eq!(max, 1);
        assert_eq!(&msgs[0].body[..], b"hello");
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn delay_wal_survives_reopen() {
        let dir = std::env::temp_dir().join(format!(
            "mq-rust-delay-{}-{}",
            std::process::id(),
            mq_common::now_ms()
        ));
        let _ = std::fs::remove_dir_all(&dir);
        let due = mq_common::now_ms() + 60_000;
        {
            let store = BrokerStore::open(&dir, 8).unwrap();
            store.schedule_delay(sample("orders", 1), due);
        }
        let store = BrokerStore::open(&dir, 8).unwrap();
        let due_msgs = store.due_delayed(due + 1);
        assert_eq!(due_msgs.len(), 1);
        assert_eq!(due_msgs[0].topic, "orders");
        assert!(store.due_delayed(due + 2).is_empty());
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn retry_counts_survive_reopen() {
        let dir = std::env::temp_dir().join(format!(
            "mq-rust-retry-{}-{}",
            std::process::id(),
            mq_common::now_ms()
        ));
        let _ = std::fs::remove_dir_all(&dir);
        {
            let store = BrokerStore::open(&dir, 8).unwrap();
            assert_eq!(store.next_retry_times("origin-1", 0), 1);
            assert_eq!(store.next_retry_times("origin-1", 0), 2);
        }
        let store = BrokerStore::open(&dir, 8).unwrap();
        assert_eq!(store.next_retry_times("origin-1", 0), 3);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn half_wal_survives_reopen_and_expires() {
        let dir = std::env::temp_dir().join(format!(
            "mq-rust-half-{}-{}",
            std::process::id(),
            mq_common::now_ms()
        ));
        let _ = std::fs::remove_dir_all(&dir);
        let msg = sample("orders", 0);
        let tx_id = msg.msg_id.clone();
        {
            let store = BrokerStore::open(&dir, 8).unwrap();
            store.prepare_half(msg).unwrap();
        }
        let store = BrokerStore::open(&dir, 8).unwrap();
        assert!(store.find_half(&tx_id, &tx_id).is_some());
        let expired = store.expire_half(0);
        assert_eq!(expired, vec![tx_id]);
        let (msgs, _, _) = store.read("orders", 0, 0, 10).unwrap();
        assert!(msgs.is_empty());
        let _ = std::fs::remove_dir_all(&dir);
    }
}
