use clap::Parser;
use mq_broker::admin;
use mq_broker::audit::AuditLog;
use mq_broker::connections::ProducerConnections;
use mq_broker::metrics::{serve, Metrics};
use mq_broker::server::{run, BrokerConfig};
use mq_broker::store::BrokerStore;
use mq_protocol::request_code;
use mq_protocol::RemotingCommand;
use std::error::Error;
use std::sync::Arc;

#[derive(Debug, Parser)]
#[command(
    name = "mq-broker",
    version,
    about = "MQ-rust Broker (RocketMQ 4.9 compatible subset)"
)]
struct Args {
    #[arg(long, default_value = "127.0.0.1:10921")]
    listen: String,
    #[arg(long, default_value = "127.0.0.1:9877")]
    namesrv: String,
    #[arg(long, default_value_t = 8)]
    queues: i32,
    #[arg(long, default_value = "./data/broker")]
    store_root: String,
    #[arg(long, default_value = "127.0.0.1:9090")]
    metrics_listen: String,
    #[arg(long, default_value_t = 10000)]
    tx_timeout_ms: i64,
    #[arg(long, default_value_t = 3000)]
    tx_check_interval_ms: i64,
    #[arg(long, default_value_t = 3000)]
    tx_check_backoff_ms: i64,
    #[arg(long, default_value_t = 15)]
    tx_check_max: u32,
    #[arg(long, default_value = "127.0.0.1:9091")]
    admin_listen: String,
    #[arg(long, default_value = "./data/audit.log")]
    audit_path: String,
    #[arg(long, default_value = "./data/inbox.log")]
    inbox_path: String,
    #[arg(long)]
    replica_listen: Option<String>,
    #[arg(long)]
    replica_of: Option<String>,
    #[arg(long)]
    promote_to: Option<String>,
    #[arg(long, default_value = "127.0.0.1:10921")]
    advertise_addr: String,
    #[arg(long, default_value_t = 1000)]
    flush_interval_ms: u64,
    #[arg(long, default_value_t = 60000)]
    metadata_compact_ms: u64,
    #[arg(long, default_value_t = 0)]
    retain_messages: usize,
    #[arg(long, default_value_t = 0)]
    retain_bytes: u64,
    #[arg(long, default_value_t = 0)]
    retain_max_age_ms: i64,
    #[arg(long, default_value_t = 60000)]
    retain_interval_ms: u64,
    #[arg(long)]
    fence_url: Option<String>,
    #[arg(long)]
    fence_token: Option<String>,
    #[arg(long)]
    admin_token: Option<String>,
    #[arg(long)]
    acl_access_keys: Option<String>,
    #[arg(long)]
    acl_secrets: Option<String>,
    #[arg(long)]
    acl_file: Option<String>,
    #[arg(long, default_value_t = 5000)]
    acl_reload_ms: u64,
    #[arg(long, default_value_t = 10000)]
    trace_capacity: usize,
    #[arg(long)]
    trace_path: Option<std::path::PathBuf>,
    #[arg(long)]
    sync_replicate: bool,
    #[arg(long, default_value_t = 1000)]
    replica_timeout_ms: u64,
    #[arg(long, default_value_t = 1)]
    replica_quorum: usize,
    #[arg(long, default_value_t = 5000)]
    fence_interval_ms: u64,
    #[arg(long, default_value_t = 1)]
    commitlog_shards: usize,
    #[arg(long, default_value_t = 268435456)]
    commitlog_segment_bytes: u64,
    #[arg(long, default_value_t = 100000)]
    cq_segment_entries: u64,
    #[arg(long, default_value_t = false)]
    group_commit: bool,
    #[arg(long)]
    tc_url: Option<String>,
    #[arg(long, default_value_t = 2000)]
    tc_reconcile_ms: u64,
    #[arg(long, default_value = "./data/tc-outbox.log")]
    tc_outbox_path: String,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    mq_observability::init_tracing(false);
    let args = Args::parse();
    let listen: std::net::SocketAddr = args.listen.parse()?;
    let store = Arc::new(BrokerStore::open_sharded_with_group_segment(
        &args.store_root,
        args.queues,
        args.commitlog_shards,
        args.group_commit,
        args.commitlog_segment_bytes,
    )?);
    let audit = Arc::new(AuditLog::open(&args.audit_path)?);
    let trace = Arc::new(match &args.trace_path {
        Some(path) => mq_broker::trace::TraceLog::open(args.trace_capacity, path)?,
        None => mq_broker::trace::TraceLog::new(args.trace_capacity),
    });
    let read_only = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let acl_secrets = Arc::new(
        args.acl_secrets
            .as_deref()
            .map(|s| {
                s.split(',')
                    .filter_map(|pair| pair.split_once(':'))
                    .map(|(key, secret)| (key.trim().to_string(), secret.trim().to_string()))
                    .collect::<std::collections::HashMap<_, _>>()
            })
            .unwrap_or_default(),
    );
    let acl_policy = match &args.acl_file {
        Some(path) => Some(Arc::new(std::sync::RwLock::new(
            mq_broker::acl::AclPolicy::load(path)
                .map_err(|err| std::io::Error::new(std::io::ErrorKind::InvalidInput, err))?,
        ))),
        None => None,
    };
    if let (Some(path), Some(policy)) = (&args.acl_file, &acl_policy) {
        let path = path.clone();
        let policy = Arc::clone(policy);
        let interval = args.acl_reload_ms.max(500);
        std::thread::spawn(move || loop {
            std::thread::sleep(std::time::Duration::from_millis(interval));
            if let Ok(next) = mq_broker::acl::AclPolicy::load(&path) {
                *policy.write().unwrap() = next;
                tracing::info!("acl policy reloaded");
            }
        });
    }
    let acl_keys = Arc::new(
        args.acl_access_keys
            .as_deref()
            .map(|s| {
                s.split(',')
                    .map(str::trim)
                    .filter(|s| !s.is_empty())
                    .map(str::to_string)
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default(),
    );
    if let Some(admin) = args.fence_url.clone() {
        let read_only = Arc::clone(&read_only);
        let advertise = args.advertise_addr.clone();
        let interval = args.fence_interval_ms.max(500);
        let epoch_path = std::path::Path::new(&args.store_root).join("fence_epoch");
        let fence_token = args.fence_token.clone();
        std::thread::spawn(move || {
            let mut last_epoch = std::fs::read_to_string(&epoch_path)
                .ok()
                .and_then(|s| s.trim().parse::<u64>().ok())
                .unwrap_or(0);
            let mut was = false;
            loop {
                if let Some((route, epoch)) = namesrv_route(&admin, fence_token.as_deref()) {
                    let fenced = route != advertise || epoch < last_epoch;
                    if fenced != was {
                        tracing::warn!(%route, epoch, last_epoch, fenced, "broker fence state changed");
                        was = fenced;
                    }
                    if epoch > last_epoch {
                        last_epoch = epoch;
                        let _ = std::fs::write(&epoch_path, last_epoch.to_string());
                    }
                    read_only.store(fenced, std::sync::atomic::Ordering::Relaxed);
                }
                std::thread::sleep(std::time::Duration::from_millis(interval));
            }
        });
    }
    store.set_cq_segment_entries(args.cq_segment_entries);
    store.set_sync_replicate(args.sync_replicate, args.replica_timeout_ms);
    store.set_replica_quorum(args.replica_quorum);
    if args.replica_listen.is_some() {
        let (tx, rx) = std::sync::mpsc::channel();
        store.set_replica_writer(tx);
        std::thread::spawn(move || mq_broker::store::run_replica_writer(rx));
    }
    if let Some(addr) = args.replica_listen.clone() {
        let store = Arc::clone(&store);
        std::thread::spawn(move || {
            let listener = std::net::TcpListener::bind(&addr).expect("bind replica listener");
            tracing::info!(%addr, "replica listener started");
            for stream in listener.incoming().flatten() {
                store.add_replica(stream);
            }
        });
    }
    if let Some(addr) = args.replica_of.clone() {
        let store = Arc::clone(&store);
        let promote_to = args.promote_to.clone();
        let advertise = args.advertise_addr.clone();
        std::thread::spawn(move || loop {
            match std::net::TcpStream::connect(&addr) {
                Ok(mut stream) => {
                    tracing::info!(%addr, "connected to replica master");
                    let mut acked = 0u64;
                    loop {
                        let mut len = [0u8; 4];
                        if std::io::Read::read_exact(&mut stream, &mut len).is_err() {
                            break;
                        }
                        let mut payload = vec![0u8; u32::from_be_bytes(len) as usize];
                        if std::io::Read::read_exact(&mut stream, &mut payload).is_err() {
                            break;
                        }
                        if let Ok((msg, _)) = mq_protocol::message::StoredMessage::decode(&payload)
                        {
                            let _ = store.append(msg);
                            acked += 1;
                            let _ = std::io::Write::write_all(&mut stream, &acked.to_be_bytes());
                        }
                    }
                    if let Some(admin) = &promote_to {
                        promote(admin, &advertise);
                        // Fencing: once promoted, stop replicating from the
                        // old master to avoid divergent stores.
                        return;
                    }
                }
                Err(_) => std::thread::sleep(std::time::Duration::from_secs(1)),
            }
        });
    }
    let tc_client = args
        .tc_url
        .as_deref()
        .map(|url| Arc::new(mq_broker::tc::TcClient::new(url)));
    let tc = tc_client.as_ref().map(|client| {
        Arc::new(mq_broker::tc_outbox::TcOutbox::start(
            Arc::clone(client),
            Some(std::path::Path::new(&args.tc_outbox_path)),
        ))
    });
    let flush_store = Arc::clone(&store);
    let flush_interval = args.flush_interval_ms.max(1);
    tokio::spawn(async move {
        loop {
            tokio::time::sleep(std::time::Duration::from_millis(flush_interval)).await;
            if let Err(err) = flush_store.sync() {
                tracing::warn!(%err, "background flush failed");
            }
        }
    });
    if args.metadata_compact_ms > 0 {
        let compact_store = Arc::clone(&store);
        let compact_interval = args.metadata_compact_ms;
        tokio::spawn(async move {
            loop {
                tokio::time::sleep(std::time::Duration::from_millis(compact_interval)).await;
                if let Err(err) = compact_store.compact_metadata() {
                    tracing::warn!(%err, "metadata compaction failed");
                }
            }
        });
    }
    if args.retain_messages > 0 || args.retain_bytes > 0 || args.retain_max_age_ms > 0 {
        let retain_store = Arc::clone(&store);
        let retain_max = args.retain_messages;
        let retain_max_bytes = args.retain_bytes;
        let retain_max_age = args.retain_max_age_ms;
        let retain_interval = args.retain_interval_ms.max(1000);
        tokio::spawn(async move {
            loop {
                tokio::time::sleep(std::time::Duration::from_millis(retain_interval)).await;
                if let Err(err) = retain_store.retain(retain_max, retain_max_bytes, retain_max_age)
                {
                    tracing::warn!(%err, "message retention failed");
                }
            }
        });
    }
    let admin_addr: std::net::SocketAddr = args.admin_listen.parse()?;
    let inbox = Arc::new(mq_broker::inbox::Inbox::open(&args.inbox_path)?);
    tokio::spawn(admin::serve(
        Arc::clone(&store),
        Arc::clone(&audit),
        Some(inbox),
        admin_addr,
        Arc::clone(&read_only),
        args.advertise_addr.clone(),
        args.fence_token.clone(),
        args.admin_token.clone(),
        Arc::clone(&trace),
    ));
    let metrics = Arc::new(Metrics::new());
    metrics.tx_pending.store(
        store.pending_half_count() as i64,
        std::sync::atomic::Ordering::Relaxed,
    );
    let producers = Arc::new(ProducerConnections::default());
    let metrics_addr: std::net::SocketAddr = args.metrics_listen.parse()?;
    tokio::spawn(serve(Arc::clone(&metrics), metrics_addr));
    {
        let stats_store = Arc::clone(&store);
        let stats_metrics = Arc::clone(&metrics);
        tokio::spawn(async move {
            loop {
                let (topics, queues, messages, body_bytes) = stats_store.stats();
                use std::sync::atomic::Ordering::Relaxed;
                stats_metrics.store_topics.store(topics as u64, Relaxed);
                stats_metrics.store_queues.store(queues as u64, Relaxed);
                stats_metrics.store_messages.store(messages as u64, Relaxed);
                stats_metrics.store_body_bytes.store(body_bytes, Relaxed);
                stats_metrics
                    .retained_dropped_total
                    .store(stats_store.retained_dropped(), Relaxed);
                stats_metrics
                    .commitlog_bytes
                    .store(stats_store.commitlog_bytes(), Relaxed);
                tokio::time::sleep(std::time::Duration::from_secs(5)).await;
            }
        });
    }

    let checker_store = Arc::clone(&store);
    let checker_producers = Arc::clone(&producers);
    let checker_metrics = Arc::clone(&metrics);
    let checker_audit = Arc::clone(&audit);
    let check_interval_ms = args.tx_check_interval_ms.max(500);
    let check_backoff_ms = args.tx_check_backoff_ms.max(500);
    let check_max = args.tx_check_max.max(1);
    tokio::spawn(async move {
        loop {
            tokio::time::sleep(std::time::Duration::from_millis(check_interval_ms as u64)).await;
            for msg in checker_store.due_half(check_interval_ms, check_backoff_ms, check_max) {
                let group = mq_protocol::message::property_value(&msg.properties, "PGROUP")
                    .unwrap_or_default();
                if group.is_empty() {
                    continue;
                }
                let mut cmd = RemotingCommand::request(request_code::CHECK_TRANSACTION_STATE, 1);
                cmd.put_ext("transactionId", msg.msg_id.clone());
                cmd.put_ext("commitLogOffset", "0");
                cmd.put_ext("msgId", msg.msg_id.clone());
                cmd.put_ext("tranStateTableOffset", "0");
                cmd.put_ext("producerGroup", group.clone());
                cmd.put_ext("fromTransactionCheck", "true");
                cmd.put_ext("topic", msg.topic.clone());
                cmd.put_ext("queueId", msg.queue_id.to_string());
                cmd.body = bytes::Bytes::from(msg.encode());
                if checker_producers.send(&group, cmd) {
                    checker_store.mark_checked(&msg.msg_id);
                    checker_metrics
                        .tx_check_total
                        .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    checker_audit.write(
                        "tx.check",
                        serde_json::json!({"tx_id": msg.msg_id, "group": group}),
                    );
                    tracing::info!(group, tx_id = %msg.msg_id, "check-back sent");
                }
            }
            let exhausted = checker_store.expire_exhausted(check_max);
            if !exhausted.is_empty() {
                checker_metrics
                    .tx_timeout_total
                    .fetch_add(exhausted.len() as u64, std::sync::atomic::Ordering::Relaxed);
                checker_metrics
                    .tx_pending
                    .fetch_sub(exhausted.len() as i64, std::sync::atomic::Ordering::Relaxed);
                for tx_id in &exhausted {
                    checker_audit.write(
                        "tx.exhausted",
                        serde_json::json!({"tx_id": tx_id, "action": "rollback"}),
                    );
                }
                tracing::warn!(?exhausted, "transactions exhausted check-back, rolled back");
            }
        }
    });

    let expiry_store = Arc::clone(&store);
    let expiry_metrics = Arc::clone(&metrics);
    let expiry_audit = Arc::clone(&audit);
    let tx_timeout_ms = args.tx_timeout_ms;
    tokio::spawn(async move {
        loop {
            tokio::time::sleep(std::time::Duration::from_secs(1)).await;
            let expired = expiry_store.expire_half(tx_timeout_ms);
            if !expired.is_empty() {
                expiry_metrics
                    .tx_timeout_total
                    .fetch_add(expired.len() as u64, std::sync::atomic::Ordering::Relaxed);
                expiry_metrics
                    .tx_pending
                    .fetch_sub(expired.len() as i64, std::sync::atomic::Ordering::Relaxed);
                for tx_id in &expired {
                    expiry_audit.write(
                        "tx.timeout",
                        serde_json::json!({"tx_id": tx_id, "action": "rollback"}),
                    );
                }
                tracing::info!(?expired, "expired half transactions rolled back");
            }
        }
    });

    let delay_store = Arc::clone(&store);
    tokio::spawn(async move {
        loop {
            tokio::time::sleep(std::time::Duration::from_millis(200)).await;
            for msg in delay_store.due_delayed(mq_common::now_ms()) {
                if let Err(err) = delay_store.append(msg) {
                    tracing::warn!(%err, "delayed message append failed");
                }
            }
        }
    });

    let config = BrokerConfig {
        listen,
        store,
        store_port: listen.port(),
        metrics,
        producers,
        audit,
        tc,
        tc_client,
        tc_reconcile_ms: args.tc_reconcile_ms,
        read_only,
        acl_keys,
        acl_secrets,
        acl_policy,
        trace,
    };
    tracing::info!(namesrv = %args.namesrv, "broker configured (static route in R1)");
    run(config).await?;
    Ok(())
}

fn namesrv_route(admin: &str, token: Option<&str>) -> Option<(String, u64)> {
    use std::io::{Read, Write};
    let mut stream = std::net::TcpStream::connect(admin).ok()?;
    let auth = token
        .map(|t| format!("Authorization: Bearer {t}\r\n"))
        .unwrap_or_default();
    let req = format!("GET / HTTP/1.1\r\nHost: {admin}\r\n{auth}Connection: close\r\n\r\n");
    stream.write_all(req.as_bytes()).ok()?;
    let mut buf = [0u8; 512];
    let n = stream.read(&mut buf).ok()?;
    let text = String::from_utf8_lossy(&buf[..n]);
    let start = text.find("\"broker_addr\":\"")? + "\"broker_addr\":\"".len();
    let rest = &text[start..];
    let end = rest.find('"')?;
    let route = rest[..end].to_string();
    let epoch = text
        .split("\"epoch\":")
        .nth(1)
        .and_then(|s| {
            s.chars()
                .take_while(|c| c.is_ascii_digit())
                .collect::<String>()
                .parse::<u64>()
                .ok()
        })
        .unwrap_or(0);
    Some((route, epoch))
}

fn promote(admin: &str, advertise: &str) {
    use std::io::{Read, Write};
    let Ok(mut stream) = std::net::TcpStream::connect(admin) else {
        return;
    };
    let req = format!("GET / HTTP/1.1\r\nHost: {admin}\r\nConnection: close\r\n\r\n");
    if stream.write_all(req.as_bytes()).is_err() {
        return;
    }
    let mut buf = [0u8; 512];
    let Ok(n) = stream.read(&mut buf) else {
        return;
    };
    let current = String::from_utf8_lossy(&buf[..n]);
    if current.contains(advertise) {
        tracing::warn!(advertise, "namesrv already points to this replica");
        return;
    }

    let Ok(mut stream) = std::net::TcpStream::connect(admin) else {
        return;
    };
    let req = format!(
        "POST /broker/{advertise} HTTP/1.1\r\nHost: {admin}\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"
    );
    if stream.write_all(req.as_bytes()).is_ok() {
        let mut buf = [0u8; 256];
        let _ = stream.read(&mut buf);
        tracing::warn!(advertise, "replica promoted in namesrv");
    }
}
