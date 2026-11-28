//! Minimal RocketMQ-compatible Broker for R1.
//!
//! Supported commands: SEND_MESSAGE (10 / 310), PULL_MESSAGE (11),
//! QUERY_CONSUMER_OFFSET (14), UPDATE_CONSUMER_OFFSET (15),
//! GET_MAX_OFFSET (30), GET_MIN_OFFSET (31), HEART_BEAT (34),
//! UNREGISTER_CLIENT (35), CONSUMER_SEND_MSG_BACK (36).

use bytes::BytesMut;
use mq_common::{MqError, Result as MqResult};
use mq_protocol::header::{
    GetMaxOffsetRequest, PullMessageRequest, PullMessageResponse, QueryConsumerOffsetRequest,
    SendMessageRequest, SendMessageResponse,
};
use mq_protocol::message::{new_msg_id, StoredMessage};
use mq_protocol::request_code;
use mq_protocol::response_code;
use mq_protocol::{decode_frame, RemotingCommand};
use std::net::SocketAddr;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio::time::{timeout, Duration};

use crate::audit::AuditLog;
use crate::connections::ProducerConnections;
use crate::metrics::Metrics;
use crate::store::BrokerStore;
use crate::tc_outbox::{TcOp, TcOutbox};
use tokio::sync::mpsc;
use tokio::sync::mpsc::UnboundedSender;

#[derive(Clone)]
pub struct BrokerConfig {
    pub listen: SocketAddr,
    pub store: Arc<BrokerStore>,
    pub store_port: u16,
    pub metrics: Arc<Metrics>,
    pub producers: Arc<ProducerConnections>,
    pub audit: Arc<AuditLog>,
    pub tc: Option<Arc<TcOutbox>>,
    pub tc_client: Option<Arc<crate::tc::TcClient>>,
    pub tc_reconcile_ms: u64,
    pub read_only: Arc<std::sync::atomic::AtomicBool>,
    pub acl_keys: Arc<Vec<String>>,
    pub acl_secrets: Arc<std::collections::HashMap<String, String>>,
    pub acl_policy: Option<Arc<std::sync::RwLock<crate::acl::AclPolicy>>>,
    pub trace: Arc<crate::trace::TraceLog>,
    pub tls_cert: Option<String>,
    pub tls_key: Option<String>,
    pub tls_client_ca: Option<String>,
    pub tls_reload_ms: u64,
    pub raft: Option<Arc<mq_raft::MessageRaft>>,
}

/// Apply TC decisions to locally pending half messages.
///
/// TC is the decision source: `committed` makes the half visible, `rolled_back`
/// drops it. `prepared`/unknown are left untouched for a later pass.
pub async fn reconcile_once(
    store: &Arc<BrokerStore>,
    tc: &crate::tc::TcClient,
    audit: &AuditLog,
) -> usize {
    let mut applied = 0;
    for half in store.pending_half() {
        let Some(status) = tc.status(&half.tx_id).await else {
            continue;
        };
        match status.as_str() {
            "committed" => {
                if let Ok(Some(msg)) = store.commit_half(&half.tx_id, &half.tx_id) {
                    audit.write(
                        "tx.reconcile_commit",
                        serde_json::json!({ "tx_id": half.tx_id, "msg_id": msg.msg_id }),
                    );
                    applied += 1;
                }
            }
            "rolled_back" if store.rollback_half(&half.tx_id, &half.tx_id) => {
                audit.write(
                    "tx.reconcile_rollback",
                    serde_json::json!({ "tx_id": half.tx_id }),
                );
                applied += 1;
            }
            _ => {}
        }
    }
    applied
}

pub async fn run(config: BrokerConfig) -> std::io::Result<()> {
    let _ = tokio_rustls::rustls::crypto::aws_lc_rs::default_provider().install_default();
    let listener = TcpListener::bind(config.listen).await?;
    let tls_state = Arc::new(std::sync::RwLock::new(build_tls_acceptor(&config)?));
    if config.tls_reload_ms > 0 {
        if let (Some(cert), Some(key)) = (config.tls_cert.clone(), config.tls_key.clone()) {
            let state = Arc::clone(&tls_state);
            let ca = config.tls_client_ca.clone();
            let interval = config.tls_reload_ms.max(500);
            std::thread::spawn(move || loop {
                std::thread::sleep(std::time::Duration::from_millis(interval));
                match build_tls_acceptor_paths(&cert, &key, ca.as_deref()) {
                    Ok(acceptor) => {
                        if let Ok(mut guard) = state.write() {
                            *guard = Some(acceptor);
                            tracing::info!("tls certificate reloaded");
                        }
                    }
                    Err(err) => tracing::warn!(%err, "tls certificate reload failed"),
                }
            });
        }
    }
    let config = Arc::new(config);
    if let (Some(tc), true) = (config.tc_client.clone(), config.tc_reconcile_ms > 0) {
        let store = Arc::clone(&config.store);
        let audit = Arc::clone(&config.audit);
        let interval = config.tc_reconcile_ms;
        tokio::spawn(async move {
            loop {
                let applied = reconcile_once(&store, &tc, &audit).await;
                if applied > 0 {
                    tracing::info!(applied, "tc reconciliation applied");
                }
                tokio::time::sleep(std::time::Duration::from_millis(interval)).await;
            }
        });
    }
    tracing::info!(listen = %config.listen, "mq-broker listening");
    loop {
        let (stream, peer) = listener.accept().await?;
        let config = Arc::clone(&config);
        let tls = tls_state.read().ok().and_then(|guard| guard.clone());
        tokio::spawn(async move {
            let result = if let Some(acceptor) = tls {
                match acceptor.accept(stream).await {
                    Ok(stream) => handle_connection(stream, config, peer).await,
                    Err(err) => {
                        tracing::warn!(%peer, error = %err, "tls handshake failed");
                        return;
                    }
                }
            } else {
                handle_connection(stream, config, peer).await
            };
            if let Err(err) = result {
                tracing::warn!(%peer, error = %err, "broker connection closed");
            }
        });
    }
}

pub fn spawn_leader_lease(
    tc_urls: Vec<String>,
    candidate: String,
    read_only: Arc<std::sync::atomic::AtomicBool>,
    ttl_ms: u64,
    route_admin: Option<(String, String)>,
) {
    std::thread::spawn(move || {
        use std::io::{Read, Write};
        let mut failures = 0u32;
        let mut was_holder = false;
        loop {
            let mut holder: Option<String> = None;
            for base in &tc_urls {
                let addr = base
                    .trim_start_matches("http://")
                    .trim_start_matches("https://")
                    .trim_end_matches('/');
                let Ok(mut stream) = std::net::TcpStream::connect(addr) else {
                    continue;
                };
                let _ = stream.set_read_timeout(Some(Duration::from_millis(1500)));
                let req = format!(
                    "GET /lease?candidate={candidate}&ttl_ms={ttl_ms} HTTP/1.1\r\nHost: {addr}\r\nConnection: close\r\n\r\n"
                );
                if stream.write_all(req.as_bytes()).is_err() {
                    continue;
                }
                let mut buf = [0u8; 1024];
                let Ok(n) = stream.read(&mut buf) else {
                    continue;
                };
                let text = String::from_utf8_lossy(&buf[..n]);
                if let Some(start) = text.find("\"holder\":\"") {
                    let rest = &text[start + "\"holder\":\"".len()..];
                    if let Some(end) = rest.find('"') {
                        holder = Some(rest[..end].to_string());
                        break;
                    }
                }
            }
            match holder {
                Some(holder) => {
                    failures = 0;
                    let is_holder = holder == candidate;
                    read_only.store(!is_holder, Ordering::Relaxed);
                    if is_holder && !was_holder {
                        if let Some((admin, advertise)) = &route_admin {
                            let addr = admin.trim_start_matches("http://").trim_end_matches('/');
                            if let Ok(mut stream) = std::net::TcpStream::connect(addr) {
                                let _ = stream.set_write_timeout(Some(Duration::from_millis(1500)));
                                let req = format!(
                                    "POST /broker/{advertise} HTTP/1.1\r\nHost: {addr}\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"
                                );
                                let _ = stream.write_all(req.as_bytes());
                                tracing::info!(%advertise, "published leader route to namesrv");
                            }
                        }
                    }
                    was_holder = is_holder;
                    tracing::debug!(%holder, %candidate, is_holder, "leader lease refreshed");
                }
                None => {
                    failures += 1;
                    if failures >= 3 {
                        read_only.store(true, Ordering::Relaxed);
                    }
                }
            }
            std::thread::sleep(Duration::from_millis(ttl_ms / 2));
        }
    });
}

pub fn spawn_raft_route(raft: Arc<mq_raft::MessageRaft>, admin: String, advertise: String) {
    std::thread::spawn(move || {
        use std::io::Write;
        let addr = admin
            .trim_start_matches("http://")
            .trim_end_matches('/')
            .to_string();
        let mut was_leader = false;
        loop {
            let leader = raft.is_leader();
            if leader && !was_leader {
                if let Ok(mut stream) = std::net::TcpStream::connect(&addr) {
                    let _ = stream.set_write_timeout(Some(Duration::from_millis(1500)));
                    let req = format!(
                        "POST /broker/{advertise} HTTP/1.1\r\nHost: {addr}\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"
                    );
                    let _ = stream.write_all(req.as_bytes());
                    tracing::info!(%advertise, "published raft leader route");
                }
            }
            was_leader = leader;
            std::thread::sleep(Duration::from_millis(500));
        }
    });
}

fn build_tls_acceptor(config: &BrokerConfig) -> std::io::Result<Option<tokio_rustls::TlsAcceptor>> {
    let (Some(cert), Some(key)) = (&config.tls_cert, &config.tls_key) else {
        return Ok(None);
    };
    build_tls_acceptor_paths(cert, key, config.tls_client_ca.as_deref()).map(Some)
}

fn build_tls_acceptor_paths(
    cert: &str,
    key: &str,
    client_ca: Option<&str>,
) -> std::io::Result<tokio_rustls::TlsAcceptor> {
    let mut cert_reader = std::io::BufReader::new(std::fs::File::open(cert)?);
    let certs = rustls_pemfile::certs(&mut cert_reader)
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
    let mut key_reader = std::io::BufReader::new(std::fs::File::open(key)?);
    let key = rustls_pemfile::private_key(&mut key_reader)
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?
        .ok_or_else(|| {
            std::io::Error::new(std::io::ErrorKind::InvalidData, "missing private key")
        })?;
    let builder = tokio_rustls::rustls::ServerConfig::builder();
    let builder = if let Some(ca) = client_ca {
        let mut ca_reader = std::io::BufReader::new(std::fs::File::open(ca)?);
        let mut roots = tokio_rustls::rustls::RootCertStore::empty();
        for cert in rustls_pemfile::certs(&mut ca_reader) {
            roots
                .add(cert.map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?)
                .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
        }
        let verifier = tokio_rustls::rustls::server::WebPkiClientVerifier::builder(Arc::new(roots))
            .build()
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
        builder.with_client_cert_verifier(verifier)
    } else {
        builder.with_no_client_auth()
    };
    let server = builder
        .with_single_cert(certs, key)
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
    Ok(tokio_rustls::TlsAcceptor::from(Arc::new(server)))
}

async fn handle_connection<S>(
    stream: S,
    config: Arc<BrokerConfig>,
    peer: SocketAddr,
) -> std::io::Result<()>
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + Send + 'static,
{
    let (mut read_half, mut write_half) = tokio::io::split(stream);
    let (tx, mut rx) = mpsc::unbounded_channel::<RemotingCommand>();
    let writer = tokio::spawn(async move {
        while let Some(cmd) = rx.recv().await {
            match cmd.encode() {
                Ok(bytes) => {
                    if write_half.write_all(&bytes).await.is_err() {
                        break;
                    }
                }
                Err(_) => break,
            }
        }
    });

    let mut buf = BytesMut::with_capacity(16 * 1024);
    loop {
        let n = read_half.read_buf(&mut buf).await?;
        if n == 0 {
            writer.abort();
            return Ok(());
        }
        loop {
            let frame = decode_frame(&mut buf)
                .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e.to_string()))?;
            let Some(frame) = frame else { break };
            let serialize_type = frame.serialize_type;
            let cmd = RemotingCommand::decode(frame)
                .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e.to_string()))?;
            if cmd.is_response() {
                continue;
            }
            let oneway = cmd.is_oneway();
            if cmd.code == request_code::PULL_MESSAGE {
                // Long-poll pulls can wait up to 1s. Run them off the
                // connection loop so SEND/CONSUMER_SEND_MSG_BACK on the same
                // connection are not blocked behind a parked pull.
                let config = Arc::clone(&config);
                let tx = tx.clone();
                tokio::spawn(async move {
                    if let Ok(Some(mut resp)) = handle_command(cmd, &config, &tx, peer).await {
                        resp.serialize_type = serialize_type;
                        let _ = tx.send(resp);
                    }
                });
                continue;
            }
            let resp = handle_command(cmd, &config, &tx, peer)
                .await
                .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e.to_string()))?;
            if !oneway {
                if let Some(mut resp) = resp {
                    resp.serialize_type = serialize_type;
                    tx.send(resp).map_err(|_| {
                        std::io::Error::new(std::io::ErrorKind::BrokenPipe, "writer closed")
                    })?;
                }
            }
        }
    }
}

async fn handle_command(
    cmd: RemotingCommand,
    config: &BrokerConfig,
    sender: &UnboundedSender<RemotingCommand>,
    peer: SocketAddr,
) -> MqResult<Option<RemotingCommand>> {
    let opaque = cmd.opaque;
    if config.read_only.load(std::sync::atomic::Ordering::Relaxed)
        && matches!(
            cmd.code,
            request_code::SEND_MESSAGE
                | request_code::SEND_MESSAGE_V2
                | request_code::SEND_BATCH_MESSAGE
                | request_code::END_TRANSACTION
        )
    {
        return Ok(Some(
            RemotingCommand::response(response_code::NO_PERMISSION, opaque)
                .with_remark("read-only: namesrv route points to another broker"),
        ));
    }
    let ext = cmd.ext_fields.as_ref();
    if let Some(policy) = &config.acl_policy {
        let policy = policy.read().unwrap();
        let access_key = ext.and_then(|fields| fields.get("AccessKey"));
        let signature = ext.and_then(|fields| fields.get("Signature"));
        let valid = match (access_key, signature) {
            (Some(key), Some(signature)) => policy
                .secret(key)
                .is_some_and(|secret| acl_signature_valid(secret, ext, &cmd.body, signature)),
            _ => false,
        };
        if !valid {
            return Ok(Some(
                RemotingCommand::response(response_code::NO_PERMISSION, opaque)
                    .with_remark("invalid AccessKey/Signature"),
            ));
        }
        let topic = ext
            .and_then(|fields| match cmd.code {
                request_code::SEND_MESSAGE_V2 | request_code::SEND_BATCH_MESSAGE => {
                    fields.get("b").or_else(|| fields.get("topic"))
                }
                _ => fields.get("topic"),
            })
            .map(String::as_str)
            .unwrap_or("");
        let action = match cmd.code {
            request_code::SEND_MESSAGE
            | request_code::SEND_MESSAGE_V2
            | request_code::SEND_BATCH_MESSAGE => Some("WRITE"),
            request_code::PULL_MESSAGE => Some("READ"),
            _ => None,
        };
        if let Some(action) = action {
            let peer_ip = peer.ip().to_string();
            if !policy.allows(
                access_key.map(String::as_str).unwrap_or_default(),
                topic,
                action,
                &peer_ip,
            ) {
                return Ok(Some(
                    RemotingCommand::response(response_code::NO_PERMISSION, opaque)
                        .with_remark(format!("ACL permission denied for topic {topic}")),
                ));
            }
            if action == "READ" {
                let group = ext
                    .and_then(|fields| fields.get("consumerGroup").or_else(|| fields.get("group")))
                    .map(String::as_str)
                    .unwrap_or("");
                if !policy.allows_group(
                    access_key.map(String::as_str).unwrap_or_default(),
                    topic,
                    group,
                    action,
                    &peer_ip,
                ) {
                    return Ok(Some(
                        RemotingCommand::response(response_code::NO_PERMISSION, opaque)
                            .with_remark(format!("ACL permission denied for group {group}")),
                    ));
                }
            }
        }
    } else if !config.acl_secrets.is_empty() {
        let access_key = ext.and_then(|fields| fields.get("AccessKey"));
        let signature = ext.and_then(|fields| fields.get("Signature"));
        let valid = match (access_key, signature) {
            (Some(key), Some(signature)) => {
                if let Some(secret) = config.acl_secrets.get(key) {
                    let mut values = String::new();
                    if let Some(fields) = ext {
                        let mut sorted: Vec<_> = fields.iter().collect();
                        sorted.sort_by(|a, b| a.0.cmp(b.0));
                        for (name, value) in sorted {
                            if name != "Signature" {
                                values.push_str(value);
                            }
                        }
                    }
                    let expected = crate::hmac::rocketmq_signature(secret, &values, &cmd.body);
                    expected.as_bytes() == signature.as_bytes()
                } else {
                    false
                }
            }
            _ => false,
        };
        if !valid {
            return Ok(Some(
                RemotingCommand::response(response_code::NO_PERMISSION, opaque)
                    .with_remark("invalid AccessKey/Signature"),
            ));
        }
    } else if !config.acl_keys.is_empty() {
        let allowed = ext
            .and_then(|fields| fields.get("AccessKey"))
            .is_some_and(|key| config.acl_keys.iter().any(|allowed| allowed == key));
        if !allowed {
            return Ok(Some(
                RemotingCommand::response(response_code::NO_PERMISSION, opaque)
                    .with_remark("access key is missing or not allowed"),
            ));
        }
    }
    if cmd.code == request_code::END_TRANSACTION {
        tracing::info!(?ext, "END_TRANSACTION request received");
    }
    match cmd.code {
        request_code::SEND_MESSAGE | request_code::SEND_MESSAGE_V2 => {
            let req = SendMessageRequest::from_ext(ext, cmd.code == request_code::SEND_MESSAGE_V2)?;
            let queue_id = if req.queue_id < 0 { 0 } else { req.queue_id };
            let body_len = cmd.body.len() as u64;
            // The Java client may fall back to sending a normal message to
            // %RETRY%group when CONSUMER_SEND_MSG_BACK fails. Enforce the
            // retry limit here too, so DLQ still works.
            let mut topic = req.topic.clone();
            let mut reconsume_times = req.reconsume_times;
            if let Some(v) = mq_protocol::message::property_value(&req.properties, "RECONSUME_TIME")
            {
                if let Ok(n) = v.parse::<i32>() {
                    reconsume_times = n;
                }
            }
            let max_reconsume =
                mq_protocol::message::property_value(&req.properties, "MAX_RECONSUME_TIME")
                    .and_then(|v| v.parse::<i32>().ok())
                    .unwrap_or(16);
            if topic.starts_with("%RETRY%") && reconsume_times > max_reconsume {
                let group = topic.trim_start_matches("%RETRY%").to_string();
                topic = format!("%DLQ%{group}");
                tracing::info!(
                    group,
                    reconsume_times,
                    max_reconsume,
                    "retry exhausted, routed to DLQ"
                );
            }
            let is_transaction = mq_protocol::message::property_value(&req.properties, "TRAN_MSG")
                .map(|v| v.eq_ignore_ascii_case("true"))
                .unwrap_or(false);
            let delay_level = mq_protocol::message::property_value(&req.properties, "DELAY")
                .and_then(|v| v.parse::<i32>().ok())
                .unwrap_or(0);
            let start_deliver_at =
                mq_protocol::message::property_value(&req.properties, "__STARTDELIVERTIME")
                    .and_then(|v| v.parse::<i64>().ok());
            let msg = StoredMessage {
                msg_id: mq_protocol::message::property_value(&req.properties, "UNIQ_KEY")
                    .unwrap_or_else(new_msg_id),
                topic,
                queue_id,
                queue_offset: 0,
                physical_offset: 0,
                sys_flag: req.sys_flag,
                flag: req.flag,
                born_timestamp: req.born_timestamp,
                born_host: [127, 0, 0, 1],
                born_port: 0,
                store_timestamp: mq_common::now_ms(),
                store_host: [127, 0, 0, 1],
                store_port: config.store_port,
                reconsume_times,
                prepared_transaction_offset: 0,
                body: cmd.body,
                properties: req.properties,
            };
            let saved = if is_transaction {
                config.store.prepare_half(msg)?
            } else {
                let now = mq_common::now_ms();
                let start_deliver = start_deliver_at.filter(|t| *t > now).map(|t| t - now);
                if let Some(delay_ms) = start_deliver.or_else(|| delay_level_ms(delay_level)) {
                    let at = now + delay_ms;
                    if let Some(raft) = &config.raft {
                        let command = mq_raft::MessageCommand {
                            topic: msg.topic.clone(),
                            queue_id: msg.queue_id,
                            msg_id: msg.msg_id.clone(),
                            body: msg.body.to_vec(),
                            properties: msg.properties.clone(),
                            deliver_at_ms: Some(at.max(0) as u64),
                            tx_id: None,
                            tx_kind: None,
                        };
                        raft.write(command).await.map_err(MqError::Store)?;
                        let mut resp = RemotingCommand::response(response_code::SUCCESS, opaque);
                        SendMessageResponse {
                            msg_id: offset_msg_id(config.store_port, 0),
                            queue_id: 0,
                            queue_offset: 0,
                            transaction_id: None,
                        }
                        .apply(&mut resp);
                        return Ok(Some(resp));
                    }
                    let msg_id = msg.msg_id.clone();
                    let topic = msg.topic.clone();
                    config.store.schedule_delay(msg, at);
                    tracing::info!(topic = %topic, msg_id, delay_ms, "delayed message scheduled");
                    let mut resp = RemotingCommand::response(response_code::SUCCESS, opaque);
                    SendMessageResponse {
                        msg_id: offset_msg_id(config.store_port, 0),
                        queue_id: 0,
                        queue_offset: 0,
                        transaction_id: None,
                    }
                    .apply(&mut resp);
                    return Ok(Some(resp));
                }
                if let Some(raft) = &config.raft {
                    let command = mq_raft::MessageCommand {
                        topic: msg.topic.clone(),
                        queue_id: msg.queue_id,
                        msg_id: msg.msg_id.clone(),
                        body: msg.body.to_vec(),
                        properties: msg.properties.clone(),
                        deliver_at_ms: None,
                        tx_id: None,
                        tx_kind: None,
                    };
                    raft.write(command).await.map_err(MqError::Store)?;
                    tracing::debug!(msg_id = %msg.msg_id, "raft write committed");
                    wait_applied(&config.store, &msg.msg_id)
                        .await
                        .ok_or_else(|| MqError::Store("raft applied message not found".into()))?
                } else {
                    config.store.append(msg)?
                }
            };
            if is_transaction {
                if let Some(raft) = &config.raft {
                    let command = mq_raft::MessageCommand {
                        topic: saved.topic.clone(),
                        queue_id: saved.queue_id,
                        msg_id: saved.msg_id.clone(),
                        body: saved.body.to_vec(),
                        properties: saved.properties.clone(),
                        deliver_at_ms: None,
                        tx_id: Some(saved.msg_id.clone()),
                        tx_kind: Some("half".into()),
                    };
                    raft.write(command).await.map_err(MqError::Store)?;
                }
            }
            config
                .metrics
                .send_total
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            config
                .metrics
                .message_bytes
                .fetch_add(body_len, std::sync::atomic::Ordering::Relaxed);
            if !is_transaction {
                config.trace.record(
                    "send",
                    &saved.topic,
                    saved.queue_id,
                    saved.queue_offset,
                    &saved.msg_id,
                    None,
                );
                if saved.topic.starts_with("%DLQ%") {
                    config.trace.record(
                        "dlq",
                        &saved.topic,
                        saved.queue_id,
                        saved.queue_offset,
                        &saved.msg_id,
                        None,
                    );
                }
            }
            if is_transaction {
                config
                    .metrics
                    .tx_prepare_total
                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                config.trace.record(
                    "tx_prepare",
                    &saved.topic,
                    saved.queue_id,
                    saved.queue_offset,
                    &saved.msg_id,
                    None,
                );
                config
                    .metrics
                    .tx_pending
                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                config.audit.write(
                    "tx.prepare",
                    serde_json::json!({
                        "tx_id": saved.msg_id.clone(),
                        "topic": saved.topic.clone(),
                        "queue_id": saved.queue_id,
                    }),
                );
                if let Some(tc) = &config.tc {
                    tc.enqueue(TcOp::Prepare, &saved.msg_id);
                }
            }
            tracing::debug!(
                topic = %saved.topic,
                queue = saved.queue_id,
                offset = saved.queue_offset,
                transaction = is_transaction,
                "message stored"
            );
            let mut resp = RemotingCommand::response(response_code::SUCCESS, opaque);
            SendMessageResponse {
                msg_id: offset_msg_id(config.store_port, saved.physical_offset.max(0) as u64),
                queue_id: saved.queue_id,
                queue_offset: saved.queue_offset,
                transaction_id: if is_transaction {
                    Some(saved.msg_id)
                } else {
                    None
                },
            }
            .apply(&mut resp);
            Ok(Some(resp))
        }
        request_code::SEND_BATCH_MESSAGE => {
            let req = SendMessageRequest::from_ext(ext, true)?;
            let queue_id = if req.queue_id < 0 { 0 } else { req.queue_id };
            let mut pos = 0usize;
            let mut decoded: Vec<StoredMessage> = Vec::new();
            while pos < cmd.body.len() {
                let (batch_msg, used) =
                    mq_protocol::message::decode_batch_message(&cmd.body[pos..])?;
                pos += used;
                decoded.push(StoredMessage {
                    msg_id: mq_protocol::message::property_value(&batch_msg.properties, "UNIQ_KEY")
                        .unwrap_or_else(new_msg_id),
                    topic: req.topic.clone(),
                    queue_id,
                    queue_offset: 0,
                    physical_offset: 0,
                    sys_flag: 0,
                    flag: batch_msg.flag,
                    born_timestamp: mq_common::now_ms(),
                    born_host: [127, 0, 0, 1],
                    born_port: 0,
                    store_timestamp: mq_common::now_ms(),
                    store_host: [127, 0, 0, 1],
                    store_port: config.store_port,
                    reconsume_times: 0,
                    prepared_transaction_offset: 0,
                    body: batch_msg.body,
                    properties: batch_msg.properties,
                });
            }
            if decoded.is_empty() {
                return Err(MqError::Protocol("empty batch message".into()));
            }
            if let Some(raft) = &config.raft {
                let commands = decoded
                    .iter()
                    .map(|msg| mq_raft::MessageCommand {
                        topic: msg.topic.clone(),
                        queue_id: msg.queue_id,
                        msg_id: msg.msg_id.clone(),
                        body: msg.body.to_vec(),
                        properties: msg.properties.clone(),
                        deliver_at_ms: None,
                        tx_id: None,
                        tx_kind: None,
                    })
                    .collect();
                raft.write_batch(commands).await.map_err(MqError::Store)?;
            }
            if config.raft.is_some() {
                if let Some(last_id) = decoded.last().map(|msg| msg.msg_id.clone()) {
                    wait_applied(&config.store, &last_id).await.ok_or_else(|| {
                        MqError::Store("raft applied batch message not found".into())
                    })?;
                }
            }
            let count = decoded.len() as u64;
            let mut last: Option<StoredMessage> = None;
            for msg in decoded {
                let saved = if config.raft.is_some() {
                    config.store.message_by_id(&msg.msg_id).ok_or_else(|| {
                        MqError::Store("raft applied batch message not found".into())
                    })?
                } else {
                    config.store.append(msg)?
                };
                config.trace.record(
                    "send",
                    &saved.topic,
                    saved.queue_id,
                    saved.queue_offset,
                    &saved.msg_id,
                    None,
                );
                last = Some(saved);
            }
            let saved = last.unwrap();
            config
                .metrics
                .send_total
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            config
                .metrics
                .message_bytes
                .fetch_add(cmd.body.len() as u64, std::sync::atomic::Ordering::Relaxed);
            let mut resp = RemotingCommand::response(response_code::SUCCESS, opaque);
            SendMessageResponse {
                msg_id: offset_msg_id(config.store_port, saved.physical_offset.max(0) as u64),
                queue_id: saved.queue_id,
                queue_offset: saved.queue_offset,
                transaction_id: None,
            }
            .apply(&mut resp);
            tracing::debug!(count, queue = queue_id, "batch message appended");
            Ok(Some(resp))
        }
        request_code::PULL_MESSAGE => {
            let req = PullMessageRequest::from_ext(ext)?;
            let max = req.max_msg_nums.clamp(1, 1024) as usize;
            let (mut raw, mut next, mut max_offset) =
                config
                    .store
                    .read(&req.topic, req.queue_id, req.queue_offset, max)?;
            // Push consumers ask the broker to suspend when there is no data.
            // R2 caps the hold at 1s so a slow/closed client does not pin a task
            // forever; the production long-polling service uses a proper
            // per-queue wait list.
            if raw.is_empty() && req.suspend_timeout_millis > 0 {
                let wait = req.suspend_timeout_millis.clamp(1, 1000) as u64;
                let _ = timeout(Duration::from_millis(wait), config.store.wait_for_message()).await;
                let again = config
                    .store
                    .read(&req.topic, req.queue_id, req.queue_offset, max)?;
                raw = again.0;
                next = again.1;
                max_offset = again.2;
            }
            let raw_len = raw.len();
            let stored_sub = config.store.subscription(&req.consumer_group, &req.topic);
            let subscription = req
                .subscription
                .clone()
                .or_else(|| stored_sub.as_ref().map(|(s, _)| s.clone()));
            let expression_type = req
                .expression_type
                .clone()
                .or_else(|| stored_sub.map(|(_, t)| t));
            let msgs: Vec<_> = raw
                .into_iter()
                .filter(|m| {
                    if expression_type.as_deref() == Some("SQL92") {
                        crate::filter::matches_sql92(
                            &m.properties,
                            subscription.as_deref().unwrap_or(""),
                        )
                    } else {
                        tag_matches(&m.properties, subscription.as_deref())
                    }
                })
                .collect();
            for msg in &msgs {
                config.trace.record(
                    "pull",
                    &msg.topic,
                    msg.queue_id,
                    msg.queue_offset,
                    &msg.msg_id,
                    Some(&req.consumer_group),
                );
            }
            let code = if msgs.is_empty() {
                response_code::PULL_NOT_FOUND
            } else {
                response_code::SUCCESS
            };
            config
                .metrics
                .pull_total
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            if msgs.is_empty() {
                config
                    .metrics
                    .pull_empty_total
                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            } else {
                config
                    .metrics
                    .deliver_messages
                    .fetch_add(msgs.len() as u64, std::sync::atomic::Ordering::Relaxed);
            }
            let mut resp = RemotingCommand::response(code, opaque);
            PullMessageResponse {
                suggest_which_broker_id: 0,
                next_begin_offset: next + raw_len as i64,
                min_offset: 0,
                max_offset,
            }
            .apply(&mut resp);
            let mut body = Vec::new();
            for m in &msgs {
                body.extend_from_slice(&m.encode());
            }
            resp.body = bytes::Bytes::from(body);
            tracing::debug!(
                topic = %req.topic,
                queue = req.queue_id,
                offset = req.queue_offset,
                count = msgs.len(),
                "pull served"
            );
            Ok(Some(resp))
        }
        request_code::QUERY_CONSUMER_OFFSET => {
            let req = QueryConsumerOffsetRequest::from_ext(ext)?;
            let offset = config
                .store
                .query_offset(&req.consumer_group, &req.topic, req.queue_id);
            tracing::info!(
                group = %req.consumer_group,
                topic = %req.topic,
                queue = req.queue_id,
                offset,
                "offset queried"
            );
            let mut resp = RemotingCommand::response(response_code::SUCCESS, opaque);
            resp.put_ext("offset", offset.to_string());
            Ok(Some(resp))
        }
        request_code::UPDATE_CONSUMER_OFFSET => {
            let group = get(ext, "consumerGroup")?;
            let topic = get(ext, "topic")?;
            let queue_id: i32 = get(ext, "queueId")?
                .parse()
                .map_err(|e| MqError::Protocol(format!("bad queueId: {e}")))?;
            let commit_offset: i64 = get(ext, "commitOffset")?
                .parse()
                .map_err(|e| MqError::Protocol(format!("bad commitOffset: {e}")))?;
            config
                .store
                .update_offset(group, topic, queue_id, commit_offset);
            tracing::info!(
                group,
                topic,
                queue = queue_id,
                offset = commit_offset,
                "offset updated"
            );
            Ok(Some(RemotingCommand::response(
                response_code::SUCCESS,
                opaque,
            )))
        }
        request_code::GET_BROKER_RUNTIME_INFO => {
            let body = serde_json::json!({
                "table": {
                    "msgPutTotalYesterdayMorning": "0",
                    "msgPutTotalTodayMorning": "0",
                    "msgPutTotalTodayNow": "0",
                    "msgGetTotalYesterdayMorning": "0",
                    "msgGetTotalTodayMorning": "0",
                    "msgGetTotalTodayNow": "0",
                    "putTps": "0.00 0.00 0.00",
                    "getTransferedTps": "0.00 0.00 0.00",
                    "sendThreadPoolQueueSize": "0",
                    "pullThreadPoolQueueSize": "0",
                    "sendThreadPoolQueueHeadWaitTimeMills": "0",
                    "pullThreadPoolQueueHeadWaitTimeMills": "0",
                    "pageCacheLockTimeMills": "0",
                    "earliestMessageTimeStamp": mq_common::now_ms().to_string(),
                    "commitLogDiskRatio": "0.0",
                    "brokerVersionDesc": "MQ_RUST_R2",
                    "brokerVersion": "MQ_RUST_R2"
                }
            })
            .to_string();
            Ok(Some(
                RemotingCommand::response(response_code::SUCCESS, opaque)
                    .with_body(bytes::Bytes::from(body)),
            ))
        }
        request_code::GET_MAX_OFFSET => {
            let req = GetMaxOffsetRequest::from_ext(ext)?;
            let offset = config.store.max_offset(&req.topic, req.queue_id);
            let mut resp = RemotingCommand::response(response_code::SUCCESS, opaque);
            resp.put_ext("offset", offset.to_string());
            Ok(Some(resp))
        }
        request_code::GET_MIN_OFFSET => {
            let req = GetMaxOffsetRequest::from_ext(ext)?;
            let offset = config.store.min_offset(&req.topic, req.queue_id);
            let mut resp = RemotingCommand::response(response_code::SUCCESS, opaque);
            resp.put_ext("offset", offset.to_string());
            Ok(Some(resp))
        }
        request_code::GET_CONSUMER_LIST_BY_GROUP => {
            let group = get(ext, "consumerGroup")?;
            let ids = config.store.consumer_ids(group);
            let body = serde_json::json!({ "consumerIdList": ids }).to_string();
            Ok(Some(
                RemotingCommand::response(response_code::SUCCESS, opaque)
                    .with_body(bytes::Bytes::from(body)),
            ))
        }
        request_code::LOCK_BATCH_MQ => {
            let body: serde_json::Value =
                serde_json::from_slice(&cmd.body).unwrap_or(serde_json::Value::Null);
            let mq_set = body
                .get("mqSet")
                .cloned()
                .unwrap_or_else(|| serde_json::json!([]));
            let response = serde_json::json!({ "lockOKMQSet": mq_set }).to_string();
            Ok(Some(
                RemotingCommand::response(response_code::SUCCESS, opaque)
                    .with_body(bytes::Bytes::from(response)),
            ))
        }
        request_code::UNLOCK_BATCH_MQ => Ok(Some(RemotingCommand::response(
            response_code::SUCCESS,
            opaque,
        ))),
        request_code::CHECK_CLIENT_CONFIG => {
            let mut resp = RemotingCommand::response(response_code::SUCCESS, opaque);
            resp.version = 409;
            Ok(Some(resp))
        }
        request_code::GET_CONSUME_STATS => {
            let group = get(ext, "consumerGroup")?;
            let topic_filter = ext.and_then(|m| m.get("topic")).map(String::as_str);
            let mut table = serde_json::Map::new();
            for (topic, queue_id, consumer_offset, broker_offset) in
                config.store.consumer_progress(group)
            {
                if let Some(filter) = topic_filter {
                    if !filter.is_empty() && filter != topic {
                        continue;
                    }
                }
                let mq = serde_json::json!({
                    "topic": topic,
                    "brokerName": "broker-a",
                    "queueId": queue_id,
                })
                .to_string();
                table.insert(
                    mq,
                    serde_json::json!({
                        "brokerOffset": broker_offset,
                        "consumerOffset": consumer_offset,
                        "lastTimestamp": mq_common::now_ms(),
                    }),
                );
            }
            let body = serde_json::json!({
                "offsetTable": table,
                "consumeTps": 0.0,
            })
            .to_string();
            Ok(Some(
                RemotingCommand::response(response_code::SUCCESS, opaque).with_body(body),
            ))
        }
        215 => {
            let topic = ext
                .and_then(|m| m.get("topic"))
                .map(String::as_str)
                .unwrap_or_default();
            if !topic.is_empty() {
                config.store.delete_topic(topic);
            }
            Ok(Some(RemotingCommand::response(
                response_code::SUCCESS,
                opaque,
            )))
        }
        17 => {
            let topic = ext
                .and_then(|m| m.get("topic"))
                .map(String::as_str)
                .unwrap_or_default();
            if !topic.is_empty() {
                config.store.create_topic(topic);
            }
            Ok(Some(RemotingCommand::response(
                response_code::SUCCESS,
                opaque,
            )))
        }
        21 => {
            let mut topics = config.store.topic_names();
            if !topics.iter().any(|t| t == "TBW102") {
                topics.push("TBW102".to_string());
            }
            let body = serde_json::json!({
                "topicList": topics,
                "brokerAddr": config.listen.to_string()
            });
            Ok(Some(
                RemotingCommand::response(response_code::SUCCESS, opaque)
                    .with_body(serde_json::to_vec(&body).unwrap_or_default()),
            ))
        }
        200 => {
            let group = ext
                .and_then(|m| m.get("groupName"))
                .or_else(|| ext.and_then(|m| m.get("subscriptionGroup")))
                .map(String::as_str)
                .map(str::to_string)
                .or_else(|| {
                    serde_json::from_slice::<serde_json::Value>(&cmd.body)
                        .ok()
                        .and_then(|v| {
                            v.get("groupName")
                                .and_then(|g| g.as_str())
                                .map(str::to_string)
                        })
                })
                .unwrap_or_default();
            if !group.is_empty() {
                config.store.create_subscription_group(&group);
            }
            Ok(Some(RemotingCommand::response(
                response_code::SUCCESS,
                opaque,
            )))
        }
        207 => {
            let group = ext
                .and_then(|m| m.get("groupName"))
                .map(String::as_str)
                .map(str::to_string)
                .or_else(|| {
                    serde_json::from_slice::<serde_json::Value>(&cmd.body)
                        .ok()
                        .and_then(|v| {
                            v.get("groupName")
                                .and_then(|g| g.as_str())
                                .map(str::to_string)
                        })
                })
                .unwrap_or_default();
            if !group.is_empty() {
                config.store.delete_subscription_group(&group);
            }
            Ok(Some(RemotingCommand::response(
                response_code::SUCCESS,
                opaque,
            )))
        }
        201 => {
            let mut table = serde_json::Map::new();
            for group in config.store.subscription_groups() {
                table.insert(
                    group.clone(),
                    serde_json::json!({
                        "groupName": group,
                        "consumeEnable": true,
                        "consumeFromMinEnable": false,
                        "consumeBroadcastEnable": true,
                        "retryMaxTimes": 16,
                        "retryQueueNums": 1,
                        "brokerId": 0,
                        "whichBrokerWhenConsumeSlowly": 1
                    }),
                );
            }
            let body = serde_json::json!({ "subscriptionGroupTable": table });
            Ok(Some(
                RemotingCommand::response(response_code::SUCCESS, opaque)
                    .with_body(serde_json::to_vec(&body).unwrap_or_default()),
            ))
        }
        202 => {
            let topic = ext
                .and_then(|m| m.get("topic"))
                .map(String::as_str)
                .unwrap_or_default();
            let mut offset_table = serde_json::Map::new();
            for queue_id in 0..config.store.queue_nums() {
                let key = format!(
                    "{{\"topic\":\"{topic}\",\"brokerName\":\"broker-a\",\"queueId\":{queue_id}}}"
                );
                offset_table.insert(
                    key,
                    serde_json::json!({
                        "minOffset": config.store.min_offset(topic, queue_id),
                        "maxOffset": config.store.max_offset(topic, queue_id),
                        "lastUpdateTimestamp": mq_common::now_ms(),
                    }),
                );
            }
            let body = serde_json::json!({ "offsetTable": offset_table });
            Ok(Some(
                RemotingCommand::response(response_code::SUCCESS, opaque)
                    .with_body(serde_json::to_vec(&body).unwrap_or_default()),
            ))
        }
        204 => {
            let group = ext
                .and_then(|m| m.get("producerGroup"))
                .map(String::as_str)
                .unwrap_or_default();
            let mut connection_set = Vec::new();
            for client_id in config.store.producer_clients(group) {
                let addr = config.store.producer_addr(&client_id);
                connection_set.push(serde_json::json!({
                    "clientId": client_id,
                    "clientAddr": addr,
                    "language": "JAVA",
                    "version": 409
                }));
            }
            let body = serde_json::json!({ "connectionSet": connection_set });
            Ok(Some(
                RemotingCommand::response(response_code::SUCCESS, opaque)
                    .with_body(serde_json::to_vec(&body).unwrap_or_default()),
            ))
        }
        203 => {
            let group = ext
                .and_then(|m| m.get("consumerGroup"))
                .map(String::as_str)
                .unwrap_or_default();
            let mut connection_set = Vec::new();
            let mut subscription_table = serde_json::Map::new();
            for (topic, sub_string, expression_type) in config.store.subscriptions_for_group(group)
            {
                subscription_table.insert(
                    topic.clone(),
                    serde_json::json!({
                        "topic": topic,
                        "subString": sub_string,
                        "tagsSet": [],
                        "codeSet": [],
                        "subVersion": mq_common::now_ms(),
                        "expressionType": expression_type,
                        "classFilterMode": false
                    }),
                );
            }
            for client_id in config.store.consumer_clients(group) {
                let addr = config.store.consumer_addr(&client_id);
                connection_set.push(serde_json::json!({
                    "clientId": client_id,
                    "clientAddr": addr,
                    "language": "JAVA",
                    "version": 409
                }));
            }
            let body = serde_json::json!({
                "connectionSet": connection_set,
                "subscriptionTable": subscription_table,
                "consumeType": 1,
                "messageModel": 1,
                "consumeFromWhere": 0
            });
            Ok(Some(
                RemotingCommand::response(response_code::SUCCESS, opaque)
                    .with_body(serde_json::to_vec(&body).unwrap_or_default()),
            ))
        }
        request_code::HEART_BEAT => {
            if let Ok(body) = serde_json::from_slice::<serde_json::Value>(&cmd.body) {
                if let Some(client_id) = body.get("clientID").and_then(|v| v.as_str()) {
                    if let Some(consumers) = body.get("consumerDataSet").and_then(|v| v.as_array())
                    {
                        for consumer in consumers {
                            if let Some(group) = consumer.get("groupName").and_then(|v| v.as_str())
                            {
                                config.store.register_consumer_addr(
                                    group,
                                    client_id,
                                    &peer.to_string(),
                                );
                                if let Some(subs) = consumer
                                    .get("subscriptionDataSet")
                                    .and_then(|v| v.as_array())
                                {
                                    for sub in subs {
                                        let topic = sub
                                            .get("topic")
                                            .and_then(|v| v.as_str())
                                            .unwrap_or_default();
                                        let sub_string = sub
                                            .get("subString")
                                            .and_then(|v| v.as_str())
                                            .unwrap_or_default();
                                        let expr_type = sub
                                            .get("expressionType")
                                            .and_then(|v| v.as_str())
                                            .unwrap_or("TAG");
                                        if !topic.is_empty() {
                                            config.store.register_subscription(
                                                group, topic, sub_string, expr_type,
                                            );
                                        }
                                    }
                                }
                                tracing::info!(group, client_id, "consumer registered");
                            }
                        }
                    }
                    if let Some(producers) = body.get("producerDataSet").and_then(|v| v.as_array())
                    {
                        for producer in producers {
                            if let Some(group) = producer.get("groupName").and_then(|v| v.as_str())
                            {
                                config.producers.register(group, sender.clone());
                                config
                                    .store
                                    .register_producer(group, client_id, &peer.to_string());
                                tracing::info!(group, client_id, "producer registered");
                            }
                        }
                    }
                }
            }
            let mut resp = RemotingCommand::response(response_code::SUCCESS, opaque);
            // Advertise RocketMQ 4.9.8 (ordinal 409); clients gate SQL92
            // filtering on broker version >= V4_1_0.
            resp.version = 409;
            Ok(Some(resp))
        }
        request_code::UNREGISTER_CLIENT => {
            let client_id = if let Some(id) = ext.and_then(|m| m.get("clientID")) {
                Some(id.to_string())
            } else {
                serde_json::from_slice::<serde_json::Value>(&cmd.body)
                    .ok()
                    .and_then(|v| {
                        v.get("clientID")
                            .and_then(|x| x.as_str())
                            .map(str::to_string)
                    })
            };
            if let Some(client_id) = client_id.as_deref() {
                config.store.unregister_client(client_id);
                tracing::info!(client_id, "client unregistered");
            }
            Ok(Some(RemotingCommand::response(
                response_code::SUCCESS,
                opaque,
            )))
        }
        request_code::END_TRANSACTION => {
            let tx_id = ext
                .and_then(|m| m.get("transactionId"))
                .map(String::as_str)
                .unwrap_or_default();
            let msg_id = ext
                .and_then(|m| m.get("msgId"))
                .map(String::as_str)
                .unwrap_or_default();
            let action = ext
                .and_then(|m| m.get("commitOrRollback"))
                .and_then(|v| v.parse::<i32>().ok())
                .unwrap_or(0);
            match action {
                8 => {
                    let raft_committed = if let Some(raft) = &config.raft {
                        let command = mq_raft::MessageCommand {
                            topic: String::new(),
                            queue_id: 0,
                            msg_id: msg_id.to_string(),
                            body: Vec::new(),
                            properties: String::new(),
                            deliver_at_ms: None,
                            tx_id: Some(tx_id.to_string()),
                            tx_kind: Some("commit".into()),
                        };
                        raft.write(command).await.map_err(MqError::Store)?;
                        true
                    } else {
                        false
                    };
                    let committed = if raft_committed {
                        config
                            .store
                            .message_by_id(if msg_id.is_empty() { tx_id } else { msg_id })
                    } else {
                        config.store.commit_half(tx_id, msg_id)?
                    };
                    tracing::info!(
                        tx_id,
                        msg_id,
                        committed = committed.is_some(),
                        "transaction commit"
                    );
                    if committed.is_some() {
                        if let Some(ref committed) = committed {
                            config.trace.record(
                                "tx_commit",
                                &committed.topic,
                                committed.queue_id,
                                committed.queue_offset,
                                &committed.msg_id,
                                None,
                            );
                        }
                        config
                            .metrics
                            .tx_commit_total
                            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                        config
                            .metrics
                            .tx_pending
                            .fetch_sub(1, std::sync::atomic::Ordering::Relaxed);
                        config.audit.write(
                            "tx.commit",
                            serde_json::json!({
                                "tx_id": tx_id,
                                "msg_id": msg_id,
                                "operator": "broker",
                            }),
                        );
                        if let Some(tc) = &config.tc {
                            tc.enqueue(TcOp::Commit, tx_id);
                        }
                    }
                }
                12 => {
                    let raft_rollback = if let Some(raft) = &config.raft {
                        let command = mq_raft::MessageCommand {
                            topic: String::new(),
                            queue_id: 0,
                            msg_id: msg_id.to_string(),
                            body: Vec::new(),
                            properties: String::new(),
                            deliver_at_ms: None,
                            tx_id: Some(tx_id.to_string()),
                            tx_kind: Some("rollback".into()),
                        };
                        raft.write(command).await.map_err(MqError::Store)?;
                        true
                    } else {
                        false
                    };
                    let rolled_back = if raft_rollback {
                        true
                    } else {
                        config.store.rollback_half(tx_id, msg_id)
                    };
                    tracing::info!(tx_id, msg_id, rolled_back, "transaction rollback");
                    if rolled_back {
                        config.trace.record("tx_rollback", "", 0, 0, tx_id, None);
                        config
                            .metrics
                            .tx_rollback_total
                            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                        config
                            .metrics
                            .tx_pending
                            .fetch_sub(1, std::sync::atomic::Ordering::Relaxed);
                        config.audit.write(
                            "tx.rollback",
                            serde_json::json!({
                                "tx_id": tx_id,
                                "msg_id": msg_id,
                                "operator": "broker",
                            }),
                        );
                        if let Some(tc) = &config.tc {
                            tc.enqueue(TcOp::Rollback, tx_id);
                        }
                    }
                }
                _ => {
                    tracing::info!(tx_id, msg_id, action, "transaction still unknown");
                }
            }
            Ok(Some(RemotingCommand::response(
                response_code::SUCCESS,
                opaque,
            )))
        }
        request_code::CONSUMER_SEND_MSG_BACK => {
            tracing::info!(?ext, "send-back request");
            let group = get(ext, "group")?;
            let origin_msg_id = ext
                .and_then(|m| m.get("originMsgId"))
                .map(String::as_str)
                .unwrap_or_default();
            let max: i32 = ext
                .and_then(|m| m.get("maxReconsumeTimes"))
                .and_then(|v| v.parse().ok())
                .unwrap_or(16);
            tracing::info!(group, origin_msg_id, max, "send-back parsed");
            if let Some(origin) = config.store.find_by_msg_id(origin_msg_id) {
                let next_retry = config
                    .store
                    .next_retry_times(origin_msg_id, origin.reconsume_times);
                tracing::info!(
                    group,
                    origin_msg_id,
                    current = origin.reconsume_times,
                    next_retry,
                    max,
                    "send-back origin found"
                );
                let is_dlq = next_retry > max;
                let topic = if is_dlq {
                    format!("%DLQ%{group}")
                } else {
                    format!("%RETRY%{group}")
                };
                let mut retry = origin;
                retry.msg_id = new_msg_id();
                retry.topic = topic.clone();
                retry.queue_id = 0;
                retry.queue_offset = 0;
                retry.physical_offset = 0;
                retry.reconsume_times = next_retry;
                retry.store_timestamp = mq_common::now_ms();
                retry.store_host = [127, 0, 0, 1];
                retry.store_port = config.store_port;
                let saved = config.store.append(retry)?;
                tracing::info!(
                    group,
                    origin_msg_id,
                    retry_topic = %topic,
                    reconsume_times = next_retry,
                    queue_offset = saved.queue_offset,
                    dlq = is_dlq,
                    "message sent back"
                );
            } else {
                tracing::warn!(group, origin_msg_id, "send-back origin not found");
            }
            Ok(Some(RemotingCommand::response(
                response_code::SUCCESS,
                opaque,
            )))
        }
        _ => Ok(Some(
            RemotingCommand::response(response_code::REQUEST_CODE_NOT_SUPPORTED, opaque)
                .with_remark(format!("unsupported request code {}", cmd.code)),
        )),
    }
}

fn tag_matches(properties: &str, subscription: Option<&str>) -> bool {
    let Some(sub) = subscription else {
        return true;
    };
    let sub = sub.trim();
    if sub.is_empty() || sub == "*" {
        return true;
    }
    let tags = properties.split('\u{2}').find_map(|kv| {
        let mut it = kv.splitn(2, '\u{1}');
        if it.next() == Some("TAGS") {
            it.next()
        } else {
            None
        }
    });
    let Some(tags) = tags else {
        return false;
    };
    sub.split("||")
        .any(|pat| tags.split("||").any(|tag| tag.trim() == pat.trim()))
}

fn delay_level_ms(level: i32) -> Option<i64> {
    let ms = match level {
        1 => 1_000,
        2 => 5_000,
        3 => 10_000,
        4 => 30_000,
        5 => 60_000,
        6 => 120_000,
        7 => 180_000,
        8 => 240_000,
        9 => 300_000,
        10 => 360_000,
        11 => 420_000,
        12 => 480_000,
        13 => 540_000,
        14 => 600_000,
        15 => 1_200_000,
        16 => 1_800_000,
        17 => 3_600_000,
        18 => 7_200_000,
        _ => return None,
    };
    Some(ms)
}

fn offset_msg_id(port: u16, offset: u64) -> String {
    // 4-byte IPv4 + 4-byte port + 8-byte commitLog offset, hex encoded.
    format!("7F000001{:08X}{:016X}", port, offset)
}

fn get<'a>(
    ext: Option<&'a std::collections::BTreeMap<String, String>>,
    key: &str,
) -> MqResult<&'a str> {
    ext.and_then(|m| m.get(key))
        .map(String::as_str)
        .ok_or_else(|| MqError::Protocol(format!("missing ext field {key}")))
}

fn acl_signature_valid(
    secret: &str,
    ext: Option<&std::collections::BTreeMap<String, String>>,
    body: &[u8],
    signature: &str,
) -> bool {
    let mut values = String::new();
    if let Some(fields) = ext {
        let mut sorted: Vec<_> = fields.iter().collect();
        sorted.sort_by(|a, b| a.0.cmp(b.0));
        for (name, value) in sorted {
            if name != "Signature" {
                values.push_str(value);
            }
        }
    }
    crate::hmac::rocketmq_signature(secret, &values, body).as_bytes() == signature.as_bytes()
}

async fn wait_applied(store: &BrokerStore, msg_id: &str) -> Option<StoredMessage> {
    for _ in 0..400 {
        if let Some(msg) = store.message_by_id(msg_id) {
            return Some(msg);
        }
        tokio::time::sleep(std::time::Duration::from_millis(5)).await;
    }
    None
}

#[cfg(test)]
mod tests {
    use super::reconcile_once;
    use super::AuditLog;
    use super::BrokerStore;
    use super::StoredMessage;
    use super::{delay_level_ms, tag_matches};
    use std::sync::Arc;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    #[test]
    fn tag_filter_matches_expression() {
        let props = "TAGS\u{1}TagA\u{2}KEYS\u{1}k\u{2}";
        assert!(tag_matches(props, Some("*")));
        assert!(tag_matches(props, Some("TagA")));
        assert!(tag_matches(props, Some("TagB || TagA")));
        assert!(!tag_matches(props, Some("TagB")));
        assert!(!tag_matches("KEYS\u{1}k\u{2}", Some("TagA")));
        assert!(tag_matches("KEYS\u{1}k\u{2}", Some("*")));
    }

    #[test]
    fn delay_levels_are_rocketmq_compatible() {
        assert_eq!(delay_level_ms(0), None);
        assert_eq!(delay_level_ms(1), Some(1_000));
        assert_eq!(delay_level_ms(2), Some(5_000));
        assert_eq!(delay_level_ms(5), Some(60_000));
        assert_eq!(delay_level_ms(18), Some(7_200_000));
        assert_eq!(delay_level_ms(19), None);
    }

    #[tokio::test]
    async fn reconcile_commits_tc_decided_half() {
        let dir = std::env::temp_dir().join(format!(
            "mq-rust-reconcile-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_millis()
        ));
        let store = Arc::new(BrokerStore::open(&dir, 1).unwrap());
        let audit = AuditLog::open(dir.join("audit.log")).unwrap();
        let msg = StoredMessage {
            msg_id: "TX-RECONCILE".into(),
            topic: "reconcile-topic".into(),
            queue_id: 0,
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
            body: bytes::Bytes::from_static(b"tx"),
            properties: String::new(),
        };
        store.prepare_half(msg).unwrap();
        assert_eq!(store.pending_half().len(), 1);

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut buf = [0u8; 1024];
            let _ = stream.read(&mut buf).await;
            let body = r#"{"status":"committed"}"#;
            let resp = format!(
                "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                body.len()
            );
            let _ = stream.write_all(resp.as_bytes()).await;
        });
        let tc = crate::tc::TcClient::new(&format!("http://{addr}"));
        let applied = reconcile_once(&store, &tc, &audit).await;
        assert_eq!(applied, 1);
        assert!(store.pending_half().is_empty());
        let _ = std::fs::remove_dir_all(&dir);
    }
}
