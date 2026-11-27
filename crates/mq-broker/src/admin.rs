//! Tiny admin HTTP endpoint for R3 transaction status and operator actions.
//!
//! `GET  /tx`                    -> JSON array of pending half transactions
//! `POST /tx/{id}/commit`        -> manually commit a half transaction
//! `POST /tx/{id}/rollback`      -> manually roll back a half transaction
//! `GET  /health`                -> OK

use crate::audit::AuditLog;
use crate::inbox::Inbox;
use crate::store::BrokerStore;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

#[allow(clippy::too_many_arguments)]
pub async fn serve(
    store: Arc<BrokerStore>,
    audit: Arc<AuditLog>,
    inbox: Option<Arc<Inbox>>,
    addr: SocketAddr,
    read_only: Arc<std::sync::atomic::AtomicBool>,
    advertise: String,
    fence_token: Option<String>,
    admin_token: Option<String>,
    trace: Arc<crate::trace::TraceLog>,
    raft: Arc<std::sync::OnceLock<Arc<mq_raft::MessageRaft>>>,
) -> std::io::Result<()> {
    let listener = TcpListener::bind(addr).await?;
    tracing::info!(%addr, "admin endpoint listening");
    let advertise = Arc::new(advertise);
    let fence_token = Arc::new(fence_token);
    let admin_token = Arc::new(admin_token);
    loop {
        let (stream, _) = listener.accept().await?;
        let store = Arc::clone(&store);
        let audit = Arc::clone(&audit);
        let inbox = inbox.clone();
        let read_only = Arc::clone(&read_only);
        let advertise = Arc::clone(&advertise);
        let fence_token = Arc::clone(&fence_token);
        let admin_token = Arc::clone(&admin_token);
        let trace = Arc::clone(&trace);
        let raft = Arc::clone(&raft);
        tokio::spawn(async move {
            let _ = handle(
                stream,
                store,
                audit,
                inbox,
                read_only,
                advertise,
                fence_token,
                admin_token,
                trace,
                raft,
            )
            .await;
        });
    }
}

#[allow(clippy::too_many_arguments)]
async fn handle(
    mut stream: TcpStream,
    store: Arc<BrokerStore>,
    audit: Arc<AuditLog>,
    inbox: Option<Arc<Inbox>>,
    read_only: Arc<std::sync::atomic::AtomicBool>,
    advertise: Arc<String>,
    fence_token: Arc<Option<String>>,
    admin_token: Arc<Option<String>>,
    trace: Arc<crate::trace::TraceLog>,
    raft: Arc<std::sync::OnceLock<Arc<mq_raft::MessageRaft>>>,
) -> std::io::Result<()> {
    let mut data = Vec::new();
    loop {
        let mut buf = [0u8; 4096];
        let n = stream.read(&mut buf).await?;
        if n == 0 {
            break;
        }
        data.extend_from_slice(&buf[..n]);
        let text = String::from_utf8_lossy(&data);
        if let Some(header_end) = text.find("\r\n\r\n") {
            let content_length = text
                .lines()
                .take_while(|line| !line.is_empty())
                .find_map(|line| {
                    let (key, value) = line.split_once(':')?;
                    if key.eq_ignore_ascii_case("content-length") {
                        value.trim().parse::<usize>().ok()
                    } else {
                        None
                    }
                })
                .unwrap_or(0);
            if data.len() >= header_end + 4 + content_length {
                break;
            }
        } else if data.len() > 64 * 1024 {
            break;
        }
    }
    let request = String::from_utf8_lossy(&data);
    let mut parts = request
        .lines()
        .next()
        .unwrap_or_default()
        .split_whitespace();
    let method = parts.next().unwrap_or("GET");
    let path = parts.next().unwrap_or("/");
    let admin_authorized = admin_token.as_ref().as_ref().is_none_or(|token| {
        let expected = format!("authorization: Bearer {token}");
        request
            .lines()
            .any(|line| line.eq_ignore_ascii_case(&expected))
    });

    let (status, body) = if path != "/fence" && path != "/health" && !admin_authorized {
        (
            "401 Unauthorized",
            "{\"error\":\"invalid admin token\"}".to_string(),
        )
    } else if let Some(inbox) = inbox.as_ref().filter(|_| path.starts_with("/inbox/")) {
        let pieces: Vec<&str> = path.split('/').collect();
        let group = pieces.get(2).copied().unwrap_or_default();
        let msg_id = pieces.get(3).copied().unwrap_or_default();
        if method == "POST" && !group.is_empty() && !msg_id.is_empty() {
            match inbox.check_and_mark(group, msg_id) {
                Ok(fresh) => (
                    "200 OK",
                    serde_json::json!({"duplicate": !fresh, "fresh": fresh}).to_string(),
                ),
                Err(err) => (
                    "500 Internal Server Error",
                    serde_json::json!({"error": err.to_string()}).to_string(),
                ),
            }
        } else {
            (
                "400 Bad Request",
                "{\"error\":\"bad inbox request\"}".to_string(),
            )
        }
    } else if path == "/fence" && method == "POST" {
        let authorized = fence_token.as_ref().as_ref().is_none_or(|token| {
            let expected = format!("authorization: Bearer {token}");
            request
                .lines()
                .any(|line| line.eq_ignore_ascii_case(&expected))
        });
        if !authorized {
            (
                "403 Forbidden",
                "{\"error\":\"invalid fence token\"}".to_string(),
            )
        } else {
            let body = request.split("\r\n\r\n").nth(1).unwrap_or_default();
            let route = serde_json::from_str::<serde_json::Value>(body)
                .ok()
                .and_then(|v| {
                    v.get("broker_addr")
                        .and_then(|v| v.as_str())
                        .map(str::to_string)
                })
                .unwrap_or_default();
            let fenced = !route.is_empty() && route != *advertise;
            read_only.store(fenced, std::sync::atomic::Ordering::Relaxed);
            tracing::warn!(%route, fenced, "broker fence notification");
            (
                "200 OK",
                serde_json::json!({"fenced": fenced, "route": route}).to_string(),
            )
        }
    } else if path == "/version" {
        (
            "200 OK",
            serde_json::json!({
                "service": "mq-broker",
                "version": env!("CARGO_PKG_VERSION"),
            })
            .to_string(),
        )
    } else if path == "/traces/page" || path.starts_with("/traces/page?") {
        if method == "GET" {
            let query = path.split_once('?').map(|(_, q)| q).unwrap_or("");
            let mut topic: Option<&str> = None;
            let mut limit = 50usize;
            let mut offset = 0usize;
            let mut from: Option<i64> = None;
            let mut to: Option<i64> = None;
            for pair in query.split('&') {
                if let Some((key, value)) = pair.split_once('=') {
                    match key {
                        "topic" => topic = Some(value),
                        "limit" => {
                            limit = value.parse::<usize>().unwrap_or(50).clamp(1, 500);
                        }
                        "offset" => {
                            offset = value.parse::<usize>().unwrap_or(0);
                        }
                        "from" => from = value.parse::<i64>().ok(),
                        "to" => to = value.parse::<i64>().ok(),
                        _ => {}
                    }
                }
            }
            let (total, events) = trace.page(topic, from, to, offset, limit);
            (
                "200 OK",
                serde_json::json!({
                    "total": total,
                    "offset": offset,
                    "limit": limit,
                    "events": events,
                })
                .to_string(),
            )
        } else {
            (
                "400 Bad Request",
                "{\"error\":\"bad traces page request\"}".into(),
            )
        }
    } else if path == "/traces" || path.starts_with("/traces?") {
        if method == "GET" {
            let query = path.split_once('?').map(|(_, q)| q).unwrap_or("");
            let mut topic: Option<&str> = None;
            let mut limit = 50usize;
            let mut offset = 0usize;
            let mut from: Option<i64> = None;
            let mut to: Option<i64> = None;
            for pair in query.split('&') {
                if let Some((key, value)) = pair.split_once('=') {
                    match key {
                        "topic" => topic = Some(value),
                        "limit" => {
                            limit = value.parse::<usize>().unwrap_or(50).clamp(1, 500);
                        }
                        "offset" => offset = value.parse::<usize>().unwrap_or(0),
                        "from" => from = value.parse::<i64>().ok(),
                        "to" => to = value.parse::<i64>().ok(),
                        _ => {}
                    }
                }
            }
            let (_, events) = trace.page(topic, from, to, offset, limit);
            (
                "200 OK",
                serde_json::to_string(&events).unwrap_or_else(|_| "[]".into()),
            )
        } else {
            (
                "400 Bad Request",
                "{\"error\":\"bad traces request\"}".into(),
            )
        }
    } else if let Some(msg_id) = path.strip_prefix("/trace/") {
        if method == "GET" && !msg_id.is_empty() {
            (
                "200 OK",
                serde_json::to_string(&trace.by_msg_id(msg_id)).unwrap_or_else(|_| "[]".into()),
            )
        } else {
            (
                "400 Bad Request",
                "{\"error\":\"bad trace request\"}".into(),
            )
        }
    } else if path == "/stats" {
        let (topics, queues, messages, bytes) = store.stats();
        (
            "200 OK",
            serde_json::json!({
                "topics": topics,
                "queues": queues,
                "messages": messages,
                "body_bytes": bytes,
                "commitlog_bytes": store.commitlog_bytes(),
                "retained_dropped": store.retained_dropped(),
                "read_only": read_only.load(std::sync::atomic::Ordering::Relaxed),
                "topics_detail": store
                    .topic_stats()
                    .into_iter()
                    .map(|(topic, messages, body_bytes)| {
                        serde_json::json!({
                            "topic": topic,
                            "messages": messages,
                            "body_bytes": body_bytes,
                        })
                    })
                    .collect::<Vec<_>>(),
            })
            .to_string(),
        )
    } else if path == "/replica" {
        (
            "200 OK",
            serde_json::json!({"acked": store.replica_acked()}).to_string(),
        )
    } else if path.starts_with("/tx/") {
        let pieces: Vec<&str> = path.split('/').collect();
        // ["", "tx", id, "commit"|"rollback"]
        let id = pieces.get(2).copied().unwrap_or_default();
        let action = pieces.get(3).copied().unwrap_or_default();
        if method == "POST" && !id.is_empty() {
            match action {
                "commit" => match store.commit_half(id, id) {
                    Ok(Some(_)) => {
                        audit.write("tx.commit.admin", serde_json::json!({"tx_id": id}));
                        ("200 OK", serde_json::json!({"committed": true}).to_string())
                    }
                    Ok(None) => (
                        "404 Not Found",
                        serde_json::json!({"error": "tx not found"}).to_string(),
                    ),
                    Err(err) => (
                        "500 Internal Server Error",
                        serde_json::json!({"error": err.to_string()}).to_string(),
                    ),
                },
                "rollback" => {
                    let ok = store.rollback_half(id, id);
                    if ok {
                        audit.write("tx.rollback.admin", serde_json::json!({"tx_id": id}));
                    }
                    (
                        if ok { "200 OK" } else { "404 Not Found" },
                        serde_json::json!({"rolled_back": ok}).to_string(),
                    )
                }
                _ => (
                    "400 Bad Request",
                    "{\"error\":\"unknown action\"}".to_string(),
                ),
            }
        } else {
            (
                "405 Method Not Allowed",
                "{\"error\":\"POST required\"}".to_string(),
            )
        }
    } else if path.starts_with("/tx") {
        (
            "200 OK",
            serde_json::to_string(&store.pending_half()).unwrap_or_else(|_| "[]".to_string()),
        )
    } else if path.starts_with("/raft/add") {
        if method != "POST" {
            (
                "405 Method Not Allowed",
                "{\"error\":\"POST required\"}".to_string(),
            )
        } else {
            let query = path.split_once('?').map(|(_, q)| q).unwrap_or("");
            let mut node = None;
            let mut addr = None;
            for part in query.split('&') {
                if let Some((k, v)) = part.split_once('=') {
                    if k == "node" {
                        node = v.parse::<u64>().ok();
                    } else if k == "addr" {
                        addr = Some(v.to_string());
                    }
                }
            }
            match (node, addr, raft.get()) {
                (Some(node), Some(addr), Some(raft)) => {
                    if !raft.is_leader() {
                        ("409 Conflict", "{\"error\":\"not leader\"}".to_string())
                    } else {
                        match raft.add_node(node, addr).await {
                            Ok(()) => ("200 OK", serde_json::json!({"added": node}).to_string()),
                            Err(err) => (
                                "500 Internal Server Error",
                                serde_json::json!({"error": err}).to_string(),
                            ),
                        }
                    }
                }
                _ => (
                    "400 Bad Request",
                    "{\"error\":\"node and addr required\"}".to_string(),
                ),
            }
        }
    } else if path == "/compact" {
        if method != "POST" {
            (
                "405 Method Not Allowed",
                "{\"error\":\"POST required\"}".to_string(),
            )
        } else {
            match store.compact_now() {
                Ok(()) => ("200 OK", "{\"compacted\":true}".to_string()),
                Err(err) => (
                    "500 Internal Server Error",
                    serde_json::json!({"error": err.to_string()}).to_string(),
                ),
            }
        }
    } else if path.starts_with("/health") {
        ("200 OK", "ok".to_string())
    } else {
        ("404 Not Found", "not found".to_string())
    };

    let response = format!(
        "HTTP/1.1 {status}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
        body.len()
    );
    stream.write_all(response.as_bytes()).await
}
