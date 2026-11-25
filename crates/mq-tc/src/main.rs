//! Minimal TC HTTP service.
//!
//! Single node (default):
//!   mq-tc --api-listen 127.0.0.1:9191
//!
//! Three-node TCP Raft:
//!   mq-tc --id 1 --raft-listen 127.0.0.1:19301 --api-listen 127.0.0.1:19401 \
//!         --peers 1@127.0.0.1:19301,2@127.0.0.1:19302,3@127.0.0.1:19303
//!
//! Endpoints:
//!   POST /tx/{id}/prepare|commit|rollback
//!   GET  /tx/{id}
//!   GET  /leader

use mq_tc::Tc;
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

struct Args {
    id: u64,
    raft_listen: String,
    api_listen: String,
    peers: Option<String>,
    wal: Option<String>,
    data_dir: Option<String>,
}

fn parse_args() -> Args {
    let mut args = Args {
        id: 1,
        raft_listen: "127.0.0.1:19301".into(),
        api_listen: "127.0.0.1:9191".into(),
        peers: None,
        wal: None,
        data_dir: None,
    };
    let mut it = std::env::args().skip(1);
    while let Some(flag) = it.next() {
        let mut next = || it.next().unwrap_or_default();
        match flag.as_str() {
            "--id" => args.id = next().parse().unwrap_or(1),
            "--raft-listen" => args.raft_listen = next(),
            "--api-listen" => args.api_listen = next(),
            "--peers" => args.peers = Some(next()),
            "--wal" => args.wal = Some(next()),
            "--data-dir" => args.data_dir = Some(next()),
            _ => {}
        }
    }
    args
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = parse_args();
    let tc = if let Some(peers) = &args.peers {
        let mut members = Vec::new();
        for item in peers.split(',') {
            if let Some((id, addr)) = item.split_once('@') {
                members.push((id.parse::<u64>()?, addr.to_string()));
            }
        }
        Arc::new(if let Some(dir) = &args.data_dir {
            Tc::start_tcp_persistent(
                args.id,
                &args.raft_listen,
                members,
                std::path::Path::new(dir),
            )
            .await?
        } else {
            Tc::start_tcp_wal(
                args.id,
                &args.raft_listen,
                members,
                args.wal.as_deref().map(std::path::Path::new),
            )
            .await?
        })
    } else if let Some(wal) = &args.wal {
        Arc::new(Tc::start_with_wal(wal).await?)
    } else {
        Arc::new(Tc::start().await?)
    };

    let listener = TcpListener::bind(&args.api_listen).await?;
    println!(
        "mq-tc node={} api={} leader={:?}",
        tc.node_id(),
        args.api_listen,
        tc.current_leader().await
    );
    loop {
        let (stream, _) = listener.accept().await?;
        let tc = Arc::clone(&tc);
        tokio::spawn(async move {
            let _ = handle(stream, tc).await;
        });
    }
}

async fn handle(mut stream: TcpStream, tc: Arc<Tc>) -> std::io::Result<()> {
    let mut buf = [0u8; 2048];
    let n = stream.read(&mut buf).await?;
    let req = String::from_utf8_lossy(&buf[..n]);
    let mut parts = req.lines().next().unwrap_or_default().split_whitespace();
    let method = parts.next().unwrap_or("GET");
    let path = parts.next().unwrap_or("/");

    let (status, body) = if path == "/leader" {
        let leader = tc.current_leader().await;
        let leader_json = leader
            .map(|v| v.to_string())
            .unwrap_or_else(|| "null".to_string());
        (
            "200 OK",
            format!(
                "{{\"node\":{},\"leader\":{},\"is_leader\":{}}}",
                tc.node_id(),
                leader_json,
                tc.is_leader()
            ),
        )
    } else if path.starts_with("/tx/") {
        let pieces: Vec<&str> = path.split('/').collect();
        let id = pieces.get(2).copied().unwrap_or_default();
        let action = pieces.get(3).copied().unwrap_or("status");
        match (method, action) {
            ("POST", "prepare") => match tc.prepare(id).await {
                Ok(()) => (
                    "200 OK",
                    format!("{{\"tx_id\":\"{id}\",\"status\":\"prepared\"}}"),
                ),
                Err(e) => (
                    "500 Internal Server Error",
                    format!("{{\"error\":\"{e}\"}}"),
                ),
            },
            ("POST", "commit") => match tc.commit(id).await {
                Ok(()) => (
                    "200 OK",
                    format!("{{\"tx_id\":\"{id}\",\"status\":\"committed\"}}"),
                ),
                Err(e) => (
                    "500 Internal Server Error",
                    format!("{{\"error\":\"{e}\"}}"),
                ),
            },
            ("POST", "rollback") => match tc.rollback(id).await {
                Ok(()) => (
                    "200 OK",
                    format!("{{\"tx_id\":\"{id}\",\"status\":\"rolled_back\"}}"),
                ),
                Err(e) => (
                    "500 Internal Server Error",
                    format!("{{\"error\":\"{e}\"}}"),
                ),
            },
            (_, _) => match tc.status(id).await {
                Some(s) => (
                    "200 OK",
                    format!("{{\"tx_id\":\"{id}\",\"status\":\"{s}\"}}"),
                ),
                None => ("404 Not Found", "{\"error\":\"not found\"}".to_string()),
            },
        }
    } else {
        ("200 OK", "ok".to_string())
    };

    let resp = format!(
        "HTTP/1.1 {status}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
        body.len()
    );
    stream.write_all(resp.as_bytes()).await
}
