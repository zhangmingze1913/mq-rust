//! Very small HTTP client for the TC service (no external HTTP dependency).

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

#[derive(Clone)]
pub struct TcClient {
    addrs: Vec<String>,
}

impl TcClient {
    pub fn new(urls: &str) -> Self {
        let addrs = urls
            .split(',')
            .map(|u| {
                u.trim()
                    .trim_start_matches("http://")
                    .trim_end_matches('/')
                    .to_string()
            })
            .filter(|u| !u.is_empty())
            .collect();
        Self { addrs }
    }

    pub async fn prepare(&self, tx_id: &str) -> bool {
        self.post(&format!("/tx/{tx_id}/prepare")).await
    }

    pub async fn commit(&self, tx_id: &str) -> bool {
        self.post(&format!("/tx/{tx_id}/commit")).await
    }

    pub async fn rollback(&self, tx_id: &str) -> bool {
        self.post(&format!("/tx/{tx_id}/rollback")).await
    }

    /// Read `tx_id` status from the first reachable TC node.
    pub async fn status(&self, tx_id: &str) -> Option<String> {
        for addr in &self.addrs {
            let Ok(mut stream) = TcpStream::connect(addr).await else {
                continue;
            };
            let req =
                format!("GET /tx/{tx_id} HTTP/1.1\r\nHost: {addr}\r\nConnection: close\r\n\r\n");
            if stream.write_all(req.as_bytes()).await.is_err() {
                continue;
            }
            let mut buf = [0u8; 4096];
            let Ok(n) = stream.read(&mut buf).await else {
                continue;
            };
            let text = String::from_utf8_lossy(&buf[..n]);
            if !text.starts_with("HTTP/1.1 200") {
                continue;
            }
            if let Some(body) = text.split("\r\n\r\n").nth(1) {
                if let Ok(value) = serde_json::from_str::<serde_json::Value>(body) {
                    if let Some(status) = value.get("status").and_then(|v| v.as_str()) {
                        return Some(status.to_string());
                    }
                }
            }
        }
        None
    }

    async fn post(&self, path: &str) -> bool {
        for addr in &self.addrs {
            let Ok(mut stream) = TcpStream::connect(addr).await else {
                continue;
            };
            let req = format!(
                "POST {path} HTTP/1.1\r\nHost: {addr}\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"
            );
            if stream.write_all(req.as_bytes()).await.is_err() {
                continue;
            }
            let mut buf = [0u8; 256];
            let Ok(n) = stream.read(&mut buf).await else {
                continue;
            };
            if String::from_utf8_lossy(&buf[..n]).starts_with("HTTP/1.1 200") {
                return true;
            }
        }
        false
    }
}
