//! Minimal Prometheus text endpoint for R1.
//!
//! The production exporter will use `metrics` + `metrics-exporter-prometheus`;
//! R1 keeps three counters so `curl /metrics` is meaningful from day one.

use std::net::SocketAddr;
use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

#[derive(Default)]
pub struct Metrics {
    pub send_total: AtomicU64,
    pub pull_total: AtomicU64,
    pub pull_empty_total: AtomicU64,
    pub message_bytes: AtomicU64,
    pub deliver_messages: AtomicU64,
    pub tx_prepare_total: AtomicU64,
    pub tx_commit_total: AtomicU64,
    pub tx_rollback_total: AtomicU64,
    pub tx_check_total: AtomicU64,
    pub tx_timeout_total: AtomicU64,
    pub tx_pending: AtomicI64,
    pub store_topics: AtomicU64,
    pub store_queues: AtomicU64,
    pub store_messages: AtomicU64,
    pub store_body_bytes: AtomicU64,
    pub retained_dropped_total: AtomicU64,
    pub commitlog_bytes: AtomicU64,
}

impl Metrics {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn render(&self) -> String {
        let mut out = String::new();
        out.push_str("# HELP mq_broker_send_total Accepted SEND_MESSAGE requests\n");
        out.push_str("# TYPE mq_broker_send_total counter\n");
        out.push_str(&format!(
            "mq_broker_send_total {}\n",
            self.send_total.load(Ordering::Relaxed)
        ));
        out.push_str("# HELP mq_broker_pull_total Served PULL_MESSAGE requests\n");
        out.push_str("# TYPE mq_broker_pull_total counter\n");
        out.push_str(&format!(
            "mq_broker_pull_total {}\n",
            self.pull_total.load(Ordering::Relaxed)
        ));
        out.push_str("# HELP mq_broker_pull_empty_total PULL_MESSAGE requests with no data\n");
        out.push_str("# TYPE mq_broker_pull_empty_total counter\n");
        out.push_str(&format!(
            "mq_broker_pull_empty_total {}\n",
            self.pull_empty_total.load(Ordering::Relaxed)
        ));
        out.push_str("# HELP mq_broker_message_bytes_total Message body bytes accepted\n");
        out.push_str("# TYPE mq_broker_message_bytes_total counter\n");
        out.push_str(&format!(
            "mq_broker_message_bytes_total {}\n",
            self.message_bytes.load(Ordering::Relaxed)
        ));
        out.push_str("# HELP mq_broker_deliver_messages_total Messages returned by pulls\n");
        out.push_str("# TYPE mq_broker_deliver_messages_total counter\n");
        out.push_str(&format!(
            "mq_broker_deliver_messages_total {}\n",
            self.deliver_messages.load(Ordering::Relaxed)
        ));
        out.push_str("# HELP mq_broker_store_topics Current topic count\n");
        out.push_str("# TYPE mq_broker_store_topics gauge\n");
        out.push_str(&format!(
            "mq_broker_store_topics {}\n",
            self.store_topics.load(Ordering::Relaxed)
        ));
        out.push_str("# HELP mq_broker_store_queues Current queue count\n");
        out.push_str("# TYPE mq_broker_store_queues gauge\n");
        out.push_str(&format!(
            "mq_broker_store_queues {}\n",
            self.store_queues.load(Ordering::Relaxed)
        ));
        out.push_str("# HELP mq_broker_store_messages Current in-memory message count\n");
        out.push_str("# TYPE mq_broker_store_messages gauge\n");
        out.push_str(&format!(
            "mq_broker_store_messages {}\n",
            self.store_messages.load(Ordering::Relaxed)
        ));
        out.push_str("# HELP mq_broker_store_body_bytes Current body bytes\n");
        out.push_str("# TYPE mq_broker_store_body_bytes gauge\n");
        out.push_str(&format!(
            "mq_broker_store_body_bytes {}\n",
            self.store_body_bytes.load(Ordering::Relaxed)
        ));
        out.push_str("# HELP mq_broker_retained_dropped_total Messages dropped by retention\n");
        out.push_str("# TYPE mq_broker_retained_dropped_total counter\n");
        out.push_str(&format!(
            "mq_broker_retained_dropped_total {}\n",
            self.retained_dropped_total.load(Ordering::Relaxed)
        ));
        out.push_str("# HELP mq_broker_commitlog_bytes CommitLog bytes on disk\n");
        out.push_str("# TYPE mq_broker_commitlog_bytes gauge\n");
        out.push_str(&format!(
            "mq_broker_commitlog_bytes {}\n",
            self.commitlog_bytes.load(Ordering::Relaxed)
        ));
        out.push_str("# HELP mq_broker_tx_prepare_total Half messages prepared\n");
        out.push_str("# TYPE mq_broker_tx_prepare_total counter\n");
        out.push_str(&format!(
            "mq_broker_tx_prepare_total {}\n",
            self.tx_prepare_total.load(Ordering::Relaxed)
        ));
        out.push_str("# HELP mq_broker_tx_commit_total Committed transactions\n");
        out.push_str("# TYPE mq_broker_tx_commit_total counter\n");
        out.push_str(&format!(
            "mq_broker_tx_commit_total {}\n",
            self.tx_commit_total.load(Ordering::Relaxed)
        ));
        out.push_str("# HELP mq_broker_tx_rollback_total Rolled-back transactions\n");
        out.push_str("# TYPE mq_broker_tx_rollback_total counter\n");
        out.push_str(&format!(
            "mq_broker_tx_rollback_total {}\n",
            self.tx_rollback_total.load(Ordering::Relaxed)
        ));
        out.push_str("# HELP mq_broker_tx_check_total Check-back requests sent\n");
        out.push_str("# TYPE mq_broker_tx_check_total counter\n");
        out.push_str(&format!(
            "mq_broker_tx_check_total {}\n",
            self.tx_check_total.load(Ordering::Relaxed)
        ));
        out.push_str("# HELP mq_broker_tx_timeout_total Half messages expired\n");
        out.push_str("# TYPE mq_broker_tx_timeout_total counter\n");
        out.push_str(&format!(
            "mq_broker_tx_timeout_total {}\n",
            self.tx_timeout_total.load(Ordering::Relaxed)
        ));
        out.push_str("# HELP mq_broker_tx_pending Pending half messages\n");
        out.push_str("# TYPE mq_broker_tx_pending gauge\n");
        out.push_str(&format!(
            "mq_broker_tx_pending {}\n",
            self.tx_pending.load(Ordering::Relaxed)
        ));
        out
    }
}

pub async fn serve(metrics: Arc<Metrics>, addr: SocketAddr) -> std::io::Result<()> {
    let listener = TcpListener::bind(addr).await?;
    tracing::info!(%addr, "metrics endpoint listening");
    loop {
        let (mut stream, _) = listener.accept().await?;
        let metrics = Arc::clone(&metrics);
        tokio::spawn(async move {
            let mut buf = [0u8; 1024];
            let _ = stream.read(&mut buf).await;
            let body = metrics.render();
            let response = format!(
                "HTTP/1.1 200 OK\r\nContent-Type: text/plain; version=0.0.4\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                body.len(),
                body
            );
            let _ = stream.write_all(response.as_bytes()).await;
        });
    }
}
