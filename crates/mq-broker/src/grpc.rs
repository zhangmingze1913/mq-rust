//! gRPC admin service (optional, `--grpc-listen`).

use std::net::SocketAddr;
use std::sync::atomic::Ordering;
use std::sync::Arc;

use tonic::{Request, Response, Status};

use crate::metrics::Metrics;
use crate::store::BrokerStore;

pub mod proto {
    include!("grpc_generated.rs");
}

use proto::admin_server::{Admin, AdminServer};
use proto::{
    HealthRequest, HealthResponse, StatsRequest, StatsResponse, TxStatusRequest, TxStatusResponse,
};

#[derive(Clone)]
pub struct AdminSvc {
    store: Arc<BrokerStore>,
    metrics: Arc<Metrics>,
}

#[tonic::async_trait]
impl Admin for AdminSvc {
    async fn health(
        &self,
        _request: Request<HealthRequest>,
    ) -> Result<Response<HealthResponse>, Status> {
        Ok(Response::new(HealthResponse {
            status: "ok".into(),
            version: "0.9.0-rc.1".into(),
        }))
    }

    async fn stats(
        &self,
        _request: Request<StatsRequest>,
    ) -> Result<Response<StatsResponse>, Status> {
        Ok(Response::new(StatsResponse {
            send_total: self.metrics.send_total.load(Ordering::Relaxed),
            message_bytes: self.metrics.message_bytes.load(Ordering::Relaxed),
            commitlog_bytes: self.store.commitlog_bytes(),
            pending_half: self.store.pending_half_count() as u64,
            topics: self.store.topic_names().len() as u32,
        }))
    }

    async fn tx_status(
        &self,
        request: Request<TxStatusRequest>,
    ) -> Result<Response<TxStatusResponse>, Status> {
        let tx_id = request.into_inner().tx_id;
        let pending = self.store.pending_half().iter().any(|h| h.tx_id == tx_id);
        Ok(Response::new(TxStatusResponse { tx_id, pending }))
    }
}

pub struct GrpcTls {
    pub cert: String,
    pub key: String,
    pub client_ca: Option<String>,
    pub reload_ms: u64,
}

fn load_tls(
    tls: &GrpcTls,
) -> Result<tonic::transport::ServerTlsConfig, Box<dyn std::error::Error + Send + Sync>> {
    let identity =
        tonic::transport::Identity::from_pem(std::fs::read(&tls.cert)?, std::fs::read(&tls.key)?);
    let mut config = tonic::transport::ServerTlsConfig::new().identity(identity);
    if let Some(ca) = &tls.client_ca {
        config = config.client_ca_root(tonic::transport::Certificate::from_pem(std::fs::read(ca)?));
    }
    Ok(config)
}

fn tls_mtime(tls: &GrpcTls) -> std::time::SystemTime {
    let mut latest = std::time::SystemTime::UNIX_EPOCH;
    for path in [Some(&tls.cert), Some(&tls.key), tls.client_ca.as_ref()]
        .into_iter()
        .flatten()
    {
        if let Ok(meta) = std::fs::metadata(path) {
            if let Ok(modified) = meta.modified() {
                latest = latest.max(modified);
            }
        }
    }
    latest
}

pub async fn serve(
    addr: SocketAddr,
    store: Arc<BrokerStore>,
    metrics: Arc<Metrics>,
    tls: Option<GrpcTls>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let _ = tokio_rustls::rustls::crypto::aws_lc_rs::default_provider().install_default();
    let svc = AdminSvc { store, metrics };
    let Some(tls) = tls else {
        tonic::transport::Server::builder()
            .add_service(AdminServer::new(svc))
            .serve(addr)
            .await?;
        return Ok(());
    };
    let mut config = load_tls(&tls)?;
    let mut last_mtime = tls_mtime(&tls);
    loop {
        let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel::<()>();
        let mut builder = tonic::transport::Server::builder();
        builder = builder.tls_config(config.clone())?;
        let server = builder
            .add_service(AdminServer::new(svc.clone()))
            .serve_with_shutdown(addr, async move {
                let _ = shutdown_rx.await;
            });
        tokio::pin!(server);
        if tls.reload_ms == 0 {
            server.await?;
            return Ok(());
        }
        let interval = tls.reload_ms.max(500);
        loop {
            tokio::select! {
                result = &mut server => {
                    result?;
                    return Ok(());
                }
                _ = tokio::time::sleep(std::time::Duration::from_millis(interval)) => {
                    let changed = tls_mtime(&tls);
                    if changed <= last_mtime {
                        continue;
                    }
                    match load_tls(&tls) {
                        Ok(next) => {
                            last_mtime = changed;
                            let _ = shutdown_tx.send(());
                            let _ = server.await;
                            config = next;
                            tracing::info!("grpc tls certificate reloaded");
                            break;
                        }
                        Err(err) => tracing::warn!(%err, "grpc tls reload failed"),
                    }
                }
            }
        }
    }
}
