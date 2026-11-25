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
}

pub async fn serve(
    addr: SocketAddr,
    store: Arc<BrokerStore>,
    metrics: Arc<Metrics>,
    tls: Option<GrpcTls>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let _ = tokio_rustls::rustls::crypto::aws_lc_rs::default_provider().install_default();
    let svc = AdminSvc { store, metrics };
    let mut builder = tonic::transport::Server::builder();
    if let Some(tls) = tls {
        let identity =
            tonic::transport::Identity::from_pem(std::fs::read(tls.cert)?, std::fs::read(tls.key)?);
        let mut config = tonic::transport::ServerTlsConfig::new().identity(identity);
        if let Some(ca) = tls.client_ca {
            config =
                config.client_ca_root(tonic::transport::Certificate::from_pem(std::fs::read(ca)?));
        }
        builder = builder.tls_config(config)?;
    }
    builder
        .add_service(AdminServer::new(svc))
        .serve(addr)
        .await?;
    Ok(())
}
