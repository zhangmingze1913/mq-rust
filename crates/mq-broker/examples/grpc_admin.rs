//! Minimal gRPC Admin client example.
//!
//! Usage:
//!   cargo run -p mq-broker --example grpc_admin -- 127.0.0.1:50051 [tx_id]

use mq_broker::grpc::proto::{HealthRequest, StatsRequest, TxStatusRequest};

use tonic::codec::ProstCodec;
use tonic::transport::Channel;
use tonic::Request;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut args = std::env::args().skip(1);
    let addr = args.next().unwrap_or_else(|| "127.0.0.1:50051".to_string());
    let tx_id = args.next().unwrap_or_else(|| "demo-tx".to_string());

    let channel = Channel::from_shared(format!("http://{addr}"))?
        .connect()
        .await?;
    let mut grpc = tonic::client::Grpc::new(channel);
    grpc.ready().await?;

    let health: mq_broker::grpc::proto::HealthResponse = grpc
        .unary(
            Request::new(HealthRequest {}),
            path("/mqrust.admin.v1.Admin/Health"),
            ProstCodec::default(),
        )
        .await?
        .into_inner();
    println!("health status={} version={}", health.status, health.version);

    grpc.ready().await?;
    let stats: mq_broker::grpc::proto::StatsResponse = grpc
        .unary(
            Request::new(StatsRequest {}),
            path("/mqrust.admin.v1.Admin/Stats"),
            ProstCodec::default(),
        )
        .await?
        .into_inner();
    println!(
        "stats send_total={} message_bytes={} commitlog_bytes={} pending_half={} topics={}",
        stats.send_total,
        stats.message_bytes,
        stats.commitlog_bytes,
        stats.pending_half,
        stats.topics
    );

    grpc.ready().await?;
    let tx: mq_broker::grpc::proto::TxStatusResponse = grpc
        .unary(
            Request::new(TxStatusRequest {
                tx_id: tx_id.clone(),
            }),
            path("/mqrust.admin.v1.Admin/TxStatus"),
            ProstCodec::default(),
        )
        .await?
        .into_inner();
    println!("tx_status tx_id={} pending={}", tx.tx_id, tx.pending);
    Ok(())
}

fn path(value: &'static str) -> tonic::codegen::http::uri::PathAndQuery {
    tonic::codegen::http::uri::PathAndQuery::from_static(value)
}
