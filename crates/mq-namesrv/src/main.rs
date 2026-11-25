use clap::Parser;
use mq_namesrv::server::{run, NameServerConfig};
use std::error::Error;

#[derive(Debug, Parser)]
#[command(
    name = "mq-namesrv",
    version,
    about = "MQ-rust NameServer (RocketMQ 4.9 compatible subset)"
)]
struct Args {
    #[arg(long, default_value = "127.0.0.1:9877")]
    listen: String,
    #[arg(long, default_value = "DefaultCluster")]
    cluster: String,
    #[arg(long, default_value = "broker-a")]
    broker_name: String,
    #[arg(long, default_value = "127.0.0.1:10921")]
    broker_addr: String,
    #[arg(long, default_value_t = 8)]
    queues: i32,
    #[arg(long)]
    admin_listen: Option<String>,
    #[arg(long)]
    fence_notify: Option<String>,
    #[arg(long)]
    fence_token: Option<String>,
    #[arg(long)]
    admin_token: Option<String>,
    #[arg(long)]
    route_file: Option<String>,
    #[arg(long)]
    tls_cert: Option<String>,
    #[arg(long)]
    tls_key: Option<String>,
    #[arg(long)]
    tls_client_ca: Option<String>,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    mq_observability::init_tracing(false);
    let args = Args::parse();
    let mut broker_addr = args.broker_addr;
    let mut epoch = 1u64;
    if let Some(path) = &args.route_file {
        if let Ok(text) = std::fs::read_to_string(path) {
            if let Ok(value) = serde_json::from_str::<serde_json::Value>(&text) {
                if let Some(addr) = value.get("broker_addr").and_then(|v| v.as_str()) {
                    broker_addr = addr.to_string();
                }
                if let Some(e) = value.get("epoch").and_then(|v| v.as_u64()) {
                    epoch = e;
                }
            }
        }
    }
    let config = NameServerConfig {
        listen: args.listen.parse()?,
        cluster: args.cluster,
        broker_name: args.broker_name,
        broker_addr: std::sync::Arc::new(std::sync::RwLock::new(broker_addr)),
        queues: args.queues,
        topics: std::sync::Arc::new(std::sync::RwLock::new(std::collections::HashSet::new())),
        admin_listen: args.admin_listen.map(|a| a.parse()).transpose()?,
        broker_epoch: std::sync::Arc::new(std::sync::RwLock::new(epoch)),
        fence_notify: std::sync::Arc::new(
            args.fence_notify
                .map(|s| {
                    s.split(',')
                        .map(str::trim)
                        .filter(|s| !s.is_empty())
                        .map(str::to_string)
                        .collect()
                })
                .unwrap_or_default(),
        ),
        fence_token: args.fence_token,
        admin_token: args.admin_token,
        route_file: args.route_file.map(Into::into),
        tls_cert: args.tls_cert,
        tls_key: args.tls_key,
        tls_client_ca: args.tls_client_ca,
    };
    run(config).await?;
    Ok(())
}
