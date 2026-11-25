//! Minimal RocketMQ-compatible NameServer for R1.
//!
//! It serves a single static broker route so a Java 4.9.8 producer/consumer can
//! resolve a topic and connect to `mq-broker`. Dynamic broker registration and
//! multi-node route merging come in R2.

use bytes::BytesMut;
use mq_common::Result as MqResult;
use mq_protocol::request_code;
use mq_protocol::response_code;
use mq_protocol::route::TopicRouteData;
use mq_protocol::{decode_frame, RemotingCommand};
use std::collections::HashSet;
use std::net::SocketAddr;
use std::sync::{Arc, RwLock};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

#[derive(Clone, Debug)]
pub struct NameServerConfig {
    pub listen: SocketAddr,
    pub cluster: String,
    pub broker_name: String,
    pub broker_addr: Arc<RwLock<String>>,
    pub queues: i32,
    pub topics: Arc<RwLock<HashSet<String>>>,
    pub admin_listen: Option<SocketAddr>,
    pub broker_epoch: Arc<RwLock<u64>>,
    pub fence_notify: Arc<Vec<String>>,
    pub fence_token: Option<String>,
    pub admin_token: Option<String>,
    pub route_file: Option<std::path::PathBuf>,
}

pub async fn run(config: NameServerConfig) -> std::io::Result<()> {
    let listener = TcpListener::bind(config.listen).await?;
    let state = Arc::new(config);
    if let Some(addr) = state.admin_listen {
        let admin_state = Arc::clone(&state);
        tokio::spawn(async move {
            let _ = run_admin(admin_state, addr).await;
        });
    }
    tracing::info!(listen = %state.listen, "mq-namesrv listening");
    loop {
        let (stream, peer) = listener.accept().await?;
        let state = Arc::clone(&state);
        tokio::spawn(async move {
            if let Err(err) = handle_connection(stream, state).await {
                tracing::warn!(%peer, error = %err, "namesrv connection closed");
            }
        });
    }
}

async fn notify_fence(target: &str, route: &str, epoch: u64, token: Option<&str>) {
    let Ok(mut stream) = TcpStream::connect(target).await else {
        return;
    };
    let body = format!("{{\"broker_addr\":\"{route}\",\"epoch\":{epoch}}}");
    let auth = token
        .map(|t| format!("Authorization: Bearer {t}\r\n"))
        .unwrap_or_default();
    let req = format!(
        "POST /fence HTTP/1.1\r\nHost: {target}\r\nContent-Type: application/json\r\n{auth}Content-Length: {}\r\nConnection: close\r\n\r\n{body}",
        body.len()
    );
    let _ = stream.write_all(req.as_bytes()).await;
    let mut buf = [0u8; 256];
    let _ = stream.read(&mut buf).await;
}

async fn run_admin(state: Arc<NameServerConfig>, addr: SocketAddr) -> std::io::Result<()> {
    let listener = TcpListener::bind(addr).await?;
    tracing::info!(%addr, "mq-namesrv admin listening");
    loop {
        let (mut stream, _) = listener.accept().await?;
        let state = Arc::clone(&state);
        tokio::spawn(async move {
            let mut buf = [0u8; 2048];
            let Ok(n) = stream.read(&mut buf).await else {
                return;
            };
            let req = String::from_utf8_lossy(&buf[..n]);
            let mut parts = req.lines().next().unwrap_or_default().split_whitespace();
            let method = parts.next().unwrap_or("GET");
            let path = parts.next().unwrap_or("/");
            let authorized = state.admin_token.as_ref().is_none_or(|token| {
                let expected = format!("authorization: Bearer {token}");
                req.lines().any(|line| line.eq_ignore_ascii_case(&expected))
            });
            let (status, body) = if !authorized {
                (
                    "401 Unauthorized",
                    "{\"error\":\"invalid admin token\"}".to_string(),
                )
            } else if method == "POST" {
                if let Some(addr) = path.strip_prefix("/broker/") {
                    *state.broker_addr.write().unwrap() = addr.to_string();
                    let epoch = {
                        let mut epoch = state.broker_epoch.write().unwrap();
                        *epoch += 1;
                        *epoch
                    };
                    for target in state.fence_notify.iter() {
                        let _ =
                            notify_fence(target, addr, epoch, state.fence_token.as_deref()).await;
                    }
                    if let Some(path) = &state.route_file {
                        let payload = serde_json::json!({
                            "broker_addr": addr,
                            "epoch": epoch,
                        });
                        let tmp = path.with_extension(format!("tmp.{}", std::process::id()));
                        if let Ok(mut file) = std::fs::File::create(&tmp) {
                            let _ = std::io::Write::write_all(
                                &mut file,
                                payload.to_string().as_bytes(),
                            );
                            let _ = file.sync_all();
                            let _ = std::fs::rename(&tmp, path);
                        }
                    }
                    (
                        "200 OK",
                        format!("{{\"broker_addr\":\"{addr}\",\"epoch\":{epoch}}}"),
                    )
                } else {
                    ("404 Not Found", "{\"error\":\"not found\"}".to_string())
                }
            } else {
                (
                    "200 OK",
                    format!(
                        "{{\"broker_addr\":\"{}\",\"epoch\":{}}}",
                        state.broker_addr.read().unwrap(),
                        state.broker_epoch.read().unwrap()
                    ),
                )
            };
            let resp = format!(
                "HTTP/1.1 {status}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                body.len()
            );
            let _ = stream.write_all(resp.as_bytes()).await;
        });
    }
}

async fn handle_connection(
    mut stream: TcpStream,
    state: Arc<NameServerConfig>,
) -> std::io::Result<()> {
    let mut buf = BytesMut::with_capacity(8192);
    loop {
        let n = stream.read_buf(&mut buf).await?;
        if n == 0 {
            return Ok(());
        }
        loop {
            let frame = decode_frame(&mut buf)
                .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e.to_string()))?;
            let Some(frame) = frame else { break };
            let serialize_type = frame.serialize_type;
            let cmd = RemotingCommand::decode(frame)
                .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e.to_string()))?;
            let oneway = cmd.is_oneway();
            let resp = handle_command(cmd, &state)
                .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e.to_string()))?;
            if !oneway {
                if let Some(mut resp) = resp {
                    resp.serialize_type = serialize_type;
                    let encoded = resp.encode().map_err(to_io)?;
                    stream.write_all(&encoded).await?;
                }
            }
        }
    }
}

fn to_io(e: mq_common::MqError) -> std::io::Error {
    std::io::Error::new(std::io::ErrorKind::InvalidData, e.to_string())
}

fn handle_command(
    cmd: RemotingCommand,
    state: &NameServerConfig,
) -> MqResult<Option<RemotingCommand>> {
    let opaque = cmd.opaque;
    let ext = cmd.ext_fields.as_ref();
    let mut resp = RemotingCommand::response(response_code::SUCCESS, opaque);
    match cmd.code {
        request_code::GET_ROUTEINFO_BY_TOPIC => {
            let topic = ext
                .and_then(|m| m.get("topic"))
                .ok_or_else(|| mq_common::MqError::Protocol("missing topic".into()))?;
            let route = TopicRouteData::single_broker(
                state.cluster.clone(),
                state.broker_name.clone(),
                state.broker_addr.read().unwrap().clone(),
                state.queues,
            );
            state.topics.write().unwrap().insert(topic.to_string());
            tracing::info!(topic, broker = %state.broker_addr.read().unwrap(), "route lookup");
            resp = resp.with_body(route.to_json()?);
        }
        request_code::GET_ALL_TOPIC_LIST_FROM_NAMESERVER => {
            let topics: Vec<String> = state.topics.read().unwrap().iter().cloned().collect();
            let body = serde_json::json!({
                "topicList": topics,
                "brokerAddr": state.broker_addr.read().unwrap().clone(),
            })
            .to_string();
            resp = resp.with_body(body);
        }
        request_code::GET_BROKER_CLUSTER_INFO => {
            let body = serde_json::json!({
                "brokerAddrTable": {
                    &state.broker_name: {
                        "cluster": &state.cluster,
                        "brokerName": &state.broker_name,
                        "brokerAddrs": { "0": &*state.broker_addr.read().unwrap() }
                    }
                },
                "clusterAddrTable": {
                    &state.cluster: [&state.broker_name]
                }
            })
            .to_string();
            resp = resp.with_body(body);
        }
        request_code::HEART_BEAT
        | request_code::REGISTER_BROKER
        | request_code::UNREGISTER_BROKER => {
            // Static R1 route: accept heartbeats/registrations without mutating it.
            tracing::debug!(code = cmd.code, "namesrv control request");
        }
        216 => {
            // DELETE_TOPIC_IN_NAMESRV: drop the static topic route.
            if let Some(topic) = ext.and_then(|m| m.get("topic")) {
                state.topics.write().unwrap().remove(topic);
            }
        }
        _ => {
            resp = RemotingCommand::response(response_code::REQUEST_CODE_NOT_SUPPORTED, opaque)
                .with_remark(format!("unsupported request code {}", cmd.code));
        }
    }
    Ok(Some(resp))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn config() -> NameServerConfig {
        NameServerConfig {
            listen: "127.0.0.1:0".parse().unwrap(),
            cluster: "DefaultCluster".into(),
            broker_name: "broker-a".into(),
            broker_addr: Arc::new(RwLock::new("127.0.0.1:10921".into())),
            queues: 8,
            topics: Arc::new(RwLock::new(HashSet::new())),
            admin_listen: None,
            broker_epoch: Arc::new(RwLock::new(1)),
            fence_notify: Arc::new(Vec::new()),
            fence_token: None,
            admin_token: None,
            route_file: None,
        }
    }

    #[test]
    fn route_lookup_returns_body() {
        let mut cmd = RemotingCommand::request(request_code::GET_ROUTEINFO_BY_TOPIC, 1);
        cmd.put_ext("topic", "orders");
        let resp = handle_command(cmd, &config()).unwrap().unwrap();
        assert_eq!(resp.code, response_code::SUCCESS);
        assert!(!resp.body.is_empty());
        let route: TopicRouteData = serde_json::from_slice(&resp.body).unwrap();
        assert_eq!(route.queue_datas[0].read_queue_nums, 8);
    }
}
