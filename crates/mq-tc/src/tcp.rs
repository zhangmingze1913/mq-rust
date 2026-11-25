//! TCP transport for the TC Raft group.
//!
//! Wire format: 4-byte big-endian length + JSON `RpcRequest`/`RpcResponse`.
//! Snapshot transfer is not implemented yet; the state is tiny and snapshots
//! are disabled in the current config.

use openraft::entry::RaftEntry;
use openraft::errors::RPCError;
use openraft::network::RPCOption;
use openraft::raft::AppendEntriesRequest;
use openraft::raft::AppendEntriesResponse;
use openraft::raft::AppendResult;
use openraft::raft::SnapshotResponse;
use openraft::raft::VoteRequest;
use openraft::raft::VoteResponse;
use openraft::type_config::alias::SnapshotOf;
use openraft::type_config::alias::VoteOf;
use openraft::OptionalSend;
use openraft::RaftNetworkFactory;
use openraft::RaftNetworkV2;
use openraft_memstore::TypeConfig;
use serde::Deserialize;
use serde::Serialize;
use std::collections::HashMap;
use std::io::Cursor;
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

pub type Raft = Arc<openraft::Raft<TypeConfig, crate::state::StatusSM>>;

#[derive(Serialize, Deserialize)]
pub enum RpcRequest {
    Append(AppendEntriesRequest<TypeConfig>),
    Vote(VoteRequest<TypeConfig>),
}

#[derive(Serialize, Deserialize)]
pub enum RpcResponse {
    Append(AppendEntriesResponse<TypeConfig>),
    Vote(VoteResponse<TypeConfig>),
}

#[derive(Clone)]
pub struct NetworkFactory {
    peers: HashMap<u64, String>,
}

impl NetworkFactory {
    pub fn new(peers: HashMap<u64, String>) -> Self {
        Self { peers }
    }
}

pub struct NetworkClient {
    addr: String,
}

impl RaftNetworkFactory<TypeConfig> for NetworkFactory {
    type Network = NetworkClient;

    async fn new_client(&mut self, target: u64, _node: &()) -> Self::Network {
        NetworkClient {
            addr: self.peers.get(&target).cloned().unwrap_or_default(),
        }
    }
}

impl RaftNetworkV2<TypeConfig> for NetworkClient {
    type SnapshotData = Cursor<Vec<u8>>;

    async fn append_entries(
        &mut self,
        rpc: AppendEntriesRequest<TypeConfig>,
        _option: RPCOption,
    ) -> Result<AppendResult<TypeConfig>, RPCError<TypeConfig>> {
        let prev = rpc.prev_log_id;
        let last = rpc.entries.last().map(|e| e.log_id()).or(rpc.prev_log_id);
        let resp: RpcResponse = self
            .call(&RpcRequest::Append(rpc))
            .await
            .map_err(unreachable)?;
        match resp {
            RpcResponse::Append(resp) => Ok(resp.into_append_result(prev, last)),
            _ => Err(unreachable("unexpected append response")),
        }
    }

    async fn vote(
        &mut self,
        rpc: VoteRequest<TypeConfig>,
        _option: RPCOption,
    ) -> Result<VoteResponse<TypeConfig>, RPCError<TypeConfig>> {
        let resp: RpcResponse = self
            .call(&RpcRequest::Vote(rpc))
            .await
            .map_err(unreachable)?;
        match resp {
            RpcResponse::Vote(resp) => Ok(resp),
            _ => Err(unreachable("unexpected vote response")),
        }
    }

    async fn full_snapshot(
        &mut self,
        _vote: VoteOf<TypeConfig>,
        _snapshot: SnapshotOf<TypeConfig, Self::SnapshotData>,
        _cancel: impl std::future::Future<Output = openraft::errors::ReplicationClosed>
            + OptionalSend
            + 'static,
        _option: RPCOption,
    ) -> Result<SnapshotResponse<TypeConfig>, openraft::errors::StreamingError<TypeConfig>> {
        Err(openraft::errors::StreamingError::Unreachable(
            openraft::errors::Unreachable::from_string("snapshot transfer not enabled"),
        ))
    }
}

impl NetworkClient {
    async fn call(&self, req: &RpcRequest) -> Result<RpcResponse, String> {
        let mut stream = TcpStream::connect(&self.addr)
            .await
            .map_err(|e| format!("connect {}: {e}", self.addr))?;
        let payload = serde_json::to_vec(req).map_err(|e| e.to_string())?;
        stream
            .write_all(&(payload.len() as u32).to_be_bytes())
            .await
            .map_err(|e| e.to_string())?;
        stream
            .write_all(&payload)
            .await
            .map_err(|e| e.to_string())?;
        let mut len = [0u8; 4];
        stream
            .read_exact(&mut len)
            .await
            .map_err(|e| e.to_string())?;
        let mut buf = vec![0u8; u32::from_be_bytes(len) as usize];
        stream
            .read_exact(&mut buf)
            .await
            .map_err(|e| e.to_string())?;
        serde_json::from_slice(&buf).map_err(|e| e.to_string())
    }
}

fn unreachable(msg: impl ToString) -> RPCError<TypeConfig> {
    RPCError::Unreachable(openraft::errors::Unreachable::from_string(msg.to_string()))
}

/// Serve Raft RPCs until the listener is closed.
pub async fn serve(listener: TcpListener, raft: Raft) {
    loop {
        let Ok((stream, _)) = listener.accept().await else {
            return;
        };
        let raft = Arc::clone(&raft);
        tokio::spawn(async move {
            let _ = handle(stream, raft).await;
        });
    }
}

async fn handle(mut stream: TcpStream, raft: Raft) -> std::io::Result<()> {
    let mut len = [0u8; 4];
    stream.read_exact(&mut len).await?;
    let mut buf = vec![0u8; u32::from_be_bytes(len) as usize];
    stream.read_exact(&mut buf).await?;
    let req: RpcRequest = serde_json::from_slice(&buf)
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
    let resp = match req {
        RpcRequest::Append(req) => match raft.append_entries(req).await {
            Ok(result) => RpcResponse::Append(AppendEntriesResponse::from(result)),
            Err(e) => return Err(std::io::Error::other(e.to_string())),
        },
        RpcRequest::Vote(req) => match raft.vote(req).await {
            Ok(v) => RpcResponse::Vote(v),
            Err(e) => return Err(std::io::Error::other(e.to_string())),
        },
    };
    let payload = serde_json::to_vec(&resp)
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
    stream
        .write_all(&(payload.len() as u32).to_be_bytes())
        .await?;
    stream.write_all(&payload).await?;
    Ok(())
}
