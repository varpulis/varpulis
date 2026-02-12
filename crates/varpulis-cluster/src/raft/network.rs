//! HTTP-based Raft network transport using reqwest.

use openraft::error::{InstallSnapshotError, RPCError, RaftError};
use openraft::network::{RPCOption, RaftNetwork, RaftNetworkFactory};
use openraft::raft::{
    AppendEntriesRequest, AppendEntriesResponse, InstallSnapshotRequest, InstallSnapshotResponse,
    VoteRequest, VoteResponse,
};

use super::{NodeId, RaftNode, TypeConfig};

// ---------------------------------------------------------------------------
// Network factory
// ---------------------------------------------------------------------------

/// Creates HTTP network clients for Raft peer communication.
#[derive(Debug, Clone)]
pub struct NetworkFactory {
    client: reqwest::Client,
}

impl Default for NetworkFactory {
    fn default() -> Self {
        Self::new()
    }
}

impl NetworkFactory {
    pub fn new() -> Self {
        let client = reqwest::Client::builder()
            .timeout(std::time::Duration::from_secs(5))
            .build()
            .expect("Failed to build Raft HTTP client");
        Self { client }
    }
}

impl RaftNetworkFactory<TypeConfig> for NetworkFactory {
    type Network = NetworkClient;

    async fn new_client(&mut self, _target: NodeId, node: &RaftNode) -> Self::Network {
        NetworkClient {
            addr: node.addr.clone(),
            client: self.client.clone(),
        }
    }
}

// ---------------------------------------------------------------------------
// Network client
// ---------------------------------------------------------------------------

/// HTTP client for a single Raft peer.
#[derive(Debug, Clone)]
pub struct NetworkClient {
    addr: String,
    client: reqwest::Client,
}

impl RaftNetwork<TypeConfig> for NetworkClient {
    async fn vote(
        &mut self,
        rpc: VoteRequest<NodeId>,
        _option: RPCOption,
    ) -> Result<VoteResponse<NodeId>, RPCError<NodeId, RaftNode, RaftError<NodeId>>> {
        let url = format!("{}/raft/vote", self.addr);
        let resp = self
            .client
            .post(&url)
            .json(&rpc)
            .send()
            .await
            .map_err(|e| RPCError::Unreachable(openraft::error::Unreachable::new(&e)))?;

        let status = resp.status();
        if !status.is_success() {
            let text = resp.text().await.unwrap_or_default();
            return Err(RPCError::Unreachable(openraft::error::Unreachable::new(
                &std::io::Error::other(format!("HTTP {status}: {text}")),
            )));
        }

        let body = resp
            .json()
            .await
            .map_err(|e| RPCError::Unreachable(openraft::error::Unreachable::new(&e)))?;
        Ok(body)
    }

    async fn append_entries(
        &mut self,
        rpc: AppendEntriesRequest<TypeConfig>,
        _option: RPCOption,
    ) -> Result<AppendEntriesResponse<NodeId>, RPCError<NodeId, RaftNode, RaftError<NodeId>>> {
        let url = format!("{}/raft/append", self.addr);
        let resp = self
            .client
            .post(&url)
            .json(&rpc)
            .send()
            .await
            .map_err(|e| RPCError::Unreachable(openraft::error::Unreachable::new(&e)))?;

        let status = resp.status();
        if !status.is_success() {
            let text = resp.text().await.unwrap_or_default();
            return Err(RPCError::Unreachable(openraft::error::Unreachable::new(
                &std::io::Error::other(format!("HTTP {status}: {text}")),
            )));
        }

        let body = resp
            .json()
            .await
            .map_err(|e| RPCError::Unreachable(openraft::error::Unreachable::new(&e)))?;
        Ok(body)
    }

    async fn install_snapshot(
        &mut self,
        rpc: InstallSnapshotRequest<TypeConfig>,
        _option: RPCOption,
    ) -> Result<
        InstallSnapshotResponse<NodeId>,
        RPCError<NodeId, RaftNode, RaftError<NodeId, InstallSnapshotError>>,
    > {
        let url = format!("{}/raft/snapshot", self.addr);
        let resp = self
            .client
            .post(&url)
            .json(&rpc)
            .send()
            .await
            .map_err(|e| RPCError::Unreachable(openraft::error::Unreachable::new(&e)))?;

        let status = resp.status();
        if !status.is_success() {
            let text = resp.text().await.unwrap_or_default();
            return Err(RPCError::Unreachable(openraft::error::Unreachable::new(
                &std::io::Error::other(format!("HTTP {status}: {text}")),
            )));
        }

        let body = resp
            .json()
            .await
            .map_err(|e| RPCError::Unreachable(openraft::error::Unreachable::new(&e)))?;
        Ok(body)
    }
}
