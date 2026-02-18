//! Raft network transport.
//!
//! Provides both HTTP (reqwest) and NATS network implementations.
//! The NATS implementation is gated behind the `nats-transport` feature.

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
    admin_key: Option<String>,
}

impl Default for NetworkFactory {
    fn default() -> Self {
        Self::new(None)
    }
}

impl NetworkFactory {
    pub fn new(admin_key: Option<String>) -> Self {
        let client = reqwest::Client::builder()
            .timeout(std::time::Duration::from_secs(5))
            .build()
            .expect("Failed to build Raft HTTP client");
        Self { client, admin_key }
    }
}

impl RaftNetworkFactory<TypeConfig> for NetworkFactory {
    type Network = NetworkClient;

    async fn new_client(&mut self, _target: NodeId, node: &RaftNode) -> Self::Network {
        NetworkClient {
            addr: node.addr.clone(),
            client: self.client.clone(),
            admin_key: self.admin_key.clone(),
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
    admin_key: Option<String>,
}

impl NetworkClient {
    /// Apply the admin API key header to a request builder, if configured.
    fn apply_auth(&self, req: reqwest::RequestBuilder) -> reqwest::RequestBuilder {
        match &self.admin_key {
            Some(key) => req.header("x-api-key", key),
            None => req,
        }
    }
}

impl RaftNetwork<TypeConfig> for NetworkClient {
    async fn vote(
        &mut self,
        rpc: VoteRequest<NodeId>,
        _option: RPCOption,
    ) -> Result<VoteResponse<NodeId>, RPCError<NodeId, RaftNode, RaftError<NodeId>>> {
        let url = format!("{}/raft/vote", self.addr);
        let req = self.apply_auth(self.client.post(&url).json(&rpc));
        let resp = req
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
        let req = self.apply_auth(self.client.post(&url).json(&rpc));
        let resp = req
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
        let req = self.apply_auth(self.client.post(&url).json(&rpc));
        let resp = req
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

// ---------------------------------------------------------------------------
// NATS-based Raft network (feature-gated)
// ---------------------------------------------------------------------------

/// Creates NATS network clients for Raft peer communication.
#[cfg(feature = "nats-transport")]
#[derive(Debug, Clone)]
pub struct NatsNetworkFactory {
    client: async_nats::Client,
}

#[cfg(feature = "nats-transport")]
impl NatsNetworkFactory {
    pub fn new(client: async_nats::Client) -> Self {
        Self { client }
    }
}

#[cfg(feature = "nats-transport")]
impl RaftNetworkFactory<TypeConfig> for NatsNetworkFactory {
    type Network = NatsNetworkClient;

    async fn new_client(&mut self, target: NodeId, _node: &RaftNode) -> Self::Network {
        NatsNetworkClient {
            node_id: target,
            client: self.client.clone(),
        }
    }
}

/// NATS client for a single Raft peer.
#[cfg(feature = "nats-transport")]
#[derive(Debug, Clone)]
pub struct NatsNetworkClient {
    node_id: NodeId,
    client: async_nats::Client,
}

#[cfg(feature = "nats-transport")]
impl NatsNetworkClient {
    async fn nats_rpc<Req: serde::Serialize, Resp: serde::de::DeserializeOwned>(
        &self,
        rpc_name: &str,
        payload: &Req,
    ) -> Result<Resp, RPCError<NodeId, RaftNode, openraft::error::Unreachable>> {
        let subject = crate::nats_transport::subject_raft(self.node_id, rpc_name);
        let timeout = std::time::Duration::from_secs(5);

        crate::nats_transport::nats_request(&self.client, &subject, payload, timeout)
            .await
            .map_err(|e| {
                RPCError::Unreachable(openraft::error::Unreachable::new(&std::io::Error::other(
                    e.to_string(),
                )))
            })
    }
}

#[cfg(feature = "nats-transport")]
impl RaftNetwork<TypeConfig> for NatsNetworkClient {
    async fn vote(
        &mut self,
        rpc: VoteRequest<NodeId>,
        _option: RPCOption,
    ) -> Result<VoteResponse<NodeId>, RPCError<NodeId, RaftNode, RaftError<NodeId>>> {
        self.nats_rpc("vote", &rpc).await.map_err(|e| match e {
            RPCError::Unreachable(u) => RPCError::Unreachable(u),
            _ => RPCError::Unreachable(openraft::error::Unreachable::new(&std::io::Error::other(
                "unexpected error",
            ))),
        })
    }

    async fn append_entries(
        &mut self,
        rpc: AppendEntriesRequest<TypeConfig>,
        _option: RPCOption,
    ) -> Result<AppendEntriesResponse<NodeId>, RPCError<NodeId, RaftNode, RaftError<NodeId>>> {
        self.nats_rpc("append", &rpc).await.map_err(|e| match e {
            RPCError::Unreachable(u) => RPCError::Unreachable(u),
            _ => RPCError::Unreachable(openraft::error::Unreachable::new(&std::io::Error::other(
                "unexpected error",
            ))),
        })
    }

    async fn install_snapshot(
        &mut self,
        rpc: InstallSnapshotRequest<TypeConfig>,
        _option: RPCOption,
    ) -> Result<
        InstallSnapshotResponse<NodeId>,
        RPCError<NodeId, RaftNode, RaftError<NodeId, InstallSnapshotError>>,
    > {
        self.nats_rpc("snapshot", &rpc).await.map_err(|e| match e {
            RPCError::Unreachable(u) => RPCError::Unreachable(u),
            _ => RPCError::Unreachable(openraft::error::Unreachable::new(&std::io::Error::other(
                "unexpected error",
            ))),
        })
    }
}
