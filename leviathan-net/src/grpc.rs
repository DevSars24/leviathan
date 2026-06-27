//! gRPC service implementation for the Leviathan control plane.
//!
//! This module provides:
//!
//! 1. **Generated types** — Re-exports from `tonic::include_proto!` for the
//!    `leviathan` package defined in `proto/leviathan.proto`.
//! 2. **`NodeServiceImpl`** — Server-side implementation of `NodeService` that
//!    translates gRPC calls into `StateCommand` messages via an `mpsc::Sender`.
//! 3. **Conversion helpers** — Bidirectional conversions between protobuf types
//!    and `leviathan-core` domain types.
//!
//! # Architecture
//!
//! ```text
//!  gRPC client (node)                gRPC server (control plane)
//!  ───────────────────               ────────────────────────────
//!  RegisterNodeRequest  ──────►  NodeServiceImpl::register_node
//!                                      │
//!                                      ▼
//!                                mpsc::Sender<StateCommand>
//!                                      │
//!                                      ▼
//!                                ClusterStateActor
//! ```

use tokio::sync::{mpsc, oneshot};
use tonic::{Request, Response, Status};
use tracing::{info, warn};

use leviathan_core::{
    Heartbeat, Node, NodeId, NodeStatus, ResourceSpec,
};

// ---------------------------------------------------------------------------
// Generated protobuf types
// ---------------------------------------------------------------------------

/// Generated protobuf types and gRPC service traits from `leviathan.proto`.
// Allow missing_docs for generated protobuf module because tonic-build doesn't generate docstrings for all fields.
#[allow(missing_docs)]
pub mod proto {
    tonic::include_proto!("leviathan");
}

pub use proto::node_service_client::NodeServiceClient;
pub use proto::node_service_server::{NodeService, NodeServiceServer};
pub use proto::{
    DeregisterNodeRequest, DeregisterNodeResponse, HeartbeatRequest, HeartbeatResponse,
    ListNodesRequest, ListNodesResponse, NodeInfo, RegisterNodeRequest, RegisterNodeResponse,
    ResourceInfo,
};

// ---------------------------------------------------------------------------
// State commands (mirrored from leviathan-control, but defined here so the
// gRPC layer can be compiled independently)
// ---------------------------------------------------------------------------

/// Commands that the gRPC service sends to the cluster state actor.
///
/// This is a subset of the control plane's `StateCommand` enum — just enough
/// for the gRPC handler to drive state changes. The control plane crate maps
/// these into its own internal command type.
#[derive(Debug)]
pub enum GrpcStateCommand {
    /// Register a new node.
    RegisterNode {
        /// The node to register.
        node: Node,
    },
    /// Record a heartbeat.
    RecordHeartbeat {
        /// The heartbeat data.
        heartbeat: Heartbeat,
    },
    /// Deregister a node.
    DeregisterNode {
        /// ID of the node.
        id: NodeId,
    },
    /// Retrieve all registered nodes.
    GetNodes {
        /// Oneshot channel to receive the response.
        resp: oneshot::Sender<Vec<Node>>,
    },
}

// ---------------------------------------------------------------------------
// Server implementation
// ---------------------------------------------------------------------------

/// Server-side implementation of the `NodeService` gRPC interface.
///
/// Each RPC method translates the protobuf request into a [`GrpcStateCommand`]
/// and sends it through the provided `mpsc::Sender` to the cluster state actor.
/// This keeps the gRPC handler stateless and thread-safe.
pub struct NodeServiceImpl {
    /// Channel to the cluster state actor.
    tx: mpsc::Sender<GrpcStateCommand>,
}

impl NodeServiceImpl {
    /// Create a new `NodeServiceImpl` backed by the given command sender.
    pub fn new(tx: mpsc::Sender<GrpcStateCommand>) -> Self {
        Self { tx }
    }
}

#[tonic::async_trait]
impl NodeService for NodeServiceImpl {
    async fn register_node(
        &self,
        request: Request<RegisterNodeRequest>,
    ) -> Result<Response<RegisterNodeResponse>, Status> {
        let req = request.into_inner();
        info!(node_id = %req.node_id, addr = %req.addr, "gRPC: RegisterNode");

        let resources = req
            .resources
            .map(resource_info_to_spec)
            .unwrap_or_default();

        let node = Node::new(&req.node_id, &req.addr, resources);

        self.tx
            .send(GrpcStateCommand::RegisterNode { node })
            .await
            .map_err(|_| Status::internal("state actor unavailable"))?;

        Ok(Response::new(RegisterNodeResponse {
            accepted: true,
            message: format!("node '{}' registered", req.node_id),
        }))
    }

    async fn send_heartbeat(
        &self,
        request: Request<HeartbeatRequest>,
    ) -> Result<Response<HeartbeatResponse>, Status> {
        let req = request.into_inner();

        let resources = req
            .resources
            .map(resource_info_to_spec)
            .unwrap_or_default();

        let status = parse_node_status(&req.status);

        let timestamp = req
            .timestamp
            .parse::<chrono::DateTime<chrono::Utc>>()
            .unwrap_or_else(|_| chrono::Utc::now());

        let heartbeat = Heartbeat {
            node_id: NodeId::new(&req.node_id),
            status,
            resources,
            timestamp,
        };

        self.tx
            .send(GrpcStateCommand::RecordHeartbeat { heartbeat })
            .await
            .map_err(|_| Status::internal("state actor unavailable"))?;

        Ok(Response::new(HeartbeatResponse {
            acknowledged: true,
        }))
    }

    async fn deregister_node(
        &self,
        request: Request<DeregisterNodeRequest>,
    ) -> Result<Response<DeregisterNodeResponse>, Status> {
        let req = request.into_inner();
        info!(node_id = %req.node_id, "gRPC: DeregisterNode");

        self.tx
            .send(GrpcStateCommand::DeregisterNode {
                id: NodeId::new(&req.node_id),
            })
            .await
            .map_err(|_| Status::internal("state actor unavailable"))?;

        Ok(Response::new(DeregisterNodeResponse {
            acknowledged: true,
        }))
    }

    async fn list_nodes(
        &self,
        _request: Request<ListNodesRequest>,
    ) -> Result<Response<ListNodesResponse>, Status> {
        let (resp_tx, resp_rx) = oneshot::channel();

        self.tx
            .send(GrpcStateCommand::GetNodes { resp: resp_tx })
            .await
            .map_err(|_| Status::internal("state actor unavailable"))?;

        let nodes = resp_rx
            .await
            .map_err(|_| Status::internal("state actor dropped response channel"))?;

        let node_infos: Vec<NodeInfo> = nodes.iter().map(node_to_proto).collect();

        Ok(Response::new(ListNodesResponse { nodes: node_infos }))
    }
}

// ---------------------------------------------------------------------------
// Conversion helpers
// ---------------------------------------------------------------------------

/// Convert a protobuf [`ResourceInfo`] to a core [`ResourceSpec`].
pub fn resource_info_to_spec(info: ResourceInfo) -> ResourceSpec {
    ResourceSpec::new(info.cpu_millicores, info.memory_mib)
}

/// Convert a core [`ResourceSpec`] to a protobuf [`ResourceInfo`].
pub fn spec_to_resource_info(spec: &ResourceSpec) -> ResourceInfo {
    ResourceInfo {
        cpu_millicores: spec.cpu_millicores,
        memory_mib: spec.memory_mib,
    }
}

/// Convert a core [`Node`] to a protobuf [`NodeInfo`].
pub fn node_to_proto(node: &Node) -> NodeInfo {
    NodeInfo {
        id: node.id.as_str().to_string(),
        addr: node.addr.clone(),
        status: format!("{}", node.status),
        resources: Some(spec_to_resource_info(&node.resources)),
    }
}

/// Parse a status string into a [`NodeStatus`].
///
/// Defaults to `Unknown` for unrecognised values — we never want to panic
/// on protocol input from a potentially buggy or version-mismatched peer.
pub fn parse_node_status(s: &str) -> NodeStatus {
    match s {
        "Ready" => NodeStatus::Ready,
        "NotReady" => NodeStatus::NotReady,
        _ => {
            if s != "Unknown" {
                warn!(status = %s, "Unrecognised node status, defaulting to Unknown");
            }
            NodeStatus::Unknown
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn resource_conversion_roundtrip() {
        let spec = ResourceSpec::new(4000, 8192);
        let proto = spec_to_resource_info(&spec);
        let back = resource_info_to_spec(proto);
        assert_eq!(spec, back);
    }

    #[test]
    fn node_to_proto_conversion() {
        let mut node = Node::new("node-1", "10.0.0.1:7001", ResourceSpec::new(2000, 4096));
        node.status = NodeStatus::Ready;

        let info = node_to_proto(&node);
        assert_eq!(info.id, "node-1");
        assert_eq!(info.addr, "10.0.0.1:7001");
        assert_eq!(info.status, "Ready");
        assert_eq!(info.resources.unwrap().cpu_millicores, 2000);
    }

    #[test]
    fn parse_node_status_known_values() {
        assert_eq!(parse_node_status("Ready"), NodeStatus::Ready);
        assert_eq!(parse_node_status("NotReady"), NodeStatus::NotReady);
        assert_eq!(parse_node_status("Unknown"), NodeStatus::Unknown);
    }

    #[test]
    fn parse_node_status_unknown_defaults() {
        assert_eq!(parse_node_status("garbage"), NodeStatus::Unknown);
        assert_eq!(parse_node_status(""), NodeStatus::Unknown);
    }

    #[test]
    fn register_node_response_fields() {
        let resp = RegisterNodeResponse {
            accepted: true,
            message: "node 'n1' registered".into(),
        };
        assert!(resp.accepted);
        assert!(resp.message.contains("n1"));
    }
}
