//! Node-related types for the Leviathan platform.
//!
//! A [`Node`] represents a worker machine that can accept and run containers.

use serde::{Deserialize, Serialize};

use crate::resources::ResourceSpec;

/// A strongly-typed identifier for a cluster node.
///
/// Wraps a [`String`] to prevent accidental confusion with other ID types.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct NodeId(pub String);

impl NodeId {
    /// Create a new `NodeId` from any string-like value.
    pub fn new(id: impl Into<String>) -> Self {
        Self(id.into())
    }

    /// Return the inner string slice.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl std::fmt::Display for NodeId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// Lifecycle state of a cluster node as seen by the control plane.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum NodeStatus {
    /// Node is registered, reachable, and accepting workloads.
    Ready,
    /// Node is registered but health checks are failing.
    NotReady,
    /// Node has not sent a heartbeat within the grace period.
    Unknown,
}

impl std::fmt::Display for NodeStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            NodeStatus::Ready => write!(f, "Ready"),
            NodeStatus::NotReady => write!(f, "NotReady"),
            NodeStatus::Unknown => write!(f, "Unknown"),
        }
    }
}

/// A worker node in the Leviathan cluster.
///
/// Holds identity, network address, current status, and available resources.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Node {
    /// Unique identifier for this node.
    pub id: NodeId,
    /// The `host:port` address this node listens on.
    pub addr: String,
    /// Current lifecycle state of the node.
    pub status: NodeStatus,
    /// Advertised resource capacity of this node.
    pub resources: ResourceSpec,
}

impl Node {
    /// Construct a new node in [`NodeStatus::Unknown`] state.
    pub fn new(id: impl Into<String>, addr: impl Into<String>, resources: ResourceSpec) -> Self {
        Self {
            id: NodeId::new(id),
            addr: addr.into(),
            status: NodeStatus::Unknown,
            resources,
        }
    }
}

/// A heartbeat message sent from worker nodes to the control plane.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Heartbeat {
    /// Unique identifier of the reporting node.
    pub node_id: NodeId,
    /// Lifecycle state reported by the node.
    pub status: NodeStatus,
    /// The node's current available resource capacity.
    pub resources: ResourceSpec,
    /// Timestamp when this heartbeat was generated.
    pub timestamp: chrono::DateTime<chrono::Utc>,
}

/// Messages exchanged between worker nodes and the control plane over TCP.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum NodeMessage {
    /// Initial registration request sent by a worker node.
    Register {
        /// Unique identifier for the node.
        id: String,
        /// Listen address of the node.
        addr: String,
        /// Resources advertised by the node.
        resources: ResourceSpec,
    },
    /// Periodic heartbeat report.
    Heartbeat(Heartbeat),
    /// Clean deregistration on node shutdown.
    Deregister {
        /// Unique identifier of the node.
        id: String,
    },
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::resources::ResourceSpec;

    #[test]
    fn test_node_message_serialization() {
        let original_msg = NodeMessage::Register {
            id: "node-test".to_string(),
            addr: "127.0.0.1:9999".to_string(),
            resources: ResourceSpec::new(1000, 2048),
        };

        // Serialize to JSON string
        let serialized = serde_json::to_string(&original_msg).expect("Failed to serialize NodeMessage");
        
        // Deserialize back
        let deserialized_msg: NodeMessage = serde_json::from_str(&serialized).expect("Failed to deserialize NodeMessage");

        match deserialized_msg {
            NodeMessage::Register { id, addr, resources } => {
                assert_eq!(id, "node-test");
                assert_eq!(addr, "127.0.0.1:9999");
                assert_eq!(resources.cpu_millicores, 1000);
                assert_eq!(resources.memory_mib, 2048);
            }
            _ => panic!("Expected NodeMessage::Register variant"),
        }
    }
}
