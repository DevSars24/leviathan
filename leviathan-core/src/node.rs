//! Node-related types for the Leviathan platform.
//!
//! A [`Node`] represents a worker machine that can accept and run containers.

use std::fmt;

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

impl fmt::Display for NodeId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
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

impl fmt::Display for NodeStatus {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
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

impl fmt::Display for Node {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{} [{}] @ {} ({})",
            self.id, self.status, self.addr, self.resources
        )
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
///
/// Uses [`NodeId`] throughout to enforce type safety — preventing accidental
/// confusion between node IDs, container IDs, and arbitrary strings.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum NodeMessage {
    /// Initial registration request sent by a worker node.
    Register {
        /// Unique identifier for the node.
        id: NodeId,
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
        id: NodeId,
    },
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::resources::ResourceSpec;

    #[test]
    fn node_message_register_roundtrip() {
        let original_msg = NodeMessage::Register {
            id: NodeId::new("node-test"),
            addr: "127.0.0.1:9999".to_string(),
            resources: ResourceSpec::new(1000, 2048),
        };

        let serialized =
            serde_json::to_string(&original_msg).expect("Failed to serialize NodeMessage");
        let deserialized: NodeMessage =
            serde_json::from_str(&serialized).expect("Failed to deserialize NodeMessage");

        match deserialized {
            NodeMessage::Register { id, addr, resources } => {
                assert_eq!(id, NodeId::new("node-test"));
                assert_eq!(addr, "127.0.0.1:9999");
                assert_eq!(resources, ResourceSpec::new(1000, 2048));
            }
            _ => panic!("Expected NodeMessage::Register variant"),
        }
    }

    #[test]
    fn heartbeat_roundtrip() {
        let hb = Heartbeat {
            node_id: NodeId::new("node-1"),
            status: NodeStatus::Ready,
            resources: ResourceSpec::new(2000, 4096),
            timestamp: chrono::Utc::now(),
        };

        let msg = NodeMessage::Heartbeat(hb);
        let json = serde_json::to_string(&msg).expect("serialize");
        let msg2: NodeMessage = serde_json::from_str(&json).expect("deserialize");

        match msg2 {
            NodeMessage::Heartbeat(hb) => {
                assert_eq!(hb.node_id, NodeId::new("node-1"));
                assert_eq!(hb.status, NodeStatus::Ready);
            }
            _ => panic!("Expected NodeMessage::Heartbeat variant"),
        }
    }

    #[test]
    fn deregister_roundtrip() {
        let msg = NodeMessage::Deregister {
            id: NodeId::new("node-99"),
        };
        let json = serde_json::to_string(&msg).expect("serialize");
        let msg2: NodeMessage = serde_json::from_str(&json).expect("deserialize");

        match msg2 {
            NodeMessage::Deregister { id } => {
                assert_eq!(id, NodeId::new("node-99"));
            }
            _ => panic!("Expected NodeMessage::Deregister variant"),
        }
    }

    #[test]
    fn node_status_display() {
        assert_eq!(format!("{}", NodeStatus::Ready), "Ready");
        assert_eq!(format!("{}", NodeStatus::NotReady), "NotReady");
        assert_eq!(format!("{}", NodeStatus::Unknown), "Unknown");
    }

    #[test]
    fn node_display() {
        let node = Node::new("node-1", "127.0.0.1:7001", ResourceSpec::new(2000, 4096));
        let s = format!("{}", node);
        assert!(s.contains("node-1"));
        assert!(s.contains("Unknown")); // default status
        assert!(s.contains("127.0.0.1:7001"));
    }
}
