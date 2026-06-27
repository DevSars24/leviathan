//! Container-related types for the Leviathan platform.
//!
//! A [`Container`] is the atomic unit of work scheduled onto a [`crate::Node`].

use std::fmt;

use serde::{Deserialize, Serialize};

use crate::node::NodeId;
use crate::resources::ResourceSpec;

/// A strongly-typed identifier for a container instance.
///
/// Wraps a [`String`] to prevent accidental confusion with other ID types.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ContainerId(pub String);

impl ContainerId {
    /// Create a new `ContainerId` from any string-like value.
    pub fn new(id: impl Into<String>) -> Self {
        Self(id.into())
    }

    /// Return the inner string slice.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for ContainerId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// Lifecycle state of a container as tracked by the control plane.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum ContainerStatus {
    /// Container is queued but not yet assigned to a node.
    Pending,
    /// Container is actively executing on a worker node.
    Running,
    /// Container has exited cleanly.
    Stopped,
    /// Container exited with an error or was forcibly killed.
    Failed,
}

impl fmt::Display for ContainerStatus {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ContainerStatus::Pending => write!(f, "Pending"),
            ContainerStatus::Running => write!(f, "Running"),
            ContainerStatus::Stopped => write!(f, "Stopped"),
            ContainerStatus::Failed => write!(f, "Failed"),
        }
    }
}

/// Specification of a workload submitted to the Leviathan cluster.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct WorkloadSpec {
    /// Unique identifier for the workload.
    pub id: String,
    /// Human-readable name.
    pub name: String,
    /// OCI image reference.
    pub image: String,
    /// Resource requirements for containers spawned by this workload.
    pub resources: ResourceSpec,
    /// Number of desired replicas.
    pub replicas: u32,
}

impl WorkloadSpec {
    /// Create a new workload specification.
    #[must_use]
    pub fn new(
        id: impl Into<String>,
        name: impl Into<String>,
        image: impl Into<String>,
        resources: ResourceSpec,
        replicas: u32,
    ) -> Self {
        Self {
            id: id.into(),
            name: name.into(),
            image: image.into(),
            resources,
            replicas,
        }
    }
}

/// A container instance managed by Leviathan.
///
/// Tracks identity, the OCI image it runs, its current status, resource
/// requirements, and which node it has been placed on (if any).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Container {
    /// Unique identifier for this container instance.
    pub id: ContainerId,
    /// Human-readable name supplied by the operator.
    pub name: String,
    /// OCI image reference (e.g. `"ubuntu:22.04"`).
    pub image: String,
    /// Current lifecycle state.
    pub status: ContainerStatus,
    /// Resource requests for scheduling — a container without resource
    /// requests cannot be meaningfully scheduled.
    pub resources: ResourceSpec,
    /// The node this container has been scheduled onto, if assigned.
    pub node_id: Option<NodeId>,
}

impl Container {
    /// Construct a new container in [`ContainerStatus::Pending`] state
    /// with no node assignment.
    pub fn new(
        id: impl Into<String>,
        name: impl Into<String>,
        image: impl Into<String>,
        resources: ResourceSpec,
    ) -> Self {
        Self {
            id: ContainerId::new(id),
            name: name.into(),
            image: image.into(),
            status: ContainerStatus::Pending,
            resources,
            node_id: None,
        }
    }
}

impl fmt::Display for Container {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{} ({}) [{}] — {}",
            self.name, self.id, self.status, self.image
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn new_container_is_pending_with_no_node() {
        let c = Container::new("c-1", "my-app", "ubuntu:22.04", ResourceSpec::new(500, 256));
        assert_eq!(c.status, ContainerStatus::Pending);
        assert!(c.node_id.is_none());
        assert_eq!(c.resources, ResourceSpec::new(500, 256));
    }

    #[test]
    fn container_display() {
        let c = Container::new("c-1", "my-app", "ubuntu:22.04", ResourceSpec::default());
        let s = format!("{}", c);
        assert!(s.contains("my-app"));
        assert!(s.contains("Pending"));
    }

    #[test]
    fn container_status_display() {
        assert_eq!(format!("{}", ContainerStatus::Running), "Running");
        assert_eq!(format!("{}", ContainerStatus::Failed), "Failed");
    }

    #[test]
    fn container_serialization_roundtrip() {
        let c = Container::new("c-1", "web", "nginx:latest", ResourceSpec::new(1000, 512));
        let json = serde_json::to_string(&c).expect("serialize");
        let c2: Container = serde_json::from_str(&json).expect("deserialize");
        assert_eq!(c.id, c2.id);
        assert_eq!(c.resources, c2.resources);
    }
}
