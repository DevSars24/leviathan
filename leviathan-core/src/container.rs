//! Container-related types for the Leviathan platform.
//!
//! A [`Container`] is the atomic unit of work scheduled onto a [`crate::Node`].

use serde::{Deserialize, Serialize};

use crate::node::NodeId;

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

impl std::fmt::Display for ContainerId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
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

impl std::fmt::Display for ContainerStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ContainerStatus::Pending => write!(f, "Pending"),
            ContainerStatus::Running => write!(f, "Running"),
            ContainerStatus::Stopped => write!(f, "Stopped"),
            ContainerStatus::Failed => write!(f, "Failed"),
        }
    }
}

/// A container instance managed by Leviathan.
///
/// Tracks identity, the OCI image it runs, its current status, and which
/// node it has been placed on (if any).
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
    ) -> Self {
        Self {
            id: ContainerId::new(id),
            name: name.into(),
            image: image.into(),
            status: ContainerStatus::Pending,
            node_id: None,
        }
    }
}
