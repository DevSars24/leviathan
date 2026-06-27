//! # leviathan-core
//!
//! Shared types, error definitions, and core traits for the Leviathan
//! distributed container orchestration platform.
//!
//! Every crate in the workspace depends on this crate. It is intentionally
//! dependency-light: only serde, thiserror, uuid, and chrono.

#![warn(missing_docs)]

pub mod container;
pub mod cooperative;
pub mod error;
pub mod node;
pub mod resources;
pub mod traits;

// Re-export everything at the crate root for convenience.
pub use container::{Container, ContainerId, ContainerStatus, WorkloadSpec};
pub use cooperative::{CooperativeYield, ExponentialBackoff};
pub use error::LeviathanError;
pub use node::{Heartbeat, Node, NodeId, NodeMessage, NodeStatus};
pub use resources::ResourceSpec;
pub use traits::{HealthStatus, Healthcheck, Reconcile, StateStore, Storage};

/// Crate-level `Result` alias backed by [`LeviathanError`].
pub type Result<T> = std::result::Result<T, LeviathanError>;
