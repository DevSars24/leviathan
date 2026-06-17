//! Platform-wide error type for Leviathan.
//!
//! All public-facing APIs return `Result<T, LeviathanError>`. Downstream
//! crates may wrap this in `anyhow::Error` for application-level contexts.

use thiserror::Error;

/// Top-level error enum for the Leviathan platform.
///
/// Variants are coarse-grained by subsystem. Each variant carries enough
/// context to produce a meaningful error message without requiring a
/// backtrace in production.
#[derive(Debug, Error)]
pub enum LeviathanError {
    /// A node with the given ID was not found in the cluster state.
    #[error("node not found: {0}")]
    NodeNotFound(String),

    /// A container with the given ID was not found.
    #[error("container not found: {0}")]
    ContainerNotFound(String),

    /// The scheduler could not find a node with sufficient resources.
    #[error("no schedulable node available: {reason}")]
    NoSchedulableNode { reason: String },

    /// An I/O error from the storage engine or network layer.
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    /// A serialization or deserialization failure.
    #[error("serialization error: {0}")]
    Serialization(String),

    /// A consensus-related error (Raft log, term mismatch, etc.).
    #[error("consensus error: {0}")]
    Consensus(String),

    /// A networking error (connection refused, timeout, etc.).
    #[error("network error: {0}")]
    Network(String),

    /// A container runtime error (namespace setup, cgroup, exec failure).
    #[error("runtime error: {0}")]
    Runtime(String),

    /// An invalid configuration was supplied.
    #[error("invalid configuration: {0}")]
    InvalidConfig(String),

    /// A catch-all for unexpected internal errors.
    #[error("internal error: {0}")]
    Internal(String),
}

impl From<serde_json::Error> for LeviathanError {
    fn from(e: serde_json::Error) -> Self {
        LeviathanError::Serialization(e.to_string())
    }
}
