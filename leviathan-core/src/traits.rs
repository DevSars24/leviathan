//! Core traits that define the behaviour contracts across Leviathan subsystems.
//!
//! These traits are intentionally minimal at Day 1. They will be fleshed out
//! as each subsystem is implemented in Days 2–7.

use std::fmt;

use serde::{Deserialize, Serialize};

use crate::error::LeviathanError;

/// A component that can reconcile desired state against actual state.
///
/// Implementors include the control plane, scheduler, and node agent.
/// The reconcile loop is the heart of Leviathan's self-healing behaviour.
#[async_trait::async_trait]
pub trait Reconcile {
    /// Run one reconciliation pass.
    ///
    /// Returns `Ok(())` if the system is converged or if recovery was
    /// attempted. Returns `Err` only on unrecoverable internal failures.
    async fn reconcile(&mut self) -> Result<(), LeviathanError>;
}

/// A persistent store for cluster state.
///
/// At Day 1 this is a placeholder. Day 4 will replace it with the WAL-backed
/// storage engine.
pub trait StateStore {
    /// Persist a key-value pair as raw bytes.
    fn put(&mut self, key: &str, value: &[u8]) -> Result<(), LeviathanError>;

    /// Retrieve a value by key.
    fn get(&self, key: &str) -> Result<Option<Vec<u8>>, LeviathanError>;

    /// Delete a key from the store.
    fn delete(&mut self, key: &str) -> Result<(), LeviathanError>;

    /// List all keys currently held in the store.
    ///
    /// Essential for iteration, debugging, and reconciliation passes that
    /// need to walk the full state.
    fn list_keys(&self) -> Result<Vec<String>, LeviathanError>;
}

/// The health status reported by a subsystem.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum HealthStatus {
    /// The component is fully operational.
    Healthy,
    /// The component is operational but experiencing issues (e.g. high
    /// latency, degraded throughput).
    Degraded {
        /// Human-readable reason for the degraded state.
        reason: String,
    },
    /// The component is not operational.
    Unhealthy {
        /// Human-readable reason for the unhealthy state.
        reason: String,
    },
}

impl fmt::Display for HealthStatus {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            HealthStatus::Healthy => write!(f, "Healthy"),
            HealthStatus::Degraded { reason } => write!(f, "Degraded: {}", reason),
            HealthStatus::Unhealthy { reason } => write!(f, "Unhealthy: {}", reason),
        }
    }
}

/// A component that can report its own health status.
///
/// Every daemon (node, control plane, scheduler) should implement this
/// trait to enable unified health monitoring.
#[async_trait::async_trait]
pub trait Healthcheck {
    /// Perform an internal health check and report the result.
    async fn health_check(&self) -> Result<HealthStatus, LeviathanError>;
}
