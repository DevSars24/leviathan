//! Core traits that define the behaviour contracts across Leviathan subsystems.
//!
//! These traits are intentionally minimal at Day 1. They will be fleshed out
//! as each subsystem is implemented in Days 2–7.

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
}
