//! # leviathan-storage
//!
//! Production-grade durable storage primitives for the Leviathan distributed
//! container orchestration platform.
//!
//! ## Modules
//!
//! - [`wal`] — Write-Ahead Log with crash recovery, length-prefixed binary
//!   frames, and Raft-compatible log truncation.
//! - [`mmap`] — Memory-mapped file store backed by [`memmap2::MmapMut`] for
//!   high-throughput, low-latency byte-level access.
//! - [`error`] — Unified [`StorageError`] type covering all failure modes.

#![warn(missing_docs)]

pub mod error;
pub mod mmap;
pub mod wal;

#[cfg(test)]
mod tests;

pub use error::StorageError;
pub use mmap::MmapStore;
pub use wal::{Wal, WalEntry};
