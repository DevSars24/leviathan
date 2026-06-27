//! # leviathan-runtime
//!
//! OCI-compliant container runtime for the Leviathan distributed container
//! orchestration platform.
//!
//! ## Architecture
//!
//! The runtime is decomposed into focused modules, each handling one aspect
//! of container isolation:
//!
//! - [`namespace`] — Linux namespace creation (PID, NET, UTS, MNT, IPC)
//! - [`filesystem`] — `pivot_root`, bind mounts, OCI layer extraction
//! - [`cgroup`] — cgroups v2 resource limits (CPU, memory, PIDs)
//! - [`seccomp`] — seccomp-BPF syscall filtering
//! - [`network`] — veth pairs, IP assignment, bridge attachment
//! - [`container`] — Orchestrates all modules for container lifecycle
//! - [`error`] — Typed error enum covering all runtime failure modes
//!
//! ## Platform Support
//!
//! All Linux-only syscalls are gated behind `#[cfg(target_os = "linux")]`.
//! On non-Linux platforms, the public API compiles and returns stubs,
//! enabling testing, documentation, and CI on any OS.

#![warn(missing_docs)]

pub mod cgroup;
pub mod container;
pub mod error;
pub mod filesystem;
pub mod namespace;
pub mod network;
pub mod seccomp;

pub use cgroup::{CgroupLimits, CgroupManager};
pub use container::{ContainerRuntime, ContainerSpec, ContainerState};
pub use error::RuntimeError;
pub use filesystem::FilesystemConfig;
pub use namespace::NamespaceConfig;
pub use network::NetworkConfig;
pub use seccomp::SeccompConfig;
