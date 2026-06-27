//! Container runtime error types.
//!
//! Every failure from namespace setup, cgroup writes, filesystem operations,
//! seccomp filter installation, and network configuration is represented by
//! a variant of [`RuntimeError`]. No `unwrap()` / `expect()` in non-test code.

use thiserror::Error;

/// Exhaustive error enum for the OCI-compliant container runtime.
///
/// Each variant maps to a specific subsystem, carrying enough context
/// for structured logging and upstream error propagation.
#[derive(Debug, Error)]
pub enum RuntimeError {
    /// Failed to create or enter a Linux namespace.
    ///
    /// Covers `clone(2)`, `unshare(2)`, and `setns(2)` failures.
    /// The inner string contains the `errno` description.
    #[error("namespace error: {operation} failed: {reason}")]
    NamespaceError {
        /// The syscall or operation that failed (e.g., "unshare", "setns").
        operation: String,
        /// Human-readable reason from `errno` or the kernel.
        reason: String,
    },

    /// Failed to configure cgroups v2.
    ///
    /// Covers file writes to `/sys/fs/cgroup`, controller enablement,
    /// and limit enforcement.
    #[error("cgroup error: {controller}: {reason}")]
    CgroupError {
        /// The cgroup controller that failed (e.g., "cpu", "memory", "pids").
        controller: String,
        /// Human-readable reason.
        reason: String,
    },

    /// Failed to install a seccomp-BPF filter.
    #[error("seccomp error: {0}")]
    SeccompError(String),

    /// Failed during container filesystem setup.
    ///
    /// Covers `pivot_root`, bind mounts, OCI layer extraction, and
    /// rootfs preparation.
    #[error("filesystem error: {operation}: {reason}")]
    FilesystemError {
        /// The filesystem operation that failed (e.g., "pivot_root", "mount").
        operation: String,
        /// Human-readable reason.
        reason: String,
    },

    /// Failed during container network setup.
    ///
    /// Covers veth pair creation, netns assignment, and IP configuration.
    #[error("network error: {operation}: {reason}")]
    NetworkError {
        /// The network operation that failed (e.g., "veth_create", "ip_assign").
        operation: String,
        /// Human-readable reason.
        reason: String,
    },

    /// OCI spec validation or extraction failure.
    #[error("OCI error: {0}")]
    OciError(String),

    /// An underlying I/O error.
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    /// The operation is not supported on this platform.
    #[error("unsupported platform: {0}")]
    UnsupportedPlatform(String),
}

/// Convenience result alias for runtime operations.
pub type Result<T> = std::result::Result<T, RuntimeError>;

// Bridge into the platform-wide error type.
impl From<RuntimeError> for leviathan_core::LeviathanError {
    fn from(e: RuntimeError) -> Self {
        leviathan_core::LeviathanError::Runtime(e.to_string())
    }
}
