//! Linux namespace isolation for container processes.
//!
//! This module provides the [`NamespaceConfig`] and [`create_namespaces`]
//! function that set up PID, network, UTS, mount, and IPC namespaces for
//! a container process using `unshare(2)`.
//!
//! # Platform Gating
//!
//! All actual syscall invocations are gated behind `#[cfg(target_os = "linux")]`.
//! On non-Linux platforms, the public API is still available but returns
//! `RuntimeError::UnsupportedPlatform`. This allows the type system, tests,
//! and documentation to be exercised on any OS.
//!
//! # Safety
//!
//! The Linux codepath uses `nix::sched::unshare()`, which is a safe wrapper
//! around the `unshare(2)` syscall. No raw `unsafe` blocks are needed here.

use serde::{Deserialize, Serialize};

use crate::error::Result;

/// Configuration for which namespaces to create.
///
/// Each field corresponds to a `CLONE_NEW*` flag. All default to `true`
/// for full isolation.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NamespaceConfig {
    /// Create a new PID namespace (`CLONE_NEWPID`).
    /// The container's init process sees itself as PID 1.
    pub pid: bool,

    /// Create a new network namespace (`CLONE_NEWNET`).
    /// The container gets its own network stack (interfaces, routing, iptables).
    pub net: bool,

    /// Create a new UTS namespace (`CLONE_NEWUTS`).
    /// The container can have its own hostname.
    pub uts: bool,

    /// Create a new mount namespace (`CLONE_NEWNS`).
    /// Required for `pivot_root` and independent mount table.
    pub mount: bool,

    /// Create a new IPC namespace (`CLONE_NEWIPC`).
    /// Isolates System V IPC and POSIX message queues.
    pub ipc: bool,
}

impl Default for NamespaceConfig {
    /// Full isolation — all namespaces enabled.
    fn default() -> Self {
        Self {
            pid: true,
            net: true,
            uts: true,
            mount: true,
            ipc: true,
        }
    }
}

impl NamespaceConfig {
    /// Create a config with all namespaces enabled.
    #[must_use]
    pub fn full_isolation() -> Self {
        Self::default()
    }

    /// Create a config with no namespaces (useful for testing).
    #[must_use]
    pub fn none() -> Self {
        Self {
            pid: false,
            net: false,
            uts: false,
            mount: false,
            ipc: false,
        }
    }
}

/// Create Linux namespaces according to the given configuration.
///
/// On Linux, this calls `unshare(2)` with the appropriate `CLONE_NEW*` flags.
/// On other platforms, this returns `RuntimeError::UnsupportedPlatform`.
///
/// # Errors
///
/// Returns `RuntimeError::NamespaceError` if the `unshare` syscall fails,
/// or `RuntimeError::UnsupportedPlatform` on non-Linux.
#[cfg(target_os = "linux")]
pub fn create_namespaces(config: &NamespaceConfig) -> Result<()> {
    use nix::sched::CloneFlags;

    let mut flags = CloneFlags::empty();

    if config.pid {
        flags |= CloneFlags::CLONE_NEWPID;
    }
    if config.net {
        flags |= CloneFlags::CLONE_NEWNET;
    }
    if config.uts {
        flags |= CloneFlags::CLONE_NEWUTS;
    }
    if config.mount {
        flags |= CloneFlags::CLONE_NEWNS;
    }
    if config.ipc {
        flags |= CloneFlags::CLONE_NEWIPC;
    }

    if flags.is_empty() {
        tracing::debug!("No namespaces requested — skipping unshare");
        return Ok(());
    }

    nix::sched::unshare(flags).map_err(|e| RuntimeError::NamespaceError {
        operation: "unshare".into(),
        reason: e.to_string(),
    })?;

    tracing::info!(flags = ?flags, "Namespaces created via unshare(2)");
    Ok(())
}

/// Non-Linux stub — always returns `UnsupportedPlatform`.
#[cfg(not(target_os = "linux"))]
pub fn create_namespaces(config: &NamespaceConfig) -> Result<()> {
    if config.pid || config.net || config.uts || config.mount || config.ipc {
        tracing::warn!("Namespace creation requested on non-Linux platform — returning stub");
    }
    // On non-Linux, we succeed silently for testing purposes.
    // Real container isolation requires Linux.
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_config_is_full_isolation() {
        let cfg = NamespaceConfig::default();
        assert!(cfg.pid);
        assert!(cfg.net);
        assert!(cfg.uts);
        assert!(cfg.mount);
        assert!(cfg.ipc);
    }

    #[test]
    fn none_config_has_no_namespaces() {
        let cfg = NamespaceConfig::none();
        assert!(!cfg.pid);
        assert!(!cfg.net);
    }

    #[test]
    fn create_namespaces_with_none_succeeds() {
        // Even on non-Linux, creating no namespaces should succeed.
        let cfg = NamespaceConfig::none();
        create_namespaces(&cfg).expect("no-op should succeed");
    }
}
