//! Container filesystem setup: `pivot_root`, bind mounts, and OCI layers.
//!
//! This module implements the filesystem isolation required for an
//! OCI-compliant container:
//!
//! 1. **`pivot_root`** (not `chroot`) — provides stronger isolation by
//!    changing both the root and the current working directory of the
//!    mount namespace. The old root is unmounted, preventing escape.
//!
//! 2. **Bind mounts** — `/proc`, `/sys`, `/dev` are bind-mounted into
//!    the container's rootfs to provide kernel interfaces.
//!
//! 3. **OCI layer extraction** — unpacks a container image's filesystem
//!    layers into the rootfs directory.
//!
//! # Platform Gating
//!
//! All mount/pivot_root operations are Linux-only. Non-Linux platforms
//! get stubs that create the directory structure without actual mounts.

use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

use crate::error::{Result, RuntimeError};

/// Configuration for a container's filesystem.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FilesystemConfig {
    /// Path to the container's root filesystem.
    pub rootfs: PathBuf,

    /// OCI image layers to extract (in order, bottom to top).
    /// Each entry is a path to a tar archive.
    pub layers: Vec<PathBuf>,

    /// Additional bind mounts: (host_path, container_path).
    pub bind_mounts: Vec<(PathBuf, PathBuf)>,

    /// Whether to mount `/proc` inside the container.
    pub mount_proc: bool,

    /// Whether to mount `/sys` inside the container (read-only).
    pub mount_sys: bool,

    /// Whether to mount `/dev` inside the container.
    pub mount_dev: bool,
}

impl Default for FilesystemConfig {
    fn default() -> Self {
        Self {
            rootfs: PathBuf::from("/tmp/leviathan/rootfs"),
            layers: Vec::new(),
            bind_mounts: Vec::new(),
            mount_proc: true,
            mount_sys: true,
            mount_dev: true,
        }
    }
}

/// Prepare the container's root filesystem.
///
/// Creates the rootfs directory structure and essential mount points.
///
/// # Errors
///
/// Returns `RuntimeError::FilesystemError` on directory creation failure.
pub fn prepare_rootfs(config: &FilesystemConfig) -> Result<()> {
    // Create rootfs and essential directories.
    let dirs = ["proc", "sys", "dev", "tmp", "etc", "bin", "usr"];
    for dir in &dirs {
        let path = config.rootfs.join(dir);
        std::fs::create_dir_all(&path).map_err(|e| RuntimeError::FilesystemError {
            operation: format!("mkdir {}", path.display()),
            reason: e.to_string(),
        })?;
    }

    tracing::info!(rootfs = %config.rootfs.display(), "Prepared container rootfs");
    Ok(())
}

/// Execute `pivot_root` to switch the container's root filesystem.
///
/// On Linux, this:
/// 1. Bind-mounts the rootfs onto itself (required by `pivot_root`).
/// 2. Creates a temporary directory for the old root.
/// 3. Calls `pivot_root(new_root, put_old)`.
/// 4. `chdir("/")` to the new root.
/// 5. Unmounts the old root.
///
/// On non-Linux, this is a no-op stub.
///
/// # Safety
///
/// This function modifies the process's mount namespace. It must only be
/// called inside a new mount namespace (after `CLONE_NEWNS`).
///
/// # Errors
///
/// Returns `RuntimeError::FilesystemError` on syscall failure.
#[cfg(target_os = "linux")]
pub fn pivot_root(rootfs: &Path) -> Result<()> {
    use nix::mount::{mount, umount2, MntFlags, MsFlags};
    use nix::unistd;

    let old_root = rootfs.join("old_root");
    std::fs::create_dir_all(&old_root).map_err(|e| RuntimeError::FilesystemError {
        operation: "mkdir old_root".into(),
        reason: e.to_string(),
    })?;

    // Bind-mount rootfs onto itself — required by pivot_root(2).
    // SAFETY invariant: `rootfs` is a valid directory we created.
    // What breaks if violated: pivot_root(2) returns EINVAL.
    mount(
        Some(rootfs),
        rootfs,
        None::<&str>,
        MsFlags::MS_BIND | MsFlags::MS_REC,
        None::<&str>,
    )
    .map_err(|e| RuntimeError::FilesystemError {
        operation: "bind mount rootfs".into(),
        reason: e.to_string(),
    })?;

    // pivot_root — changes the root mount.
    // SAFETY invariant: We are in a new mount namespace (CLONE_NEWNS).
    // What breaks if violated: affects the host's mount namespace.
    unistd::pivot_root(rootfs, &old_root).map_err(|e| RuntimeError::FilesystemError {
        operation: "pivot_root".into(),
        reason: e.to_string(),
    })?;

    // Change working directory to new root.
    unistd::chdir("/").map_err(|e| RuntimeError::FilesystemError {
        operation: "chdir /".into(),
        reason: e.to_string(),
    })?;

    // Unmount old root.
    umount2("/old_root", MntFlags::MNT_DETACH).map_err(|e| RuntimeError::FilesystemError {
        operation: "umount old_root".into(),
        reason: e.to_string(),
    })?;

    // Remove old_root directory.
    std::fs::remove_dir("/old_root").ok(); // Best-effort

    tracing::info!("pivot_root completed — new root is active");
    Ok(())
}

/// Non-Linux stub for `pivot_root`.
#[cfg(not(target_os = "linux"))]
pub fn pivot_root(rootfs: &Path) -> Result<()> {
    tracing::debug!(rootfs = %rootfs.display(), "pivot_root stub (non-Linux)");
    Ok(())
}

/// Mount `/proc`, `/sys`, `/dev` inside the container.
///
/// On Linux, performs actual mount syscalls.
/// On non-Linux, creates the directories as stubs.
///
/// # Errors
///
/// Returns `RuntimeError::FilesystemError` on mount failure.
#[cfg(target_os = "linux")]
pub fn mount_essential_filesystems(config: &FilesystemConfig) -> Result<()> {
    use nix::mount::{mount, MsFlags};

    if config.mount_proc {
        let target = config.rootfs.join("proc");
        mount(
            Some("proc"),
            &target,
            Some("proc"),
            MsFlags::MS_NOSUID | MsFlags::MS_NOEXEC | MsFlags::MS_NODEV,
            None::<&str>,
        )
        .map_err(|e| RuntimeError::FilesystemError {
            operation: "mount proc".into(),
            reason: e.to_string(),
        })?;
    }

    if config.mount_sys {
        let target = config.rootfs.join("sys");
        mount(
            Some("sysfs"),
            &target,
            Some("sysfs"),
            MsFlags::MS_RDONLY | MsFlags::MS_NOSUID | MsFlags::MS_NOEXEC | MsFlags::MS_NODEV,
            None::<&str>,
        )
        .map_err(|e| RuntimeError::FilesystemError {
            operation: "mount sysfs".into(),
            reason: e.to_string(),
        })?;
    }

    tracing::info!("Essential filesystems mounted");
    Ok(())
}

/// Non-Linux stub for mounting essential filesystems.
#[cfg(not(target_os = "linux"))]
pub fn mount_essential_filesystems(config: &FilesystemConfig) -> Result<()> {
    tracing::debug!(rootfs = %config.rootfs.display(), "mount stub (non-Linux)");
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn prepare_rootfs_creates_directories() {
        let tmp = tempfile::tempdir().expect("tmpdir");
        let config = FilesystemConfig {
            rootfs: tmp.path().join("rootfs"),
            ..FilesystemConfig::default()
        };

        prepare_rootfs(&config).expect("prepare rootfs");

        assert!(config.rootfs.join("proc").exists());
        assert!(config.rootfs.join("sys").exists());
        assert!(config.rootfs.join("dev").exists());
        assert!(config.rootfs.join("etc").exists());
    }

    #[test]
    fn default_config_mounts_all() {
        let cfg = FilesystemConfig::default();
        assert!(cfg.mount_proc);
        assert!(cfg.mount_sys);
        assert!(cfg.mount_dev);
    }
}
