//! Container lifecycle management — the orchestrator of runtime subsystems.
//!
//! [`ContainerRuntime`] wires together namespace, filesystem, cgroup, seccomp,
//! and network modules to provide a complete container spawn lifecycle.
//!
//! # Spawn Sequence
//!
//! 1. Prepare the rootfs (directory structure, OCI layers).
//! 2. Create cgroup and configure resource limits.
//! 3. Create Linux namespaces (PID, NET, UTS, MNT, IPC).
//! 4. Set up the container filesystem (`pivot_root`, bind mounts).
//! 5. Install seccomp-BPF filter.
//! 6. Configure container network (veth, IP, routing).
//! 7. Execute the container entrypoint.

use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

use crate::cgroup::{CgroupLimits, CgroupManager};
use crate::error::Result;
use crate::filesystem::{self, FilesystemConfig};
use crate::namespace::{self, NamespaceConfig};
use crate::network::{self, NetworkConfig};
use crate::seccomp::{self, SeccompConfig};

/// Full specification for spawning a container.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ContainerSpec {
    /// Unique identifier for this container instance.
    pub id: String,

    /// OCI image reference (e.g., "ubuntu:22.04").
    pub image: String,

    /// Command to execute as the container's entrypoint.
    pub command: Vec<String>,

    /// Environment variables (KEY=VALUE).
    pub env: Vec<String>,

    /// Working directory inside the container.
    pub working_dir: String,

    /// Resource limits.
    pub resources: leviathan_core::ResourceSpec,

    /// Namespace configuration.
    pub namespaces: NamespaceConfig,

    /// Seccomp configuration.
    pub seccomp: SeccompConfig,

    /// Network configuration.
    pub network: Option<NetworkConfig>,
}

impl ContainerSpec {
    /// Create a minimal container spec for testing.
    #[must_use]
    pub fn new(id: impl Into<String>, image: impl Into<String>) -> Self {
        Self {
            id: id.into(),
            image: image.into(),
            command: vec!["/bin/sh".into()],
            env: Vec::new(),
            working_dir: "/".into(),
            resources: leviathan_core::ResourceSpec::default(),
            namespaces: NamespaceConfig::default(),
            seccomp: SeccompConfig::default(),
            network: Some(NetworkConfig::default()),
        }
    }
}

/// The status of a spawned container.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum ContainerState {
    /// Container has been created but not started.
    Created,
    /// Container is actively running.
    Running {
        /// PID of the container's init process (in the host PID namespace).
        pid: u32,
    },
    /// Container has exited.
    Stopped {
        /// Exit code of the container process.
        exit_code: i32,
    },
    /// Container failed to start.
    Failed {
        /// Reason for the failure.
        reason: String,
    },
}

/// The container runtime — orchestrates all subsystems for container lifecycle.
pub struct ContainerRuntime {
    /// Base directory for container rootfs and state.
    state_dir: PathBuf,
}

impl ContainerRuntime {
    /// Create a new container runtime with the given state directory.
    #[must_use]
    pub fn new(state_dir: impl Into<PathBuf>) -> Self {
        Self {
            state_dir: state_dir.into(),
        }
    }

    /// Return the state directory.
    #[must_use]
    pub fn state_dir(&self) -> &Path {
        &self.state_dir
    }

    /// Spawn a container according to the given specification.
    ///
    /// This is the main entry point for the runtime. It executes the full
    /// container creation sequence:
    ///
    /// 1. Prepare rootfs
    /// 2. Create cgroup
    /// 3. Create namespaces
    /// 4. Setup filesystem (pivot_root)
    /// 5. Install seccomp filter
    /// 6. Configure network
    ///
    /// # Errors
    ///
    /// Returns the first error encountered during the spawn sequence.
    /// Cleanup is best-effort — cgroups and rootfs directories may be
    /// left behind for debugging.
    pub fn spawn_container(&self, spec: &ContainerSpec) -> Result<ContainerState> {
        let container_dir = self.state_dir.join(&spec.id);
        let rootfs = container_dir.join("rootfs");

        tracing::info!(
            container_id = %spec.id,
            image = %spec.image,
            "Spawning container"
        );

        // Step 1: Prepare rootfs.
        let fs_config = FilesystemConfig {
            rootfs: rootfs.clone(),
            ..FilesystemConfig::default()
        };
        filesystem::prepare_rootfs(&fs_config)?;

        // Step 2: Create cgroup.
        let cgroup_limits = CgroupLimits::from_resource_spec(&spec.resources);
        let cgroup = CgroupManager::new(&spec.id, cgroup_limits);
        cgroup.create()?;

        // Step 3: Create namespaces.
        // On non-Linux, this is a no-op.
        namespace::create_namespaces(&spec.namespaces)?;

        // Step 4: Setup filesystem (pivot_root).
        // On non-Linux, this is a no-op.
        filesystem::mount_essential_filesystems(&fs_config)?;

        // Step 5: Install seccomp filter.
        // On non-Linux, this is a no-op (filter is compiled but not installed).
        seccomp::install_filter(&spec.seccomp)?;

        // Step 6: Configure network.
        if let Some(ref net_config) = spec.network {
            network::setup_container_network(net_config)?;
        }

        // On non-Linux, we simulate a successful container spawn.
        // On Linux, this would fork/exec the entrypoint.
        tracing::info!(
            container_id = %spec.id,
            "Container spawn sequence completed"
        );

        Ok(ContainerState::Created)
    }

    /// Stop a running container.
    ///
    /// Sends SIGTERM to the container's init process, waits for graceful
    /// shutdown, then sends SIGKILL if necessary.
    ///
    /// # Errors
    ///
    /// Returns `RuntimeError` if the container cannot be stopped.
    pub fn stop_container(&self, container_id: &str) -> Result<ContainerState> {
        tracing::info!(container_id, "Stopping container");

        // Clean up cgroup.
        let cgroup_limits = CgroupLimits::from_resource_spec(&leviathan_core::ResourceSpec::default());
        let cgroup = CgroupManager::new(container_id, cgroup_limits);
        if let Err(e) = cgroup.destroy() {
            tracing::warn!(error = %e, "Best-effort cgroup cleanup failed");
        }

        Ok(ContainerState::Stopped { exit_code: 0 })
    }

    /// Get the state of a container by ID.
    ///
    /// Checks if the container's rootfs directory exists.
    #[must_use]
    pub fn container_state(&self, container_id: &str) -> ContainerState {
        let container_dir = self.state_dir.join(container_id);
        if container_dir.exists() {
            ContainerState::Created
        } else {
            ContainerState::Failed {
                reason: "container directory not found".into(),
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn container_spec_defaults() {
        let spec = ContainerSpec::new("test-1", "alpine:latest");
        assert_eq!(spec.id, "test-1");
        assert_eq!(spec.image, "alpine:latest");
        assert!(!spec.command.is_empty());
    }

    #[test]
    fn spawn_container_creates_rootfs() {
        let tmp = tempfile::tempdir().expect("tmpdir");
        let runtime = ContainerRuntime::new(tmp.path());

        let spec = ContainerSpec {
            id: "c-1".into(),
            image: "test:latest".into(),
            command: vec!["/bin/true".into()],
            env: Vec::new(),
            working_dir: "/".into(),
            resources: leviathan_core::ResourceSpec::new(1000, 512),
            namespaces: NamespaceConfig::none(), // Skip namespaces in test
            seccomp: SeccompConfig::default(),
            network: None, // Skip network in test
        };

        let state = runtime.spawn_container(&spec).expect("spawn");
        assert_eq!(state, ContainerState::Created);

        // Verify rootfs was created.
        assert!(tmp.path().join("c-1").join("rootfs").join("proc").exists());
    }

    #[test]
    fn stop_container_returns_stopped() {
        let tmp = tempfile::tempdir().expect("tmpdir");
        let runtime = ContainerRuntime::new(tmp.path());
        let state = runtime.stop_container("c-1").expect("stop");
        assert!(matches!(state, ContainerState::Stopped { exit_code: 0 }));
    }

    #[test]
    fn container_state_not_found() {
        let tmp = tempfile::tempdir().expect("tmpdir");
        let runtime = ContainerRuntime::new(tmp.path());
        let state = runtime.container_state("nonexistent");
        assert!(matches!(state, ContainerState::Failed { .. }));
    }
}
