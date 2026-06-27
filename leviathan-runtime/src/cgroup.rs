//! cgroups v2 resource control for container processes.
//!
//! This module writes directly to the `/sys/fs/cgroup` unified hierarchy
//! to enforce CPU, memory, and PID limits on container processes.
//!
//! # cgroups v2 Interface
//!
//! On a cgroups v2 system, each cgroup is a directory under
//! `/sys/fs/cgroup`. Resource limits are configured by writing to
//! controller-specific files:
//!
//! - `cpu.max`: `"$QUOTA $PERIOD"` — e.g., `"100000 100000"` for 1 CPU.
//! - `memory.max`: bytes — e.g., `"536870912"` for 512 MiB.
//! - `pids.max`: integer — e.g., `"100"` for max 100 processes.
//! - `cgroup.procs`: write PID to assign a process to this cgroup.
//!
//! # Platform Gating
//!
//! Actual cgroup writes are Linux-only. On other platforms, the
//! `CgroupManager` tracks configuration in-memory without writing to
//! the filesystem, enabling testing and validation.

use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

use crate::error::Result;

/// The default cgroups v2 mount point on modern Linux systems.
pub const CGROUP_ROOT: &str = "/sys/fs/cgroup";

/// The default CPU period in microseconds (100ms).
pub const DEFAULT_CPU_PERIOD_US: u64 = 100_000;

/// Resource limits for a container, expressed in cgroups v2 terms.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CgroupLimits {
    /// CPU quota in millicores (1000 = 1 full CPU core).
    /// Translated to `cpu.max` as `quota_us period_us`.
    /// A value of 0 means no limit.
    pub cpu_millicores: u64,

    /// Memory limit in bytes. Written to `memory.max`.
    /// A value of 0 means no limit.
    pub memory_bytes: u64,

    /// Maximum number of processes. Written to `pids.max`.
    /// A value of 0 means no limit.
    pub pids_max: u64,
}

impl CgroupLimits {
    /// Create limits from the core `ResourceSpec`.
    ///
    /// Converts millicores to CPU quota and MiB to bytes.
    #[must_use]
    pub fn from_resource_spec(spec: &leviathan_core::ResourceSpec) -> Self {
        Self {
            cpu_millicores: spec.cpu_millicores,
            // Convert MiB to bytes: 1 MiB = 1048576 bytes.
            memory_bytes: spec.memory_mib * 1_048_576,
            pids_max: 512, // Sensible default.
        }
    }

    /// Convert millicores to a `cpu.max` string.
    ///
    /// `cpu.max` format: `"$QUOTA $PERIOD"` where both are in microseconds.
    /// 1000 millicores = 1 full core = quota == period.
    #[must_use]
    pub fn cpu_max_value(&self) -> String {
        if self.cpu_millicores == 0 {
            return "max 100000".to_string();
        }
        // quota_us = (millicores / 1000) * period_us
        let quota_us = (self.cpu_millicores * DEFAULT_CPU_PERIOD_US) / 1000;
        format!("{quota_us} {DEFAULT_CPU_PERIOD_US}")
    }

    /// Format the memory limit for `memory.max`.
    #[must_use]
    pub fn memory_max_value(&self) -> String {
        if self.memory_bytes == 0 {
            return "max".to_string();
        }
        self.memory_bytes.to_string()
    }

    /// Format the PID limit for `pids.max`.
    #[must_use]
    pub fn pids_max_value(&self) -> String {
        if self.pids_max == 0 {
            return "max".to_string();
        }
        self.pids_max.to_string()
    }
}

/// Manages the lifecycle of a cgroup for a container.
///
/// Creates, configures, and destroys a cgroup directory under the
/// `/sys/fs/cgroup` hierarchy.
pub struct CgroupManager {
    /// Name of this cgroup (used as directory name).
    name: String,

    /// Full path to the cgroup directory.
    path: PathBuf,

    /// Configured resource limits.
    limits: CgroupLimits,
}

impl CgroupManager {
    /// Create a new `CgroupManager` for the given container.
    ///
    /// Does NOT create the cgroup directory on disk — call [`create`] for that.
    #[must_use]
    pub fn new(name: impl Into<String>, limits: CgroupLimits) -> Self {
        let name = name.into();
        let path = Path::new(CGROUP_ROOT).join("leviathan").join(&name);
        Self { name, path, limits }
    }

    /// Return the cgroup directory path.
    #[must_use]
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Return the cgroup name.
    #[must_use]
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Create the cgroup directory and write resource limits.
    ///
    /// On Linux, creates the directory under `/sys/fs/cgroup/leviathan/`
    /// and writes to `cpu.max`, `memory.max`, and `pids.max`.
    ///
    /// On non-Linux, logs the operation and succeeds.
    ///
    /// # Errors
    ///
    /// Returns `RuntimeError::CgroupError` on write failure.
    pub fn create(&self) -> Result<()> {
        #[cfg(target_os = "linux")]
        {
            std::fs::create_dir_all(&self.path).map_err(|e| RuntimeError::CgroupError {
                controller: "cgroup".into(),
                reason: format!("mkdir {}: {e}", self.path.display()),
            })?;

            self.write_controller("cpu.max", &self.limits.cpu_max_value())?;
            self.write_controller("memory.max", &self.limits.memory_max_value())?;
            self.write_controller("pids.max", &self.limits.pids_max_value())?;

            tracing::info!(
                cgroup = %self.name,
                path = %self.path.display(),
                "cgroup v2 created and configured"
            );
        }

        #[cfg(not(target_os = "linux"))]
        {
            tracing::debug!(
                cgroup = %self.name,
                cpu_max = %self.limits.cpu_max_value(),
                memory_max = %self.limits.memory_max_value(),
                pids_max = %self.limits.pids_max_value(),
                "cgroup stub (non-Linux)"
            );
        }

        Ok(())
    }

    /// Assign a process to this cgroup by writing its PID to `cgroup.procs`.
    ///
    /// # Errors
    ///
    /// Returns `RuntimeError::CgroupError` on write failure.
    pub fn assign_pid(&self, pid: u32) -> Result<()> {
        #[cfg(target_os = "linux")]
        {
            self.write_controller("cgroup.procs", &pid.to_string())?;
            tracing::debug!(pid, cgroup = %self.name, "Assigned PID to cgroup");
        }

        #[cfg(not(target_os = "linux"))]
        {
            tracing::debug!(pid, cgroup = %self.name, "cgroup.assign_pid stub");
        }

        Ok(())
    }

    /// Destroy the cgroup by removing its directory.
    ///
    /// The cgroup must have no active processes.
    ///
    /// # Errors
    ///
    /// Returns `RuntimeError::CgroupError` on removal failure.
    pub fn destroy(&self) -> Result<()> {
        #[cfg(target_os = "linux")]
        {
            if self.path.exists() {
                std::fs::remove_dir(&self.path).map_err(|e| RuntimeError::CgroupError {
                    controller: "cgroup".into(),
                    reason: format!("rmdir {}: {e}", self.path.display()),
                })?;
                tracing::info!(cgroup = %self.name, "cgroup destroyed");
            }
        }

        #[cfg(not(target_os = "linux"))]
        {
            tracing::debug!(cgroup = %self.name, "cgroup.destroy stub");
        }

        Ok(())
    }

    /// Write a value to a cgroup controller file.
    #[cfg(target_os = "linux")]
    fn write_controller(&self, filename: &str, value: &str) -> Result<()> {
        let path = self.path.join(filename);
        std::fs::write(&path, value).map_err(|e| RuntimeError::CgroupError {
            controller: filename.to_string(),
            reason: format!("write {}: {e}", path.display()),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use leviathan_core::ResourceSpec;

    #[test]
    fn cpu_max_value_one_core() {
        let limits = CgroupLimits {
            cpu_millicores: 1000,
            memory_bytes: 0,
            pids_max: 0,
        };
        assert_eq!(limits.cpu_max_value(), "100000 100000");
    }

    #[test]
    fn cpu_max_value_half_core() {
        let limits = CgroupLimits {
            cpu_millicores: 500,
            memory_bytes: 0,
            pids_max: 0,
        };
        assert_eq!(limits.cpu_max_value(), "50000 100000");
    }

    #[test]
    fn cpu_max_value_no_limit() {
        let limits = CgroupLimits {
            cpu_millicores: 0,
            memory_bytes: 0,
            pids_max: 0,
        };
        assert_eq!(limits.cpu_max_value(), "max 100000");
    }

    #[test]
    fn memory_max_512mib() {
        let limits = CgroupLimits::from_resource_spec(&ResourceSpec::new(1000, 512));
        assert_eq!(limits.memory_max_value(), "536870912");
    }

    #[test]
    fn pids_max_value() {
        let limits = CgroupLimits {
            cpu_millicores: 0,
            memory_bytes: 0,
            pids_max: 100,
        };
        assert_eq!(limits.pids_max_value(), "100");
    }

    #[test]
    fn from_resource_spec() {
        let spec = ResourceSpec::new(2000, 1024);
        let limits = CgroupLimits::from_resource_spec(&spec);
        assert_eq!(limits.cpu_millicores, 2000);
        assert_eq!(limits.memory_bytes, 1024 * 1_048_576);
    }

    #[test]
    fn cgroup_manager_path() {
        let limits = CgroupLimits::from_resource_spec(&ResourceSpec::new(1000, 512));
        let mgr = CgroupManager::new("test-container", limits);
        assert!(mgr.path().to_str().unwrap_or("").contains("test-container"));
    }
}
