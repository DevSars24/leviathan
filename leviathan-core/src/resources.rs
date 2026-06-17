//! Resource capacity and request types.
//!
//! Used to describe what a node offers and what a container needs.

use serde::{Deserialize, Serialize};

/// A description of compute resources — used both as node capacity and
/// as container resource requests/limits.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ResourceSpec {
    /// Number of logical CPU cores (millicores, e.g. 1000 = 1 core).
    pub cpu_millicores: u64,
    /// Memory in mebibytes (MiB).
    pub memory_mib: u64,
}

impl ResourceSpec {
    /// Create a new resource specification.
    pub fn new(cpu_millicores: u64, memory_mib: u64) -> Self {
        Self {
            cpu_millicores,
            memory_mib,
        }
    }

    /// Returns `true` if this spec fits within the `available` capacity.
    pub fn fits_within(&self, available: &ResourceSpec) -> bool {
        self.cpu_millicores <= available.cpu_millicores
            && self.memory_mib <= available.memory_mib
    }
}

impl Default for ResourceSpec {
    fn default() -> Self {
        // Sensible defaults: 1 vCPU, 512 MiB
        Self::new(1000, 512)
    }
}
