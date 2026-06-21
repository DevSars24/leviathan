//! Resource capacity and request types.
//!
//! Used to describe what a node offers and what a container needs.
//! Supports arithmetic operations for scheduler placement calculations.

use std::fmt;
use std::ops::{Add, Sub};

use serde::{Deserialize, Serialize};

/// A description of compute resources — used both as node capacity and
/// as container resource requests/limits.
///
/// # Examples
///
/// ```
/// use leviathan_core::ResourceSpec;
///
/// let node_capacity = ResourceSpec::new(4000, 8192);
/// let container_req = ResourceSpec::new(1000, 2048);
///
/// assert!(container_req.fits_within(&node_capacity));
///
/// let remaining = node_capacity - container_req;
/// assert_eq!(remaining.cpu_millicores, 3000);
/// assert_eq!(remaining.memory_mib, 6144);
/// ```
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
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

    /// Returns `true` if both CPU and memory are zero.
    pub fn is_zero(&self) -> bool {
        self.cpu_millicores == 0 && self.memory_mib == 0
    }
}

impl Default for ResourceSpec {
    fn default() -> Self {
        // Sensible defaults: 1 vCPU, 512 MiB
        Self::new(1000, 512)
    }
}

impl Add for ResourceSpec {
    type Output = Self;

    fn add(self, rhs: Self) -> Self::Output {
        Self {
            cpu_millicores: self.cpu_millicores + rhs.cpu_millicores,
            memory_mib: self.memory_mib + rhs.memory_mib,
        }
    }
}

/// Subtraction uses saturating arithmetic to prevent underflow. If a
/// container's request exceeds available capacity the result is clamped
/// to zero rather than panicking.
impl Sub for ResourceSpec {
    type Output = Self;

    fn sub(self, rhs: Self) -> Self::Output {
        Self {
            cpu_millicores: self.cpu_millicores.saturating_sub(rhs.cpu_millicores),
            memory_mib: self.memory_mib.saturating_sub(rhs.memory_mib),
        }
    }
}

impl fmt::Display for ResourceSpec {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}m CPU / {}Mi", self.cpu_millicores, self.memory_mib)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fits_within_returns_true_when_capacity_is_sufficient() {
        let request = ResourceSpec::new(1000, 2048);
        let available = ResourceSpec::new(4000, 8192);
        assert!(request.fits_within(&available));
    }

    #[test]
    fn fits_within_returns_false_when_cpu_exceeds() {
        let request = ResourceSpec::new(5000, 2048);
        let available = ResourceSpec::new(4000, 8192);
        assert!(!request.fits_within(&available));
    }

    #[test]
    fn add_resources() {
        let a = ResourceSpec::new(1000, 512);
        let b = ResourceSpec::new(2000, 1024);
        assert_eq!(a + b, ResourceSpec::new(3000, 1536));
    }

    #[test]
    fn sub_resources_saturates_at_zero() {
        let a = ResourceSpec::new(1000, 512);
        let b = ResourceSpec::new(2000, 1024);
        assert_eq!(a - b, ResourceSpec::new(0, 0));
    }

    #[test]
    fn is_zero() {
        assert!(ResourceSpec::new(0, 0).is_zero());
        assert!(!ResourceSpec::new(1, 0).is_zero());
    }

    #[test]
    fn display_format() {
        let r = ResourceSpec::new(2000, 4096);
        assert_eq!(format!("{}", r), "2000m CPU / 4096Mi");
    }
}
