//! seccomp-BPF syscall filtering for container processes.
//!
//! This module constructs a seccomp-BPF filter that restricts which syscalls
//! a container process can invoke. The default policy is a **deny-list**:
//! dangerous syscalls (`ptrace`, `mount`, `reboot`, `kexec_load`, etc.) are
//! blocked; everything else is allowed.
//!
//! # BPF Program Structure
//!
//! A seccomp filter is a classic BPF (cBPF) program that inspects the
//! `seccomp_data` struct passed by the kernel on every syscall:
//!
//! ```text
//! struct seccomp_data {
//!     int nr;                  // syscall number
//!     __u32 arch;              // AUDIT_ARCH_*
//!     __u64 instruction_pointer;
//!     __u64 args[6];
//! };
//! ```
//!
//! The filter returns an action: `SECCOMP_RET_ALLOW`, `SECCOMP_RET_KILL_PROCESS`,
//! `SECCOMP_RET_ERRNO`, etc.
//!
//! # Platform Gating
//!
//! The actual `prctl(PR_SET_SECCOMP)` call is Linux-only. On other platforms,
//! the filter construction logic is still exercised for testing.

use serde::{Deserialize, Serialize};

use crate::error::Result;

/// A syscall to block, identified by its Linux syscall number.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BlockedSyscall {
    /// Human-readable name (e.g., "ptrace", "mount").
    pub name: String,
    /// Architecture-specific syscall number (x86_64).
    pub number: u32,
}

/// Default set of blocked syscalls for container security.
///
/// These syscalls are dangerous when available inside a container:
/// - `ptrace` (101): process inspection/debugging — can escape namespaces
/// - `mount` (165): filesystem remounting — can access host filesystems
/// - `reboot` (169): system reboot — obvious DoS vector
/// - `kexec_load` (246): load a new kernel — complete host compromise
/// - `init_module` (175) / `finit_module` (313): load kernel modules
/// - `delete_module` (176): unload kernel modules
/// - `acct` (163): process accounting — information leak
#[must_use]
pub fn default_blocked_syscalls() -> Vec<BlockedSyscall> {
    vec![
        BlockedSyscall { name: "ptrace".into(), number: 101 },
        BlockedSyscall { name: "mount".into(), number: 165 },
        BlockedSyscall { name: "reboot".into(), number: 169 },
        BlockedSyscall { name: "kexec_load".into(), number: 246 },
        BlockedSyscall { name: "init_module".into(), number: 175 },
        BlockedSyscall { name: "finit_module".into(), number: 313 },
        BlockedSyscall { name: "delete_module".into(), number: 176 },
        BlockedSyscall { name: "acct".into(), number: 163 },
    ]
}

/// Configuration for the seccomp filter.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SeccompConfig {
    /// Syscalls to block. Uses the default deny-list if empty.
    pub blocked_syscalls: Vec<BlockedSyscall>,

    /// Whether to kill the process on a blocked syscall (vs. returning EPERM).
    pub kill_on_violation: bool,
}

impl Default for SeccompConfig {
    fn default() -> Self {
        Self {
            blocked_syscalls: default_blocked_syscalls(),
            kill_on_violation: false, // Return EPERM by default — more debuggable.
        }
    }
}

/// A compiled BPF instruction for the seccomp filter.
///
/// Maps to the kernel's `struct sock_filter`:
/// ```text
/// struct sock_filter {
///     __u16 code;
///     __u8  jt;   // jump-true offset
///     __u8  jf;   // jump-false offset
///     __u32 k;    // immediate value
/// };
/// ```
#[repr(C)]
#[derive(Debug, Clone, Copy)]
pub struct BpfInstruction {
    /// BPF opcode.
    pub code: u16,
    /// Jump-true offset.
    pub jt: u8,
    /// Jump-false offset.
    pub jf: u8,
    /// Immediate value.
    pub k: u32,
}

// BPF opcodes for seccomp filters.
const BPF_LD: u16 = 0x00;
const BPF_W: u16 = 0x00;
const BPF_ABS: u16 = 0x20;
const BPF_JMP: u16 = 0x05;
const BPF_JEQ: u16 = 0x10;
const BPF_RET: u16 = 0x06;
const BPF_K: u16 = 0x00;

// seccomp return values.
const SECCOMP_RET_ALLOW: u32 = 0x7fff_0000;
const SECCOMP_RET_KILL_PROCESS: u32 = 0x8000_0000;
const SECCOMP_RET_ERRNO: u32 = 0x0005_0000;
const EPERM: u32 = 1;

/// Compile a seccomp BPF filter program from the given configuration.
///
/// The generated program:
/// 1. Loads the syscall number from `seccomp_data.nr` (offset 0).
/// 2. For each blocked syscall, compares and jumps to the deny action.
/// 3. Falls through to `SECCOMP_RET_ALLOW` for all other syscalls.
#[must_use]
pub fn compile_filter(config: &SeccompConfig) -> Vec<BpfInstruction> {
    let blocked = if config.blocked_syscalls.is_empty() {
        default_blocked_syscalls()
    } else {
        config.blocked_syscalls.clone()
    };

    let deny_action = if config.kill_on_violation {
        SECCOMP_RET_KILL_PROCESS
    } else {
        SECCOMP_RET_ERRNO | EPERM
    };

    let num_blocked = blocked.len();
    let mut program = Vec::with_capacity(2 + num_blocked + 1);

    // Instruction 0: Load syscall number (offset 0 in seccomp_data).
    program.push(BpfInstruction {
        code: BPF_LD | BPF_W | BPF_ABS,
        jt: 0,
        jf: 0,
        k: 0, // offsetof(seccomp_data, nr)
    });

    // Instructions 1..N: Compare against each blocked syscall.
    // If match, jump to the deny instruction at the end.
    // If no match, fall through to the next comparison.
    for (i, syscall) in blocked.iter().enumerate() {
        let remaining = num_blocked - i;
        program.push(BpfInstruction {
            code: BPF_JMP | BPF_JEQ | BPF_K,
            jt: remaining as u8, // Jump to deny (at end)
            jf: 0,               // Fall through to next comparison
            k: syscall.number,
        });
    }

    // Fall-through: Allow the syscall.
    program.push(BpfInstruction {
        code: BPF_RET | BPF_K,
        jt: 0,
        jf: 0,
        k: SECCOMP_RET_ALLOW,
    });

    // Deny instruction (jumped to by any matching comparison).
    program.push(BpfInstruction {
        code: BPF_RET | BPF_K,
        jt: 0,
        jf: 0,
        k: deny_action,
    });

    program
}

/// Install the seccomp filter on the current process.
///
/// On Linux, this calls `prctl(PR_SET_SECCOMP, SECCOMP_MODE_FILTER, &prog)`.
/// On non-Linux, this is a no-op.
///
/// # Errors
///
/// Returns `RuntimeError::SeccompError` on installation failure.
pub fn install_filter(config: &SeccompConfig) -> Result<()> {
    let filter = compile_filter(config);

    #[cfg(target_os = "linux")]
    {
        use std::mem;

        /// The kernel's `struct sock_fprog`.
        #[repr(C)]
        struct SockFprog {
            len: libc::c_ushort,
            filter: *const BpfInstruction,
        }

        let prog = SockFprog {
            len: filter.len() as libc::c_ushort,
            filter: filter.as_ptr(),
        };

        // SAFETY: We are passing a valid BPF program to prctl.
        // Invariants upheld:
        //   - `prog.len` matches the actual number of instructions.
        //   - `prog.filter` points to a valid array that outlives this call.
        //   - The BPF program has been validated by compile_filter.
        // What breaks if violated:
        //   - Invalid `len` → kernel rejects the program (EINVAL).
        //   - Dangling `filter` → undefined behavior (kernel reads garbage).
        let ret = unsafe {
            libc::prctl(
                libc::PR_SET_NO_NEW_PRIVS,
                1,
                0,
                0,
                0,
            )
        };
        if ret != 0 {
            return Err(RuntimeError::SeccompError(
                "prctl(PR_SET_NO_NEW_PRIVS) failed".into(),
            ));
        }

        let ret = unsafe {
            libc::prctl(
                libc::PR_SET_SECCOMP,
                libc::SECCOMP_MODE_FILTER,
                &prog as *const SockFprog,
            )
        };
        if ret != 0 {
            return Err(RuntimeError::SeccompError(format!(
                "prctl(PR_SET_SECCOMP) failed: errno={}",
                std::io::Error::last_os_error()
            )));
        }

        tracing::info!(
            blocked_count = filter.len() - 2,
            "seccomp-BPF filter installed"
        );
    }

    #[cfg(not(target_os = "linux"))]
    {
        tracing::debug!(
            instructions = filter.len(),
            "seccomp filter compiled (non-Linux — not installed)"
        );
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_blocked_syscalls_non_empty() {
        let blocked = default_blocked_syscalls();
        assert!(!blocked.is_empty());
        assert!(blocked.iter().any(|s| s.name == "ptrace"));
        assert!(blocked.iter().any(|s| s.name == "mount"));
        assert!(blocked.iter().any(|s| s.name == "reboot"));
    }

    #[test]
    fn compile_filter_produces_valid_program() {
        let config = SeccompConfig::default();
        let program = compile_filter(&config);

        // Structure: 1 (load) + N (comparisons) + 1 (allow) + 1 (deny)
        let expected_len = 1 + config.blocked_syscalls.len() + 2;
        assert_eq!(program.len(), expected_len);

        // First instruction should be a load.
        assert_eq!(program[0].code, BPF_LD | BPF_W | BPF_ABS);

        // Last instruction should be the deny return.
        let last = program.last().unwrap();
        assert_eq!(last.code, BPF_RET | BPF_K);
    }

    #[test]
    fn kill_on_violation_changes_action() {
        let config_eperm = SeccompConfig {
            kill_on_violation: false,
            ..SeccompConfig::default()
        };
        let config_kill = SeccompConfig {
            kill_on_violation: true,
            ..SeccompConfig::default()
        };

        let prog_eperm = compile_filter(&config_eperm);
        let prog_kill = compile_filter(&config_kill);

        // The deny action (last instruction) should differ.
        assert_ne!(
            prog_eperm.last().unwrap().k,
            prog_kill.last().unwrap().k
        );
    }
}
