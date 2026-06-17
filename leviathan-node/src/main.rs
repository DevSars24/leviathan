//! Leviathan worker node daemon.
//!
//! This binary runs on every machine that participates as a cluster worker.
//! It accepts container workloads from the control plane, manages local
//! container lifecycle, and reports health back via heartbeat.
//!
//! **Day 1:** stub only — real implementation starts on Day 2 (Tokio runtime)
//! and Day 6 (container runtime / Linux namespaces).

fn main() {
    println!("leviathan-node: worker node daemon");
    println!("[NOT IMPLEMENTED YET] — full implementation begins on Day 2.");
}
