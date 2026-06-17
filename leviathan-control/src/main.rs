//! Leviathan control plane daemon.
//!
//! The control plane maintains authoritative cluster state — the registry of
//! nodes, containers, and their current status. It drives reconciliation,
//! delegates scheduling decisions to `leviathan-scheduler`, and exposes an
//! API for the CLI to query.
//!
//! **Day 1:** stub only — real implementation starts on Day 2 (Tokio) and
//! Day 3 (TCP/gRPC API layer).

fn main() {
    println!("leviathan-control: control plane daemon");
    println!("[NOT IMPLEMENTED YET] — full implementation begins on Day 2.");
}
