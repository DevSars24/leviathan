//! Leviathan placement scheduler.
//!
//! The scheduler watches for `Pending` containers and selects the best worker
//! node based on available resources, affinity rules, and cluster topology.
//! It is the direct analogue of the `kube-scheduler` in Kubernetes.
//!
//! **Day 1:** stub only — real implementation starts on Day 7 (scheduling
//! algorithms), building on the Raft state from Day 5.

fn main() {
    println!("leviathan-scheduler: placement scheduler");
    println!("[NOT IMPLEMENTED YET] — full implementation begins on Day 7.");
}
