//! Leviathan placement scheduler binary.
//!
//! This is the entry point for the standalone scheduler daemon.
//! The actual scheduling logic lives in the `leviathan-scheduler` library crate.

fn main() {
    println!("leviathan-scheduler: placement scheduler");
    println!("  Strategies: FirstFitScheduler, BestFitScheduler, FirstFitDecreasingScheduler");
    println!("  Pluggable scoring via Scorer trait");
    println!("  Full integration with Raft consensus for replicated decisions.");
}
