//! Cluster-level subcommands.
//!
//! Handles `leviathan cluster <action>`.

use clap::Subcommand;

/// Subcommands available under `leviathan cluster`.
#[derive(Subcommand, Debug)]
pub enum ClusterCommand {
    /// Print overall cluster health — node count, container count, Raft leader.
    Status,
}

/// Dispatch a [`ClusterCommand`] to the appropriate handler.
pub fn handle(cmd: ClusterCommand) -> anyhow::Result<()> {
    match cmd {
        ClusterCommand::Status => {
            println!("[NOT IMPLEMENTED YET] cluster status");
            println!("  → Day 5: will contact Raft leader and print cluster summary.");
        }
    }
    Ok(())
}
