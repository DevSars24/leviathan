//! Cluster-level subcommands.
//!
//! Handles `leviathan cluster <action>`.

use clap::Subcommand;
use tracing::info;

/// Subcommands available under `leviathan cluster`.
#[derive(Subcommand, Debug)]
pub enum ClusterCommand {
    /// Print overall cluster health — node count, container count, Raft leader.
    Status,
    /// Print the CLI version and build information.
    Version,
}

/// Dispatch a [`ClusterCommand`] to the appropriate handler.
pub fn handle(cmd: ClusterCommand) -> anyhow::Result<()> {
    match cmd {
        ClusterCommand::Status => {
            info!("Querying cluster status");
            println!("[NOT IMPLEMENTED YET] cluster status");
            println!("  → Day 5: will contact Raft leader and print cluster summary.");
        }
        ClusterCommand::Version => {
            println!(
                "leviathan {} ({})",
                env!("CARGO_PKG_VERSION"),
                env!("CARGO_PKG_NAME"),
            );
        }
    }
    Ok(())
}
