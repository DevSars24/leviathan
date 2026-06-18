//! Node management subcommands.
//!
//! Handles `leviathan node <action>`.

use clap::Subcommand;

/// Subcommands available under `leviathan node`.
#[derive(Subcommand, Debug)]
pub enum NodeCommand {
    /// Start the node daemon and register with the control plane.
    Start {
        /// Unique identifier for this node (e.g. `node-1`).
        #[arg(long)]
        id: String,

        /// The `host:port` address this node will listen on.
        #[arg(long)]
        addr: String,
    },
    /// List all nodes registered in the cluster.
    List,
}

/// Dispatch a [`NodeCommand`] to the appropriate handler.
pub fn handle(cmd: NodeCommand) -> anyhow::Result<()> {
    match cmd {
        NodeCommand::Start { id, addr } => {
            println!("Starting node CLI agent wrapper... To run the official high-performance background daemon, please execute:");
            println!("  cargo run -p leviathan-node -- --id {} --addr {}", id, addr);
        }
        NodeCommand::List => {
            println!("[NOT IMPLEMENTED YET] node list");
            println!("  → Day 3: will query control plane over TCP and print node table.");
        }
    }
    Ok(())
}
