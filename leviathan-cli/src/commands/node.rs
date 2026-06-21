//! Node management subcommands.
//!
//! Handles `leviathan node <action>`.

use clap::Subcommand;
use tracing::info;

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

        /// CPU capacity in millicores (default: 2000 = 2 vCPUs).
        #[arg(long, default_value_t = 2000)]
        cpu: u64,

        /// Memory capacity in MiB (default: 4096 = 4 GiB).
        #[arg(long, default_value_t = 4096)]
        memory: u64,
    },
    /// List all nodes registered in the cluster.
    List,
}

/// Dispatch a [`NodeCommand`] to the appropriate handler.
pub fn handle(cmd: NodeCommand) -> anyhow::Result<()> {
    match cmd {
        NodeCommand::Start {
            id,
            addr,
            cpu,
            memory,
        } => {
            info!(
                node_id = %id,
                addr = %addr,
                cpu_millicores = cpu,
                memory_mib = memory,
                "Starting node CLI agent wrapper"
            );
            println!("To run the official high-performance background daemon, please execute:");
            println!(
                "  cargo run -p leviathan-node -- --id {} --addr {}",
                id, addr
            );
            println!(
                "  Node resources: {}m CPU / {}Mi",
                cpu, memory
            );
        }
        NodeCommand::List => {
            info!("Listing cluster nodes");
            println!("[NOT IMPLEMENTED YET] node list");
            println!("  → Day 3: will query control plane over TCP and print node table.");
        }
    }
    Ok(())
}
