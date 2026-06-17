//! Container management subcommands.
//!
//! Handles `leviathan container <action>`.

use clap::Subcommand;

/// Subcommands available under `leviathan container`.
#[derive(Subcommand, Debug)]
pub enum ContainerCommand {
    /// Submit a container workload to the cluster.
    Run {
        /// OCI image reference to execute (e.g. `ubuntu:22.04`).
        #[arg(long)]
        image: String,

        /// A human-readable name for this container instance.
        #[arg(long)]
        name: String,
    },
    /// List all containers tracked by the control plane.
    List,
}

/// Dispatch a [`ContainerCommand`] to the appropriate handler.
pub fn handle(cmd: ContainerCommand) -> anyhow::Result<()> {
    match cmd {
        ContainerCommand::Run { image, name } => {
            println!("[NOT IMPLEMENTED YET] container run  image={image}  name={name}");
            println!("  → Day 6: will set up Linux namespaces and cgroups via unsafe Rust.");
        }
        ContainerCommand::List => {
            println!("[NOT IMPLEMENTED YET] container list");
            println!("  → Day 3: will query control plane and render container table.");
        }
    }
    Ok(())
}
