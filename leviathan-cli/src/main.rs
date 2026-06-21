//! # leviathan
//!
//! Command-line interface for the Leviathan distributed container
//! orchestration platform.
//!
//! ## Usage
//!
//! ```text
//! leviathan node start --id node-1 --addr 127.0.0.1:7001
//! leviathan node list
//! leviathan container run --image ubuntu:22.04 --name my-container
//! leviathan container list
//! leviathan cluster status
//! leviathan cluster version
//! ```

use clap::{Parser, Subcommand};

mod commands;

use commands::{cluster, container, node};

// ---------------------------------------------------------------------------
// Top-level CLI
// ---------------------------------------------------------------------------

/// Leviathan — a distributed container orchestration platform.
///
/// Control your cluster from a single binary.
#[derive(Parser, Debug)]
#[command(
    name = "leviathan",
    version,
    about = "☠  Leviathan — Distributed Container Orchestration",
    long_about = "Leviathan is a minimal Kubernetes-like orchestration platform \
                  written entirely in Rust. Use this CLI to manage nodes, \
                  containers, and cluster state."
)]
struct Cli {
    /// Increase output verbosity. Pass multiple times for higher levels.
    #[arg(short, long, action = clap::ArgAction::Count, global = true)]
    verbose: u8,

    #[command(subcommand)]
    command: Commands,
}

/// Top-level subcommands.
#[derive(Subcommand, Debug)]
enum Commands {
    /// Manage cluster nodes (worker daemons).
    Node {
        #[command(subcommand)]
        action: node::NodeCommand,
    },
    /// Manage container workloads.
    Container {
        #[command(subcommand)]
        action: container::ContainerCommand,
    },
    /// Inspect overall cluster health.
    Cluster {
        #[command(subcommand)]
        action: cluster::ClusterCommand,
    },
}

// ---------------------------------------------------------------------------
// Entry point
// ---------------------------------------------------------------------------

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();

    // Initialise structured logging based on verbosity flag.
    let log_level = match cli.verbose {
        0 => "warn",
        1 => "info",
        2 => "debug",
        _ => "trace",
    };
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new(log_level)),
        )
        .init();

    tracing::debug!(command = ?cli.command, "Dispatching CLI command");

    match cli.command {
        Commands::Node { action } => node::handle(action)?,
        Commands::Container { action } => container::handle(action)?,
        Commands::Cluster { action } => cluster::handle(action)?,
    }

    Ok(())
}
