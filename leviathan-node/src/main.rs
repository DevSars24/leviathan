//! Leviathan worker node daemon.
//!
//! This binary runs on every machine that participates as a cluster worker.
//! It connects to the control plane, reports node health via heartbeat,
//! and runs simulated workloads.

use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use clap::Parser;
use tokio::io::AsyncWriteExt;
use tokio::net::TcpStream;
use tokio::sync::mpsc;
use tracing::{error, info, warn};

use leviathan_core::{
    CooperativeYield, ExponentialBackoff, Heartbeat, NodeId, NodeMessage, NodeStatus, ResourceSpec,
};

/// CLI arguments for the worker node daemon.
#[derive(Parser, Debug)]
#[command(name = "leviathan-node", about = "Leviathan worker node daemon")]
struct Args {
    /// Unique identifier for this node.
    #[arg(long, default_value = "node-1")]
    id: String,

    /// Listen address for this node's services (host:port).
    #[arg(long, default_value = "127.0.0.1:7001")]
    addr: String,

    /// Control plane server address.
    #[arg(long, default_value = "127.0.0.1:8000")]
    control_addr: String,
}

/// Counter tracking total messages sent to the control plane for observability.
static MESSAGES_SENT: AtomicU64 = AtomicU64::new(0);

/// Serialize and send a [`NodeMessage`] over TCP using newline-delimited JSON.
#[tracing::instrument(skip(stream, msg), fields(msg_type = ?std::mem::discriminant(msg)))]
async fn send_msg(stream: &mut TcpStream, msg: &NodeMessage) -> std::io::Result<()> {
    let mut serialized = serde_json::to_string(msg).map_err(|e| {
        std::io::Error::new(std::io::ErrorKind::InvalidData, e)
    })?;
    serialized.push('\n');
    stream.write_all(serialized.as_bytes()).await?;
    stream.flush().await?;
    MESSAGES_SENT.fetch_add(1, Ordering::Relaxed);
    Ok(())
}

/// A simulated heavy CPU calculation that yields cooperatively to prevent
/// starvation of other tasks on the Tokio executor.
#[tracing::instrument(skip_all, fields(input))]
async fn perform_cooperative_calculation(input: usize) -> u64 {
    let mut sum: u64 = 0;
    // Perform computation in stages, yielding to the Tokio executor at each stage
    for stage in 0..5 {
        for i in 0..1_000_000 {
            sum = sum.wrapping_add((stage * i * input) as u64);
        }
        // Yield control back to the executor cooperatively
        CooperativeYield::new(1).await;
    }
    sum
}

/// Spawns a mock workload manager to calculate system telemetry.
#[tracing::instrument(skip_all, fields(node_id = %node_id))]
async fn run_mock_workloads(node_id: NodeId, resources: ResourceSpec, tx: mpsc::Sender<NodeMessage>) {
    info!("Starting mock workload manager...");
    let mut iteration = 0;
    loop {
        tokio::time::sleep(Duration::from_secs(4)).await;
        iteration += 1;

        info!(
            iteration,
            "Simulating heavy CPU telemetry computation..."
        );

        let result = perform_cooperative_calculation(iteration).await;

        info!(
            iteration,
            result,
            "Telemetry computation complete. Reporting status."
        );

        let hb = Heartbeat {
            node_id: node_id.clone(),
            status: NodeStatus::Ready,
            resources: resources.clone(),
            timestamp: chrono::Utc::now(),
        };

        if tx.send(NodeMessage::Heartbeat(hb)).await.is_err() {
            break;
        }
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Parse command-line args
    let args = Args::parse();

    // Initialize subscriber for logging
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();

    info!(
        node_id = %args.id,
        addr = %args.addr,
        control_addr = %args.control_addr,
        "Initializing Leviathan Worker Node Daemon"
    );

    // Setup exponential backoff for connection retries
    let mut backoff = ExponentialBackoff::new(
        Duration::from_millis(500),
        Duration::from_secs(10),
        1.5,
    );

    let node_id = NodeId::new(args.id.clone());
    let resources = ResourceSpec::new(2000, 4096); // Mock resources

    let shutdown_signal = tokio::signal::ctrl_c();
    tokio::pin!(shutdown_signal);

    loop {
        info!(control_addr = %args.control_addr, "Attempting to connect to Control Plane");

        let connect_fut = TcpStream::connect(&args.control_addr);
        let stream_result = tokio::select! {
            res = connect_fut => res,
            _ = &mut shutdown_signal => {
                info!("Shutdown signal received during connection attempt. Exiting.");
                return Ok(());
            }
        };

        match stream_result {
            Ok(mut stream) => {
                info!("Connected successfully. Registering node...");
                backoff.reset(Duration::from_millis(500));

                // --- NodeMessage now uses NodeId, not raw String ---
                let register_msg = NodeMessage::Register {
                    id: node_id.clone(),
                    addr: args.addr.clone(),
                    resources: resources.clone(),
                };

                if let Err(e) = send_msg(&mut stream, &register_msg).await {
                    error!(error = %e, "Failed to send registration message. Retrying...");
                    continue;
                }

                // Channel to funnel messages from concurrent tasks to the single stream writer.
                let (tx, mut rx) = mpsc::channel::<NodeMessage>(50);

                // Spawn mock container workload manager
                let runner_tx = tx.clone();
                let runner_node_id = node_id.clone();
                let runner_resources = resources.clone();
                let runner_handle = tokio::spawn(async move {
                    run_mock_workloads(runner_node_id, runner_resources, runner_tx).await;
                });

                // Spawn periodic heartbeat reporter task.
                // NOTE: Use `tx` directly here (not `tx.clone()`) so the last
                // sender reference is consumed — when both spawned tasks exit,
                // the channel closes cleanly instead of lingering.
                let hb_node_id = node_id.clone();
                let hb_resources = resources.clone();
                let hb_handle = tokio::spawn(async move {
                    let mut interval = tokio::time::interval(Duration::from_secs(2));
                    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
                    loop {
                        interval.tick().await;
                        let hb = Heartbeat {
                            node_id: hb_node_id.clone(),
                            status: NodeStatus::Ready,
                            resources: hb_resources.clone(),
                            timestamp: chrono::Utc::now(),
                        };
                        if tx.send(NodeMessage::Heartbeat(hb)).await.is_err() {
                            break;
                        }
                    }
                });

                // Central message coordinator loop
                let mut graceful_exit = false;
                loop {
                    tokio::select! {
                        _ = &mut shutdown_signal => {
                            info!("Shutdown signal received. Deregistering node gracefully...");
                            graceful_exit = true;
                            let deregister_msg = NodeMessage::Deregister { id: node_id.clone() };
                            let _ = send_msg(&mut stream, &deregister_msg).await;
                            break;
                        }
                        maybe_msg = rx.recv() => {
                            if let Some(msg) = maybe_msg {
                                if let Err(e) = send_msg(&mut stream, &msg).await {
                                    error!(error = %e, "Connection error writing to control plane");
                                    break;
                                }
                            } else {
                                break;
                            }
                        }
                    }
                }

                // Clean up tasks
                runner_handle.abort();
                hb_handle.abort();

                info!(
                    total_messages_sent = MESSAGES_SENT.load(Ordering::Relaxed),
                    "Session ended"
                );

                if graceful_exit {
                    break;
                }
            }
            Err(e) => {
                let retry_delay = backoff.next_backoff();
                warn!(
                    error = %e,
                    retry_ms = retry_delay.as_millis() as u64,
                    "Connection failed. Retrying..."
                );
                tokio::select! {
                    _ = tokio::time::sleep(retry_delay) => {}
                    _ = &mut shutdown_signal => {
                        info!("Shutdown signal received during backoff sleep. Exiting.");
                        return Ok(());
                    }
                }
            }
        }
    }

    info!(
        total_messages_sent = MESSAGES_SENT.load(Ordering::Relaxed),
        "Leviathan Worker Node shut down cleanly."
    );
    Ok(())
}
