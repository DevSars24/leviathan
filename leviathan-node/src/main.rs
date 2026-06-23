//! Leviathan worker node daemon.
//!
//! This binary runs on every machine that participates as a cluster worker.
//! It connects to the control plane, reports node health via heartbeat,
//! and runs simulated workloads.
//!
//! # Protocol Support
//!
//! The node daemon supports two transport modes, selectable via `--protocol`:
//!
//! - **`tcp`** (default) — Length-prefixed bincode frames over raw TCP.
//!   Compact, fast, zero external dependencies on the wire.
//! - **`grpc`** — Protobuf over HTTP/2 via `tonic`. Cross-language
//!   interop, streaming, and TLS support out of the box.
//!
//! Both modes carry the same logical messages: Register, Heartbeat, Deregister.

use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use clap::{Parser, ValueEnum};
use tokio::net::TcpStream;
use tokio::sync::mpsc;
use tracing::{error, info, warn};

use leviathan_core::{
    CooperativeYield, ExponentialBackoff, Heartbeat, NodeId, NodeMessage, NodeStatus, ResourceSpec,
};
use leviathan_net::codec;
use leviathan_net::error::NetError;
use leviathan_net::grpc::{
    NodeServiceClient, RegisterNodeRequest, HeartbeatRequest, DeregisterNodeRequest, ResourceInfo,
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

    /// Control plane server address (TCP).
    #[arg(long, default_value = "127.0.0.1:8000")]
    control_addr: String,

    /// Control plane gRPC address (only used when --protocol grpc).
    #[arg(long, default_value = "http://127.0.0.1:50051")]
    grpc_addr: String,

    /// Transport protocol to use for control plane communication.
    #[arg(long, value_enum, default_value_t = Protocol::Tcp)]
    protocol: Protocol,
}

/// Supported transport protocols.
#[derive(Debug, Clone, ValueEnum)]
enum Protocol {
    /// Length-prefixed bincode frames over raw TCP.
    Tcp,
    /// Protobuf over HTTP/2 via tonic gRPC.
    Grpc,
}

/// Counter tracking total messages sent to the control plane for observability.
static MESSAGES_SENT: AtomicU64 = AtomicU64::new(0);

// ---------------------------------------------------------------------------
// TCP transport — bincode-framed messaging
// ---------------------------------------------------------------------------

/// Send a [`NodeMessage`] over TCP using length-prefixed bincode framing.
///
/// Replaces the previous newline-delimited JSON approach. Each message is now
/// a 4-byte big-endian length header followed by bincode-serialized bytes.
#[tracing::instrument(skip(stream, msg), fields(msg_type = ?std::mem::discriminant(msg)))]
async fn send_msg_tcp(stream: &mut TcpStream, msg: &NodeMessage) -> Result<(), NetError> {
    codec::send_message(stream, msg).await?;
    MESSAGES_SENT.fetch_add(1, Ordering::Relaxed);
    Ok(())
}

// ---------------------------------------------------------------------------
// Cooperative computation (carried over from Phase 2)
// ---------------------------------------------------------------------------

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
async fn run_mock_workloads(
    node_id: NodeId,
    resources: ResourceSpec,
    tx: mpsc::Sender<NodeMessage>,
) {
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

// ---------------------------------------------------------------------------
// TCP connection loop
// ---------------------------------------------------------------------------

/// Run the TCP-mode connection loop.
///
/// Connects to the control plane using length-prefixed bincode framing,
/// registers, spawns heartbeat + workload tasks, and forwards messages
/// through a central coordinator loop.
///
/// Handles connection resets and partial reads via the `leviathan-net`
/// frame layer.
async fn run_tcp_mode(args: &Args, node_id: &NodeId, resources: &ResourceSpec) -> anyhow::Result<()> {
    let mut backoff = ExponentialBackoff::new(
        Duration::from_millis(500),
        Duration::from_secs(10),
        1.5,
    );

    let shutdown_signal = tokio::signal::ctrl_c();
    tokio::pin!(shutdown_signal);

    loop {
        info!(control_addr = %args.control_addr, "TCP: Attempting to connect to Control Plane");

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
                info!("TCP: Connected successfully. Registering node (bincode framing)...");
                backoff.reset(Duration::from_millis(500));

                let register_msg = NodeMessage::Register {
                    id: node_id.clone(),
                    addr: args.addr.clone(),
                    resources: resources.clone(),
                };

                if let Err(e) = send_msg_tcp(&mut stream, &register_msg).await {
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
                            let _ = send_msg_tcp(&mut stream, &deregister_msg).await;
                            break;
                        }
                        maybe_msg = rx.recv() => {
                            if let Some(msg) = maybe_msg {
                                match send_msg_tcp(&mut stream, &msg).await {
                                    Ok(()) => {}
                                    Err(NetError::ConnectionReset(reason)) => {
                                        warn!(reason = %reason, "Connection reset by control plane");
                                        break;
                                    }
                                    Err(e) => {
                                        error!(error = %e, "Connection error writing to control plane");
                                        break;
                                    }
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
                    "TCP session ended"
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

    Ok(())
}

// ---------------------------------------------------------------------------
// gRPC connection loop
// ---------------------------------------------------------------------------

/// Run the gRPC-mode connection loop.
///
/// Connects to the control plane's gRPC endpoint, registers, then sends
/// periodic heartbeats via the `NodeService` RPC interface. On shutdown,
/// sends a `DeregisterNode` RPC.
async fn run_grpc_mode(args: &Args, node_id: &NodeId, resources: &ResourceSpec) -> anyhow::Result<()> {
    let mut backoff = ExponentialBackoff::new(
        Duration::from_millis(500),
        Duration::from_secs(10),
        1.5,
    );

    let shutdown_signal = tokio::signal::ctrl_c();
    tokio::pin!(shutdown_signal);

    loop {
        info!(grpc_addr = %args.grpc_addr, "gRPC: Attempting to connect to Control Plane");

        let connect_result = tokio::select! {
            res = NodeServiceClient::connect(args.grpc_addr.clone()) => res,
            _ = &mut shutdown_signal => {
                info!("Shutdown signal received during gRPC connection. Exiting.");
                return Ok(());
            }
        };

        match connect_result {
            Ok(mut client) => {
                info!("gRPC: Connected successfully. Registering node...");
                backoff.reset(Duration::from_millis(500));

                // --- Register ---
                let register_req = RegisterNodeRequest {
                    node_id: node_id.as_str().to_string(),
                    addr: args.addr.clone(),
                    resources: Some(ResourceInfo {
                        cpu_millicores: resources.cpu_millicores,
                        memory_mib: resources.memory_mib,
                    }),
                };

                match client.register_node(register_req).await {
                    Ok(resp) => {
                        let inner = resp.into_inner();
                        info!(
                            accepted = inner.accepted,
                            message = %inner.message,
                            "gRPC: Node registered"
                        );
                        MESSAGES_SENT.fetch_add(1, Ordering::Relaxed);
                    }
                    Err(e) => {
                        error!(error = %e, "gRPC: Registration failed. Retrying...");
                        continue;
                    }
                }

                // --- Heartbeat loop ---
                let mut interval = tokio::time::interval(Duration::from_secs(2));
                interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

                let mut graceful_exit = false;

                loop {
                    tokio::select! {
                        _ = &mut shutdown_signal => {
                            info!("Shutdown signal received. Deregistering via gRPC...");
                            graceful_exit = true;

                            let deregister_req = DeregisterNodeRequest {
                                node_id: node_id.as_str().to_string(),
                            };
                            match client.deregister_node(deregister_req).await {
                                Ok(_) => info!("gRPC: Node deregistered successfully."),
                                Err(e) => warn!(error = %e, "gRPC: Deregistration failed (best-effort)"),
                            }
                            MESSAGES_SENT.fetch_add(1, Ordering::Relaxed);
                            break;
                        }
                        _ = interval.tick() => {
                            let hb_req = HeartbeatRequest {
                                node_id: node_id.as_str().to_string(),
                                status: "Ready".to_string(),
                                resources: Some(ResourceInfo {
                                    cpu_millicores: resources.cpu_millicores,
                                    memory_mib: resources.memory_mib,
                                }),
                                timestamp: chrono::Utc::now().to_rfc3339(),
                            };

                            match client.send_heartbeat(hb_req).await {
                                Ok(_) => {
                                    MESSAGES_SENT.fetch_add(1, Ordering::Relaxed);
                                    tracing::debug!("gRPC: Heartbeat acknowledged");
                                }
                                Err(e) => {
                                    error!(error = %e, "gRPC: Heartbeat failed. Connection may be lost.");
                                    break;
                                }
                            }
                        }
                    }
                }

                info!(
                    total_messages_sent = MESSAGES_SENT.load(Ordering::Relaxed),
                    "gRPC session ended"
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
                    "gRPC connection failed. Retrying..."
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

    Ok(())
}

// ---------------------------------------------------------------------------
// Entry point
// ---------------------------------------------------------------------------

#[tokio::main]
async fn main() -> anyhow::Result<()> {
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
        protocol = ?args.protocol,
        "Initializing Leviathan Worker Node Daemon"
    );

    let node_id = NodeId::new(args.id.clone());
    let resources = ResourceSpec::new(2000, 4096); // Mock resources

    match args.protocol {
        Protocol::Tcp => run_tcp_mode(&args, &node_id, &resources).await?,
        Protocol::Grpc => run_grpc_mode(&args, &node_id, &resources).await?,
    }

    info!(
        total_messages_sent = MESSAGES_SENT.load(Ordering::Relaxed),
        "Leviathan Worker Node shut down cleanly."
    );
    Ok(())
}
