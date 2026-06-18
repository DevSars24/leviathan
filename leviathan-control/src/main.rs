//! Leviathan control plane daemon.
//!
//! The control plane maintains authoritative cluster state — the registry of
//! nodes, containers, and their current status. It drives reconciliation,
//! delegates scheduling decisions to `leviathan-scheduler`, and exposes an
//! API for the CLI to query.

use std::collections::HashMap;
use std::net::SocketAddr;
use std::time::Duration;
use chrono::{DateTime, Utc};
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{mpsc, oneshot, watch};
use tracing::{error, info, warn};

use leviathan_core::{
    Heartbeat, LeviathanError, Node, NodeId, NodeMessage, NodeStatus, Reconcile,
};

/// Commands that can be sent to the ClusterStateActor.
#[derive(Debug)]
pub enum StateCommand {
    /// Register a new node or update its address.
    RegisterNode {
        node: Node,
    },
    /// Record a heartbeat received from a node.
    RecordHeartbeat {
        heartbeat: Heartbeat,
    },
    /// Mark a node as Deregistered (Unknown/Offline).
    DeregisterNode {
        id: NodeId,
    },
    /// Retrieve all registered nodes.
    GetNodes {
        resp: oneshot::Sender<Vec<Node>>,
    },
    /// Run liveness reconciliation checks on all registered nodes.
    ReconcileLiveness {
        grace_period: Duration,
    },
}

/// The centralized actor that owns and manages the cluster state.
///
/// This avoids lock contention and race conditions by funneling all state
/// updates and queries through a single message-passing actor loop.
struct ClusterStateActor {
    nodes: HashMap<NodeId, (Node, DateTime<Utc>)>,
    receiver: mpsc::Receiver<StateCommand>,
}

impl ClusterStateActor {
    fn new(receiver: mpsc::Receiver<StateCommand>) -> Self {
        Self {
            nodes: HashMap::new(),
            receiver,
        }
    }

    async fn run(mut self, mut shutdown: watch::Receiver<bool>) {
        info!("Cluster State Actor started.");
        loop {
            tokio::select! {
                _ = shutdown.changed() => {
                    if *shutdown.borrow() {
                        info!("Cluster State Actor received shutdown signal. Exiting.");
                        break;
                    }
                }
                Some(cmd) = self.receiver.recv() => {
                    match cmd {
                        StateCommand::RegisterNode { node } => {
                            info!("Registering node: {} at {}", node.id, node.addr);
                            let mut registered = node;
                            registered.status = NodeStatus::Ready;
                            self.nodes.insert(registered.id.clone(), (registered, Utc::now()));
                        }
                        StateCommand::RecordHeartbeat { heartbeat } => {
                            if let Some((node, last_seen)) = self.nodes.get_mut(&heartbeat.node_id) {
                                node.status = NodeStatus::Ready;
                                node.resources = heartbeat.resources;
                                *last_seen = heartbeat.timestamp;
                                tracing::debug!("Heartbeat recorded for node: {}", heartbeat.node_id);
                            } else {
                                warn!("Heartbeat received from unregistered node: {}. Registering now.", heartbeat.node_id);
                                let node = Node {
                                    id: heartbeat.node_id.clone(),
                                    addr: "unknown".to_string(), // Will be updated on next connection
                                    status: NodeStatus::Ready,
                                    resources: heartbeat.resources,
                                };
                                self.nodes.insert(heartbeat.node_id, (node, Utc::now()));
                            }
                        }
                        StateCommand::DeregisterNode { id } => {
                            if let Some((node, _)) = self.nodes.get_mut(&id) {
                                node.status = NodeStatus::Unknown;
                                info!("Node {} deregistered gracefully. Marked status as Unknown.", id);
                            }
                        }
                        StateCommand::GetNodes { resp } => {
                            let list: Vec<Node> = self.nodes.values().map(|(node, _)| node.clone()).collect();
                            let _ = resp.send(list);
                        }
                        StateCommand::ReconcileLiveness { grace_period } => {
                            let now = Utc::now();
                            for (id, (node, last_seen)) in self.nodes.iter_mut() {
                                if node.status == NodeStatus::Ready {
                                    let elapsed = now.signed_duration_since(*last_seen).to_std().unwrap_or_default();
                                    if elapsed > grace_period {
                                        node.status = NodeStatus::Unknown;
                                        warn!(
                                            "Node {} missed heartbeats (last seen {}s ago). Marking as Unknown.",
                                            id,
                                            elapsed.as_secs()
                                        );
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    }
}

/// Reconciliation worker that invokes liveness updates on the actor.
struct LivenessReconciler {
    sender: mpsc::Sender<StateCommand>,
    grace_period: Duration,
}

#[async_trait::async_trait]
impl Reconcile for LivenessReconciler {
    async fn reconcile(&mut self) -> Result<(), LeviathanError> {
        self.sender
            .send(StateCommand::ReconcileLiveness {
                grace_period: self.grace_period,
            })
            .await
            .map_err(|e| LeviathanError::Internal(format!("Failed to send reconcile command: {}", e)))?;
        Ok(())
    }
}

/// Handle an individual worker node connection stream.
async fn handle_node_connection(stream: TcpStream, peer_addr: SocketAddr, tx: mpsc::Sender<StateCommand>) {
    info!("New node connection established from {}", peer_addr);
    let mut reader = BufReader::new(stream);
    let mut line = String::new();

    loop {
        line.clear();
        match reader.read_line(&mut line).await {
            Ok(0) => {
                info!("Connection closed by remote node {}", peer_addr);
                break;
            }
            Ok(_) => {
                let trimmed = line.trim();
                if trimmed.is_empty() {
                    continue;
                }
                match serde_json::from_str::<NodeMessage>(trimmed) {
                    Ok(message) => match message {
                        NodeMessage::Register { id, addr, resources } => {
                            let mut node = Node::new(id, addr, resources);
                            // If registration gives "local" or empty address, fallback to TCP peer address
                            if node.addr.is_empty() || node.addr.contains("0.0.0.0") {
                                node.addr = format!("{}:{}", peer_addr.ip(), peer_addr.port());
                            }
                            let _ = tx.send(StateCommand::RegisterNode { node }).await;
                        }
                        NodeMessage::Heartbeat(hb) => {
                            let _ = tx.send(StateCommand::RecordHeartbeat { heartbeat: hb }).await;
                        }
                        NodeMessage::Deregister { id } => {
                            let _ = tx.send(StateCommand::DeregisterNode { id: NodeId::new(id) }).await;
                            break; // Stop connection handler on deregistration
                        }
                    },
                    Err(e) => {
                        error!("Failed to parse node message: {}. Raw data: {:?}", e, trimmed);
                    }
                }
            }
            Err(e) => {
                error!("Error reading node connection from {}: {}", peer_addr, e);
                break;
            }
        }
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Initialize subscriber for logging
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();

    info!("Initializing Leviathan Control Plane Daemon...");

    let bind_addr = "127.0.0.1:8000";
    let listener = TcpListener::bind(bind_addr).await?;
    info!("Control plane listening for heartbeats on: {}", bind_addr);

    // Setup channels
    let (tx, rx) = mpsc::channel(100);
    let (shutdown_tx, shutdown_rx) = watch::channel(false);

    // Spawn state actor task
    let actor = ClusterStateActor::new(rx);
    let actor_shutdown_rx = shutdown_rx.clone();
    let actor_handle = tokio::spawn(async move {
        actor.run(actor_shutdown_rx).await;
    });

    // Spawn reconciler task
    let reconciler_shutdown_rx = shutdown_rx.clone();
    let reconciler_tx = tx.clone();
    let reconciler_handle = tokio::spawn(async move {
        let mut reconciler = LivenessReconciler {
            sender: reconciler_tx,
            grace_period: Duration::from_secs(6),
        };
        let mut interval = tokio::time::interval(Duration::from_secs(2));
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

        let mut shutdown = reconciler_shutdown_rx;
        loop {
            tokio::select! {
                _ = shutdown.changed() => {
                    if *shutdown.borrow() {
                        info!("Reconciler task received shutdown. Exiting.");
                        break;
                    }
                }
                _ = interval.tick() => {
                    if let Err(e) = reconciler.reconcile().await {
                        error!("Reconciliation pass failed: {}", e);
                    }
                }
            }
        }
    });

    // Spawn connection listener loop
    let listener_tx = tx.clone();
    let listener_shutdown_rx = shutdown_rx.clone();
    let listener_handle = tokio::spawn(async move {
        let mut shutdown = listener_shutdown_rx;
        loop {
            tokio::select! {
                _ = shutdown.changed() => {
                    if *shutdown.borrow() {
                        info!("TCP listener loop received shutdown. Exiting.");
                        break;
                    }
                }
                accept_result = listener.accept() => {
                    match accept_result {
                        Ok((stream, peer_addr)) => {
                            let tx_clone = listener_tx.clone();
                            tokio::spawn(async move {
                                handle_node_connection(stream, peer_addr, tx_clone).await;
                            });
                        }
                        Err(e) => {
                            error!("TCP accept error: {}", e);
                        }
                    }
                }
            }
        }
    });

    // Wait for shutdown signal (Ctrl+C)
    tokio::signal::ctrl_c().await?;
    info!("Shutting down gracefully...");

    // Trigger cancellation
    let _ = shutdown_tx.send(true);

    // Wait for tasks to complete
    let _ = tokio::join!(actor_handle, reconciler_handle, listener_handle);
    info!("Leviathan Control Plane shut down completely.");

    Ok(())
}
