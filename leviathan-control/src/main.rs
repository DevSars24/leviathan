//! Leviathan control plane daemon.
//!
//! The control plane maintains authoritative cluster state — the registry of
//! nodes, containers, and their current status. It drives reconciliation,
//! delegates scheduling decisions to `leviathan-scheduler`, and exposes an
//! API for the CLI to query.

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use chrono::{DateTime, Utc};
use clap::Parser;
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{mpsc, oneshot, watch};
use tracing::{error, info, warn};

use leviathan_core::{
    Heartbeat, LeviathanError, Node, NodeId, NodeMessage, NodeStatus, Reconcile,
};

/// CLI arguments for the control plane daemon.
#[derive(Parser, Debug)]
#[command(
    name = "leviathan-control",
    about = "Leviathan control plane daemon"
)]
struct Args {
    /// Address to bind the TCP listener on.
    #[arg(long, default_value = "127.0.0.1:8000")]
    bind_addr: String,

    /// Grace period (in seconds) before a node is marked Unknown for missing
    /// heartbeats.
    #[arg(long, default_value_t = 6)]
    grace_period_secs: u64,
}

/// Counter tracking total inbound node connections for observability.
static CONNECTIONS_ACCEPTED: AtomicU64 = AtomicU64::new(0);

/// Commands that can be sent to the [`ClusterStateActor`].
#[derive(Debug)]
pub enum StateCommand {
    /// Register a new node or update its address.
    RegisterNode {
        /// The node to register.
        node: Node,
    },
    /// Record a heartbeat received from a node.
    RecordHeartbeat {
        /// The heartbeat data.
        heartbeat: Heartbeat,
    },
    /// Mark a node as Deregistered (Unknown/Offline).
    DeregisterNode {
        /// ID of the node to deregister.
        id: NodeId,
    },
    /// Retrieve all registered nodes.
    GetNodes {
        /// Oneshot channel to receive the response.
        resp: oneshot::Sender<Vec<Node>>,
    },
    /// Run liveness reconciliation checks on all registered nodes.
    ReconcileLiveness {
        /// How long a node can go without a heartbeat before being marked Unknown.
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
                        info!(
                            registered_nodes = self.nodes.len(),
                            "Cluster State Actor received shutdown signal. Exiting."
                        );
                        break;
                    }
                }
                Some(cmd) = self.receiver.recv() => {
                    self.handle_command(cmd);
                }
            }
        }
    }

    /// Dispatch a single [`StateCommand`] — extracted from the `run` loop for
    /// readability and testability.
    fn handle_command(&mut self, cmd: StateCommand) {
        match cmd {
            StateCommand::RegisterNode { node } => {
                info!(node_id = %node.id, addr = %node.addr, "Registering node");
                let mut registered = node;
                registered.status = NodeStatus::Ready;
                self.nodes
                    .insert(registered.id.clone(), (registered, Utc::now()));
            }
            StateCommand::RecordHeartbeat { heartbeat } => {
                if let Some((node, last_seen)) = self.nodes.get_mut(&heartbeat.node_id) {
                    node.status = NodeStatus::Ready;
                    node.resources = heartbeat.resources;
                    *last_seen = heartbeat.timestamp;
                    tracing::debug!(node_id = %heartbeat.node_id, "Heartbeat recorded");
                } else {
                    warn!(
                        node_id = %heartbeat.node_id,
                        "Heartbeat from unregistered node. Auto-registering."
                    );
                    let node = Node {
                        id: heartbeat.node_id.clone(),
                        addr: "unknown".to_string(),
                        status: NodeStatus::Ready,
                        resources: heartbeat.resources,
                    };
                    self.nodes
                        .insert(heartbeat.node_id, (node, Utc::now()));
                }
            }
            StateCommand::DeregisterNode { id } => {
                if let Some((node, _)) = self.nodes.get_mut(&id) {
                    node.status = NodeStatus::Unknown;
                    info!(node_id = %id, "Node deregistered gracefully. Marked as Unknown.");
                }
            }
            StateCommand::GetNodes { resp } => {
                let list: Vec<Node> = self
                    .nodes
                    .values()
                    .map(|(node, _)| node.clone())
                    .collect();
                let _ = resp.send(list);
            }
            StateCommand::ReconcileLiveness { grace_period } => {
                let now = Utc::now();
                for (id, (node, last_seen)) in self.nodes.iter_mut() {
                    if node.status == NodeStatus::Ready {
                        let elapsed = now
                            .signed_duration_since(*last_seen)
                            .to_std()
                            .unwrap_or_default();
                        if elapsed > grace_period {
                            node.status = NodeStatus::Unknown;
                            warn!(
                                node_id = %id,
                                last_seen_secs_ago = elapsed.as_secs(),
                                "Node missed heartbeats. Marking as Unknown."
                            );
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
            .map_err(|e| {
                LeviathanError::Internal(format!("Failed to send reconcile command: {}", e))
            })?;
        Ok(())
    }
}

/// Handle an individual worker node connection stream.
#[tracing::instrument(skip(stream, tx), fields(peer = %peer_addr))]
async fn handle_node_connection(
    stream: TcpStream,
    peer_addr: SocketAddr,
    tx: mpsc::Sender<StateCommand>,
) {
    info!("New node connection established");
    let mut reader = BufReader::new(stream);
    let mut line = String::new();

    loop {
        line.clear();
        match reader.read_line(&mut line).await {
            Ok(0) => {
                info!("Connection closed by remote node");
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
                            let mut node = Node::new(id.as_str(), addr, resources);
                            // If registration gives "local" or empty address,
                            // fallback to TCP peer address.
                            if node.addr.is_empty() || node.addr.contains("0.0.0.0") {
                                node.addr =
                                    format!("{}:{}", peer_addr.ip(), peer_addr.port());
                            }
                            let _ = tx.send(StateCommand::RegisterNode { node }).await;
                        }
                        NodeMessage::Heartbeat(hb) => {
                            let _ = tx
                                .send(StateCommand::RecordHeartbeat { heartbeat: hb })
                                .await;
                        }
                        NodeMessage::Deregister { id } => {
                            let _ = tx
                                .send(StateCommand::DeregisterNode { id })
                                .await;
                            break; // Stop connection handler on deregistration
                        }
                    },
                    Err(e) => {
                        error!(
                            error = %e,
                            raw_data = trimmed,
                            "Failed to parse node message"
                        );
                    }
                }
            }
            Err(e) => {
                error!(error = %e, "Error reading node connection");
                break;
            }
        }
    }
}

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
        bind_addr = %args.bind_addr,
        grace_period_secs = args.grace_period_secs,
        "Initializing Leviathan Control Plane Daemon"
    );

    let listener = TcpListener::bind(&args.bind_addr).await?;
    info!(addr = %args.bind_addr, "Control plane listening for heartbeats");

    let grace_period = Duration::from_secs(args.grace_period_secs);

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
            grace_period,
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
                        error!(error = %e, "Reconciliation pass failed");
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
                            CONNECTIONS_ACCEPTED.fetch_add(1, Ordering::Relaxed);
                            let tx_clone = listener_tx.clone();
                            tokio::spawn(async move {
                                handle_node_connection(stream, peer_addr, tx_clone).await;
                            });
                        }
                        Err(e) => {
                            error!(error = %e, "TCP accept error");
                        }
                    }
                }
            }
        }
    });

    // Wait for shutdown signal (Ctrl+C)
    tokio::signal::ctrl_c().await?;
    info!(
        total_connections = CONNECTIONS_ACCEPTED.load(Ordering::Relaxed),
        "Shutting down gracefully..."
    );

    // Trigger cancellation
    let _ = shutdown_tx.send(true);

    // Wait for tasks to complete
    let _ = tokio::join!(actor_handle, reconciler_handle, listener_handle);
    info!("Leviathan Control Plane shut down completely.");

    Ok(())
}
