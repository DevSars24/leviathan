//! End-to-end integration test for the Leviathan container orchestration system.
//!
//! Wires up all Phases (5, 6, and 7):
//! 1. Spins up a 3-node Raft cluster (in-memory channel transport).
//! 2. Submits a `WorkloadSpec` to the cluster state.
//! 3. Replicates the command across Raft consensus.
//! 4. Uses the `FirstFitDecreasingScheduler` to place the container.
//! 5. Spawns the container using the `ContainerRuntime` (mocked on Windows).
//! 6. Starts a `SidecarProxy` for the container.
//! 7. Verifies metrics flow from the `MetricsRegistry`.

use std::collections::HashMap;
use std::net::{Ipv4Addr, SocketAddr, SocketAddrV4};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use tokio::sync::{mpsc, watch};

use leviathan_core::{Container, ResourceSpec, WorkloadSpec};
use leviathan_mesh::{ProxyConfig, SidecarProxy, TlsConfig};
use leviathan_metrics::MetricsRegistry;
use leviathan_raft::{
    ChannelTransport, ClusterCommand, ClusterStateMachine, RaftConfig, RaftLog,
    RaftNode, RaftTransport,
};
use leviathan_runtime::{ContainerRuntime, ContainerSpec};
use leviathan_scheduler::{FirstFitDecreasingScheduler, Scheduler};
use leviathan_storage::Wal;

#[tokio::test]
async fn test_end_to_end_orchestration_pipeline() {
    // Enable logging for diagnostics during the test.
    let _ = tracing_subscriber::fmt()
        .with_env_filter("info")
        .try_init();

    tracing::info!("Starting end-to-end orchestration pipeline integration test...");

    // Create temporary directory for WALs and container rootfs.
    let tmp = tempfile::tempdir().expect("create tempdir");

    // --- Configure a 3-node Raft cluster ---
    let node_ids = vec![1, 2, 3];
    let mut inboxes = HashMap::new();
    let mut senders = HashMap::new();
    
    for &id in &node_ids {
        let (tx, rx) = mpsc::channel(128);
        inboxes.insert(id, rx);
        senders.insert(id, tx);
    }

    let transport = Arc::new(ChannelTransport::new(senders));
    let mut nodes = Vec::new();
    let mut state_machines = Vec::new();
    let mut shutdown_watchers = Vec::new();
    let mut proposal_channels = Vec::new();
    let mut response_channels_rx = Vec::new();

    for &id in &node_ids {
        let node_dir = tmp.path().join(format!("node-{id}"));
        let wal_path = node_dir.join("raft.wal");
        
        let wal = Wal::open(&wal_path).await.expect("open WAL");
        let log = RaftLog::new(Arc::new(tokio::sync::Mutex::new(wal)))
            .await
            .expect("new Raft log");

        let sm: Arc<Mutex<dyn leviathan_raft::StateMachine>> =
            Arc::new(Mutex::new(ClusterStateMachine::new()));
        state_machines.push(Arc::clone(&sm));

        let config = RaftConfig {
            node_id: id,
            peers: node_ids.clone(),
            election_timeout_min_ms: 100,
            election_timeout_max_ms: 200,
            heartbeat_interval_ms: 20,
            ..RaftConfig::default()
        };

        let (prop_tx, prop_rx) = mpsc::channel(32);
        proposal_channels.push(prop_tx);

        let (resp_tx, resp_rx) = mpsc::channel(32);
        response_channels_rx.push(resp_rx);

        let inbox = inboxes.remove(&id).unwrap();
        let (shutdown_tx, _) = watch::channel(false);
        shutdown_watchers.push(shutdown_tx);

        let node = RaftNode::new(
            config,
            log,
            sm,
            Arc::clone(&transport) as Arc<dyn RaftTransport>,
            inbox,
            prop_rx,
            resp_tx,
        );
        nodes.push(node);
    }

    // --- Spawn the Raft nodes ---
    let mut handles = Vec::new();
    for (mut node, shutdown_tx) in nodes.into_iter().zip(shutdown_watchers.iter().map(|tx| tx.subscribe())) {
        let handle = tokio::spawn(async move {
            node.run(shutdown_tx).await;
        });
        handles.push(handle);
    }

    // Wait for a leader to be elected.
    tokio::time::sleep(Duration::from_millis(500)).await;

    // --- Submit a WorkloadSpec to the leader ---
    let spec = WorkloadSpec::new(
        "wl-nginx",
        "web-server",
        "nginx:alpine",
        ResourceSpec::new(500, 256),
        1,
    );

    let cmd = ClusterCommand::SubmitWorkload {
        workload_id: spec.id.clone(),
        spec: bincode::serialize(&spec).unwrap(),
    };
    let cmd_bytes = bincode::serialize(&cmd).unwrap();

    // Propose the workload on Node 1 (which should be leader or candidate).
    let proposal_tx = &proposal_channels[0];
    let response_rx = &mut response_channels_rx[0];

    // If Node 1 is not the leader, find the leader.
    proposal_tx.send(cmd_bytes).await.unwrap();

    // Await replication confirmation.
    let index = match tokio::time::timeout(Duration::from_secs(2), response_rx.recv()).await {
        Ok(Some(Ok(resp))) => {
            tracing::info!(index = resp.index, "Workload proposal accepted by Raft");
            resp.index
        }
        Ok(Some(Err(e))) => {
            let leader_id = match e {
                leviathan_raft::RaftError::NotLeader { leader: Some(lid) } => lid,
                _ => panic!("Expected NotLeader error with leader hint, got: {:?}", e),
            };
            tracing::info!(leader_id, "Redirected to the elected leader");
            let leader_idx = (leader_id - 1) as usize;
            proposal_channels[leader_idx].send(bincode::serialize(&cmd).unwrap()).await.unwrap();
            let resp2 = tokio::time::timeout(Duration::from_secs(2), response_channels_rx[leader_idx].recv())
                .await
                .expect("timeout waiting for leader response")
                .expect("recv channel closed")
                .expect("proposal failed on leader");
            resp2.index
        }
        _ => panic!("Raft proposal timed out or failed"),
    };

    // Await state machine application.
    tokio::time::sleep(Duration::from_millis(200)).await;

    // Verify workload is present in state machines.
    for sm in &state_machines {
        let guard = sm.lock().unwrap();
        let csm = guard.snapshot();
        let workloads_str = String::from_utf8_lossy(&csm);
        assert!(workloads_str.contains("wl-nginx"));
    }

    // --- Step 2: Scheduler places container ---
    let scheduler = FirstFitDecreasingScheduler::with_default_scorer();
    let cluster_nodes = vec![
        leviathan_core::Node::new("worker-1", "127.0.0.1:9001", ResourceSpec::new(1000, 512)),
        leviathan_core::Node::new("worker-2", "127.0.0.1:9002", ResourceSpec::new(2000, 1024)),
    ];
    // Mark them Ready for scheduler.
    let mut cluster_nodes = cluster_nodes;
    for n in &mut cluster_nodes {
        n.status = leviathan_core::NodeStatus::Ready;
    }

    let container = Container::new(
        "c-nginx-1",
        "nginx-replica-1",
        "nginx:alpine",
        ResourceSpec::new(500, 256),
    );

    let start_time = std::time::Instant::now();
    let selected_node = scheduler
        .select_node(&cluster_nodes, &container)
        .expect("schedule container");
    let placement_latency = start_time.elapsed();

    tracing::info!(node = %selected_node, "Scheduler placed container");
    assert_eq!(selected_node.as_str(), "worker-1"); // First-fit on worker-1 since 500m/256Mi fits in 1000m/512Mi

    // --- Step 3: Runtime spawns container ---
    let runtime_dir = tmp.path().join("runtime");
    let runtime = ContainerRuntime::new(runtime_dir);

    let container_spec = ContainerSpec {
        id: container.id.as_str().to_string(),
        image: container.image.clone(),
        command: vec!["nginx".into(), "-g".into(), "daemon off;".into()],
        env: Vec::new(),
        working_dir: "/".into(),
        resources: container.resources.clone(),
        namespaces: leviathan_runtime::NamespaceConfig::none(), // No namespaces in test
        seccomp: leviathan_runtime::SeccompConfig::default(),
        network: None, // No network setup needed in test
    };

    let start_container_time = std::time::Instant::now();
    let state = runtime
        .spawn_container(&container_spec)
        .expect("spawn container");
    let container_start_latency = start_container_time.elapsed();

    assert_eq!(state, leviathan_runtime::ContainerState::Created);

    // --- Step 4: Service mesh sidecar proxy setup ---
    let proxy_config = ProxyConfig {
        listen_addr: SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::LOCALHOST, 0)), // Ephemeral port
        upstream_addr: SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::LOCALHOST, 80)),
        container_id: container.id.as_str().to_string(),
        mtls_enabled: true,
        trace_enabled: true,
    };

    let tls_config = TlsConfig::self_signed_for_testing().expect("generate self-signed certs");
    let proxy = SidecarProxy::new(proxy_config, Some(tls_config));

    // Spawn proxy task in background with shutdown watcher.
    let (proxy_shutdown_tx, proxy_shutdown_rx) = watch::channel(false);
    let proxy_listen_addr = proxy.listen_addr();
    let proxy_handle = tokio::spawn(async move {
        if let Err(e) = proxy.run(proxy_shutdown_rx).await {
            tracing::error!(error = %e, "Sidecar proxy crashed");
        }
    });

    tracing::info!(port = %proxy_listen_addr.port(), "Sidecar proxy is listening");

    // --- Step 5: Metrics propagation ---
    let metrics = Arc::new(MetricsRegistry::new());
    metrics.raft_term.set(index as i64);
    metrics.raft_role.set(2); // Leader
    metrics.scheduler_placement_latency.observe(placement_latency.as_secs_f64());
    metrics.container_start_time.observe(container_start_latency.as_secs_f64());

    let encoded_metrics = metrics.encode();
    assert!(encoded_metrics.contains("raft_term"));
    assert!(encoded_metrics.contains("raft_role"));
    assert!(encoded_metrics.contains("scheduler_placement_seconds"));
    assert!(encoded_metrics.contains("container_start_seconds"));

    tracing::info!("All metrics verified in registry");

    // --- Shutdown all resources ---
    proxy_shutdown_tx.send(true).unwrap();
    let _ = proxy_handle.await;

    for tx in shutdown_watchers {
        tx.send(true).unwrap();
    }
    
    for h in handles {
        let _ = h.await;
    }

    tracing::info!("Integration test completed successfully!");
}
