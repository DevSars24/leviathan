//! # leviathan-metrics
//!
//! Prometheus metrics and observability for the Leviathan platform.
//!
//! Exposes a `/metrics` endpoint via `hyper` serving metrics in the
//! Prometheus text exposition format via `prometheus-client`.
//!
//! ## Instrumented Metrics
//!
//! | Name | Type | Description |
//! |------|------|-------------|
//! | `raft_heartbeat_latency_seconds` | Histogram | Leader heartbeat round-trip latency |
//! | `container_start_seconds` | Histogram | Time from schedule to running |
//! | `scheduler_placement_seconds` | Histogram | Scheduler placement decision time |
//! | `cgroup_memory_pressure_total` | Counter | Memory pressure events from cgroups |
//! | `raft_term` | Gauge | Current Raft term |
//! | `raft_role` | Gauge | Current Raft role (0=follower, 1=candidate, 2=leader) |

#![warn(missing_docs)]

use std::net::SocketAddr;
use std::sync::Arc;

use http_body_util::Full;
use hyper::body::Bytes;
use hyper::server::conn::http1;
use hyper::service::service_fn;
use hyper::{Request, Response};
use hyper_util::rt::TokioIo;
use prometheus_client::encoding::text::encode;
use prometheus_client::metrics::counter::Counter;
use prometheus_client::metrics::gauge::Gauge;
use prometheus_client::metrics::histogram::{exponential_buckets, Histogram};
use prometheus_client::registry::Registry;
use tokio::net::TcpListener;
use tracing::{error, info};

/// The central metrics registry for the Leviathan platform.
///
/// Holds all registered metrics and provides the serialization
/// endpoint for Prometheus scraping.
pub struct MetricsRegistry {
    /// The `prometheus-client` registry.
    registry: Registry,

    /// Raft heartbeat latency histogram.
    pub raft_heartbeat_latency: Histogram,

    /// Container start time histogram.
    pub container_start_time: Histogram,

    /// Scheduler placement latency histogram.
    pub scheduler_placement_latency: Histogram,

    /// cgroup memory pressure event counter.
    pub cgroup_memory_pressure: Counter,

    /// Current Raft term gauge.
    pub raft_term: Gauge,

    /// Current Raft role gauge (0=follower, 1=candidate, 2=leader).
    pub raft_role: Gauge,
}

impl MetricsRegistry {
    /// Create a new metrics registry with all metrics registered.
    #[must_use]
    pub fn new() -> Self {
        let mut registry = Registry::default();

        let raft_heartbeat_latency =
            Histogram::new(exponential_buckets(0.001, 2.0, 12));
        registry.register(
            "raft_heartbeat_latency_seconds",
            "Raft leader heartbeat round-trip latency in seconds",
            raft_heartbeat_latency.clone(),
        );

        let container_start_time =
            Histogram::new(exponential_buckets(0.01, 2.0, 12));
        registry.register(
            "container_start_seconds",
            "Time from scheduling to container running in seconds",
            container_start_time.clone(),
        );

        let scheduler_placement_latency =
            Histogram::new(exponential_buckets(0.0001, 2.0, 12));
        registry.register(
            "scheduler_placement_seconds",
            "Scheduler placement decision latency in seconds",
            scheduler_placement_latency.clone(),
        );

        let cgroup_memory_pressure = Counter::default();
        registry.register(
            "cgroup_memory_pressure_total",
            "Number of cgroup memory pressure events",
            cgroup_memory_pressure.clone(),
        );

        let raft_term = Gauge::<i64, _>::default();
        registry.register(
            "raft_term",
            "Current Raft consensus term",
            raft_term.clone(),
        );

        let raft_role = Gauge::<i64, _>::default();
        registry.register(
            "raft_role",
            "Current Raft role (0=follower, 1=candidate, 2=leader)",
            raft_role.clone(),
        );

        Self {
            registry,
            raft_heartbeat_latency,
            container_start_time,
            scheduler_placement_latency,
            cgroup_memory_pressure,
            raft_term,
            raft_role,
        }
    }

    /// Encode all metrics as Prometheus text exposition format.
    #[must_use]
    pub fn encode(&self) -> String {
        let mut buf = String::new();
        encode(&mut buf, &self.registry).unwrap_or_else(|e| {
            error!(error = %e, "Failed to encode metrics");
        });
        buf
    }
}

impl Default for MetricsRegistry {
    fn default() -> Self {
        Self::new()
    }
}

/// Start the Prometheus `/metrics` HTTP server.
///
/// Listens on `addr` and serves metrics from the given registry.
/// Runs until the `shutdown` watch channel is signalled.
///
/// # Errors
///
/// Returns an I/O error if the listener cannot be bound.
pub async fn serve_metrics(
    addr: SocketAddr,
    registry: Arc<MetricsRegistry>,
    mut shutdown: tokio::sync::watch::Receiver<bool>,
) -> std::io::Result<()> {
    let listener = TcpListener::bind(addr).await?;
    info!(addr = %addr, "Metrics server started on /metrics");

    loop {
        tokio::select! {
            _ = shutdown.changed() => {
                if *shutdown.borrow() {
                    info!("Metrics server shutting down");
                    return Ok(());
                }
            }
            accept = listener.accept() => {
                match accept {
                    Ok((stream, _peer)) => {
                        let reg = Arc::clone(&registry);
                        tokio::spawn(async move {
                            let io = TokioIo::new(stream);
                            let svc = service_fn(move |req: Request<hyper::body::Incoming>| {
                                let reg = Arc::clone(&reg);
                                async move {
                                    handle_request(req, &reg)
                                }
                            });
                            if let Err(e) = http1::Builder::new()
                                .serve_connection(io, svc)
                                .await
                            {
                                error!(error = %e, "HTTP connection error");
                            }
                        });
                    }
                    Err(e) => {
                        error!(error = %e, "Metrics accept error");
                    }
                }
            }
        }
    }
}

/// Handle a single HTTP request to the metrics server.
fn handle_request(
    req: Request<hyper::body::Incoming>,
    registry: &MetricsRegistry,
) -> std::result::Result<Response<Full<Bytes>>, hyper::Error> {
    match req.uri().path() {
        "/metrics" => {
            let body = registry.encode();
            Ok(Response::builder()
                .status(200)
                .header("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
                .body(Full::new(Bytes::from(body)))
                .unwrap_or_else(|_| {
                    Response::new(Full::new(Bytes::from("internal error")))
                }))
        }
        "/health" => Ok(Response::builder()
            .status(200)
            .body(Full::new(Bytes::from("ok")))
            .unwrap_or_else(|_| Response::new(Full::new(Bytes::from("ok"))))),
        _ => Ok(Response::builder()
            .status(404)
            .body(Full::new(Bytes::from("not found")))
            .unwrap_or_else(|_| Response::new(Full::new(Bytes::from("not found"))))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn metrics_registry_creation() {
        let registry = MetricsRegistry::new();
        let output = registry.encode();
        // Should contain our registered metric names.
        assert!(output.contains("raft_heartbeat_latency_seconds"));
        assert!(output.contains("container_start_seconds"));
        assert!(output.contains("scheduler_placement_seconds"));
        assert!(output.contains("cgroup_memory_pressure_total"));
    }

    #[test]
    fn record_histogram() {
        let registry = MetricsRegistry::new();
        registry.raft_heartbeat_latency.observe(0.005);
        registry.raft_heartbeat_latency.observe(0.010);

        let output = registry.encode();
        assert!(output.contains("raft_heartbeat_latency_seconds"));
    }

    #[test]
    fn increment_counter() {
        let registry = MetricsRegistry::new();
        registry.cgroup_memory_pressure.inc();
        registry.cgroup_memory_pressure.inc();

        let output = registry.encode();
        assert!(output.contains("cgroup_memory_pressure_total"));
    }

    #[test]
    fn set_gauge() {
        let registry = MetricsRegistry::new();
        registry.raft_term.set(42);
        registry.raft_role.set(2); // leader

        let output = registry.encode();
        assert!(output.contains("raft_term"));
        assert!(output.contains("raft_role"));
    }

    #[tokio::test]
    async fn handle_metrics_endpoint() {
        let registry = MetricsRegistry::new();
        // Test via encode() to verify registration.
        let body = registry.encode();
        assert!(!body.is_empty());
    }
}
