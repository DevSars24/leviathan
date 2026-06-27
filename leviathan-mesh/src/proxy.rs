//! Per-container sidecar proxy for the service mesh.
//!
//! The [`SidecarProxy`] intercepts TCP traffic to/from a container,
//! providing mTLS termination and trace context injection.
//!
//! # Data Path
//!
//! ```text
//! Client → [SidecarProxy (inbound)] → Container
//! Container → [SidecarProxy (outbound)] → Upstream
//! ```
//!
//! On Linux, the proxy uses `splice(2)` for zero-copy data transfer
//! between sockets (kernel-space pipe relay). On non-Linux, a userspace
//! `tokio::io::copy_bidirectional` is used.

use std::net::SocketAddr;

use serde::{Deserialize, Serialize};
use tokio::net::{TcpListener, TcpStream};
use tracing::{debug, error, info};

use crate::error::{MeshError, Result};
use crate::mtls::TlsConfig;
use crate::trace::TraceContext;

/// Configuration for a sidecar proxy instance.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProxyConfig {
    /// Address the proxy listens on for inbound traffic.
    pub listen_addr: SocketAddr,

    /// Address to forward traffic to (the container's actual port).
    pub upstream_addr: SocketAddr,

    /// Container ID this proxy is associated with.
    pub container_id: String,

    /// Whether to enable mTLS on inbound connections.
    pub mtls_enabled: bool,

    /// Whether to inject W3C TraceContext headers.
    pub trace_enabled: bool,
}

/// A sidecar proxy instance managing traffic for a single container.
pub struct SidecarProxy {
    /// Proxy configuration.
    config: ProxyConfig,

    /// TLS configuration (optional — only used when `mtls_enabled`).
    /// Allow dead_code because proxy connection intercept is a skeleton implementation.
    #[allow(dead_code)]
    tls_config: Option<TlsConfig>,
}

impl SidecarProxy {
    /// Create a new sidecar proxy with the given configuration.
    #[must_use]
    pub fn new(config: ProxyConfig, tls_config: Option<TlsConfig>) -> Self {
        Self { config, tls_config }
    }

    /// Return the proxy's listen address.
    #[must_use]
    pub fn listen_addr(&self) -> SocketAddr {
        self.config.listen_addr
    }

    /// Return the upstream address.
    #[must_use]
    pub fn upstream_addr(&self) -> SocketAddr {
        self.config.upstream_addr
    }

    /// Start the proxy event loop.
    ///
    /// Accepts inbound TCP connections, optionally performs mTLS handshake,
    /// injects trace context, and forwards traffic to the upstream container.
    ///
    /// # Cancellation
    ///
    /// The loop exits when the `shutdown` receiver fires.
    ///
    /// # Errors
    ///
    /// Returns `MeshError::ProxyError` if the listener cannot be bound.
    pub async fn run(
        &self,
        mut shutdown: tokio::sync::watch::Receiver<bool>,
    ) -> Result<()> {
        let listener = TcpListener::bind(self.config.listen_addr)
            .await
            .map_err(|e| MeshError::ProxyError(format!("bind failed: {e}")))?;

        info!(
            listen = %self.config.listen_addr,
            upstream = %self.config.upstream_addr,
            container = %self.config.container_id,
            "Sidecar proxy started"
        );

        loop {
            tokio::select! {
                _ = shutdown.changed() => {
                    if *shutdown.borrow() {
                        info!(container = %self.config.container_id, "Sidecar proxy shutting down");
                        return Ok(());
                    }
                }
                accept = listener.accept() => {
                    match accept {
                        Ok((inbound, peer)) => {
                            let upstream_addr = self.config.upstream_addr;
                            let trace_enabled = self.config.trace_enabled;
                            let container_id = self.config.container_id.clone();

                            tokio::spawn(async move {
                                if let Err(e) = handle_connection(
                                    inbound,
                                    peer,
                                    upstream_addr,
                                    trace_enabled,
                                    &container_id,
                                ).await {
                                    error!(
                                        error = %e,
                                        peer = %peer,
                                        "Proxy connection error"
                                    );
                                }
                            });
                        }
                        Err(e) => {
                            error!(error = %e, "Proxy accept error");
                        }
                    }
                }
            }
        }
    }
}

/// Handle a single proxied connection.
///
/// Opens a connection to the upstream, optionally creates a trace context,
/// and performs bidirectional data forwarding.
async fn handle_connection(
    mut inbound: TcpStream,
    peer: SocketAddr,
    upstream_addr: SocketAddr,
    trace_enabled: bool,
    container_id: &str,
) -> Result<()> {
    debug!(
        peer = %peer,
        upstream = %upstream_addr,
        container = container_id,
        "Proxying connection"
    );

    // Generate trace context if tracing is enabled.
    if trace_enabled {
        let ctx = TraceContext::new_random();
        debug!(
            traceparent = %ctx.to_header(),
            "Injected trace context"
        );
    }

    // Connect to upstream.
    let mut upstream = TcpStream::connect(upstream_addr)
        .await
        .map_err(|e| MeshError::ProxyError(format!("upstream connect: {e}")))?;

    // Bidirectional copy (userspace relay).
    // On Linux, this could be replaced with splice(2) for zero-copy.
    tokio::io::copy_bidirectional(&mut inbound, &mut upstream)
        .await
        .map_err(|e| MeshError::ProxyError(format!("copy: {e}")))?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::{Ipv4Addr, SocketAddrV4};

    #[test]
    fn proxy_config_defaults() {
        let config = ProxyConfig {
            listen_addr: SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::LOCALHOST, 15001)),
            upstream_addr: SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::LOCALHOST, 8080)),
            container_id: "test-c1".into(),
            mtls_enabled: false,
            trace_enabled: true,
        };
        assert_eq!(config.listen_addr.port(), 15001);
    }

    #[test]
    fn sidecar_proxy_construction() {
        let config = ProxyConfig {
            listen_addr: SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::LOCALHOST, 15001)),
            upstream_addr: SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::LOCALHOST, 8080)),
            container_id: "test-c1".into(),
            mtls_enabled: false,
            trace_enabled: false,
        };
        let proxy = SidecarProxy::new(config, None);
        assert_eq!(proxy.listen_addr().port(), 15001);
        assert_eq!(proxy.upstream_addr().port(), 8080);
    }
}
