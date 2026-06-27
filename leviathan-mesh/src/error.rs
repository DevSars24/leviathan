//! Service mesh error types.

use thiserror::Error;

/// Error enum for the service mesh subsystem.
#[derive(Debug, Error)]
pub enum MeshError {
    /// TLS configuration or handshake failure.
    #[error("TLS error: {0}")]
    TlsError(String),

    /// Proxy connection or forwarding failure.
    #[error("proxy error: {0}")]
    ProxyError(String),

    /// Invalid trace context header.
    #[error("trace context error: {0}")]
    TraceContextError(String),

    /// I/O error in proxy operation.
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    /// Certificate generation error.
    #[error("certificate error: {0}")]
    CertificateError(String),
}

/// Convenience result alias.
pub type Result<T> = std::result::Result<T, MeshError>;

impl From<MeshError> for leviathan_core::LeviathanError {
    fn from(e: MeshError) -> Self {
        leviathan_core::LeviathanError::Network(e.to_string())
    }
}
