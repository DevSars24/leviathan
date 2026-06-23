//! Network-layer error types for Leviathan.
//!
//! [`NetError`] captures every failure mode that can occur at the transport
//! layer: I/O failures, serialization corruption, connection resets, and
//! protocol violations like oversized frames. It converts cleanly into
//! [`leviathan_core::LeviathanError::Network`] for propagation up the stack.

use thiserror::Error;

/// Maximum frame payload size (16 MiB). Frames exceeding this are rejected
/// to prevent OOM from malicious or corrupted streams.
pub const MAX_FRAME_SIZE: u32 = 16 * 1024 * 1024;

/// Transport-layer error enum.
///
/// Each variant carries enough context to produce a meaningful log line
/// without requiring a backtrace in production.
#[derive(Debug, Error)]
pub enum NetError {
    /// An underlying I/O error from the TCP stack.
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    /// `bincode` serialization or deserialization failed.
    #[error("serialization error: {0}")]
    Serialization(String),

    /// The remote peer closed the connection or the OS reported a reset.
    ///
    /// This is **not** an error in the traditional sense — it's expected
    /// during graceful shutdown and network partitions. Callers should
    /// log at `warn` level and proceed with reconnection.
    #[error("connection reset: {0}")]
    ConnectionReset(String),

    /// A received frame header advertised a payload larger than
    /// [`MAX_FRAME_SIZE`].
    ///
    /// This prevents a single corrupt or malicious frame from exhausting
    /// memory. The connection should be dropped immediately.
    #[error("frame too large: {size} bytes (max {MAX_FRAME_SIZE})")]
    FrameTooLarge {
        /// The advertised payload size.
        size: u32,
    },

    /// The connection closed cleanly (EOF) in the middle of reading a frame.
    ///
    /// Distinct from `ConnectionReset`: this means the remote peer called
    /// `shutdown(Write)` or dropped the socket after writing a partial frame.
    #[error("incomplete frame: connection closed after {bytes_read} of {expected} bytes")]
    IncompleteFrame {
        /// How many bytes were successfully read.
        bytes_read: usize,
        /// How many bytes the frame header promised.
        expected: usize,
    },

    /// A gRPC transport-level error (connection refused, TLS failure, etc.).
    #[error("gRPC transport error: {0}")]
    GrpcTransport(String),
}

impl From<Box<bincode::ErrorKind>> for NetError {
    fn from(e: Box<bincode::ErrorKind>) -> Self {
        NetError::Serialization(e.to_string())
    }
}

impl From<tonic::transport::Error> for NetError {
    fn from(e: tonic::transport::Error) -> Self {
        NetError::GrpcTransport(e.to_string())
    }
}

impl From<NetError> for leviathan_core::LeviathanError {
    fn from(e: NetError) -> Self {
        leviathan_core::LeviathanError::Network(e.to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn net_error_converts_to_leviathan_error() {
        let net_err = NetError::ConnectionReset("peer gone".into());
        let lev_err: leviathan_core::LeviathanError = net_err.into();
        let msg = format!("{}", lev_err);
        assert!(msg.contains("connection reset"));
    }

    #[test]
    fn frame_too_large_display() {
        let err = NetError::FrameTooLarge { size: 99_999_999 };
        let msg = format!("{}", err);
        assert!(msg.contains("99999999"));
        assert!(msg.contains("16777216")); // MAX_FRAME_SIZE
    }

    #[test]
    fn bincode_error_converts() {
        // Trigger a bincode error by deserializing garbage.
        let result: Result<String, _> = bincode::deserialize(&[0xFF, 0xFF, 0xFF, 0xFF]);
        let bincode_err = result.unwrap_err();
        let net_err: NetError = bincode_err.into();
        assert!(matches!(net_err, NetError::Serialization(_)));
    }
}
