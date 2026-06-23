//! Binary serialization codec over length-prefixed TCP frames.
//!
//! This module provides the primary wire-protocol API for Leviathan's
//! inter-node communication. It combines the [`frame`](crate::frame) layer
//! (handles partial reads, connection resets, and framing) with `bincode`
//! (compact, zero-copy-friendly binary serialization).
//!
//! # Wire Format
//!
//! ```text
//! ┌──────────────┬───────────────────────────────────┐
//! │  4 bytes BE  │  bincode-encoded payload (N bytes) │
//! │  length = N  │                                     │
//! └──────────────┴───────────────────────────────────┘
//! ```
//!
//! # Usage
//!
//! ```no_run
//! use leviathan_core::NodeMessage;
//! use leviathan_net::codec::{send_message, recv_message};
//! use tokio::net::TcpStream;
//!
//! # async fn example() -> Result<(), leviathan_net::error::NetError> {
//! let mut stream = TcpStream::connect("127.0.0.1:8000").await?;
//!
//! // Send a message
//! let msg = NodeMessage::Deregister {
//!     id: leviathan_core::NodeId::new("node-1"),
//! };
//! send_message(&mut stream, &msg).await?;
//!
//! // Receive a message
//! if let Some(reply) = recv_message::<NodeMessage>(&mut stream).await? {
//!     println!("got: {:?}", reply);
//! }
//! # Ok(())
//! # }
//! ```

use serde::{de::DeserializeOwned, Serialize};
use tokio::net::TcpStream;
use tracing::trace;

use crate::error::NetError;
use crate::frame;

/// Serialize a value to `bincode` bytes.
///
/// # Errors
///
/// Returns [`NetError::Serialization`] if `bincode` encoding fails (e.g.
/// the type contains unsized sequences without a known length).
pub fn encode<T: Serialize>(msg: &T) -> Result<Vec<u8>, NetError> {
    bincode::serialize(msg).map_err(|e| NetError::Serialization(e.to_string()))
}

/// Deserialize a value from `bincode` bytes.
///
/// # Errors
///
/// Returns [`NetError::Serialization`] if the bytes do not represent a valid
/// `T` in bincode format. This can happen due to version mismatch, corruption,
/// or a type mismatch between sender and receiver.
pub fn decode<T: DeserializeOwned>(bytes: &[u8]) -> Result<T, NetError> {
    bincode::deserialize(bytes).map_err(|e| NetError::Serialization(e.to_string()))
}

/// Encode a message to `bincode` and send it as a length-prefixed frame.
///
/// This is the primary send API for inter-node communication. It handles:
/// - Serialization (`bincode`)
/// - Framing (4-byte BE length header)
/// - Flushing (ensures the entire frame reaches the kernel send buffer)
/// - Error classification (connection resets → `NetError::ConnectionReset`)
///
/// # Errors
///
/// Propagates errors from [`encode`] and [`frame::write_frame`].
pub async fn send_message<T: Serialize>(
    stream: &mut TcpStream,
    msg: &T,
) -> Result<(), NetError> {
    let payload = encode(msg)?;
    trace!(payload_len = payload.len(), "Sending bincode-encoded message");
    frame::write_frame(stream, &payload).await
}

/// Read a length-prefixed frame and decode it from `bincode`.
///
/// This is the primary receive API for inter-node communication.
///
/// # Returns
///
/// - `Ok(Some(msg))` — a complete message was received and decoded.
/// - `Ok(None)` — the connection was closed cleanly (EOF between frames).
/// - `Err(_)` — framing error, deserialization failure, or connection reset.
///
/// # Errors
///
/// Propagates errors from [`frame::read_frame`] and [`decode`].
pub async fn recv_message<T: DeserializeOwned>(
    stream: &mut TcpStream,
) -> Result<Option<T>, NetError> {
    match frame::read_frame(stream).await? {
        Some(payload) => {
            trace!(payload_len = payload.len(), "Decoding bincode message");
            let msg = decode(&payload)?;
            Ok(Some(msg))
        }
        None => Ok(None),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use leviathan_core::{Heartbeat, NodeId, NodeMessage, NodeStatus, ResourceSpec};
    use tokio::net::TcpListener;

    async fn tcp_pair() -> (TcpStream, TcpStream) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let client = TcpStream::connect(addr).await.unwrap();
        let (server, _) = listener.accept().await.unwrap();
        (client, server)
    }

    #[test]
    fn encode_decode_roundtrip() {
        let msg = NodeMessage::Register {
            id: NodeId::new("node-1"),
            addr: "127.0.0.1:7001".into(),
            resources: ResourceSpec::new(2000, 4096),
        };
        let bytes = encode(&msg).unwrap();
        let decoded: NodeMessage = decode(&bytes).unwrap();

        match decoded {
            NodeMessage::Register { id, addr, resources } => {
                assert_eq!(id, NodeId::new("node-1"));
                assert_eq!(addr, "127.0.0.1:7001");
                assert_eq!(resources, ResourceSpec::new(2000, 4096));
            }
            _ => panic!("Expected Register variant"),
        }
    }

    #[test]
    fn decode_corrupt_bytes_fails() {
        let result = decode::<NodeMessage>(&[0xFF, 0x00, 0xDE, 0xAD]);
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn send_recv_register_message() {
        let (mut client, mut server) = tcp_pair().await;

        let msg = NodeMessage::Register {
            id: NodeId::new("node-42"),
            addr: "10.0.0.1:9000".into(),
            resources: ResourceSpec::new(4000, 8192),
        };

        send_message(&mut client, &msg).await.unwrap();
        let received: NodeMessage = recv_message(&mut server).await.unwrap().unwrap();

        match received {
            NodeMessage::Register { id, .. } => assert_eq!(id, NodeId::new("node-42")),
            _ => panic!("Expected Register"),
        }
    }

    #[tokio::test]
    async fn send_recv_heartbeat_message() {
        let (mut client, mut server) = tcp_pair().await;

        let hb = Heartbeat {
            node_id: NodeId::new("node-7"),
            status: NodeStatus::Ready,
            resources: ResourceSpec::new(1000, 2048),
            timestamp: chrono::Utc::now(),
        };
        let msg = NodeMessage::Heartbeat(hb);

        send_message(&mut client, &msg).await.unwrap();
        let received: NodeMessage = recv_message(&mut server).await.unwrap().unwrap();

        match received {
            NodeMessage::Heartbeat(hb) => {
                assert_eq!(hb.node_id, NodeId::new("node-7"));
                assert_eq!(hb.status, NodeStatus::Ready);
            }
            _ => panic!("Expected Heartbeat"),
        }
    }

    #[tokio::test]
    async fn send_recv_multiple_messages_in_sequence() {
        let (mut client, mut server) = tcp_pair().await;

        for i in 0..5 {
            let msg = NodeMessage::Register {
                id: NodeId::new(format!("node-{}", i)),
                addr: format!("10.0.0.{}:9000", i),
                resources: ResourceSpec::new(1000 * (i as u64 + 1), 512),
            };
            send_message(&mut client, &msg).await.unwrap();
        }

        for i in 0..5 {
            let received: NodeMessage = recv_message(&mut server).await.unwrap().unwrap();
            match received {
                NodeMessage::Register { id, .. } => {
                    assert_eq!(id, NodeId::new(format!("node-{}", i)));
                }
                _ => panic!("Expected Register"),
            }
        }
    }

    #[tokio::test]
    async fn eof_returns_none() {
        let (client, mut server) = tcp_pair().await;
        drop(client);

        let result: Option<NodeMessage> = recv_message(&mut server).await.unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn bincode_is_compact() {
        // Verify bincode is meaningfully smaller than JSON for the same message.
        let msg = NodeMessage::Register {
            id: NodeId::new("node-1"),
            addr: "127.0.0.1:7001".into(),
            resources: ResourceSpec::new(2000, 4096),
        };
        let bincode_bytes = encode(&msg).unwrap();
        let json_bytes = serde_json::to_vec(&msg).unwrap();

        // bincode should be significantly more compact than JSON.
        assert!(
            bincode_bytes.len() < json_bytes.len(),
            "bincode ({} bytes) should be smaller than JSON ({} bytes)",
            bincode_bytes.len(),
            json_bytes.len()
        );
    }
}
