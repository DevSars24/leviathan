//! Length-prefixed binary framing over raw TCP.
//!
//! Every message on the wire is preceded by a 4-byte big-endian length header
//! that describes the payload size. This solves the fundamental TCP framing
//! problem: TCP is a byte stream, not a message stream. Without framing,
//! `read()` can return any number of bytes — a partial message, multiple
//! messages concatenated, or a message split across kernel buffers.
//!
//! ```text
//! ┌──────────────┬──────────────────────────────┐
//! │  4 bytes BE  │        payload (N bytes)      │
//! │  length = N  │                                │
//! └──────────────┴──────────────────────────────┘
//! ```
//!
//! # Partial-Read Handling
//!
//! `read_exact` loops internally until the buffer is full or an error
//! occurs. We detect clean connection close (`UnexpectedEof` with zero
//! bytes read for the header) versus mid-frame disconnection.
//!
//! # Connection Reset Handling
//!
//! `ErrorKind::ConnectionReset`, `ConnectionAborted`, and `BrokenPipe`
//! are all mapped to [`NetError::ConnectionReset`] — the caller should
//! reconnect rather than panic.

use std::io::ErrorKind;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tracing::{debug, trace};

use crate::error::{NetError, MAX_FRAME_SIZE};

/// Write a length-prefixed frame to a TCP stream.
///
/// Writes a 4-byte big-endian length header followed by the payload bytes,
/// then flushes the stream to ensure the entire frame is pushed to the kernel
/// send buffer.
///
/// # Errors
///
/// Returns [`NetError::FrameTooLarge`] if `payload` exceeds [`MAX_FRAME_SIZE`].
/// Returns [`NetError::ConnectionReset`] if the peer has disconnected.
/// Returns [`NetError::Io`] for other I/O failures.
pub async fn write_frame(stream: &mut TcpStream, payload: &[u8]) -> Result<(), NetError> {
    let len = payload.len();
    if len > MAX_FRAME_SIZE as usize {
        return Err(NetError::FrameTooLarge {
            size: len as u32,
        });
    }

    let header = (len as u32).to_be_bytes();

    // Write header + payload in a single logical operation. We catch
    // connection-related errors and map them to NetError::ConnectionReset
    // so callers don't need to pattern-match on raw io::ErrorKind.
    if let Err(e) = stream.write_all(&header).await {
        return Err(classify_io_error(e));
    }
    if let Err(e) = stream.write_all(payload).await {
        return Err(classify_io_error(e));
    }
    if let Err(e) = stream.flush().await {
        return Err(classify_io_error(e));
    }

    trace!(payload_len = len, "Frame written");
    Ok(())
}

/// Read a length-prefixed frame from a TCP stream.
///
/// Reads the 4-byte big-endian header, validates the advertised size against
/// [`MAX_FRAME_SIZE`], then reads exactly that many payload bytes.
///
/// # Returns
///
/// - `Ok(Some(payload))` — a complete frame was read.
/// - `Ok(None)` — the connection was closed cleanly (EOF on the header read).
/// - `Err(NetError::IncompleteFrame)` — the connection closed mid-frame.
/// - `Err(NetError::FrameTooLarge)` — the header advertised a size > max.
/// - `Err(NetError::ConnectionReset)` — the OS reported a reset/abort.
pub async fn read_frame(stream: &mut TcpStream) -> Result<Option<Vec<u8>>, NetError> {
    // --- Read the 4-byte length header ---
    let mut header = [0u8; 4];
    match stream.read_exact(&mut header).await {
        Ok(_) => {}
        Err(e) if e.kind() == ErrorKind::UnexpectedEof => {
            // Clean EOF — no partial data. The remote peer closed the
            // connection between frames, which is the normal shutdown path.
            debug!("Clean EOF on frame header read — connection closed");
            return Ok(None);
        }
        Err(e) => return Err(classify_io_error(e)),
    }

    let payload_len = u32::from_be_bytes(header) as usize;

    // --- Validate frame size ---
    if payload_len > MAX_FRAME_SIZE as usize {
        return Err(NetError::FrameTooLarge {
            size: payload_len as u32,
        });
    }

    // --- Read exactly `payload_len` bytes ---
    let mut payload = vec![0u8; payload_len];
    if payload_len > 0 {
        match stream.read_exact(&mut payload).await {
            Ok(_) => {}
            Err(e) if e.kind() == ErrorKind::UnexpectedEof => {
                // The connection closed *after* we read the header but
                // *before* we got all payload bytes. This is a protocol
                // violation or an abrupt disconnect.
                return Err(NetError::IncompleteFrame {
                    bytes_read: 0, // We can't know exactly how many — read_exact is all-or-nothing.
                    expected: payload_len,
                });
            }
            Err(e) => return Err(classify_io_error(e)),
        }
    }

    trace!(payload_len, "Frame read");
    Ok(Some(payload))
}

/// Classify an [`std::io::Error`] into the appropriate [`NetError`] variant.
///
/// Connection-related errors (`ConnectionReset`, `ConnectionAborted`,
/// `BrokenPipe`) are mapped to [`NetError::ConnectionReset`]. Everything
/// else becomes [`NetError::Io`].
fn classify_io_error(e: std::io::Error) -> NetError {
    match e.kind() {
        ErrorKind::ConnectionReset | ErrorKind::ConnectionAborted | ErrorKind::BrokenPipe => {
            NetError::ConnectionReset(e.to_string())
        }
        _ => NetError::Io(e),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::net::TcpListener;

    /// Helper: spin up a local TCP pair for testing.
    async fn tcp_pair() -> (TcpStream, TcpStream) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let client = TcpStream::connect(addr).await.unwrap();
        let (server, _) = listener.accept().await.unwrap();
        (client, server)
    }

    #[tokio::test]
    async fn roundtrip_single_frame() {
        let (mut client, mut server) = tcp_pair().await;
        let payload = b"hello leviathan";

        write_frame(&mut client, payload).await.unwrap();
        let received = read_frame(&mut server).await.unwrap().unwrap();

        assert_eq!(received, payload);
    }

    #[tokio::test]
    async fn roundtrip_multiple_frames() {
        let (mut client, mut server) = tcp_pair().await;

        for i in 0..10 {
            let msg = format!("frame-{}", i);
            write_frame(&mut client, msg.as_bytes()).await.unwrap();
        }

        for i in 0..10 {
            let received = read_frame(&mut server).await.unwrap().unwrap();
            assert_eq!(received, format!("frame-{}", i).as_bytes());
        }
    }

    #[tokio::test]
    async fn empty_payload_roundtrip() {
        let (mut client, mut server) = tcp_pair().await;

        write_frame(&mut client, b"").await.unwrap();
        let received = read_frame(&mut server).await.unwrap().unwrap();
        assert!(received.is_empty());
    }

    #[tokio::test]
    async fn clean_eof_returns_none() {
        let (client, mut server) = tcp_pair().await;
        // Drop the client to close the connection.
        drop(client);

        let result = read_frame(&mut server).await.unwrap();
        assert!(result.is_none());
    }

    #[tokio::test]
    async fn frame_too_large_rejected_on_write() {
        let (mut client, _server) = tcp_pair().await;
        let oversized = vec![0u8; MAX_FRAME_SIZE as usize + 1];

        let result = write_frame(&mut client, &oversized).await;
        assert!(matches!(result, Err(NetError::FrameTooLarge { .. })));
    }

    #[tokio::test]
    async fn frame_too_large_rejected_on_read() {
        let (mut client, mut server) = tcp_pair().await;

        // Manually write a header that claims a huge payload.
        let fake_len: u32 = MAX_FRAME_SIZE + 1;
        client
            .write_all(&fake_len.to_be_bytes())
            .await
            .unwrap();
        client.flush().await.unwrap();

        let result = read_frame(&mut server).await;
        assert!(matches!(result, Err(NetError::FrameTooLarge { .. })));
    }

    #[tokio::test]
    async fn incomplete_frame_detected() {
        let (mut client, mut server) = tcp_pair().await;

        // Write a header promising 100 bytes, then close without sending them.
        let header = (100u32).to_be_bytes();
        client.write_all(&header).await.unwrap();
        client.flush().await.unwrap();
        drop(client);

        let result = read_frame(&mut server).await;
        assert!(matches!(result, Err(NetError::IncompleteFrame { .. })));
    }

    #[tokio::test]
    async fn large_frame_roundtrip() {
        let (mut client, mut server) = tcp_pair().await;

        // 1 MiB payload — tests that read_exact handles multi-packet data.
        let payload = vec![0xAB; 1024 * 1024];
        write_frame(&mut client, &payload).await.unwrap();

        let received = read_frame(&mut server).await.unwrap().unwrap();
        assert_eq!(received.len(), payload.len());
        assert_eq!(received, payload);
    }
}
