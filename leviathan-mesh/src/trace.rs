//! W3C TraceContext propagation for the service mesh.
//!
//! Implements the W3C Trace Context specification (Level 1) for distributed
//! tracing across service mesh proxies. The `traceparent` header carries
//! trace identity through the system:
//!
//! ```text
//! traceparent: 00-<trace-id>-<parent-id>-<trace-flags>
//! ```
//!
//! Reference: <https://www.w3.org/TR/trace-context/>

use serde::{Deserialize, Serialize};

/// A W3C TraceContext, parsed from or serialized to the `traceparent` header.
///
/// Format: `{version}-{trace_id}-{parent_id}-{trace_flags}`
/// - `version`: 2 hex chars (always "00" for current spec)
/// - `trace_id`: 32 hex chars (128-bit)
/// - `parent_id`: 16 hex chars (64-bit)
/// - `trace_flags`: 2 hex chars (bitmask, 01 = sampled)
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TraceContext {
    /// Trace ID — 128-bit identifier shared across all spans in a trace.
    pub trace_id: String,

    /// Parent span ID — 64-bit identifier of the calling span.
    pub parent_id: String,

    /// Trace flags bitmask. Bit 0 (0x01) = sampled.
    pub trace_flags: u8,
}

impl TraceContext {
    /// Parse a `traceparent` header value into a `TraceContext`.
    ///
    /// # Format
    ///
    /// `00-{trace_id:32hex}-{parent_id:16hex}-{flags:2hex}`
    ///
    /// # Errors
    ///
    /// Returns `None` if the header is malformed.
    #[must_use]
    pub fn parse(header: &str) -> Option<Self> {
        let parts: Vec<&str> = header.split('-').collect();
        if parts.len() != 4 {
            return None;
        }

        let version = parts[0];
        if version != "00" {
            return None; // Unknown version — fail open per spec.
        }

        let trace_id = parts[1];
        if trace_id.len() != 32 {
            return None;
        }

        let parent_id = parts[2];
        if parent_id.len() != 16 {
            return None;
        }

        let trace_flags = u8::from_str_radix(parts[3], 16).ok()?;

        Some(Self {
            trace_id: trace_id.to_string(),
            parent_id: parent_id.to_string(),
            trace_flags,
        })
    }

    /// Serialize this trace context to a `traceparent` header value.
    #[must_use]
    pub fn to_header(&self) -> String {
        format!(
            "00-{}-{}-{:02x}",
            self.trace_id, self.parent_id, self.trace_flags
        )
    }

    /// Create a new trace context with a random trace ID and parent ID.
    #[must_use]
    pub fn new_random() -> Self {
        use std::time::{SystemTime, UNIX_EPOCH};

        // Use timestamp + simple hash for uniqueness without pulling in
        // a full UUID crate (which is in core, but we keep mesh minimal).
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default();
        let nanos = now.as_nanos();

        Self {
            trace_id: format!("{:032x}", nanos),
            parent_id: format!("{:016x}", nanos as u64),
            trace_flags: 0x01, // sampled
        }
    }

    /// Return `true` if the trace is sampled (flag bit 0 is set).
    #[must_use]
    pub fn is_sampled(&self) -> bool {
        self.trace_flags & 0x01 != 0
    }

    /// Create a child span context with a new parent ID.
    #[must_use]
    pub fn child_span(&self, new_parent_id: &str) -> Self {
        Self {
            trace_id: self.trace_id.clone(),
            parent_id: new_parent_id.to_string(),
            trace_flags: self.trace_flags,
        }
    }
}

/// The `tracestate` header — vendor-specific trace data.
///
/// Format: comma-separated list of `key=value` pairs.
/// We propagate it unchanged through the proxy.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct TraceState {
    /// Raw header value, propagated as-is.
    pub value: String,
}

impl TraceState {
    /// Parse a `tracestate` header value.
    #[must_use]
    pub fn parse(header: &str) -> Self {
        Self {
            value: header.to_string(),
        }
    }

    /// Serialize to header value.
    #[must_use]
    pub fn to_header(&self) -> &str {
        &self.value
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_valid_traceparent() {
        let header = "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01";
        let ctx = TraceContext::parse(header).expect("valid traceparent");
        assert_eq!(ctx.trace_id, "0af7651916cd43dd8448eb211c80319c");
        assert_eq!(ctx.parent_id, "b7ad6b7169203331");
        assert_eq!(ctx.trace_flags, 0x01);
        assert!(ctx.is_sampled());
    }

    #[test]
    fn roundtrip_traceparent() {
        let ctx = TraceContext {
            trace_id: "0af7651916cd43dd8448eb211c80319c".into(),
            parent_id: "b7ad6b7169203331".into(),
            trace_flags: 0x01,
        };
        let header = ctx.to_header();
        let parsed = TraceContext::parse(&header).expect("roundtrip");
        assert_eq!(ctx, parsed);
    }

    #[test]
    fn parse_invalid_version() {
        let header = "ff-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01";
        assert!(TraceContext::parse(header).is_none());
    }

    #[test]
    fn parse_invalid_length() {
        let header = "00-short-b7ad6b7169203331-01";
        assert!(TraceContext::parse(header).is_none());
    }

    #[test]
    fn new_random_is_sampled() {
        let ctx = TraceContext::new_random();
        assert!(ctx.is_sampled());
        assert_eq!(ctx.trace_id.len(), 32);
    }

    #[test]
    fn child_span_preserves_trace_id() {
        let parent = TraceContext::new_random();
        let child = parent.child_span("deadbeef01234567");
        assert_eq!(child.trace_id, parent.trace_id);
        assert_eq!(child.parent_id, "deadbeef01234567");
    }
}
