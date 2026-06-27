//! # leviathan-mesh
//!
//! Service mesh with mTLS proxy and W3C TraceContext propagation for the
//! Leviathan distributed container orchestration platform.
//!
//! ## Modules
//!
//! - [`proxy`] — Per-container sidecar proxy for traffic interception
//! - [`mtls`] — mTLS configuration using `rustls`
//! - [`trace`] — W3C TraceContext header injection/extraction
//! - [`error`] — Service mesh error types

#![warn(missing_docs)]

pub mod error;
pub mod mtls;
pub mod proxy;
pub mod trace;

pub use error::MeshError;
pub use mtls::TlsConfig;
pub use proxy::{ProxyConfig, SidecarProxy};
pub use trace::TraceContext;
