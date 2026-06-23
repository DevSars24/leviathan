//! # leviathan-net
//!
//! Networking primitives for the Leviathan distributed container orchestration
//! platform.
//!
//! This crate provides three layers of abstraction:
//!
//! 1. **Frame** — Length-prefixed binary framing over raw TCP, with robust
//!    handling of partial reads and connection resets.
//! 2. **Codec** — `bincode`-based serialization/deserialization of
//!    `leviathan-core` domain types over framed TCP streams.
//! 3. **gRPC** — `tonic`-generated service stubs and a server-side
//!    implementation of the `NodeService` RPC interface.
//!
//! Downstream crates (`leviathan-node`, `leviathan-control`) depend on this
//! crate for all wire-protocol concerns. The core domain types remain in
//! `leviathan-core`; this crate owns only transport and encoding.

#![warn(missing_docs)]

pub mod codec;
pub mod error;
pub mod frame;
pub mod grpc;
