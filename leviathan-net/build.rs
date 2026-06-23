//! Build script for `leviathan-net`.
//!
//! Compiles `proto/leviathan.proto` into Rust types and gRPC service stubs
//! using `tonic-build`. The generated code lives in `OUT_DIR` and is
//! included at compile time via `tonic::include_proto!`.

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Tell Cargo to re-run this script if the proto file changes.
    println!("cargo:rerun-if-changed=proto/leviathan.proto");

    tonic_build::compile_protos("proto/leviathan.proto")?;

    Ok(())
}
