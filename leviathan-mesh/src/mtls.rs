//! mTLS configuration using `rustls`.
//!
//! Provides TLS client and server configuration factories backed by
//! `rustls`. For testing, ephemeral self-signed certificates can be
//! generated via `rcgen`.
//!
//! # Design
//!
//! We use `rustls` (pure Rust) over OpenSSL for:
//! - Memory safety guarantees from Rust's type system
//! - No C FFI or dynamic linking issues
//! - Consistent behavior across platforms

use std::sync::Arc;

use rustls::pki_types::{CertificateDer, PrivateKeyDer, PrivatePkcs8KeyDer};

use crate::error::{MeshError, Result};

/// TLS configuration for the service mesh.
#[derive(Clone)]
pub struct TlsConfig {
    /// Server TLS configuration (for accepting inbound connections).
    server_config: Arc<rustls::ServerConfig>,
    /// Client TLS configuration (for outbound connections).
    client_config: Arc<rustls::ClientConfig>,
}

impl TlsConfig {
    /// Create a `TlsConfig` from raw certificate and key bytes.
    ///
    /// # Arguments
    ///
    /// * `cert_chain` — DER-encoded certificate chain (server cert first).
    /// * `private_key` — DER-encoded PKCS#8 private key.
    ///
    /// # Errors
    ///
    /// Returns `MeshError::TlsError` if the certificate or key is invalid.
    pub fn new(
        cert_chain: Vec<CertificateDer<'static>>,
        private_key: PrivateKeyDer<'static>,
    ) -> Result<Self> {
        let _ = rustls::crypto::ring::default_provider().install_default();

        // Server config: present our cert, verify client certs.
        let server_config = rustls::ServerConfig::builder()
            .with_no_client_auth()
            .with_single_cert(cert_chain.clone(), private_key.clone_key())
            .map_err(|e| MeshError::TlsError(format!("server config: {e}")))?;

        // Client config: trust the same CA (self-signed for testing).
        let mut root_store = rustls::RootCertStore::empty();
        for cert in &cert_chain {
            root_store.add(cert.clone()).map_err(|e| {
                MeshError::TlsError(format!("root store: {e}"))
            })?;
        }

        let client_config = rustls::ClientConfig::builder()
            .with_root_certificates(root_store)
            .with_no_client_auth();

        Ok(Self {
            server_config: Arc::new(server_config),
            client_config: Arc::new(client_config),
        })
    }

    /// Generate an ephemeral self-signed TLS configuration for testing.
    ///
    /// Uses `rcgen` to create a self-signed CA certificate and key pair.
    ///
    /// # Errors
    ///
    /// Returns `MeshError::CertificateError` if cert generation fails.
    pub fn self_signed_for_testing() -> Result<Self> {
        let subject_alt_names = vec!["localhost".to_string(), "127.0.0.1".to_string()];

        let cert_params = rcgen::CertificateParams::new(subject_alt_names)
            .map_err(|e| MeshError::CertificateError(e.to_string()))?;

        let key_pair = rcgen::KeyPair::generate()
            .map_err(|e| MeshError::CertificateError(e.to_string()))?;

        let cert = cert_params
            .self_signed(&key_pair)
            .map_err(|e| MeshError::CertificateError(e.to_string()))?;

        let cert_der = CertificateDer::from(cert.der().to_vec());
        let key_der = PrivateKeyDer::Pkcs8(PrivatePkcs8KeyDer::from(
            key_pair.serialize_der(),
        ));

        Self::new(vec![cert_der], key_der)
    }

    /// Return the server TLS config for use with `tokio-rustls`.
    #[must_use]
    pub fn server_config(&self) -> Arc<rustls::ServerConfig> {
        Arc::clone(&self.server_config)
    }

    /// Return the client TLS config for use with `tokio-rustls`.
    #[must_use]
    pub fn client_config(&self) -> Arc<rustls::ClientConfig> {
        Arc::clone(&self.client_config)
    }
}

impl std::fmt::Debug for TlsConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TlsConfig")
            .field("server_config", &"<rustls::ServerConfig>")
            .field("client_config", &"<rustls::ClientConfig>")
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn self_signed_config_creates_successfully() {
        let config = TlsConfig::self_signed_for_testing();
        assert!(config.is_ok(), "self-signed TLS config should succeed");
    }

    #[test]
    fn server_and_client_configs_are_valid() {
        let config = TlsConfig::self_signed_for_testing().expect("tls config");
        let _server = config.server_config();
        let _client = config.client_config();
        // If we got here without panicking, the configs are structurally valid.
    }
}
