//! What the three HTTP clients share: name resolution, connecting (to a pinned address when a
//! policy requires one), TLS, and a small HTTP/1.1 exchange that behaves like `Net::HTTP`.
//! Each client keeps its own policy (see plans/rust-conversion.md, "HTTP clients: three
//! distinct policies"); none of them uses a proxy.

pub mod guard;
pub mod http;

use std::io;
use std::net::{IpAddr, SocketAddr};
use std::sync::{Arc, OnceLock};

use futures_util::future::BoxFuture;
use tokio::net::TcpStream;

/// Name resolution. The system one is `getaddrinfo` (Ruby's `Resolv.getaddresses` reads
/// /etc/hosts, then DNS); tests substitute fixed answers.
pub trait Resolver: Send + Sync {
    fn lookup<'a>(&'a self, host: &'a str) -> BoxFuture<'a, io::Result<Vec<IpAddr>>>;
}

/// Opens the TCP connection to an address. Tests redirect fake public addresses to a local
/// server; everything else connects for real.
pub trait Dialer: Send + Sync {
    fn connect(&self, addr: SocketAddr) -> BoxFuture<'_, io::Result<TcpStream>>;
}

pub struct SystemResolver;

impl Resolver for SystemResolver {
    fn lookup<'a>(&'a self, host: &'a str) -> BoxFuture<'a, io::Result<Vec<IpAddr>>> {
        Box::pin(async move {
            let addresses = tokio::net::lookup_host((host, 0)).await?;
            Ok(addresses.map(|a| a.ip()).collect())
        })
    }
}

pub struct TcpDialer;

impl Dialer for TcpDialer {
    fn connect(&self, addr: SocketAddr) -> BoxFuture<'_, io::Result<TcpStream>> {
        Box::pin(TcpStream::connect(addr))
    }
}

/// The resolver, dialer and TLS trust the clients use. [`Network::system`] in production.
#[derive(Clone)]
pub struct Network {
    pub resolver: Arc<dyn Resolver>,
    pub dialer: Arc<dyn Dialer>,
    pub tls: Arc<rustls::ClientConfig>,
}

impl Network {
    /// The system resolver, real connections, and the system's CA certificates (OpenSSL's
    /// default store, which `Net::HTTP` verifies peers against).
    pub fn system() -> Self {
        static TLS: OnceLock<Arc<rustls::ClientConfig>> = OnceLock::new();
        let tls = TLS.get_or_init(|| {
            let mut roots = rustls::RootCertStore::empty();
            let native = rustls_native_certs::load_native_certs();
            for error in &native.errors {
                tracing::warn!("Failed to load a system CA certificate: {error}");
            }
            roots.add_parsable_certificates(native.certs);
            tls_config(roots)
        });
        Self { resolver: Arc::new(SystemResolver), dialer: Arc::new(TcpDialer), tls: tls.clone() }
    }
}

pub fn tls_config(roots: rustls::RootCertStore) -> Arc<rustls::ClientConfig> {
    let provider = Arc::new(rustls::crypto::ring::default_provider());
    let config = rustls::ClientConfig::builder_with_provider(provider)
        .with_safe_default_protocol_versions()
        .expect("ring supports the default protocol versions")
        .with_root_certificates(roots)
        .with_no_client_auth();
    Arc::new(config)
}
