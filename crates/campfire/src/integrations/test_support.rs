//! Fakes for the integration tests: DNS answers, a dialer that sends fake public addresses to
//! a local server, and that server (plain or TLS), which replays canned responses and records
//! what it was asked.

use std::collections::{HashMap, HashSet};
use std::io;
use std::net::{IpAddr, SocketAddr};
use std::sync::{Arc, Mutex};

use futures_util::future::BoxFuture;
use tokio::io::{AsyncBufReadExt, AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, BufReader};
use tokio::net::{TcpListener, TcpStream};

use super::net::{Dialer, Network, Resolver};

/// Fixed answers per host. A host with several answer lists gives the next one on each lookup
/// and then keeps giving the last (like the Ruby tests' `Resolv.stubs(...).returns(a, b)`).
#[derive(Default)]
pub struct FakeResolver {
    answers: Mutex<HashMap<String, Vec<Vec<IpAddr>>>>,
    lookups: Mutex<Vec<String>>,
}

impl FakeResolver {
    pub fn new<'a>(hosts: impl IntoIterator<Item = (&'a str, Vec<&'a str>)>) -> Self {
        let resolver = Self::default();
        for (host, answers) in hosts {
            resolver.set(host, vec![answers.iter().map(|a| a.parse().unwrap()).collect()]);
        }
        resolver
    }

    pub fn set(&self, host: &str, answers: Vec<Vec<IpAddr>>) {
        self.answers.lock().unwrap().insert(host.to_string(), answers);
    }

    pub fn lookups(&self) -> Vec<String> {
        self.lookups.lock().unwrap().clone()
    }
}

impl Resolver for FakeResolver {
    fn lookup<'a>(&'a self, host: &'a str) -> BoxFuture<'a, io::Result<Vec<IpAddr>>> {
        self.lookups.lock().unwrap().push(host.to_string());
        let mut answers = self.answers.lock().unwrap();
        let result = match answers.get_mut(host) {
            Some(list) if list.len() > 1 => Ok(list.remove(0)),
            Some(list) => Ok(list[0].clone()),
            None => Err(io::Error::new(io::ErrorKind::NotFound, format!("no address for {host}"))),
        };
        Box::pin(async move { result })
    }
}

/// Connects the given addresses (any port) to `to`; anything else is dialed for real.
pub struct MappingDialer {
    pub public: HashSet<IpAddr>,
    pub to: SocketAddr,
    pub dialed: Mutex<Vec<SocketAddr>>,
}

impl Dialer for MappingDialer {
    fn connect(&self, addr: SocketAddr) -> BoxFuture<'_, io::Result<TcpStream>> {
        self.dialed.lock().unwrap().push(addr);
        let target = if self.public.contains(&addr.ip()) { self.to } else { addr };
        Box::pin(TcpStream::connect(target))
    }
}

pub fn test_tls_roots() -> rustls::RootCertStore {
    use rustls::pki_types::CertificateDer;
    use rustls::pki_types::pem::PemObject;
    let mut roots = rustls::RootCertStore::empty();
    let ca = CertificateDer::from_pem_slice(include_bytes!("testdata/tls/ca.pem")).unwrap();
    roots.add(ca).unwrap();
    roots
}

pub fn network(resolver: Arc<FakeResolver>, dialer: Arc<MappingDialer>) -> Network {
    Network { resolver, dialer, tls: super::net::tls_config(test_tls_roots()) }
}

#[derive(Debug, Clone)]
pub struct Route {
    pub method: String,
    pub host: String,
    pub path: String,
    pub status: u16,
    pub reason: String,
    pub headers: Vec<(String, String)>,
    pub body: Vec<u8>,
    pub chunked: bool,
    pub gzip: bool,
    /// Wait this long before answering.
    pub delay: std::time::Duration,
}

impl Route {
    pub fn new(method: &str, host: &str, path: &str, status: u16) -> Self {
        Self {
            method: method.into(),
            host: host.into(),
            path: path.into(),
            status,
            reason: "Status".into(),
            headers: Vec::new(),
            body: Vec::new(),
            chunked: false,
            gzip: false,
            delay: std::time::Duration::ZERO,
        }
    }

    pub fn header(mut self, name: &str, value: &str) -> Self {
        self.headers.push((name.into(), value.into()));
        self
    }

    pub fn body(mut self, body: impl Into<Vec<u8>>) -> Self {
        self.body = body.into();
        self
    }
}

/// A request as the server saw it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Received {
    pub method: String,
    pub target: String,
    /// Header lines as sent (name case preserved), in order.
    pub headers: Vec<(String, String)>,
    pub body: Vec<u8>,
}

impl Received {
    pub fn header(&self, name: &str) -> Option<&str> {
        self.headers.iter().find(|(n, _)| n.eq_ignore_ascii_case(name)).map(|(_, v)| v.as_str())
    }
}

pub struct FakeServer {
    pub addr: SocketAddr,
    pub received: Arc<Mutex<Vec<Received>>>,
}

impl FakeServer {
    pub async fn start(routes: Vec<Route>) -> Self {
        Self::start_with(routes, None).await
    }

    pub async fn start_tls(routes: Vec<Route>) -> Self {
        use rustls::pki_types::pem::PemObject;
        use rustls::pki_types::{CertificateDer, PrivateKeyDer};
        let certs = vec![CertificateDer::from_pem_slice(include_bytes!("testdata/tls/server.pem")).unwrap()];
        let key = PrivateKeyDer::from_pem_slice(include_bytes!("testdata/tls/server.key")).unwrap();
        let provider = Arc::new(rustls::crypto::ring::default_provider());
        let config = rustls::ServerConfig::builder_with_provider(provider)
            .with_safe_default_protocol_versions()
            .unwrap()
            .with_no_client_auth()
            .with_single_cert(certs, key)
            .unwrap();
        Self::start_with(routes, Some(tokio_rustls::TlsAcceptor::from(Arc::new(config)))).await
    }

    async fn start_with(routes: Vec<Route>, tls: Option<tokio_rustls::TlsAcceptor>) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let received = Arc::new(Mutex::new(Vec::new()));
        let routes = Arc::new(routes);
        let log = received.clone();
        tokio::spawn(async move {
            loop {
                let Ok((stream, _)) = listener.accept().await else { break };
                let (routes, log, tls) = (routes.clone(), log.clone(), tls.clone());
                tokio::spawn(async move {
                    match tls {
                        Some(acceptor) => {
                            if let Ok(stream) = acceptor.accept(stream).await {
                                let _ = serve(stream, &routes, &log).await;
                            }
                        }
                        None => {
                            let _ = serve(stream, &routes, &log).await;
                        }
                    }
                });
            }
        });
        Self { addr, received }
    }

    pub fn received(&self) -> Vec<Received> {
        self.received.lock().unwrap().clone()
    }
}

async fn serve<S: AsyncRead + AsyncWrite + Unpin>(stream: S, routes: &[Route], log: &Mutex<Vec<Received>>) -> io::Result<()> {
    let mut reader = BufReader::new(stream);
    let mut line = String::new();
    reader.read_line(&mut line).await?;
    let mut parts = line.split_whitespace();
    let method = parts.next().unwrap_or("").to_string();
    let target = parts.next().unwrap_or("").to_string();
    let mut headers = Vec::new();
    loop {
        let mut line = String::new();
        if reader.read_line(&mut line).await? == 0 || line == "\r\n" {
            break;
        }
        if let Some((name, value)) = line.trim_end().split_once(": ") {
            headers.push((name.to_string(), value.to_string()));
        }
    }
    let length = headers.iter().find(|(n, _)| n.eq_ignore_ascii_case("content-length")).and_then(|(_, v)| v.parse().ok()).unwrap_or(0);
    let mut body = vec![0; length];
    reader.read_exact(&mut body).await?;
    let host = headers.iter().find(|(n, _)| n.eq_ignore_ascii_case("host")).map(|(_, v)| v.clone()).unwrap_or_default();
    let host = host.rsplit_once(':').filter(|(_, p)| p.bytes().all(|b| b.is_ascii_digit())).map(|(h, _)| h.to_string()).unwrap_or(host);
    log.lock().unwrap().push(Received { method: method.clone(), target: target.clone(), headers, body });

    let not_found = Route::new(&method, &host, &target, 404).header("Content-Type", "text/plain").body("not found");
    let route = routes
        .iter()
        .find(|r| r.method == method && (r.host == host || r.host == "*") && r.path == target)
        .unwrap_or(&not_found);
    tokio::time::sleep(route.delay).await;
    let mut body = route.body.clone();
    if route.gzip {
        use std::io::Write;
        let mut encoder = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
        encoder.write_all(&body)?;
        body = encoder.finish()?;
    }
    let mut head = format!("HTTP/1.1 {} {}\r\n", route.status, route.reason);
    for (name, value) in &route.headers {
        head.push_str(&format!("{name}: {value}\r\n"));
    }
    if route.gzip {
        head.push_str("Content-Encoding: gzip\r\n");
    }
    if route.chunked {
        head.push_str("Transfer-Encoding: chunked\r\n");
    } else if !route.headers.iter().any(|(n, _)| n.eq_ignore_ascii_case("content-length")) {
        head.push_str(&format!("Content-Length: {}\r\n", body.len()));
    }
    head.push_str("Connection: close\r\n\r\n");
    let mut stream = reader.into_inner();
    stream.write_all(head.as_bytes()).await?;
    if method != "HEAD" {
        if route.chunked {
            for chunk in body.chunks(64 * 1024) {
                stream.write_all(format!("{:x}\r\n", chunk.len()).as_bytes()).await?;
                stream.write_all(chunk).await?;
                stream.write_all(b"\r\n").await?;
            }
            stream.write_all(b"0\r\n\r\n").await?;
        } else {
            stream.write_all(&body).await?;
        }
    }
    stream.flush().await?;
    stream.shutdown().await
}

/// A server that answers every request with `head` and then a byte of body every 50 ms, until
/// the client hangs up.
pub async fn trickling_server(head: &'static str) -> SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        while let Ok((mut stream, _)) = listener.accept().await {
            tokio::spawn(async move {
                let _ = stream.read(&mut [0; 4096]).await;
                let _ = stream.write_all(head.as_bytes()).await;
                while stream.write_all(b" ").await.is_ok() {
                    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
                }
            });
        }
    });
    addr
}

/// A gzip bomb:`megabytes` gzip members of a megabyte of zeros each, about 1 KB apiece.
pub fn gzip_bomb(megabytes: usize) -> Vec<u8> {
    use std::io::Write;
    let mut encoder = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::best());
    encoder.write_all(&vec![0; 1024 * 1024]).unwrap();
    encoder.finish().unwrap().repeat(megabytes)
}

/// The reference fixtures in a fresh database (like `fixtures :all`).
pub struct TestDb {
    pub db: campfire_db::Database,
    path: std::path::PathBuf,
}

impl TestDb {
    pub fn new() -> Self {
        use campfire_db::{BasicRichText, Config, Database, Env, NullSink, TestClock, fixtures};
        static COUNTER: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);
        let n = COUNTER.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        let path = std::env::temp_dir().join(format!("campfire-integrations-{}-{n}", std::process::id()));
        let _ = std::fs::remove_dir_all(&path);
        std::fs::create_dir_all(&path).unwrap();
        let env = Env { clock: Arc::new(TestClock::new()), sink: Arc::new(NullSink), rich_text: Arc::new(BasicRichText), bcrypt_cost: 4 };
        let mut config = Config::new(path.join("test.sqlite3"));
        config.environment = "test".into();
        let db = Database::open(config, env).unwrap();
        db.write_blocking(|tx| {
            let options = fixtures::Options { now: tx.now(), bcrypt_cost: 4 };
            fixtures::load(tx.conn(), &fixtures::reference_dir(), &options)
        })
        .unwrap();
        Self { db, path }
    }

    pub fn id(label: &str) -> i64 {
        campfire_db::fixtures::identify(label)
    }
}

impl Drop for TestDb {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.path);
    }
}
