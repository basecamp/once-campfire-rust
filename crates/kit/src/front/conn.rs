//! Accepting and serving connections the way Thruster's `http.Server`s do (`internal/server.go`):
//! HTTP/1.1, HTTP/2 over TLS (and cleartext HTTP/2 with H2C_ENABLED), a `Date` on every response,
//! and Go's three timeouts:
//!
//! - HTTP_READ_TIMEOUT bounds reading a request, headers and body;
//! - HTTP_WRITE_TIMEOUT bounds the rest of the exchange, from the request to the response's end;
//! - HTTP_IDLE_TIMEOUT closes a keep-alive connection with no request in flight.
//!
//! An upgraded connection (Action Cable's WebSocket) has none: Go clears the deadlines when
//! `httputil.ReverseProxy` hijacks it.

use std::future::Future;
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};
use std::time::Duration;

use axum::body::Body;
use axum::http::{HeaderValue, Request, Response, StatusCode, header};
use bytes::Bytes;
use futures_util::future::BoxFuture;
use hyper::body::{Frame, Incoming, SizeHint};
use hyper_util::rt::{TokioExecutor, TokioIo, TokioTimer};
use hyper_util::server::conn::auto::Builder;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{Notify, watch};
use tokio::time::{Instant, Sleep};

use super::handler::ConnInfo;

pub type BoxError = Box<dyn std::error::Error + Send + Sync>;

/// What serves the requests of a connection.
pub type Service = Arc<dyn Fn(Request<Body>, ConnInfo) -> BoxFuture<'static, Response<Body>> + Send + Sync>;

#[derive(Debug, Clone, Copy, Default)]
pub struct Options {
    pub idle_timeout: Option<Duration>,
    pub read_timeout: Option<Duration>,
    pub write_timeout: Option<Duration>,
    /// Send a `Date` header (Go does; Puma doesn't).
    pub date: bool,
}

/// Which protocols a connection may speak.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Protocol {
    Http1,
    Http2,
    /// HTTP/1.1, or HTTP/2 with prior knowledge.
    Auto,
}

/// Tracks connections so a shutdown can wait for them to finish.
#[derive(Clone)]
pub struct Shutdown {
    closing: watch::Receiver<bool>,
    active: Arc<(AtomicUsize, Notify)>,
}

impl Shutdown {
    pub fn new(closing: watch::Receiver<bool>) -> Self {
        Self { closing, active: Arc::new((AtomicUsize::new(0), Notify::new())) }
    }

    /// Closes when `signal` resolves.
    pub fn when(signal: impl Future<Output = ()> + Send + 'static) -> Self {
        let (closing_tx, closing_rx) = watch::channel(false);
        tokio::spawn(async move {
            signal.await;
            let _ = closing_tx.send(true);
        });
        Self::new(closing_rx)
    }

    pub fn is_closing(&self) -> bool {
        *self.closing.borrow()
    }

    pub async fn closing(&self) {
        let mut closing = self.closing.clone();
        let _ = closing.wait_for(|closing| *closing).await;
    }

    fn track(&self) -> ActiveGuard {
        self.active.0.fetch_add(1, Ordering::SeqCst);
        ActiveGuard(self.active.clone())
    }

    /// Resolves once every tracked connection has finished.
    pub async fn drained(&self) {
        loop {
            let notified = self.active.1.notified();
            if self.active.0.load(Ordering::SeqCst) == 0 {
                return;
            }
            notified.await;
        }
    }
}

struct ActiveGuard(Arc<(AtomicUsize, Notify)>);

impl Drop for ActiveGuard {
    fn drop(&mut self) {
        if self.0.0.fetch_sub(1, Ordering::SeqCst) == 1 {
            self.0.1.notify_waiters();
        }
    }
}

/// Binds `:port` the way Go's `net.Listen("tcp", ":port")` does: every address, IPv6 and IPv4,
/// or IPv4 alone where there's no IPv6.
pub async fn bind(port: u16) -> std::io::Result<TcpListener> {
    match TcpListener::bind(SocketAddr::from(([0u16; 8], port))).await {
        Ok(listener) => Ok(listener),
        Err(_) => TcpListener::bind(SocketAddr::from(([0u8; 4], port))).await,
    }
}

/// Accepts connections until shutdown, serving each with `serve_connection`.
pub async fn accept_loop<F, Fut>(listener: TcpListener, shutdown: Shutdown, mut on_connection: F)
where
    F: FnMut(TcpStream, SocketAddr) -> Fut,
    Fut: Future<Output = ()> + Send + 'static,
{
    loop {
        let (stream, remote) = tokio::select! {
            accepted = listener.accept() => match accepted {
                Ok(accepted) => accepted,
                Err(error) => {
                    tracing::debug!(%error, "accept failed");
                    tokio::time::sleep(Duration::from_millis(10)).await;
                    continue;
                }
            },
            _ = shutdown.closing() => break,
        };
        let _ = stream.set_nodelay(true);
        let remote = SocketAddr::new(remote.ip().to_canonical(), remote.port());
        let guard = shutdown.track();
        let connection = on_connection(stream, remote);
        tokio::spawn(async move {
            connection.await;
            drop(guard);
        });
    }
}

/// Serves one connection until it closes, goes idle, or the server shuts down.
pub async fn serve_connection<I>(io: I, info: ConnInfo, protocol: Protocol, service: Service, options: Options, shutdown: Shutdown)
where
    I: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    let activity = Arc::new(Activity::default());
    let hyper_service = {
        let activity = activity.clone();
        hyper::service::service_fn(move |request: Request<Incoming>| {
            let service = service.clone();
            let in_flight = activity.begin();
            let started = Instant::now();
            let read_deadline = options.read_timeout.map(|t| started + t);
            let write_deadline = options.write_timeout.map(|t| started + t);
            let request = request.map(|body| Body::new(Deadline::new(body, read_deadline, "read")));
            async move {
                let response = service(request, info);
                let response = match write_deadline {
                    Some(deadline) => tokio::time::timeout_at(deadline, response).await.map_err(|_| std::io::Error::new(std::io::ErrorKind::TimedOut, "write timeout"))?,
                    None => response.await,
                };
                let mut response = response;
                if options.date {
                    response.headers_mut().entry(header::DATE).or_insert_with(http_date);
                }
                if response.status() == StatusCode::SWITCHING_PROTOCOLS {
                    return Ok::<_, std::io::Error>(response);
                }
                Ok(response.map(|body| Body::new(InFlight { inner: Deadline::new(body, write_deadline, "write"), _in_flight: in_flight })))
            }
        })
    };

    let idle = async {
        match options.idle_timeout {
            Some(timeout) => activity.idle(timeout).await,
            None => std::future::pending().await,
        }
    };
    // Runs a connection until it ends, or shuts it down gracefully when it idles or the server
    // stops. (The connection types share no trait covering upgrades.)
    macro_rules! drive {
        ($connection:expr) => {{
            let mut connection = std::pin::pin!($connection);
            tokio::select! {
                result = connection.as_mut() => {
                    if let Err(error) = result { tracing::trace!(%error, "connection failed"); }
                    return;
                }
                _ = shutdown.closing() => {}
                _ = idle => {}
            }
            connection.as_mut().graceful_shutdown();
            let _ = connection.await;
        }};
    }
    let io = TokioIo::new(io);
    match protocol {
        Protocol::Http1 => {
            let mut builder = hyper::server::conn::http1::Builder::new();
            builder.timer(TokioTimer::new()).header_read_timeout(options.read_timeout).auto_date_header(false);
            drive!(builder.serve_connection(io, hyper_service).with_upgrades())
        }
        Protocol::Http2 => {
            let mut builder = hyper::server::conn::http2::Builder::new(TokioExecutor::new());
            builder.timer(TokioTimer::new()).auto_date_header(false);
            drive!(builder.serve_connection(io, hyper_service))
        }
        Protocol::Auto => {
            let mut builder = Builder::new(TokioExecutor::new());
            builder.http1().timer(TokioTimer::new()).header_read_timeout(options.read_timeout).auto_date_header(false);
            builder.http2().timer(TokioTimer::new()).auto_date_header(false);
            drive!(builder.serve_connection_with_upgrades(io, hyper_service))
        }
    }
}

/// The `Date` Go's `http.Server` writes, on the real clock.
///
/// The raw system call is only for the parity harness (`parity/docker/entrypoint`), which runs the
/// app under libfaketime to freeze its clock. libfaketime intercepts libc's `clock_gettime`, which
/// `std::time::SystemTime` uses; Thruster's Go reads the clock without libc, so its `Date` stayed
/// real, and a frozen one would make every asset stale on arrival in the harness's browsers.
/// Without libfaketime the two clocks are the same.
fn http_date() -> HeaderValue {
    let printer = jiff::fmt::rfc2822::DateTimePrinter::new();
    let date = printer.timestamp_to_rfc9110_string(&wall_clock()).unwrap_or_default();
    HeaderValue::from_str(&date).unwrap_or_else(|_| HeaderValue::from_static(""))
}

#[cfg(target_os = "linux")]
fn wall_clock() -> jiff::Timestamp {
    let mut now = libc::timespec { tv_sec: 0, tv_nsec: 0 };
    // SAFETY: clock_gettime(2) writes one `timespec` through a valid pointer.
    let status = unsafe { libc::syscall(libc::SYS_clock_gettime, libc::CLOCK_REALTIME, &mut now as *mut libc::timespec) };
    match status {
        0 => jiff::Timestamp::new(now.tv_sec, 0).unwrap_or_else(|_| jiff::Timestamp::now()),
        _ => jiff::Timestamp::now(),
    }
}

#[cfg(not(target_os = "linux"))]
fn wall_clock() -> jiff::Timestamp {
    jiff::Timestamp::now()
}

/// Requests in flight on a connection, and when the last one ended.
struct Activity {
    in_flight: AtomicUsize,
    last_active: Mutex<Instant>,
    ended: Notify,
}

impl Default for Activity {
    fn default() -> Self {
        Self { in_flight: AtomicUsize::new(0), last_active: Mutex::new(Instant::now()), ended: Notify::new() }
    }
}

impl Activity {
    fn begin(self: &Arc<Self>) -> InFlightGuard {
        self.in_flight.fetch_add(1, Ordering::SeqCst);
        InFlightGuard(self.clone())
    }

    /// Resolves once the connection has had nothing in flight for `timeout`.
    async fn idle(&self, timeout: Duration) {
        loop {
            if self.in_flight.load(Ordering::SeqCst) > 0 {
                self.ended.notified().await;
                continue;
            }
            let deadline = *self.last_active.lock().unwrap() + timeout;
            if Instant::now() >= deadline {
                if self.in_flight.load(Ordering::SeqCst) == 0 {
                    return;
                }
                continue;
            }
            tokio::select! {
                _ = tokio::time::sleep_until(deadline) => {}
                _ = self.ended.notified() => {}
            }
        }
    }
}

struct InFlightGuard(Arc<Activity>);

impl Drop for InFlightGuard {
    fn drop(&mut self) {
        *self.0.last_active.lock().unwrap() = Instant::now();
        self.0.in_flight.fetch_sub(1, Ordering::SeqCst);
        self.0.ended.notify_one();
    }
}

/// A response body that keeps its request counted as in flight until it's done.
struct InFlight<B> {
    inner: B,
    _in_flight: InFlightGuard,
}

impl<B: hyper::body::Body<Data = Bytes> + Unpin> hyper::body::Body for InFlight<B> {
    type Data = Bytes;
    type Error = B::Error;

    fn poll_frame(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Result<Frame<Bytes>, B::Error>>> {
        Pin::new(&mut self.inner).poll_frame(cx)
    }

    fn is_end_stream(&self) -> bool {
        self.inner.is_end_stream()
    }

    fn size_hint(&self) -> SizeHint {
        self.inner.size_hint()
    }
}

/// A body that fails once its deadline passes (Go's connection read/write deadlines).
struct Deadline<B> {
    inner: B,
    sleep: Option<Pin<Box<Sleep>>>,
    what: &'static str,
}

impl<B> Deadline<B> {
    fn new(inner: B, deadline: Option<Instant>, what: &'static str) -> Self {
        Self { inner, sleep: deadline.map(|d| Box::pin(tokio::time::sleep_until(d))), what }
    }
}

impl<B> hyper::body::Body for Deadline<B>
where
    B: hyper::body::Body<Data = Bytes> + Unpin,
    B::Error: Into<BoxError>,
{
    type Data = Bytes;
    type Error = axum::Error;

    fn poll_frame(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Result<Frame<Bytes>, axum::Error>>> {
        if let Some(sleep) = self.sleep.as_mut()
            && sleep.as_mut().poll(cx).is_ready()
            && !self.inner.is_end_stream()
        {
            let error = std::io::Error::new(std::io::ErrorKind::TimedOut, format!("{} timeout", self.what));
            return Poll::Ready(Some(Err(axum::Error::new(error))));
        }
        Pin::new(&mut self.inner).poll_frame(cx).map_err(axum::Error::new)
    }

    fn is_end_stream(&self) -> bool {
        self.inner.is_end_stream()
    }

    fn size_hint(&self) -> SizeHint {
        self.inner.size_hint()
    }
}
