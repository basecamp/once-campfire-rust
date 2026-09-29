//! The front server: what Thruster 0.1.23 (github.com/basecamp/thruster, `internal/`) did in
//! front of the reference's Puma (`thrust bin/start-app`), in process.
//!
//! - HTTP/1.1 on HTTP_PORT (80), and HTTP/2 without TLS with H2C_ENABLED (`server.go`).
//! - With TLS_DOMAIN: HTTPS with HTTP/2 on HTTPS_PORT (443) and certificates from ACME
//!   (`acme.rs`, stored where Thruster stores them), while HTTP_PORT only answers HTTP-01
//!   challenges and redirects to HTTPS (`tls.rs`).
//! - For every request: the in-memory response cache (`cache.rs`), compression on top of the
//!   app's own gzip (`compression.rs`), `X-Forwarded-*` and `X-Request-Start`, MAX_REQUEST_BODY,
//!   the HTTP_*_TIMEOUTs and a request log line (`handler.rs`, `conn.rs`).
//! - The app still listens on TARGET_PORT (3000) by itself, as Puma did behind Thruster, but only
//!   on loopback unless TARGET_BIND says otherwise, and with the front's timeouts and
//!   MAX_REQUEST_BODY.
//!
//! Not carried over, because nothing reaches them: X-Sendfile (Rack 3.2's `Rack::Sendfile` no
//! longer honours the `X-Sendfile-Type` request header Thruster sends, and the reference
//! configures no `x_sendfile_header`, so it never sends `X-Sendfile`), BAD_GATEWAY_PAGE (there's
//! no upstream process to be unreachable), and RSA certificates (autocert gives them only to
//! clients that can't do ECDSA).

mod acme;
mod cache;
mod compression;
mod config;
mod conn;
mod handler;
mod tls;

use std::sync::Arc;
use std::time::Duration;

use axum::Router;
use axum::body::Body;
use axum::extract::ConnectInfo;
use tower::ServiceExt;

pub use acme::{AcmeOptions, CertManager};
pub use config::{FrontConfig, LETS_ENCRYPT_URL};
pub use conn::{Options, Protocol, Service, Shutdown, bind};
pub use handler::{ConnInfo, Handler};

/// How long connections get to finish after shutdown (Thruster's `Stop`).
const STOP_GRACE: Duration = Duration::from_secs(5);

/// Serves `app` the way Thruster served the reference until `shutdown` resolves.
pub async fn serve(config: FrontConfig, app: Router, shutdown: impl Future<Output = ()> + Send + 'static) -> std::io::Result<()> {
    serve_with(config, app, None, shutdown).await
}

/// `serve`, with ACME options other than the configuration's (tests use a local CA).
pub async fn serve_with(
    config: FrontConfig,
    app: Router,
    acme: Option<AcmeOptions>,
    shutdown: impl Future<Output = ()> + Send + 'static,
) -> std::io::Result<()> {
    let shutdown_state = Shutdown::when(shutdown);

    let options = Options {
        idle_timeout: nonzero(config.http_idle_timeout),
        read_timeout: nonzero(config.http_read_timeout),
        write_timeout: nonzero(config.http_write_timeout),
        date: true,
    };
    let handler = Arc::new(Handler::new(&config, app.clone()));
    let front: Service = Arc::new(move |request, conn| {
        let handler = handler.clone();
        Box::pin(async move { handler.call(request, conn).await })
    });

    let upstream = serve_upstream(&config, app, options, shutdown_state.clone()).await?;
    let http = bind(config.http_port).await?;
    let mut servers = vec![upstream];
    if config.has_tls() {
        let acme = acme.unwrap_or_else(|| AcmeOptions::from_config(&config));
        let certs = CertManager::new(acme);
        let tls = Arc::new(tls::Tls::new(certs.clone()).map_err(std::io::Error::other)?);
        let https = bind(config.https_port).await?;
        tracing::info!(http = %format!(":{}", config.http_port), https = %format!(":{}", config.https_port), tls_domain = ?config.tls_domains, "Server started");
        let redirect: Service = Arc::new(move |request, _conn| {
            let response = tls::http_handler(&certs, &request);
            Box::pin(async move { response })
        });
        servers.push(tokio::spawn(serve_plain(http, redirect, Protocol::Http1, options, shutdown_state.clone())));
        servers.push(tokio::spawn(serve_tls(https, tls, front, options, shutdown_state.clone())));
    } else {
        tracing::info!(http = %format!(":{}", config.http_port), "Server started");
        let protocol = if config.h2c_enabled { Protocol::Auto } else { Protocol::Http1 };
        servers.push(tokio::spawn(serve_plain(http, front, protocol, options, shutdown_state.clone())));
    }
    for server in servers {
        let _ = server.await;
    }
    let _ = tokio::time::timeout(STOP_GRACE, shutdown_state.drained()).await;
    tracing::info!("Server stopped");
    Ok(())
}

/// The app on its own on TARGET_PORT, as Puma listened behind Thruster (Thruster set `PORT` to
/// TARGET_PORT for it): HTTP/1.1, no `Date`, no cache or compression. Unlike Puma it listens on
/// TARGET_BIND (loopback) and keeps to the front's timeouts and body limit, since whoever reaches
/// it can claim any `X-Forwarded-*`.
async fn serve_upstream(config: &FrontConfig, app: Router, options: Options, shutdown: Shutdown) -> std::io::Result<tokio::task::JoinHandle<()>> {
    if config.target_port == config.http_port || (config.has_tls() && config.target_port == config.https_port) {
        tracing::warn!(port = config.target_port, "TARGET_PORT is the front server's port; not listening on it separately");
        return Ok(tokio::spawn(async {}));
    }
    let listener = tokio::net::TcpListener::bind((config.target_bind, config.target_port)).await?;
    let service = limited_app_service(app, config.max_request_body.max(0) as u64);
    Ok(tokio::spawn(serve_plain(listener, service, Protocol::Http1, Options { date: false, ..options }, shutdown)))
}

/// `app_service` behind MAX_REQUEST_BODY.
fn limited_app_service(app: Router, max_request_body: u64) -> Service {
    let app = app_service(app);
    Arc::new(move |request, conn| {
        let app = app.clone();
        Box::pin(async move {
            match handler::within_limit(request, max_request_body).await {
                Ok(request) => app(request, conn).await,
                Err(()) => handler::too_large(),
            }
        })
    })
}

/// The bare app as a connection service.
pub fn app_service(app: Router) -> Service {
    Arc::new(move |mut request: axum::http::Request<Body>, conn: ConnInfo| {
        request.extensions_mut().insert(ConnectInfo(conn.remote));
        let app = app.clone();
        Box::pin(async move {
            match app.oneshot(request).await {
                Ok(response) => response,
                Err(infallible) => match infallible {},
            }
        })
    })
}

/// Serves plain HTTP connections from `listener` until shutdown.
pub async fn serve_plain(listener: tokio::net::TcpListener, service: Service, protocol: Protocol, options: Options, shutdown: Shutdown) {
    let connections = shutdown.clone();
    conn::accept_loop(listener, shutdown, move |stream, remote| {
        let (service, shutdown) = (service.clone(), connections.clone());
        let info = ConnInfo { remote, tls: false };
        conn::serve_connection(stream, info, protocol, service, options, shutdown)
    })
    .await;
}

async fn serve_tls(listener: tokio::net::TcpListener, tls: Arc<tls::Tls>, service: Service, options: Options, shutdown: Shutdown) {
    let connections = shutdown.clone();
    conn::accept_loop(listener, shutdown, move |stream, remote| {
        let (service, shutdown, tls) = (service.clone(), connections.clone(), tls.clone());
        async move {
            // Go bounds the handshake by the connection's read deadline.
            let handshake = tls.accept(stream);
            let accepted = match options.read_timeout {
                Some(timeout) => tokio::time::timeout(timeout, handshake).await.unwrap_or_else(|_| Err("TLS handshake timeout".into())),
                None => handshake.await,
            };
            match accepted {
                Ok(Some((stream, protocol))) => {
                    let info = ConnInfo { remote, tls: true };
                    conn::serve_connection(stream, info, protocol, service, options, shutdown).await;
                }
                Ok(None) => {}
                Err(error) => tracing::debug!(%remote, %error, "http: TLS handshake error"),
            }
        }
    })
    .await;
}

fn nonzero(duration: Duration) -> Option<Duration> {
    (!duration.is_zero()).then_some(duration)
}
