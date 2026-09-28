//! `ActionCable::Server::Base`: configuration, the channel registry, broadcasting, the heartbeat,
//! remote disconnects and the `/cable` endpoint.
use std::collections::HashMap;
use std::sync::{Arc, OnceLock};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use axum::extract::Request;
use axum::http::{HeaderMap, HeaderValue, Method, StatusCode, Uri, header};
use axum::response::{IntoResponse, Response};
use futures_util::future::BoxFuture;
use serde::Serialize;
use tokio::sync::{broadcast, watch};

use crate::channel::Channel;
use crate::pubsub::{Frame, Hub};
use crate::socket::Handshake;
use crate::{connection, json, naming, protocol};

/// `config.action_cable.*` as the production reference runs it.
#[derive(Debug, Clone)]
pub struct Config {
    pub disable_request_forgery_protection: bool,
    /// Exact `Origin` values accepted in addition to the same-origin rule.
    pub allowed_request_origins: Vec<String>,
    pub allow_same_origin_as_host: bool,
    /// `config.assume_ssl` (on unless `DISABLE_SSL`): `ActionDispatch::AssumeSSL` makes every
    /// request look like HTTPS, so the same-origin check compares against `https://<host>`.
    pub assume_ssl: bool,
    /// Messages buffered per broadcasting (shared by its subscribers) before a slow subscriber
    /// counts as lagging and is disconnected with `reconnect: true`.
    pub stream_capacity: usize,
    /// Frames coalesced into one socket write at most, which also bounds what a connection
    /// buffers beyond the socket.
    pub max_write_batch: usize,
    /// How long to wait for the client's close frame after we close.
    pub close_timeout: Duration,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            disable_request_forgery_protection: false,
            allowed_request_origins: Vec::new(),
            allow_same_origin_as_host: true,
            assume_ssl: true,
            stream_capacity: 256,
            max_write_batch: 64,
            close_timeout: Duration::from_secs(5),
        }
    }
}

/// What the connection's `connect` sees of the upgrade request.
#[derive(Debug, Clone)]
pub struct ConnectRequest {
    pub uri: Uri,
    pub headers: HeaderMap,
}

/// `ApplicationCable::Connection#connect`: resolve the request (the `session_token` cookie) to
/// the connection's identity, or `None` for `reject_unauthorized_connection`.
pub trait Authenticate<U>: Send + Sync + 'static {
    fn connect(&self, request: &ConnectRequest) -> impl Future<Output = Option<U>> + Send;
}

pub(crate) trait DynAuthenticate<U>: Send + Sync + 'static {
    fn connect<'a>(&'a self, request: &'a ConnectRequest) -> BoxFuture<'a, Option<U>>;
}

impl<U: 'static, A: Authenticate<U>> DynAuthenticate<U> for A {
    fn connect<'a>(&'a self, request: &'a ConnectRequest) -> BoxFuture<'a, Option<U>> {
        Box::pin(Authenticate::connect(self, request))
    }
}

/// `identified_by`: the connection identifier used for remote disconnects. For
/// `identified_by :current_user` that's the user's GID param ([`naming::gid_param`]).
pub trait Identified {
    fn connection_identifier(&self) -> String;
}

type ChannelFactory<U> = Arc<dyn Fn() -> Box<dyn Channel<U>> + Send + Sync>;

pub struct ServerBuilder<U: Send + Sync + 'static> {
    config: Config,
    authenticator: Arc<dyn DynAuthenticate<U>>,
    channels: HashMap<String, ChannelFactory<U>>,
}

impl<U: Identified + Send + Sync + 'static> ServerBuilder<U> {
    /// Registers a channel under its Ruby class name (`"RoomChannel"`, `"Turbo::StreamsChannel"`),
    /// which is what clients put in the identifier's `channel`.
    pub fn channel<C, F>(mut self, class_name: &str, factory: F) -> Self
    where
        C: Channel<U>,
        F: Fn() -> C + Send + Sync + 'static,
    {
        self.channels.insert(class_name.to_string(), Arc::new(move || Box::new(factory()) as Box<dyn Channel<U>>));
        self
    }

    pub fn build(self) -> Server<U> {
        let (restart, _) = broadcast::channel(1);
        Server {
            inner: Arc::new(Inner {
                hub: Hub::new(self.config.stream_capacity),
                config: self.config,
                authenticator: self.authenticator,
                channels: self.channels,
                heartbeat: OnceLock::new(),
                restart,
            }),
        }
    }
}

pub struct Server<U: Send + Sync + 'static> {
    inner: Arc<Inner<U>>,
}

impl<U: Send + Sync + 'static> Clone for Server<U> {
    fn clone(&self) -> Self {
        Self { inner: self.inner.clone() }
    }
}

struct Inner<U: Send + Sync + 'static> {
    config: Config,
    hub: Arc<Hub>,
    authenticator: Arc<dyn DynAuthenticate<U>>,
    channels: HashMap<String, ChannelFactory<U>>,
    heartbeat: OnceLock<watch::Receiver<Frame>>,
    restart: broadcast::Sender<()>,
}

impl<U: Identified + Send + Sync + 'static> Server<U> {
    pub fn builder(config: Config, authenticator: impl Authenticate<U>) -> ServerBuilder<U> {
        ServerBuilder { config, authenticator: Arc::new(authenticator), channels: HashMap::new() }
    }

    /// The `/cable` endpoint (`ActionCable::Server::Base#call`). Anything that isn't a WebSocket
    /// upgrade from an allowed origin gets Rails' 404 "Page not found".
    pub async fn call(&self, request: Request) -> Response {
        let (mut parts, _body) = request.into_parts();
        self.start_heartbeat();

        if !websocket_request(&parts.method, &parts.headers) || !self.allow_request_origin(&parts.headers) {
            return page_not_found();
        }
        let (Some(handshake), Some(on_upgrade)) =
            (Handshake::accept(&parts.headers), parts.extensions.remove::<hyper::upgrade::OnUpgrade>())
        else {
            return page_not_found();
        };
        let mut response = Response::new(axum::body::Body::empty());
        *response.status_mut() = StatusCode::SWITCHING_PROTOCOLS;
        handshake.response_headers(response.headers_mut());
        if let Some(protocol) = negotiate_protocol(&parts.headers) {
            response.headers_mut().insert(header::SEC_WEBSOCKET_PROTOCOL, HeaderValue::from_static(protocol));
        }
        let request = ConnectRequest { uri: parts.uri, headers: parts.headers };
        let server = self.clone();
        connections_runtime().spawn(async move {
            if let Ok(upgraded) = on_upgrade.await {
                connection::run(server, hyper_util::rt::TokioIo::new(upgraded), handshake.deflate(), request).await;
            }
        });
        response
    }

    /// An Axum router serving [`Server::call`] at `path` (normally [`protocol::DEFAULT_MOUNT_PATH`]).
    pub fn router<S: Clone + Send + Sync + 'static>(&self, path: &str) -> axum::Router<S> {
        let server = self.clone();
        axum::Router::new().route(path, axum::routing::any(move |request: Request| {
            let server = server.clone();
            async move { server.call(request).await }
        }))
    }
}

impl<U: Send + Sync + 'static> Server<U> {
    pub fn config(&self) -> &Config {
        &self.inner.config
    }

    pub(crate) fn hub(&self) -> &Arc<Hub> {
        &self.inner.hub
    }

    pub(crate) fn authenticator(&self) -> &Arc<dyn DynAuthenticate<U>> {
        &self.inner.authenticator
    }

    pub(crate) fn channel_factory(&self, class_name: &str) -> Option<&ChannelFactory<U>> {
        // `safe_constantize` resolves "::RoomChannel" too.
        self.inner.channels.get(class_name.strip_prefix("::").unwrap_or(class_name))
    }

    pub(crate) fn heartbeat(&self) -> watch::Receiver<Frame> {
        self.start_heartbeat().clone()
    }

    pub(crate) fn restarts(&self) -> broadcast::Receiver<()> {
        self.inner.restart.subscribe()
    }

    /// `ActionCable.server.broadcast(broadcasting, message)`.
    pub fn broadcast<T: Serialize + ?Sized>(&self, broadcasting: &str, message: &T) -> usize {
        self.broadcast_json(broadcasting, json::encode(message))
    }

    /// Broadcasts an already-encoded JSON document.
    pub fn broadcast_json(&self, broadcasting: &str, encoded: String) -> usize {
        tracing::debug!(broadcasting, "[ActionCable] Broadcasting");
        self.inner.hub.broadcast(broadcasting, &json::escape_html_entities(encoded))
    }

    /// `SomeChannel.broadcast_to(broadcastables, message)`.
    pub fn broadcast_to<T: Serialize + ?Sized>(&self, class_name: &str, broadcastables: &[&str], message: &T) -> usize {
        self.broadcast(&naming::broadcasting_for(class_name, broadcastables), message)
    }

    /// `ActionCable.server.remote_connections.where(current_user: user).disconnect(reconnect:)`:
    /// every connection with this identifier, on any socket, is sent
    /// `{"type":"disconnect","reason":"remote","reconnect":...}` and closed.
    pub fn disconnect(&self, connection_identifier: &str, reconnect: bool) -> usize {
        #[derive(Serialize)]
        struct Disconnect {
            r#type: &'static str,
            reconnect: bool,
        }
        self.broadcast(&internal_channel(connection_identifier), &Disconnect { r#type: "disconnect", reconnect })
    }

    /// Broadcastings that currently have subscribers (including connections' internal channels).
    pub fn stream_count(&self) -> usize {
        self.inner.hub.stream_count()
    }

    /// `ActionCable.server.restart`: closes every connection with `server_restart`.
    pub fn restart(&self) {
        let _ = self.inner.restart.send(());
    }

    fn allow_request_origin(&self, headers: &HeaderMap) -> bool {
        let config = &self.inner.config;
        if config.disable_request_forgery_protection {
            return true;
        }
        let origin = headers.get(header::ORIGIN).and_then(|v| v.to_str().ok());
        let host = headers.get(header::HOST).and_then(|v| v.to_str().ok()).unwrap_or("");
        let proto = if config.assume_ssl || ssl_request(headers) { "https" } else { "http" };
        let same_origin = origin == Some(format!("{proto}://{host}").as_str());
        if config.allow_same_origin_as_host && same_origin {
            return true;
        }
        if origin.is_some_and(|origin| config.allowed_request_origins.iter().any(|allowed| allowed == origin)) {
            return true;
        }
        tracing::error!(origin, "Request origin not allowed");
        false
    }

    /// The server-wide heartbeat timer, started on the first request like Rails'
    /// `setup_heartbeat_timer`, so every connection pings in step.
    fn start_heartbeat(&self) -> &watch::Receiver<Frame> {
        self.inner.heartbeat.get_or_init(|| {
            let (sender, receiver) = watch::channel(Frame::from(protocol::ping(unix_now())));
            tokio::spawn(async move {
                let period = Duration::from_secs(protocol::BEAT_INTERVAL);
                let mut interval = tokio::time::interval_at(tokio::time::Instant::now() + period, period);
                loop {
                    interval.tick().await;
                    if sender.send(protocol::ping(unix_now()).into()).is_err() {
                        break;
                    }
                }
            });
            receiver
        })
    }
}

/// `ActionCable::Connection::InternalChannel#internal_channel`.
pub(crate) fn internal_channel(connection_identifier: &str) -> String {
    format!("action_cable/{connection_identifier}")
}

fn unix_now() -> i64 {
    SystemTime::now().duration_since(UNIX_EPOCH).map_or(0, |d| d.as_secs() as i64)
}

/// `WebSocket::Driver.websocket?(env)`: a GET with `Connection: upgrade` and `Upgrade: websocket`.
fn websocket_request(method: &Method, headers: &HeaderMap) -> bool {
    let connection_upgrade = headers.get_all(header::CONNECTION).iter().any(|value| {
        value.to_str().is_ok_and(|v| v.split(',').any(|token| token.trim().eq_ignore_ascii_case("upgrade")))
    });
    let upgrade_websocket = headers
        .get(header::UPGRADE)
        .and_then(|v| v.to_str().ok())
        .is_some_and(|v| v.eq_ignore_ascii_case("websocket"));
    method == Method::GET && connection_upgrade && upgrade_websocket
}

/// `Rack::Request#ssl?` for a request that didn't arrive over TLS itself.
fn ssl_request(headers: &HeaderMap) -> bool {
    let first = |name: &str| {
        headers.get(name).and_then(|v| v.to_str().ok()).map(|v| v.split(',').next().unwrap_or("").trim().to_ascii_lowercase())
    };
    first("x-forwarded-ssl").as_deref() == Some("on")
        || first("x-forwarded-scheme").as_deref() == Some("https")
        || first("x-forwarded-proto").as_deref() == Some("https")
}

/// The first protocol in the client's list that Action Cable supports.
fn negotiate_protocol(headers: &HeaderMap) -> Option<&'static str> {
    headers
        .get_all(header::SEC_WEBSOCKET_PROTOCOL)
        .iter()
        .filter_map(|v| v.to_str().ok())
        .flat_map(|v| v.split(','))
        .map(str::trim)
        .find_map(|requested| protocol::PROTOCOLS.into_iter().find(|supported| *supported == requested))
}

/// `Connection::Base#respond_to_invalid_request`.
fn page_not_found() -> Response {
    (StatusCode::NOT_FOUND, [(header::CONTENT_TYPE, "text/plain; charset=utf-8")], "Page not found").into_response()
}

/// The runtime connections run on, apart from the app's. A broadcast to a big room wakes every
/// subscriber's task at once; on a shared runtime the HTTP requests that arrive meanwhile (the
/// POST that made the broadcast among them) queue behind that whole wave. On threads of their own,
/// the OS shares the cores between requests and the wave.
fn connections_runtime() -> &'static tokio::runtime::Handle {
    static RUNTIME: OnceLock<tokio::runtime::Runtime> = OnceLock::new();
    RUNTIME
        .get_or_init(|| {
            tokio::runtime::Builder::new_multi_thread()
                .thread_name("cable")
                .enable_all()
                .build()
                .expect("the cable runtime starts")
        })
        .handle()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn headers(protocols: &str) -> HeaderMap {
        let mut headers = HeaderMap::new();
        headers.insert(header::SEC_WEBSOCKET_PROTOCOL, protocols.parse().unwrap());
        headers
    }

    #[test]
    fn negotiation_follows_the_clients_order() {
        assert_eq!(negotiate_protocol(&headers("actioncable-v1-json, actioncable-unsupported")), Some("actioncable-v1-json"));
        assert_eq!(negotiate_protocol(&headers("actioncable-unsupported, actioncable-v1-json")), Some("actioncable-unsupported"));
        assert_eq!(negotiate_protocol(&headers("foo")), None);
        assert_eq!(negotiate_protocol(&HeaderMap::new()), None);
    }
}
