//! `ActionCable::Server::Base`: configuration, the channel registry, broadcasting, the heartbeat,
//! remote disconnects and the `/cable` endpoint.
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, Once, Weak};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use axum::extract::Request;
use axum::http::{HeaderMap, HeaderValue, Method, StatusCode, Uri, header};
use axum::response::{IntoResponse, Response};
use serde::Serialize;

use crate::channel::Channel;
use crate::socket::Handshake;
use crate::{connection, naming, protocol};
use campfire_bus::{Frame, Hub, Limits, wake};

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
    /// Messages a subscriber may fall behind its broadcasting before it counts as lagging and is
    /// disconnected with `reconnect: true`. Frames are shared by every subscriber, so a lane holds
    /// at most this many frames for its live subscribers.
    pub stream_capacity: usize,
    /// Payload bytes a subscriber may fall behind, with the same effect.
    pub stream_capacity_bytes: usize,
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
            stream_capacity: 4096,
            stream_capacity_bytes: 64 << 20,
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
#[async_trait::async_trait]
pub trait Authenticate<U>: Send + Sync + 'static {
    async fn connect(&self, request: &ConnectRequest) -> Option<U>;
}

/// `identified_by`: the connection identifier used for remote disconnects. For
/// `identified_by :current_user` that's the user's GID param ([`naming::gid_param`]).
pub trait Identified {
    fn connection_identifier(&self) -> String;
}

type ChannelFactory<U> = Arc<dyn Fn() -> Box<dyn Channel<U>> + Send + Sync>;

pub struct ServerBuilder<U: Send + Sync + 'static> {
    config: Config,
    authenticator: Arc<dyn Authenticate<U>>,
    channels: HashMap<Arc<str>, ChannelFactory<U>>,
}

impl<U: Identified + Send + Sync + 'static> ServerBuilder<U> {
    /// Registers a channel under its Ruby class name (`"RoomChannel"`, `"Turbo::StreamsChannel"`),
    /// which is what clients put in the identifier's `channel`.
    pub fn channel<C, F>(mut self, class_name: &str, factory: F) -> Self
    where
        C: Channel<U>,
        F: Fn() -> C + Send + Sync + 'static,
    {
        self.channels.insert(class_name.into(), Arc::new(move || Box::new(factory()) as Box<dyn Channel<U>>));
        self
    }

    pub fn build(self) -> Server<U> {
        let limits = Limits { frames: self.config.stream_capacity as u64, bytes: self.config.stream_capacity_bytes as u64 };
        Server {
            inner: Arc::new(Inner {
                hub: Hub::new(limits, protocol::message),
                config: self.config,
                authenticator: self.authenticator,
                channels: self.channels,
                heartbeat: Once::new(),
                beats: AtomicU64::new(0),
                ping: Mutex::new(protocol::ping(unix_now()).into()),
                restarts: AtomicU64::new(0),
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
    authenticator: Arc<dyn Authenticate<U>>,
    channels: HashMap<Arc<str>, ChannelFactory<U>>,
    heartbeat: Once,
    /// Heartbeats so far, and the latest one's ping frame, shared by every connection.
    beats: AtomicU64,
    ping: Mutex<Frame>,
    restarts: AtomicU64,
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
        let shard = wake::next();
        wake::handle(shard).spawn(async move {
            if let Ok(upgraded) = on_upgrade.await {
                connection::run(server, hyper_util::rt::TokioIo::new(upgraded), handshake.deflate(), request, shard).await;
            }
        });
        response
    }

    /// An Axum router serving [`Server::call`] at `path` (normally [`protocol::DEFAULT_MOUNT_PATH`]).
    pub fn router<S: Clone + Send + Sync + 'static>(&self, path: &str) -> axum::Router<S> {
        let server = self.clone();
        axum::Router::new().route(
            path,
            axum::routing::any(move |request: Request| {
                let server = server.clone();
                async move { server.call(request).await }
            }),
        )
    }
}

impl<U: Send + Sync + 'static> Server<U> {
    pub fn config(&self) -> &Config {
        &self.inner.config
    }

    /// The bus hub that this server's subscriptions read.
    pub fn hub(&self) -> &Arc<Hub> {
        &self.inner.hub
    }

    pub(crate) fn authenticator(&self) -> &Arc<dyn Authenticate<U>> {
        &self.inner.authenticator
    }

    /// The channel a client's `channel` names, with its class name: `safe_constantize` resolves
    /// "::RoomChannel" too, but the class (and so every broadcasting it names) is "RoomChannel".
    pub(crate) fn channel(&self, requested: &str) -> Option<(&Arc<str>, &ChannelFactory<U>)> {
        self.inner.channels.get_key_value(requested.strip_prefix("::").unwrap_or(requested))
    }

    /// Heartbeats so far.
    pub(crate) fn beats(&self) -> u64 {
        self.inner.beats.load(Ordering::Acquire)
    }

    /// The latest heartbeat's ping frame.
    pub(crate) fn ping(&self) -> Frame {
        self.inner.ping.lock().unwrap().clone()
    }

    /// Restarts so far.
    pub(crate) fn restarts(&self) -> u64 {
        self.inner.restarts.load(Ordering::Acquire)
    }

    /// `ActionCable.server.broadcast(broadcasting, message)`.
    pub fn broadcast<T: Serialize + ?Sized>(&self, broadcasting: &str, message: &T) -> usize {
        tracing::debug!(broadcasting, "[ActionCable] Broadcasting");
        self.inner.hub.broadcast(broadcasting, &rails_compat::json::encode(message))
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
        self.inner.restarts.fetch_add(1, Ordering::AcqRel);
        self.inner.hub.ring_all();
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
    /// `setup_heartbeat_timer`, so every connection pings in step. It stops with the server.
    fn start_heartbeat(&self) {
        self.inner.heartbeat.call_once(|| {
            let inner: Weak<Inner<U>> = Arc::downgrade(&self.inner);
            tokio::spawn(async move {
                let period = Duration::from_secs(protocol::BEAT_INTERVAL);
                let mut interval = tokio::time::interval_at(tokio::time::Instant::now() + period, period);
                loop {
                    interval.tick().await;
                    let Some(inner) = inner.upgrade() else { break };
                    *inner.ping.lock().unwrap() = protocol::ping(unix_now()).into();
                    inner.beats.fetch_add(1, Ordering::AcqRel);
                    inner.hub.ring_all();
                }
            });
        });
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
    let connection_upgrade = headers
        .get_all(header::CONNECTION)
        .iter()
        .any(|value| value.to_str().is_ok_and(|v| v.split(',').any(|token| token.trim().eq_ignore_ascii_case("upgrade"))));
    let upgrade_websocket = headers.get(header::UPGRADE).and_then(|v| v.to_str().ok()).is_some_and(|v| v.eq_ignore_ascii_case("websocket"));
    method == Method::GET && connection_upgrade && upgrade_websocket
}

/// `Rack::Request#ssl?` for a request that didn't arrive over TLS itself.
fn ssl_request(headers: &HeaderMap) -> bool {
    let first =
        |name: &str| headers.get(name).and_then(|v| v.to_str().ok()).map(|v| v.split(',').next().unwrap_or("").trim().to_ascii_lowercase());
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
