//! Thruster's handler chain (`internal/handler.go`), outermost first: request logging, the request
//! size limit, compression, `X-Request-Start`, the response cache, and the proxy's
//! `X-Forwarded-*` headers in front of the app.

use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::{Instant, SystemTime, UNIX_EPOCH};

use axum::Router;
use axum::body::Body;
use axum::extract::ConnectInfo;
use axum::http::{HeaderValue, Method, Request, Response, StatusCode, header};
use bytes::{Bytes, BytesMut};
use http_body_util::BodyExt;
use hyper::body::{Body as _, Frame, SizeHint};
use tower::ServiceExt;

use super::cache::{self, CachedResponse, MemoryCache, Variant};
use super::compression::{Compression, HeaderMerge};
use super::config::FrontConfig;

/// The connection a request came in on.
#[derive(Debug, Clone, Copy)]
pub struct ConnInfo {
    pub remote: SocketAddr,
    pub tls: bool,
}

pub struct Handler {
    app: Router,
    cache: Arc<MemoryCache>,
    max_cacheable_body: usize,
    compression: Option<Compression>,
    max_request_body: u64,
    forward_headers: bool,
    log_requests: bool,
}

impl Handler {
    pub fn new(config: &FrontConfig, app: Router) -> Self {
        Self {
            app,
            cache: Arc::new(MemoryCache::new(config.cache_size, config.max_cache_item_size)),
            max_cacheable_body: config.max_cache_item_size.max(0) as usize,
            compression: config
                .gzip_compression_enabled
                .then(|| Compression::new(config.gzip_compression_jitter, config.gzip_compression_disable_on_auth)),
            max_request_body: config.max_request_body.max(0) as u64,
            forward_headers: config.forward_headers,
            log_requests: config.log_requests,
        }
    }

    pub async fn call(self: Arc<Self>, request: Request<Body>, conn: ConnInfo) -> Response<Body> {
        let log = self.log_requests.then(|| RequestLog::new(&request, &conn));
        let mut response = self.compress(request, conn).await;
        suppress_bodiless_headers(&mut response);
        match log {
            Some(log) => log.attach(response),
            None => response,
        }
    }

    async fn compress(&self, mut request: Request<Body>, conn: ConnInfo) -> Response<Body> {
        let negotiation = self.compression.as_ref().map(|c| c.negotiate(&request));
        set_request_start(&mut request);
        let (response, merge) = self.cached(request, conn).await;
        match (&self.compression, negotiation) {
            (Some(compression), Some(negotiation)) => compression.apply(negotiation, response, merge).await,
            _ => response,
        }
    }

    /// `CacheHandler.ServeHTTP`, except that requests the cache can't serve bypass it before the
    /// lookup (Thruster looks them up first, so a range request could get a whole cached body).
    async fn cached(&self, request: Request<Body>, conn: ConnInfo) -> (Response<Body>, HeaderMerge) {
        if !cache::should_cache_request(&request) {
            let mut response = self.proxy(request, conn).await;
            response.headers_mut().insert("x-cache", HeaderValue::from_static("bypass"));
            return (response, HeaderMerge::Append);
        }

        let now = Instant::now();
        let mut variant = Variant::new(&request);
        let mut key = variant.cache_key();
        let mut found = self.cache.get(&key, now);
        if let Some(cached) = &found {
            variant.set_response_headers(&cached.headers);
            if !variant.matches(&cached.variant) {
                key = variant.cache_key();
                found = self.cache.get(&key, now);
            }
        }
        if let Some(cached) = found {
            return (hit(&cached, &request), HeaderMerge::Replace);
        }

        let head = request.method() == Method::HEAD;
        let mut response = self.proxy(request, conn).await;
        let lifetime = cache::cache_lifetime(response.status(), response.headers());
        if lifetime.is_some() {
            response.headers_mut().remove(header::SET_COOKIE);
        }
        response.headers_mut().insert("x-cache", HeaderValue::from_static("miss"));
        let Some(lifetime) = lifetime else { return (response, HeaderMerge::Replace) };

        variant.set_response_headers(response.headers());
        let (status, mut headers) = (response.status(), response.headers().clone());
        headers.remove("x-cache");
        let (cache, variant) = (self.cache.clone(), variant.variant_headers());
        let store = move |body: Bytes| cache.set(key, CachedResponse { status, headers, body, variant, metadata: () }, now + lifetime, now);
        if head || hyper::body::Body::is_end_stream(response.body()) {
            // A HEAD response has no body to record (the connection never reads one), and neither
            // has an empty one (it's never polled).
            store(Bytes::new());
            return (response, HeaderMerge::Replace);
        }
        let declared = response.headers().get(header::CONTENT_LENGTH).and_then(|v| v.to_str().ok()?.parse().ok());
        let (parts, body) = response.into_parts();
        let body = RecordingBody {
            inner: body,
            limit: self.max_cacheable_body,
            recorded: BytesMut::new(),
            seen: 0,
            declared,
            overflowed: false,
            store: Some(Box::new(store)),
        };
        (Response::from_parts(parts, Body::new(body)), HeaderMerge::Replace)
    }

    /// The proxy (`internal/proxy_handler.go`): the request size limit Thruster's
    /// `http.MaxBytesHandler` enforces (an oversized body never reaches the app and gets an empty
    /// 413), the `X-Forwarded-*` headers `httputil.ReverseProxy` sets, then the app.
    async fn proxy(&self, request: Request<Body>, conn: ConnInfo) -> Response<Body> {
        let Ok(mut request) = within_limit(request, self.max_request_body).await else { return too_large() };
        as_proxied_http1(&mut request);
        set_forwarded_headers(&mut request, &conn, self.forward_headers);
        request.extensions_mut().insert(ConnectInfo(conn.remote));
        match self.app.clone().oneshot(request).await {
            Ok(response) => response,
            Err(infallible) => match infallible {},
        }
    }
}

/// Go's `http.Server` drops the headers a status can't carry when it writes them
/// (`suppressedHeaders`): `Content-Type`, `Content-Length` and `Transfer-Encoding` on a 304
/// (whether Rails or the cache produced it), the length on 1xx and 204.
fn suppress_bodiless_headers(response: &mut Response<Body>) {
    let status = response.status().as_u16();
    let suppressed: &[header::HeaderName] = match status {
        304 => &[header::CONTENT_TYPE, header::CONTENT_LENGTH, header::TRANSFER_ENCODING],
        100..=199 | 204 => &[header::CONTENT_LENGTH, header::TRANSFER_ENCODING],
        _ => &[],
    };
    for name in suppressed {
        response.headers_mut().remove(name);
    }
}

/// A cache hit (`WriteCachedResponse`): the stored response, or a 304 when the request already
/// has it.
fn hit(cached: &CachedResponse, request: &Request<Body>) -> Response<Body> {
    let not_modified = cache::was_not_modified(cached, request);
    let mut response = Response::new(if not_modified { Body::empty() } else { Body::from(cached.body.clone()) });
    *response.status_mut() = if not_modified { StatusCode::NOT_MODIFIED } else { cached.status };
    *response.headers_mut() = cached.headers.clone();
    response.headers_mut().insert("x-cache", HeaderValue::from_static("hit"));
    response
}

/// Records a cacheable response's body as it's sent (`stashingWriter`), and stores it at the end
/// unless it grew past the cache's item limit. The end is the body's end of stream or, for a body
/// of declared length, its last byte: hyper stops polling a streamed body (`send_file`'s) once the
/// `Content-Length` is written, so its end of stream would never be seen. (Thruster stores the
/// response when the handler returns, however it was written.)
struct RecordingBody {
    inner: Body,
    limit: usize,
    recorded: BytesMut,
    seen: u64,
    declared: Option<u64>,
    overflowed: bool,
    store: Option<Box<dyn FnOnce(Bytes) + Send>>,
}

impl RecordingBody {
    fn finish(&mut self) {
        if let Some(store) = self.store.take()
            && !self.overflowed
        {
            store(std::mem::take(&mut self.recorded).freeze());
        }
    }
}

impl hyper::body::Body for RecordingBody {
    type Data = Bytes;
    type Error = axum::Error;

    fn poll_frame(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Result<Frame<Bytes>, axum::Error>>> {
        let this = &mut *self;
        match Pin::new(&mut this.inner).poll_frame(cx) {
            Poll::Ready(Some(Ok(frame))) => {
                if let Some(data) = frame.data_ref() {
                    this.seen += data.len() as u64;
                    if this.recorded.len() + data.len() > this.limit {
                        this.overflowed = true;
                    } else if !this.overflowed {
                        this.recorded.extend_from_slice(data);
                    }
                }
                if this.inner.is_end_stream() || this.declared == Some(this.seen) {
                    this.finish();
                }
                Poll::Ready(Some(Ok(frame)))
            }
            Poll::Ready(None) => {
                this.finish();
                Poll::Ready(None)
            }
            Poll::Ready(Some(Err(error))) => {
                this.store = None;
                Poll::Ready(Some(Err(error)))
            }
            Poll::Pending => Poll::Pending,
        }
    }

    fn is_end_stream(&self) -> bool {
        self.inner.is_end_stream()
    }

    fn size_hint(&self) -> SizeHint {
        self.inner.size_hint()
    }
}

/// `NewRequestStartHandler`
fn set_request_start(request: &mut Request<Body>) {
    if request.headers().get("x-request-start").is_none_or(|v| v.is_empty()) {
        let millis = SystemTime::now().duration_since(UNIX_EPOCH).map(|d| d.as_millis()).unwrap_or(0);
        request.headers_mut().insert("x-request-start", HeaderValue::from_str(&format!("t={millis}")).unwrap());
    }
}

/// The app saw every request as Thruster's HTTP/1.1 request to Puma: an HTTP/2 request's
/// `:authority` becomes its `Host`, its path is in origin form, and its cookie fields are one
/// `Cookie` header (Go's HTTP/2 server joins them with "; ").
fn as_proxied_http1(request: &mut Request<Body>) {
    if request.version() != axum::http::Version::HTTP_2 {
        return;
    }
    *request.version_mut() = axum::http::Version::HTTP_11;
    if !request.headers().contains_key(header::HOST)
        && let Some(authority) = request.uri().authority().and_then(|a| HeaderValue::from_str(a.as_str()).ok())
    {
        request.headers_mut().insert(header::HOST, authority);
    }
    if let Some(path) = request.uri().path_and_query().and_then(|p| p.as_str().parse().ok()) {
        *request.uri_mut() = path;
    }
    let cookies: Vec<String> =
        request.headers().get_all(header::COOKIE).iter().filter_map(|v| v.to_str().ok()).map(str::to_string).collect();
    if cookies.len() > 1
        && let Ok(joined) = HeaderValue::from_str(&cookies.join("; "))
    {
        request.headers_mut().insert(header::COOKIE, joined);
    }
}

/// `setXForwarded` over `ProxyRequest.SetXForwarded`: the client's address is appended to
/// `X-Forwarded-For`, and `X-Forwarded-Host`/`-Proto` name this request's host and scheme. With
/// FORWARD_HEADERS the client's own `X-Forwarded-*` are kept (it's trusted to be a proxy);
/// without it they're replaced. `Forwarded` never reaches the app (`Rewrite` drops it).
fn set_forwarded_headers(request: &mut Request<Body>, conn: &ConnInfo, forward_headers: bool) {
    let headers = request.headers();
    let join = |name| headers.get_all(name).iter().filter_map(|v| v.to_str().ok()).collect::<Vec<_>>().join(", ");
    let prior_for = if forward_headers { join("x-forwarded-for") } else { String::new() };
    let incoming_host = headers.get("x-forwarded-host").filter(|v| !v.is_empty()).cloned();
    let incoming_proto = headers.get("x-forwarded-proto").filter(|v| !v.is_empty()).cloned();
    let host =
        headers.get(header::HOST).cloned().or_else(|| request.uri().authority().and_then(|a| HeaderValue::from_str(a.as_str()).ok()));

    let client = conn.remote.ip().to_canonical().to_string();
    let forwarded_for = if prior_for.is_empty() { client } else { format!("{prior_for}, {client}") };
    let headers = request.headers_mut();
    headers.remove(header::FORWARDED);
    if let Ok(value) = HeaderValue::from_str(&forwarded_for) {
        headers.insert("x-forwarded-for", value);
    }
    match host {
        Some(host) => headers.insert("x-forwarded-host", host),
        None => headers.insert("x-forwarded-host", HeaderValue::from_static("")),
    };
    headers.insert("x-forwarded-proto", HeaderValue::from_static(if conn.tls { "https" } else { "http" }));
    if forward_headers {
        if let Some(host) = incoming_host {
            headers.insert("x-forwarded-host", host);
        }
        if let Some(proto) = incoming_proto {
            headers.insert("x-forwarded-proto", proto);
        }
    }
}

/// MAX_REQUEST_BODY, 0 for none.
pub(super) async fn within_limit(request: Request<Body>, limit: u64) -> Result<Request<Body>, ()> {
    if limit == 0 { Ok(request) } else { limit_body(request, limit).await }
}

/// The empty 413 for a body over MAX_REQUEST_BODY.
pub(super) fn too_large() -> Response<Body> {
    let mut response = Response::new(Body::empty());
    *response.status_mut() = StatusCode::PAYLOAD_TOO_LARGE;
    response
}

/// `http.MaxBytesHandler`: a body over the limit fails the proxied request with 413 before the
/// app sees it (Puma reads the whole body before calling Rails).
async fn limit_body(request: Request<Body>, limit: u64) -> Result<Request<Body>, ()> {
    let declared = request.headers().get(header::CONTENT_LENGTH).and_then(|v| v.to_str().ok()).and_then(|v| v.parse::<u64>().ok());
    if declared.is_some_and(|length| length > limit) {
        return Err(());
    }
    if declared.is_some() || request.body().is_end_stream() {
        return Ok(request);
    }
    let (parts, mut body) = request.into_parts();
    let mut buffered = BytesMut::new();
    while let Some(frame) = body.frame().await {
        let Ok(frame) = frame else { return Err(()) };
        if let Some(data) = frame.data_ref() {
            buffered.extend_from_slice(data);
            if buffered.len() as u64 > limit {
                return Err(());
            }
        }
    }
    Ok(Request::from_parts(parts, Body::from(buffered.freeze())))
}

/// Thruster's request log line (`internal/logging_handler.go`), written when the response ends.
struct RequestLog {
    started: Instant,
    path: String,
    query: String,
    method: String,
    proto: &'static str,
    req_content_length: i64,
    req_content_type: String,
    remote_addr: String,
    user_agent: String,
}

impl RequestLog {
    fn new(request: &Request<Body>, conn: &ConnInfo) -> Self {
        let header = |name| request.headers().get(name).and_then(|v: &HeaderValue| v.to_str().ok()).unwrap_or("").to_string();
        let forwarded_for = header("x-forwarded-for");
        Self {
            started: Instant::now(),
            path: request.uri().path().to_string(),
            query: request.uri().query().unwrap_or("").to_string(),
            method: request.method().to_string(),
            proto: match request.version() {
                axum::http::Version::HTTP_2 => "HTTP/2.0",
                axum::http::Version::HTTP_10 => "HTTP/1.0",
                _ => "HTTP/1.1",
            },
            req_content_length: header("content-length").parse().unwrap_or(if request.body().is_end_stream() { 0 } else { -1 }),
            req_content_type: header("content-type"),
            remote_addr: if forwarded_for.is_empty() { conn.remote.to_string() } else { forwarded_for },
            user_agent: header("user-agent"),
        }
    }

    fn attach(self, response: Response<Body>) -> Response<Body> {
        let status = if response.status() == StatusCode::SWITCHING_PROTOCOLS { 101 } else { response.status().as_u16() };
        let header = |name| response.headers().get(name).and_then(|v: &HeaderValue| v.to_str().ok()).unwrap_or("").to_string();
        let entry = LogEntry { request: self, status, resp_content_type: header("content-type"), cache: header("x-cache"), bytes: 0 };
        let (parts, body) = response.into_parts();
        Response::from_parts(parts, Body::new(LoggedBody { inner: body, entry: Some(entry) }))
    }
}

struct LogEntry {
    request: RequestLog,
    status: u16,
    resp_content_type: String,
    cache: String,
    bytes: u64,
}

impl LogEntry {
    fn write(self) {
        let r = &self.request;
        tracing::info!(
            target: "thruster",
            path = %r.path, status = self.status, dur = r.started.elapsed().as_millis() as u64, method = %r.method,
            req_content_length = r.req_content_length, req_content_type = %r.req_content_type,
            resp_content_length = self.bytes, resp_content_type = %self.resp_content_type,
            remote_addr = %r.remote_addr, user_agent = %r.user_agent, cache = %self.cache, query = %r.query, proto = r.proto,
            "Request"
        );
    }
}

/// Counts the bytes sent and logs the request when the body ends (or the client goes away).
struct LoggedBody {
    inner: Body,
    entry: Option<LogEntry>,
}

impl hyper::body::Body for LoggedBody {
    type Data = Bytes;
    type Error = axum::Error;

    fn poll_frame(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Result<Frame<Bytes>, axum::Error>>> {
        let polled = Pin::new(&mut self.inner).poll_frame(cx);
        if let Poll::Ready(Some(Ok(frame))) = &polled
            && let (Some(data), Some(entry)) = (frame.data_ref(), self.entry.as_mut())
        {
            entry.bytes += data.len() as u64;
        }
        if (matches!(polled, Poll::Ready(None)) || self.inner.is_end_stream())
            && let Some(entry) = self.entry.take()
        {
            entry.write();
        }
        polled
    }

    fn is_end_stream(&self) -> bool {
        self.inner.is_end_stream()
    }

    fn size_hint(&self) -> SizeHint {
        self.inner.size_hint()
    }
}

impl Drop for LoggedBody {
    fn drop(&mut self) {
        if let Some(entry) = self.entry.take() {
            entry.write();
        }
    }
}
