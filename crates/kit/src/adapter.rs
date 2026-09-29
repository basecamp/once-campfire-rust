//! The only place Axum shows through: route handlers that build a [`Ctx`], run a plain
//! `async fn action(c: &mut Ctx) -> Result<Response>`, and turn the result into an HTTP response;
//! plus the outer middleware that must run before routing (`Rack::MethodOverride`,
//! `ActionDispatch::SSL`, `ActionDispatch::RequestId`).

use std::net::SocketAddr;

use axum::Router;
use axum::body::{Body as AxumBody, Bytes};
use axum::extract::{ConnectInfo, FromRequestParts, RawPathParams, State};
use axum::handler::Handler;
use axum::http::header::{self, HeaderValue};
use axum::http::{Method, StatusCode};
use axum::middleware::Next;
use axum::routing::MethodRouter;
use futures_util::FutureExt;
use futures_util::future::BoxFuture;

use crate::app::Kit;
use crate::body::{self, ParsedBody};
use crate::cookies::CookieJar;
use crate::ctx::Ctx;
use crate::format::{self, NegotiationInput};
use crate::params::{self, Param, ParamMap};
use crate::request::{self, Request};
use crate::response::{Body, Response};
use crate::{Error, Result};

/// An action: any `async fn(&mut Ctx) -> Result<Response>`.
pub trait ActionFn<'a>: Send + Sync + 'static {
    type Fut: Future<Output = Result<Response>> + Send + 'a;
    fn call(&self, c: &'a mut Ctx) -> Self::Fut;
}

impl<'a, F, Fut> ActionFn<'a> for F
where
    F: Fn(&'a mut Ctx) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = Result<Response>> + Send + 'a,
{
    type Fut = Fut;

    fn call(&self, c: &'a mut Ctx) -> Fut {
        self(c)
    }
}

/// An Axum handler running one action.
#[derive(Clone)]
pub struct ActionHandler<F>(F);

/// Wrap an action for `axum::routing` (`get(action(rooms::show))`).
pub fn action<F>(f: F) -> ActionHandler<F>
where
    F: for<'a> ActionFn<'a> + Clone,
{
    ActionHandler(f)
}

#[doc(hidden)]
pub struct ActionMarker;

impl<F> Handler<ActionMarker, Kit> for ActionHandler<F>
where
    F: for<'a> ActionFn<'a> + Clone,
{
    type Future = BoxFuture<'static, axum::response::Response>;

    fn call(self, req: axum::extract::Request, kit: Kit) -> Self::Future {
        Box::pin(async move { dispatch(kit, req, self.0).await })
    }
}

macro_rules! method_fns {
    ($($name:ident),*) => {$(
        #[doc = concat!("`axum::routing::", stringify!($name), "` for an action.")]
        pub fn $name<F>(f: F) -> MethodRouter<Kit>
        where
            F: for<'a> ActionFn<'a> + Clone,
        {
            axum::routing::$name(action(f))
        }
    )*};
}

method_fns!(get, post, put, patch, delete);

/// The method on the wire when `_method` overrode it (`rack.methodoverride.original_method`).
#[derive(Debug, Clone)]
pub struct OriginalMethod(pub Method);

/// The request id (`ActionDispatch::RequestId`), for logging.
#[derive(Debug, Clone)]
pub struct RequestId(pub String);

async fn dispatch<F>(kit: Kit, req: axum::extract::Request, action: F) -> axum::response::Response
where
    F: for<'a> ActionFn<'a>,
{
    let (mut parts, body) = req.into_parts();
    let path_params = RawPathParams::from_request_parts(&mut parts, &kit).await.map(|raw| {
        raw.iter().map(|(k, v)| (k.to_string(), Param::Str(v.to_string()))).collect::<ParamMap>()
    });
    let original_method = parts.extensions.get::<OriginalMethod>().map(|m| m.0.clone()).unwrap_or(parts.method.clone());
    let parsed = match parts.extensions.remove::<ParsedBody>() {
        Some(parsed) => Ok(parsed),
        None => body::parse(&original_method, &parts.headers, body, kit.config().max_body_bytes).await,
    };
    let peer = parts.extensions.get::<ConnectInfo<SocketAddr>>().map(|c| c.0.ip());
    let head = parts.method == Method::HEAD;

    let (raw, body_params) = match parsed {
        Ok(ParsedBody { raw, params }) => (raw, params.map_err(Error::from)),
        Err(error) => (Bytes::new(), Err(Error::Status(error.status()))),
    };
    let request = Request::new(parts.method, original_method, parts.uri, parts.headers, peer, raw, &kit.config().proxy);
    let query_params = params::from_query_string(request.query_string()).map_err(Error::from);
    let cookies = CookieJar::from_headers(
        request.headers.get_all(header::COOKIE).iter().filter_map(|v| v.to_str().ok()),
        kit.crypto().clone(),
        kit.clock().clone(),
    );

    let failure = match (&path_params, &query_params, &body_params) {
        (Err(_), _, _) => Some(Error::BadRequest("Invalid path parameters".into())),
        (_, Err(e), _) | (_, _, Err(e)) => Some(clone_error(e)),
        _ => None,
    };
    let mut ctx = Ctx::new(
        kit,
        request,
        path_params.unwrap_or_default(),
        query_params.unwrap_or_default(),
        body_params.unwrap_or_default(),
        cookies,
    );
    let result = match failure {
        Some(error) => Err(error),
        None => {
            let future: BoxFuture<'_, Result<Response>> = Box::pin(action.call(&mut ctx));
            // A panicking action is an exception like any other: Rails' `ShowExceptions` answers
            // 500 with `public/500.html`, where an unwinding handler would drop the connection.
            std::panic::AssertUnwindSafe(future).catch_unwind().await.unwrap_or_else(|panic| Err(panic_error(panic)))
        }
    };
    into_axum(ctx.finish(result), head).await
}

fn panic_error(panic: Box<dyn std::any::Any + Send>) -> Error {
    let message = panic
        .downcast_ref::<String>()
        .map(String::as_str)
        .or_else(|| panic.downcast_ref::<&str>().copied())
        .unwrap_or("panic");
    Error::internal(anyhow::anyhow!("action panicked: {message}"))
}

fn clone_error(error: &Error) -> Error {
    match error {
        Error::Status(status) => Error::Status(*status),
        other => Error::BadRequest(other.to_string()),
    }
}

/// Hand a finished response to hyper, streaming files and dropping HEAD bodies (`Rack::Head`)
/// while keeping their `Content-Length`.
pub async fn into_axum(response: Response, head: bool) -> axum::response::Response {
    let Response { status, mut headers, body, page_parts, .. } = response;
    let app_set_length = headers.contains_key(header::CONTENT_LENGTH);
    let body = match body {
        Body::Empty => AxumBody::empty(),
        Body::Bytes(bytes) => {
            headers.insert(header::CONTENT_LENGTH, HeaderValue::from(bytes.len()));
            AxumBody::from(bytes)
        }
        Body::Stream(stream) => stream,
        Body::File(file) => {
            headers.insert(header::CONTENT_LENGTH, HeaderValue::from(file.len));
            if head {
                AxumBody::empty()
            } else {
                match file_stream(&file).await {
                    Ok(body) => body,
                    Err(error) => {
                        tracing::error!(%error, path = %file.path.display(), "send_file failed");
                        let mut response = axum::response::Response::new(AxumBody::empty());
                        *response.status_mut() = StatusCode::INTERNAL_SERVER_ERROR;
                        return response;
                    }
                }
            }
        }
    };
    let body = if head { AxumBody::empty() } else { body };
    let mut response = axum::response::Response::new(body);
    *response.status_mut() = status;
    *response.headers_mut() = headers;
    if app_set_length {
        response.extensions_mut().insert(crate::deflater::AppContentLength);
    }
    if let Some(parts) = page_parts {
        response.extensions_mut().insert(parts);
    }
    response
}

async fn file_stream(file: &crate::response::FileBody) -> std::io::Result<AxumBody> {
    use tokio::io::{AsyncReadExt, AsyncSeekExt};
    let mut handle = tokio::fs::File::open(&file.path).await?;
    handle.seek(std::io::SeekFrom::Start(file.offset)).await?;
    let reader = handle.take(file.len);
    let stream = futures_util::stream::unfold(Some(reader), |state| async move {
        let mut reader = state?;
        let mut buf = vec![0u8; 64 * 1024];
        match reader.read(&mut buf).await {
            Ok(0) => None,
            Ok(n) => {
                buf.truncate(n);
                Some((Ok::<_, std::io::Error>(Bytes::from(buf)), Some(reader)))
            }
            Err(error) => Some((Err(error), None)),
        }
    });
    Ok(AxumBody::from_stream(stream))
}

/// `Rack::MethodOverride::HTTP_METHODS`
const OVERRIDABLE_METHODS: [&str; 9] = ["GET", "HEAD", "PUT", "POST", "DELETE", "OPTIONS", "PATCH", "LINK", "UNLINK"];

/// Middleware that runs before routing: request id, forced SSL, and `_method` override (which
/// needs the parsed form body, so POST bodies are parsed here and handed on).
pub async fn rails_middleware(State(kit): State<Kit>, req: axum::extract::Request, next: Next) -> axum::response::Response {
    let started = std::time::Instant::now();
    let request_id = request_id(req.headers().get("x-request-id").and_then(|v| v.to_str().ok()));
    let config = kit.config();
    let ssl = config.proxy.assume_ssl || request::scheme_is_https(req.headers(), req.uri());

    let mut response = if config.force_ssl && !ssl {
        redirect_to_https(&req)
    } else {
        match method_override(&kit, req).await {
            Ok(req) => {
                let mut req = req;
                req.extensions_mut().insert(RequestId(request_id.clone()));
                next.run(req).await
            }
            Err(response) => *response,
        }
    };

    // `Rack::Runtime` and `ActionDispatch::RequestId` sit below `ActionDispatch::Static`: public
    // files get neither header.
    if response.extensions().get::<crate::deflater::StaticFile>().is_none() {
        if let Ok(value) = HeaderValue::from_str(&request_id) {
            response.headers_mut().insert("x-request-id", value);
        }
        if !response.headers().contains_key("x-runtime") {
            let runtime = format!("{:.6}", started.elapsed().as_secs_f64());
            response.headers_mut().insert("x-runtime", HeaderValue::from_str(&runtime).unwrap());
        }
    }
    if config.force_ssl && ssl {
        if let Ok(value) = HeaderValue::from_str(&config.hsts) {
            response.headers_mut().insert(header::STRICT_TRANSPORT_SECURITY, value);
        }
        flag_cookies_as_secure(response.headers_mut());
    }
    response
}

async fn method_override(
    kit: &Kit,
    req: axum::extract::Request,
) -> std::result::Result<axum::extract::Request, Box<axum::response::Response>> {
    if req.method() != Method::POST {
        return Ok(req);
    }
    let media = request::media_type(req.headers().get(header::CONTENT_TYPE).and_then(|v| v.to_str().ok()));
    let form_data = match media.as_deref() {
        None => true,
        Some(media) => ["application/x-www-form-urlencoded", "multipart/form-data", "multipart/related", "multipart/mixed"]
            .contains(&media),
    };
    let (mut parts, body) = req.into_parts();
    // Only form data can carry `_method`, so other bodies (JSON, a raw upload) are left for the
    // action to read, rather than parsed here for every POST, before routing.
    let (parsed, from_param, body) = if form_data {
        let parsed = match body::parse(&Method::POST, &parts.headers, body, kit.config().max_body_bytes).await {
            Ok(parsed) => parsed,
            Err(error) => {
                let mut response = axum::response::Response::new(AxumBody::empty());
                *response.status_mut() = error.status();
                return Err(Box::new(response));
            }
        };
        let from_param = parsed.params.as_ref().ok().and_then(|p| p.str("_method")).map(str::to_string);
        (Some(parsed), from_param, AxumBody::empty())
    } else {
        (None, None, body)
    };
    let from_header = || parts.headers.get("x-http-method-override").and_then(|v| v.to_str().ok()).map(str::to_string);
    if let Some(method) = from_param.or_else(from_header).map(|m| m.to_uppercase())
        && OVERRIDABLE_METHODS.contains(&method.as_str())
            && let Ok(method) = Method::from_bytes(method.as_bytes()) {
                parts.extensions.insert(OriginalMethod(Method::POST));
                parts.method = method;
            }
    if let Some(parsed) = parsed {
        parts.extensions.insert(parsed);
    }
    Ok(axum::extract::Request::from_parts(parts, body))
}

/// `ActionDispatch::RequestId#make_request_id`
fn request_id(incoming: Option<&str>) -> String {
    match incoming.filter(|id| !id.trim().is_empty()) {
        Some(id) => id.chars().filter(|c| c.is_alphanumeric() || *c == '_' || *c == '-' || *c == '@').take(255).collect(),
        None => uuid::Uuid::new_v4().to_string(),
    }
}

/// `ActionDispatch::SSL#redirect_to_https`: 301 for GET/HEAD, 308 otherwise.
fn redirect_to_https(req: &axum::extract::Request) -> axum::response::Response {
    let host = req
        .headers()
        .get("x-forwarded-host")
        .or_else(|| req.headers().get(header::HOST))
        .and_then(|v| v.to_str().ok())
        .map(|h| h.rsplit(',').next().unwrap_or(h).trim().to_string())
        .unwrap_or_else(|| "localhost".into());
    let host = host.rsplit_once(':').filter(|(_, p)| p.bytes().all(|b| b.is_ascii_digit())).map(|(h, _)| h.to_string()).unwrap_or(host);
    let path = req.uri().path_and_query().map(|p| p.as_str()).unwrap_or("/");
    let status = if matches!(*req.method(), Method::GET | Method::HEAD) {
        StatusCode::MOVED_PERMANENTLY
    } else {
        StatusCode::PERMANENT_REDIRECT
    };
    let mut response = axum::response::Response::new(AxumBody::empty());
    *response.status_mut() = status;
    response.headers_mut().insert(header::CONTENT_TYPE, HeaderValue::from_static("text/html"));
    if let Ok(location) = HeaderValue::from_str(&format!("https://{host}{path}")) {
        response.headers_mut().insert(header::LOCATION, location);
    }
    response
}

/// `ActionDispatch::SSL#flag_cookies_as_secure!`
fn flag_cookies_as_secure(headers: &mut axum::http::HeaderMap) {
    let cookies: Vec<String> =
        headers.get_all(header::SET_COOKIE).iter().filter_map(|v| v.to_str().ok()).map(str::to_string).collect();
    if cookies.is_empty() {
        return;
    }
    headers.remove(header::SET_COOKIE);
    for cookie in cookies {
        let secure = cookie.split(';').skip(1).any(|attr| attr.trim().eq_ignore_ascii_case("secure"));
        let cookie = if secure { cookie } else { format!("{cookie}; secure") };
        if let Ok(value) = HeaderValue::from_str(&cookie) {
            headers.append(header::SET_COOKIE, value);
        }
    }
}

/// Rails' answer for an unmatched route (`ActionController::RoutingError`): the public 404 page.
pub async fn not_found(State(kit): State<Kit>, req: axum::extract::Request) -> axum::response::Response {
    let input = NegotiationInput {
        format_param: None,
        accept: req.headers().get(header::ACCEPT).and_then(|v| v.to_str().ok()),
        content_type: req.headers().get(header::CONTENT_TYPE).and_then(|v| v.to_str().ok()),
        path: req.uri().path(),
        xhr: req
            .headers()
            .get("x-requested-with")
            .and_then(|v| v.to_str().ok())
            .is_some_and(|v| v.eq_ignore_ascii_case("xmlhttprequest")),
    };
    let format = format::formats(&input).ok().and_then(|f| f.first().copied());
    let head = req.method() == Method::HEAD;
    into_axum(crate::exceptions::render(kit.error_pages(), StatusCode::NOT_FOUND, format, head), head).await
}

/// Finish an app router: Rails-style 404s for unknown paths *and* unknown methods (Axum would say
/// 405), the Kit state, the pre-routing middleware, and the configured request timeout.
pub fn app(router: Router<Kit>, kit: Kit) -> Router {
    let routed = router.fallback(not_found).method_not_allowed_fallback(not_found).with_state(kit.clone());
    let mut app = Router::new().fallback_service(routed).layer(axum::middleware::from_fn_with_state(kit.clone(), rails_middleware));
    if let Some(timeout) = kit.config().request_timeout {
        app = app.layer(tower_http::timeout::TimeoutLayer::with_status_code(StatusCode::REQUEST_TIMEOUT, timeout));
    }
    app
}
