//! End-to-end behavior of the HTTP layer through a real Axum router (`tower::ServiceExt::oneshot`).

use std::net::SocketAddr;
use std::sync::{Arc, LazyLock};

use axum::Router;
use axum::body::Body as AxumBody;
use axum::extract::ConnectInfo;
use axum::http::{Request as HttpRequest, header};
use campfire_kit::exceptions::ErrorPages;
use campfire_kit::format::{HTML, JSON, TURBO_STREAM};
use campfire_kit::{
    Cookie, Ctx, ExpiresIn, Freshness, Kit, KitConfig, Redirect, Result, SendOptions, SharedClock, StatusCode, TestClock, action, front,
    halt,
};
use rails_compat::Secrets;
use serde_json::json;
use tower::ServiceExt;

#[derive(Debug)]
struct AppState {
    name: &'static str,
}

#[derive(Clone)]
struct CurrentUser(String);

async fn show(c: &mut Ctx) -> Result {
    let id = c.param_str("id").unwrap_or("").to_string();
    let name = c.state::<AppState>().name;
    Ok(c.html(format!("<p>{name} room {id}</p>")))
}

async fn form(c: &mut Ctx) -> Result {
    Ok(c.html("<form method=\"post\" action=\"/form\"></form>"))
}

async fn create(c: &mut Ctx) -> Result {
    c.verify_authenticity_token()?;
    let body = json!({
        "method": c.request.method.as_str(),
        "original_method": c.request.original_method.as_str(),
        "params": c.params.to_json(),
    });
    c.json(StatusCode::OK, &body)
}

async fn echo(c: &mut Ctx) -> Result {
    let body = json!({
        "method": c.request.method.as_str(),
        "original_method": c.request.original_method.as_str(),
        "params": c.params.to_json(),
        "raw": String::from_utf8_lossy(c.request.raw_post()),
        "remote_ip": c.request.remote_ip()?,
    });
    c.json(StatusCode::OK, &body)
}

async fn upload(c: &mut Ctx) -> Result {
    let user = c.params.require("user")?.clone();
    let file = user.get("avatar").and_then(|p| p.as_file()).cloned();
    let body = json!({
        "method": c.request.method.as_str(),
        "name": user.get("name").and_then(|p| p.as_str()),
        "filename": file.as_ref().map(|f| f.original_filename.clone()),
        "content": file.map(|f| String::from_utf8(f.read().unwrap()).unwrap()),
    });
    c.json(StatusCode::OK, &body)
}

async fn session_set(c: &mut Ctx) -> Result {
    let value = c.param_str("value").unwrap_or("").to_string();
    c.session().insert("return_to_after_authenticating", value);
    Ok(c.head(StatusCode::OK))
}

async fn session_get(c: &mut Ctx) -> Result {
    let session = c.session();
    let body = json!({ "value": session.get("return_to_after_authenticating"), "id": session.id() });
    c.json(StatusCode::OK, &body)
}

async fn session_reset(c: &mut Ctx) -> Result {
    c.reset_session();
    Ok(c.head(StatusCode::OK))
}

async fn noop(c: &mut Ctx) -> Result {
    Ok(c.head(StatusCode::NO_CONTENT))
}

async fn notice(c: &mut Ctx) -> Result {
    c.redirect_to_with("/flash", Redirect { notice: Some("✓".into()), ..Redirect::default() })
}

async fn show_flash(c: &mut Ctx) -> Result {
    let notice = c.flash().notice().map(str::to_string);
    c.json(StatusCode::OK, &json!({ "notice": notice }))
}

async fn sign_in(c: &mut Ctx) -> Result {
    c.cookies.set_signed("session_token", Cookie::new("tok123").permanent().httponly())?;
    c.cookies.set("last_room", Cookie::new("7").permanent());
    Ok(c.head(StatusCode::OK))
}

/// `sign_in` from an `ActionController::Live` controller.
async fn live_sign_in(c: &mut Ctx) -> Result {
    c.use_live_response();
    sign_in(c).await
}

async fn whoami(c: &mut Ctx) -> Result {
    let token = c.cookies.signed("session_token");
    c.json(StatusCode::OK, &json!({ "token": token, "last_room": c.cookies.get("last_room") }))
}

async fn sign_out(c: &mut Ctx) -> Result {
    c.cookies.delete("session_token");
    Ok(c.head(StatusCode::OK))
}

fn require_user(c: &mut Ctx) -> Result<()> {
    match c.request.header("x-user") {
        Some(user) => {
            c.set_current(CurrentUser(user.to_string()));
            Ok(())
        }
        None => halt(c.redirect_to("/session/new")?),
    }
}

fn ensure_admin(c: &mut Ctx) -> Result<()> {
    if c.current::<CurrentUser>().is_some_and(|u| u.0 == "admin") { Ok(()) } else { halt(c.head(StatusCode::FORBIDDEN)) }
}

async fn admin(c: &mut Ctx) -> Result {
    c.set_header("x-version", "42");
    require_user(c)?;
    ensure_admin(c)?;
    let user = c.current::<CurrentUser>().unwrap().0.clone();
    Ok(c.html(format!("hi {user}")))
}

async fn messages(c: &mut Ctx) -> Result {
    let format = c.respond_to(&[&HTML, &TURBO_STREAM, &JSON])?;
    if format == &TURBO_STREAM {
        Ok(c.turbo_stream("<turbo-stream action=\"append\"></turbo-stream>"))
    } else if format == &JSON {
        c.json(StatusCode::OK, &json!([]))
    } else {
        Ok(c.render(StatusCode::OK, &HTML, "<p>messages</p>"))
    }
}

async fn autocomplete(c: &mut Ctx) -> Result {
    let format = c.respond_to(&[&HTML, &JSON])?;
    Ok(c.render(StatusCode::OK, format, "x"))
}

async fn redirects(c: &mut Ctx) -> Result {
    match c.param_str("to").unwrap_or("") {
        "relative" => c.redirect_to("rooms/1"),
        "other" => c.redirect_to("https://evil.example/"),
        "other_allowed" => c.redirect_to_with("https://docs.example/", Redirect { allow_other_host: true, ..Redirect::default() }),
        "see_other" => c.redirect_to_with("/rooms", Redirect { status: Some(StatusCode::SEE_OTHER), ..Redirect::default() }),
        "back" => c.redirect_back_or_to("/fallback"),
        _ => c.redirect_to("/rooms/1?x=1"),
    }
}

async fn created(c: &mut Ctx) -> Result {
    c.head_with_location(StatusCode::CREATED, "/rooms/1/messages/2")
}

async fn file(c: &mut Ctx) -> Result {
    let path = c.param_str("path").unwrap().to_string();
    c.send_file(path, SendOptions::inline("image/png"))
}

async fn logo(c: &mut Ctx) -> Result {
    if c.stale(Freshness::etag("accounts/1-20240601")) {
        c.expires_in(300, ExpiresIn { public: true, stale_while_revalidate: Some(604800), ..ExpiresIn::default() });
        return Ok(c.send_data("PNG", SendOptions::inline("image/png")));
    }
    Ok(c.head(StatusCode::NOT_MODIFIED))
}

async fn index_fresh(c: &mut Ctx) -> Result {
    if let Some(not_modified) = c.fresh_when(Freshness::etag("messages/1-2")) {
        return Ok(not_modified);
    }
    Ok(c.html("<p>page</p>"))
}

fn kit_with(config: KitConfig) -> Kit {
    static SECRETS: LazyLock<Arc<Secrets>> = LazyLock::new(|| Arc::new(Secrets::new("test-secret")));
    let clock: SharedClock = Arc::new(TestClock::frozen_at("2024-06-01T12:00:00Z".parse().unwrap()));
    Kit::new(config, SECRETS.clone(), clock, AppState { name: "Campfire" })
}

fn app_with(config: KitConfig) -> Router {
    let router = Router::new()
        .route("/rooms/{id}", campfire_kit::get(show))
        .route("/form", campfire_kit::get(form).post(action(create)))
        .route("/echo/{id}", campfire_kit::get(echo).post(action(echo)).patch(action(echo)).delete(action(echo)))
        .route("/upload", campfire_kit::patch(upload).post(action(upload)))
        .route("/session", campfire_kit::get(session_get).post(action(session_set)).delete(action(session_reset)))
        .route("/noop", campfire_kit::get(noop))
        .route("/notice", campfire_kit::get(notice))
        .route("/flash", campfire_kit::get(show_flash))
        .route("/sign_in", campfire_kit::get(sign_in))
        .route("/live_sign_in", campfire_kit::get(live_sign_in))
        .route("/whoami", campfire_kit::get(whoami))
        .route("/sign_out", campfire_kit::get(sign_out))
        .route("/admin", campfire_kit::get(admin))
        .route("/messages", campfire_kit::get(messages).post(action(messages)))
        .route("/messages.{format}", campfire_kit::get(messages))
        .route("/autocomplete", campfire_kit::get(autocomplete))
        .route("/redirect", campfire_kit::get(redirects))
        .route("/created", campfire_kit::get(created))
        .route("/file", campfire_kit::get(file))
        .route("/logo", campfire_kit::get(logo))
        .route("/fresh", campfire_kit::get(index_fresh));
    campfire_kit::app(router, kit_with(config))
}

fn app() -> Router {
    let error_pages = ErrorPages::new([(404, "<h1>Not found</h1>".into()), (422, "<h1>Unprocessable</h1>".into())]);
    app_with(KitConfig { error_pages, ..KitConfig::default() })
}

struct Reply {
    status: StatusCode,
    headers: axum::http::HeaderMap,
    body: Vec<u8>,
}

impl Reply {
    fn header(&self, name: &str) -> Option<&str> {
        self.headers.get(name).and_then(|v| v.to_str().ok())
    }

    fn cookies(&self) -> Vec<String> {
        self.headers.get_all(header::SET_COOKIE).iter().map(|v| v.to_str().unwrap().to_string()).collect()
    }

    /// A `Cookie` request header carrying every cookie this response set.
    fn cookie_jar(&self) -> String {
        self.cookies().iter().map(|c| c.split(';').next().unwrap().to_string()).collect::<Vec<_>>().join("; ")
    }

    fn json(&self) -> serde_json::Value {
        serde_json::from_slice(&self.body).unwrap()
    }

    fn text(&self) -> String {
        String::from_utf8(self.body.clone()).unwrap()
    }
}

async fn send(app: &Router, request: HttpRequest<AxumBody>) -> Reply {
    let response = app.clone().oneshot(request).await.unwrap();
    let status = response.status();
    let headers = response.headers().clone();
    let body = axum::body::to_bytes(response.into_body(), usize::MAX).await.unwrap().to_vec();
    Reply { status, headers, body }
}

fn get(uri: &str) -> axum::http::request::Builder {
    HttpRequest::get(uri).header(header::HOST, "chat.example.com")
}

fn post(uri: &str) -> axum::http::request::Builder {
    HttpRequest::post(uri).header(header::HOST, "chat.example.com")
}

fn form_post(uri: &str, body: &str) -> HttpRequest<AxumBody> {
    post(uri).header(header::CONTENT_TYPE, "application/x-www-form-urlencoded").body(AxumBody::from(body.to_string())).unwrap()
}

#[tokio::test]
async fn an_action_panic_after_suspension_still_renders_the_error_page() {
    async fn panicking(_c: &mut Ctx) -> Result {
        tokio::task::yield_now().await;
        panic!("action failed after yielding");
    }
    let config = KitConfig { error_pages: ErrorPages::new([(500, "<h1>Internal error</h1>".into())]), ..KitConfig::default() };
    let router = Router::new().route("/panic", campfire_kit::get(panicking)).route("/rooms/{id}", campfire_kit::get(show));
    let app = campfire_kit::app(router, kit_with(config));
    let reply = send(&app, get("/panic").body(AxumBody::empty()).unwrap()).await;
    assert_eq!(reply.status, StatusCode::INTERNAL_SERVER_ERROR);
    assert_eq!(reply.text(), "<h1>Internal error</h1>");
    assert!(reply.header("x-request-id").is_some());
    let next = send(&app, get("/rooms/1").body(AxumBody::empty()).unwrap()).await;
    assert_eq!(next.status, StatusCode::OK);
}

#[tokio::test]
async fn renders_html_with_rails_headers() {
    let reply = send(&app(), get("/rooms/5").body(AxumBody::empty()).unwrap()).await;
    assert_eq!(reply.status, StatusCode::OK);
    assert_eq!(reply.text(), "<p>Campfire room 5</p>");
    assert_eq!(reply.header("content-type"), Some("text/html; charset=utf-8"));
    assert_eq!(reply.header("x-frame-options"), Some("SAMEORIGIN"));
    assert_eq!(reply.header("x-content-type-options"), Some("nosniff"));
    assert_eq!(reply.header("referrer-policy"), Some("strict-origin-when-cross-origin"));
    assert_eq!(reply.header("cache-control"), Some("max-age=0, private, must-revalidate"));
    assert!(reply.header("etag").unwrap().starts_with("W/\""));
    assert_eq!(reply.header("x-request-id").map(str::len), Some(36));
    assert!(reply.cookies().is_empty(), "no session touched, no cookie");
}

#[tokio::test]
async fn conditional_get_on_body_etag() {
    let app = app();
    let first = send(&app, get("/rooms/5").body(AxumBody::empty()).unwrap()).await;
    let etag = first.header("etag").unwrap().to_string();
    let second = send(&app, get("/rooms/5").header(header::IF_NONE_MATCH, &etag).body(AxumBody::empty()).unwrap()).await;
    assert_eq!(second.status, StatusCode::NOT_MODIFIED);
    assert!(second.body.is_empty());
    assert_eq!(second.header("content-type"), None);

    // Lists and `*` count, as they do for `fresh_when` (RFC 9110).
    for if_none_match in [format!("\"other\", {etag}"), "*".to_string()] {
        let listed = send(&app, get("/rooms/5").header(header::IF_NONE_MATCH, &if_none_match).body(AxumBody::empty()).unwrap()).await;
        assert_eq!(listed.status, StatusCode::NOT_MODIFIED, "{if_none_match}");
    }
    let other = send(&app, get("/rooms/5").header(header::IF_NONE_MATCH, "\"other\"").body(AxumBody::empty()).unwrap()).await;
    assert_eq!(other.status, StatusCode::OK);
}

#[tokio::test]
async fn head_requests_drop_the_body() {
    let reply = send(&app(), get("/rooms/5").method("HEAD").body(AxumBody::empty()).unwrap()).await;
    assert_eq!(reply.status, StatusCode::OK);
    assert!(reply.body.is_empty());
    assert_eq!(reply.header("content-length"), Some("22"));
}

#[tokio::test]
async fn unknown_routes_and_methods_are_rails_404s() {
    let app = app();
    let reply = send(&app, get("/nope").body(AxumBody::empty()).unwrap()).await;
    assert_eq!(reply.status, StatusCode::NOT_FOUND);
    assert_eq!(reply.text(), "<h1>Not found</h1>");
    let reply = send(&app, HttpRequest::put("/rooms/1").body(AxumBody::empty()).unwrap()).await;
    assert_eq!(reply.status, StatusCode::NOT_FOUND);
    let reply = send(&app, get("/nope.json").body(AxumBody::empty()).unwrap()).await;
    assert_eq!(reply.text(), r#"{"status":404,"error":"Not Found"}"#);
}

#[tokio::test]
async fn params_merge_query_over_body_and_path_over_both() {
    let reply = send(&app(), form_post("/echo/9?b=query&id=q", "a[b][]=1&a[b][]=2&b=body&id=body")).await;
    let json = reply.json();
    assert_eq!(json["params"], json!({"a": {"b": ["1", "2"]}, "b": "query", "id": "9"}));
    assert_eq!(json["raw"], "a[b][]=1&a[b][]=2&b=body&id=body");
}

#[tokio::test]
async fn malformed_params_are_400() {
    let reply = send(&app(), get("/echo/1?a=1&a[b]=2").body(AxumBody::empty()).unwrap()).await;
    assert_eq!(reply.status, StatusCode::BAD_REQUEST);
    let reply = send(&app(), form_post("/echo/1", "a=%")).await;
    assert_eq!(reply.status, StatusCode::BAD_REQUEST);
    let request = post("/echo/1").header(header::CONTENT_TYPE, "application/json").body(AxumBody::from("{nope")).unwrap();
    assert_eq!(send(&app(), request).await.status, StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn method_override_from_form_param_and_header() {
    let app = app();
    let reply = send(&app, form_post("/echo/1", "_method=patch&x=1")).await;
    assert_eq!(reply.json()["method"], "PATCH");
    assert_eq!(reply.json()["original_method"], "POST");

    let reply = send(&app, form_post("/echo/1", "_method=delete")).await;
    assert_eq!(reply.json()["method"], "DELETE");

    let request = post("/echo/1").header("x-http-method-override", "patch").body(AxumBody::from("")).unwrap();
    assert_eq!(send(&app, request).await.json()["method"], "PATCH");

    // JSON bodies aren't forms: `_method` there doesn't count.
    let request = post("/echo/1")
        .header(header::CONTENT_TYPE, "application/json")
        .body(AxumBody::from(r#"{"_method":"patch","a":[1,null,2]}"#))
        .unwrap();
    let reply = send(&app, request).await;
    assert_eq!(reply.json()["method"], "POST");
    assert_eq!(reply.json()["params"]["a"], json!([1, 2]));

    let reply = send(&app, form_post("/echo/1", "_method=bogus")).await;
    assert_eq!(reply.json()["method"], "POST");
}

#[tokio::test]
async fn multipart_uploads_with_method_override() {
    let boundary = "----campfire";
    let body = format!(
        "--{b}\r\nContent-Disposition: form-data; name=\"_method\"\r\n\r\npatch\r\n\
         --{b}\r\nContent-Disposition: form-data; name=\"user[name]\"\r\n\r\nJo\r\n\
         --{b}\r\nContent-Disposition: form-data; name=\"user[avatar]\"; filename=\"me.png\"\r\nContent-Type: image/png\r\n\r\nPNGDATA\r\n\
         --{b}--\r\n",
        b = boundary
    );
    let request = post("/upload")
        .header(header::CONTENT_TYPE, format!("multipart/form-data; boundary={boundary}"))
        .body(AxumBody::from(body))
        .unwrap();
    let reply = send(&app(), request).await;
    assert_eq!(reply.json(), json!({"method": "PATCH", "name": "Jo", "filename": "me.png", "content": "PNGDATA"}));
}

#[tokio::test]
async fn missing_required_param_is_400() {
    let reply = send(&app(), form_post("/upload", "other=1")).await;
    assert_eq!(reply.status, StatusCode::BAD_REQUEST);
}

/// The test app as Campfire runs it in production: behind TLS (`assume_ssl`) with `force_ssl`.
fn ssl_app() -> Router {
    let error_pages = ErrorPages::new([(422, "<h1>Unprocessable</h1>".into())]);
    let mut config = KitConfig { error_pages, force_ssl: true, ..KitConfig::default() };
    config.proxy.assume_ssl = true;
    app_with(config)
}

fn post_from(site: Option<&str>) -> HttpRequest<AxumBody> {
    let request = post("/form");
    let request = match site {
        Some(site) => request.header("sec-fetch-site", site),
        None => request,
    };
    request.header(header::CONTENT_TYPE, "application/x-www-form-urlencoded").body(AxumBody::from("x=1")).unwrap()
}

#[tokio::test]
async fn forgery_protection_trusts_same_site_requests_by_sec_fetch_site() {
    let app = ssl_app();
    for site in ["same-origin", "same-site"] {
        let ok = send(&app, post_from(Some(site))).await;
        assert_eq!(ok.status, StatusCode::OK, "{site}");
        assert_eq!(ok.json()["params"]["x"], "1");
    }
    for site in [Some("cross-site"), Some("none"), Some("bogus"), None] {
        let forged = send(&app, post_from(site)).await;
        assert_eq!(forged.status, StatusCode::UNPROCESSABLE_ENTITY, "{site:?}");
        assert_eq!(forged.text(), "<h1>Unprocessable</h1>");
        assert!(forged.cookies().is_empty(), "errors don't commit cookies");
    }
}

#[tokio::test]
async fn forgery_protection_allows_a_missing_header_only_without_ssl() {
    // Browsers send `Sec-Fetch-Site` only to secure origins, so plain HTTP can't require it.
    let app = app();
    assert_eq!(send(&app, post_from(None)).await.status, StatusCode::OK);
    for site in ["cross-site", "none", "bogus", "", "Same-Origin", "same-origin, same-site"] {
        assert_eq!(send(&app, post_from(Some(site))).await.status, StatusCode::UNPROCESSABLE_ENTITY, "{site:?}");
    }
}

#[tokio::test]
async fn pages_carry_no_forgery_token_or_session() {
    let page = send(&app(), get("/form").body(AxumBody::empty()).unwrap()).await;
    assert!(!page.text().contains("authenticity_token"));
    assert!(page.cookies().is_empty(), "rendering a form doesn't start a session");
}

#[tokio::test]
async fn forgery_protection_checks_the_origin() {
    let app = ssl_app();
    let with_origin = |origin: &str| {
        post("/form").header("sec-fetch-site", "same-origin").header(header::ORIGIN, origin).body(AxumBody::empty()).unwrap()
    };
    assert_eq!(send(&app, with_origin("https://chat.example.com")).await.status, StatusCode::OK);
    assert_eq!(send(&app, with_origin("https://evil.example")).await.status, StatusCode::UNPROCESSABLE_ENTITY);
    assert_eq!(send(&app, with_origin("null")).await.status, StatusCode::UNPROCESSABLE_ENTITY);
}

#[tokio::test]
async fn session_cookie_is_written_only_when_the_session_changes() {
    let app = app();
    let untouched = send(&app, get("/noop").body(AxumBody::empty()).unwrap()).await;
    assert!(untouched.cookies().is_empty());

    let set = send(&app, form_post("/session", "value=%2Frooms%2F1")).await;
    assert_eq!(set.status, StatusCode::OK);
    let cookie = set.cookie_jar();
    assert!(cookie.starts_with("_campfire_session="));

    // Reading it, or not touching it, sends no cookie back: the one the browser has stays.
    let read = send(&app, get("/session").header(header::COOKIE, &cookie).body(AxumBody::empty()).unwrap()).await;
    assert_eq!(read.json()["value"], "/rooms/1");
    assert!(read.cookies().is_empty(), "{:?}", read.cookies());
    let id = read.json()["id"].as_str().unwrap().to_string();
    assert_eq!(id.len(), 32);
    let noop = send(&app, get("/noop").header(header::COOKIE, &cookie).body(AxumBody::empty()).unwrap()).await;
    assert!(noop.cookies().is_empty());
    let same = form_post("/session", "value=%2Frooms%2F1");
    let (mut parts, body) = same.into_parts();
    parts.headers.insert(header::COOKIE, cookie.parse().unwrap());
    let same = send(&app, HttpRequest::from_parts(parts, body)).await;
    assert!(same.cookies().is_empty(), "writing the value it already holds changes nothing");
    let again = send(&app, get("/session").header(header::COOKIE, &cookie).body(AxumBody::empty()).unwrap()).await;
    assert_eq!(again.json()["id"], id.as_str());

    // Resetting leaves nothing to keep, so the cookie goes.
    let reset = send(&app, HttpRequest::delete("/session").header(header::COOKIE, &cookie).body(AxumBody::empty()).unwrap()).await;
    assert!(reset.cookies().iter().any(|c| c.starts_with("_campfire_session=;")), "{:?}", reset.cookies());
    let after = send(&app, get("/session").header(header::COOKIE, reset.cookie_jar()).body(AxumBody::empty()).unwrap()).await;
    assert_eq!(after.json()["value"], serde_json::Value::Null);
    assert_ne!(after.json()["id"], id.as_str());

    // A tampered cookie reads as an empty session.
    let bogus = send(&app, get("/session").header(header::COOKIE, "_campfire_session=garbage").body(AxumBody::empty()).unwrap()).await;
    assert_eq!(bogus.json()["value"], serde_json::Value::Null);
}

#[tokio::test]
async fn flash_survives_exactly_one_redirect() {
    let app = app();
    let redirect = send(&app, get("/notice").body(AxumBody::empty()).unwrap()).await;
    assert_eq!(redirect.status, StatusCode::FOUND);
    assert_eq!(redirect.header("location"), Some("http://chat.example.com/flash"));
    let cookie = redirect.cookie_jar();

    let shown = send(&app, get("/flash").header(header::COOKIE, &cookie).body(AxumBody::empty()).unwrap()).await;
    assert_eq!(shown.json()["notice"], "✓");
    // Shown, the flash is swept, and with it the only thing the session held.
    assert!(shown.cookies().iter().any(|c| c.starts_with("_campfire_session=;")), "{:?}", shown.cookies());
    let gone = send(&app, get("/flash").header(header::COOKIE, shown.cookie_jar()).body(AxumBody::empty()).unwrap()).await;
    assert_eq!(gone.json()["notice"], serde_json::Value::Null);
}

#[tokio::test]
async fn signed_permanent_cookies_and_deletion() {
    let app = app();
    let signed_in = send(&app, get("/sign_in").body(AxumBody::empty()).unwrap()).await;
    let cookies = signed_in.cookies();
    assert!(
        cookies
            .iter()
            .any(|c| c.starts_with("session_token=")
                && c.ends_with("; path=/; expires=Wed, 01 Jun 2044 12:00:00 GMT; httponly; samesite=lax"))
    );
    assert!(cookies.contains(&"last_room=7; path=/; expires=Wed, 01 Jun 2044 12:00:00 GMT; samesite=lax".to_string()));

    // Rails' Live responses send the action's cookies twice; this sends them once.
    let live = send(&app, get("/live_sign_in").body(AxumBody::empty()).unwrap()).await;
    assert_eq!(live.cookies().len(), 2, "{:?}", live.cookies());

    let jar = signed_in.cookie_jar();
    let me = send(&app, get("/whoami").header(header::COOKIE, &jar).body(AxumBody::empty()).unwrap()).await;
    assert_eq!(me.json(), json!({"token": "tok123", "last_room": "7"}));

    let forged = send(&app, get("/whoami").header(header::COOKIE, "session_token=tok123").body(AxumBody::empty()).unwrap()).await;
    assert_eq!(forged.json()["token"], serde_json::Value::Null);

    let out = send(&app, get("/sign_out").header(header::COOKIE, &jar).body(AxumBody::empty()).unwrap()).await;
    assert_eq!(out.cookies(), vec!["session_token=; path=/; max-age=0; expires=Thu, 01 Jan 1970 00:00:00 GMT; samesite=lax"]);
    let nothing = send(&app, get("/sign_out").body(AxumBody::empty()).unwrap()).await;
    assert!(nothing.cookies().is_empty());
}

#[tokio::test]
async fn before_actions_halt_the_chain() {
    let app = app();
    let anonymous = send(&app, get("/admin").body(AxumBody::empty()).unwrap()).await;
    assert_eq!(anonymous.status, StatusCode::FOUND);
    assert_eq!(anonymous.header("location"), Some("http://chat.example.com/session/new"));
    assert_eq!(anonymous.header("x-version"), Some("42"), "headers set before the halt survive");

    let member = send(&app, get("/admin").header("x-user", "jo").body(AxumBody::empty()).unwrap()).await;
    assert_eq!(member.status, StatusCode::FORBIDDEN);
    assert_eq!(member.header("content-type"), Some("text/html"));
    assert!(member.body.is_empty());

    let admin = send(&app, get("/admin").header("x-user", "admin").body(AxumBody::empty()).unwrap()).await;
    assert_eq!(admin.text(), "hi admin");
}

#[tokio::test]
async fn respond_to_negotiates_like_rails() {
    let app = app();
    let browser = "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8";
    let html = send(&app, get("/messages").header(header::ACCEPT, browser).body(AxumBody::empty()).unwrap()).await;
    assert_eq!(html.header("content-type"), Some("text/html; charset=utf-8"));

    let turbo = "text/vnd.turbo-stream.html, text/html, application/xhtml+xml";
    let stream = send(&app, post("/messages").header(header::ACCEPT, turbo).body(AxumBody::empty()).unwrap()).await;
    assert_eq!(stream.header("content-type"), Some("text/vnd.turbo-stream.html; charset=utf-8"));

    let json = send(&app, get("/messages.json").body(AxumBody::empty()).unwrap()).await;
    assert_eq!(json.header("content-type"), Some("application/json; charset=utf-8"));
    let json = send(&app, get("/messages?format=json").body(AxumBody::empty()).unwrap()).await;
    assert_eq!(json.header("content-type"), Some("application/json; charset=utf-8"));

    let unacceptable = send(&app, get("/messages").header(header::ACCEPT, "image/png").body(AxumBody::empty()).unwrap()).await;
    assert_eq!(unacceptable.status, StatusCode::NOT_ACCEPTABLE);
    let invalid = send(&app, get("/messages").header(header::ACCEPT, "garbage").body(AxumBody::empty()).unwrap()).await;
    assert_eq!(invalid.status, StatusCode::NOT_ACCEPTABLE);

    let any = send(&app, get("/autocomplete").header(header::ACCEPT, "*/*").body(AxumBody::empty()).unwrap()).await;
    assert_eq!(any.header("content-type"), Some("text/html; charset=utf-8"));
}

#[tokio::test]
async fn redirects_like_rails() {
    let app = app();
    let plain = send(&app, get("/redirect").body(AxumBody::empty()).unwrap()).await;
    assert_eq!(plain.status, StatusCode::FOUND);
    assert_eq!(plain.header("location"), Some("http://chat.example.com/rooms/1?x=1"));
    assert_eq!(plain.header("content-type"), Some("text/html; charset=utf-8"));
    assert_eq!(plain.header("cache-control"), Some("no-cache"));

    let see_other = send(&app, get("/redirect?to=see_other").body(AxumBody::empty()).unwrap()).await;
    assert_eq!(see_other.status, StatusCode::SEE_OTHER);

    assert_eq!(send(&app, get("/redirect?to=relative").body(AxumBody::empty()).unwrap()).await.status, StatusCode::INTERNAL_SERVER_ERROR);
    assert_eq!(send(&app, get("/redirect?to=other").body(AxumBody::empty()).unwrap()).await.status, StatusCode::INTERNAL_SERVER_ERROR);
    let allowed = send(&app, get("/redirect?to=other_allowed").body(AxumBody::empty()).unwrap()).await;
    assert_eq!(allowed.header("location"), Some("https://docs.example/"));

    let back =
        send(&app, get("/redirect?to=back").header(header::REFERER, "http://chat.example.com/rooms/3").body(AxumBody::empty()).unwrap())
            .await;
    assert_eq!(back.header("location"), Some("http://chat.example.com/rooms/3"));
    let foreign =
        send(&app, get("/redirect?to=back").header(header::REFERER, "https://evil.example/x").body(AxumBody::empty()).unwrap()).await;
    assert_eq!(foreign.header("location"), Some("http://chat.example.com/fallback"));
}

#[tokio::test]
async fn head_with_location() {
    let reply = send(&app(), get("/created").body(AxumBody::empty()).unwrap()).await;
    assert_eq!(reply.status, StatusCode::CREATED);
    assert_eq!(reply.header("location"), Some("http://chat.example.com/rooms/1/messages/2"));
    assert_eq!(reply.header("content-type"), Some("text/html"));
}

#[tokio::test]
async fn send_file_with_disposition_and_the_whole_file() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("logo.png");
    std::fs::write(&path, b"0123456789").unwrap();
    let encoded = campfire_kit::cookies::escape(path.to_str().unwrap());
    let app = app();

    let whole = send(&app, get(&format!("/file?path={encoded}")).body(AxumBody::empty()).unwrap()).await;
    assert_eq!(whole.body, b"0123456789");
    assert_eq!(whole.header("content-type"), Some("image/png"));
    assert_eq!(whole.header("content-disposition"), Some("inline; filename=\"logo.png\"; filename*=UTF-8''logo.png"));
    assert_eq!(whole.header("content-transfer-encoding"), Some("binary"));
    assert_eq!(whole.header("content-length"), Some("10"));

    // Rails' `send_file` ignores Range.
    let ignored =
        send(&app, get(&format!("/file?path={encoded}")).header(header::RANGE, "bytes=2-4").body(AxumBody::empty()).unwrap()).await;
    assert_eq!(ignored.status, StatusCode::OK);
    assert_eq!(ignored.body, b"0123456789");

    let missing = send(&app, get("/file?path=%2Fnope").body(AxumBody::empty()).unwrap()).await;
    assert_eq!(missing.status, StatusCode::INTERNAL_SERVER_ERROR);
}

#[tokio::test]
async fn stale_and_expires_in() {
    let app = app();
    let first = send(&app, get("/logo").body(AxumBody::empty()).unwrap()).await;
    assert_eq!(first.status, StatusCode::OK);
    assert_eq!(first.header("cache-control"), Some("max-age=300, public, stale-while-revalidate=604800"));
    let etag = first.header("etag").unwrap().to_string();
    assert!(etag.starts_with("W/\""));

    let second = send(&app, get("/logo").header(header::IF_NONE_MATCH, &etag).body(AxumBody::empty()).unwrap()).await;
    assert_eq!(second.status, StatusCode::NOT_MODIFIED);

    let fresh = send(&app, get("/fresh").body(AxumBody::empty()).unwrap()).await;
    assert_eq!(fresh.header("cache-control"), Some("max-age=0, private, must-revalidate"));
    let etag = fresh.header("etag").unwrap().to_string();
    let cached = send(&app, get("/fresh").header(header::IF_NONE_MATCH, &etag).body(AxumBody::empty()).unwrap()).await;
    assert_eq!(cached.status, StatusCode::NOT_MODIFIED);
    let listed =
        send(&app, get("/fresh").header(header::IF_NONE_MATCH, format!("\"other\", {etag}")).body(AxumBody::empty()).unwrap()).await;
    assert_eq!(listed.status, StatusCode::NOT_MODIFIED);

    // Turbo Frame requests get a different ETag (turbo-rails' frame etagger).
    let frame = send(&app, get("/fresh").header("turbo-frame", "x").body(AxumBody::empty()).unwrap()).await;
    assert_ne!(frame.header("etag").unwrap(), etag);
}

#[tokio::test]
async fn remote_ip_from_peer_and_proxies() {
    let app = app();
    let mut request = get("/echo/1").header("x-forwarded-for", "203.0.113.9, 10.0.0.1").body(AxumBody::empty()).unwrap();
    request.extensions_mut().insert(ConnectInfo("127.0.0.1:5000".parse::<SocketAddr>().unwrap()));
    assert_eq!(send(&app, request).await.json()["remote_ip"], "203.0.113.9");

    let mut request = get("/echo/1").body(AxumBody::empty()).unwrap();
    request.extensions_mut().insert(ConnectInfo("198.51.100.4:5000".parse::<SocketAddr>().unwrap()));
    assert_eq!(send(&app, request).await.json()["remote_ip"], "198.51.100.4");

    let request = get("/echo/1").header("client-ip", "1.1.1.1").header("x-forwarded-for", "2.2.2.2").body(AxumBody::empty()).unwrap();
    assert_eq!(send(&app, request).await.status, StatusCode::INTERNAL_SERVER_ERROR);
}

#[tokio::test]
async fn force_ssl_redirects_and_hardens() {
    let app = app_with(KitConfig { force_ssl: true, ..KitConfig::default() });
    let plain = send(&app, get("/rooms/1?x=1").body(AxumBody::empty()).unwrap()).await;
    assert_eq!(plain.status, StatusCode::MOVED_PERMANENTLY);
    assert_eq!(plain.header("location"), Some("https://chat.example.com/rooms/1?x=1"));
    let plain_post = send(&app, form_post("/echo/1", "")).await;
    assert_eq!(plain_post.status, StatusCode::PERMANENT_REDIRECT);

    let production = app_with(KitConfig::production(false));
    let secure = send(&production, get("/sign_in").body(AxumBody::empty()).unwrap()).await;
    assert_eq!(secure.status, StatusCode::OK);
    assert_eq!(secure.header("strict-transport-security"), Some("max-age=63072000; includeSubDomains"));
    assert!(secure.cookies().iter().all(|c| c.ends_with("; secure")));

    let https = send(&production, get("/redirect").body(AxumBody::empty()).unwrap()).await;
    assert_eq!(https.header("location"), Some("https://chat.example.com/rooms/1?x=1"));
}

#[tokio::test]
async fn body_limit_is_413() {
    let app = app_with(KitConfig { max_body_bytes: Some(16), ..KitConfig::default() });
    let reply = send(&app, form_post("/echo/1", &"a=1&".repeat(20))).await;
    assert_eq!(reply.status, StatusCode::PAYLOAD_TOO_LARGE);
    let reply = send(&app, HttpRequest::patch("/echo/1").body(AxumBody::from("x".repeat(40))).unwrap()).await;
    assert_eq!(reply.status, StatusCode::PAYLOAD_TOO_LARGE);
}

#[tokio::test]
async fn request_ids_are_sanitized_passthroughs() {
    let reply = send(&app(), get("/rooms/1").header("x-request-id", "abc-123<script>").body(AxumBody::empty()).unwrap()).await;
    assert_eq!(reply.header("x-request-id"), Some("abc-123script"));
}

#[tokio::test]
async fn serves_with_peer_addresses_and_shuts_down_gracefully() {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let (stop, stopped) = tokio::sync::oneshot::channel::<()>();
    let shutdown = front::Shutdown::when(async {
        let _ = stopped.await;
    });
    let service = front::app_service(app());
    let server = tokio::spawn(front::serve_plain(listener, service, front::Protocol::Http1, front::Options::default(), shutdown.clone()));

    let mut stream = tokio::net::TcpStream::connect(addr).await.unwrap();
    stream.write_all(b"GET /echo/1 HTTP/1.1\r\nHost: chat.example.com\r\nConnection: close\r\n\r\n").await.unwrap();
    let mut response = String::new();
    stream.read_to_string(&mut response).await.unwrap();
    assert!(response.starts_with("HTTP/1.1 200 OK"));
    assert!(response.contains(r#""remote_ip":"127.0.0.1""#));

    stop.send(()).unwrap();
    tokio::time::timeout(std::time::Duration::from_secs(5), server).await.unwrap().unwrap();
    tokio::time::timeout(std::time::Duration::from_secs(5), shutdown.drained()).await.unwrap();
}

#[tokio::test]
async fn forgery_protection_requires_metadata_on_proxy_https_without_force_ssl() {
    let app = app();
    let request = |site: Option<&str>| {
        let mut request = post_from(site);
        request.headers_mut().insert("x-forwarded-proto", "https".parse().unwrap());
        request
    };
    assert_eq!(send(&app, request(None)).await.status, StatusCode::UNPROCESSABLE_ENTITY);
    assert_eq!(send(&app, request(Some("same-origin"))).await.status, StatusCode::OK);
    assert_eq!(send(&app, request(Some("same-site"))).await.status, StatusCode::OK);
}
