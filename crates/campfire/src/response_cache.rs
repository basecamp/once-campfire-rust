//! Private, authenticated whole responses, following the C port's versioned response cache.
//!
//! Authentication and room access run before admission. Exact request replays may reuse that
//! authority until the database changes, the cookie expires or hourly activity is due.
//! A separate SQLite connection observes commits from every writer. The generation captured
//! before authentication is checked again at lookup and admission, so a render racing a commit
//! cannot populate the new generation with old authorization. Each early hit observes SQLite
//! too. Cookie side effects are regenerated; flash, HEAD and conditional GET use the kit.
//! Only the completed, bounded identity/gzip representation is retained; Rust's Sec-Fetch-Site
//! protection does not put CSRF secrets in these pages.

use std::path::Path;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use axum::body::{Body as AxumBody, Bytes, HttpBody};
use axum::extract::State;
use axum::http::{HeaderMap, Method, StatusCode, header};
use axum::middleware::Next;
use campfire_kit::front::{CachedResponse, MemoryCache};
use campfire_kit::{Body, Ctx, Response};
use http_body_util::BodyExt;
use rusqlite::{Connection, OpenFlags};
use sha2::{Digest, Sha256};

use crate::app::{App, AppCtx};
use crate::concerns::{self, AuthenticatedBy};

const MAX_BODY: usize = 1024 * 1024;
const MAX_KEY_INPUT: usize = 8192;
// Time-dependent links and dates still expire even without a database commit.
const TTL: Duration = Duration::from_secs(15);

/// A successful pipeline's authority, never an independently guessed authentication result.
/// Secrets are immutable for an app instance. Checking the exact signed cookie at the horizon
/// proves it remains valid throughout this interval, without interpreting unverified metadata.
#[derive(Clone, Copy, Debug)]
struct Authority {
    not_before: jiff::Timestamp,
    until: jiff::Timestamp,
    last_room: Option<i64>,
}

impl Authority {
    fn capture(c: &Ctx) -> Option<Self> {
        // Let the kit own encrypted session data, including flash and its expiry/consumption.
        if c.cookies.contains("_campfire_session") {
            return None;
        }
        let session = concerns::current_session(c)?;
        let now = c.now();
        let until = (now + jiff::SignedDuration::from_secs(TTL.as_secs() as i64))
            .min(session.last_active_at.jiff() + campfire_db::models::session::ACTIVITY_REFRESH_RATE);
        if until <= now {
            return None;
        }
        let raw = c.cookies.get("session_token")?;
        let token = rails_compat::cookies::verify_signed(&c.app().secrets, "session_token", raw, until)?;
        let last_room = c.current::<concerns::LastRoomPreference>().map(|preference| preference.0);
        (token == session.token).then_some(Self { not_before: now, until, last_room })
    }
}

/// Include all original header values and the transport peer. Only tracing is irrelevant.
/// Hashing length-delimited fields avoids retaining credentials or attacker-sized keys.
fn early_key(request: &axum::extract::Request) -> Option<String> {
    if request.method() != Method::GET
        || !(request.uri().path().starts_with("/rooms/")
            || request.uri().path().starts_with("/users/")
            || request.uri().path() == "/searches")
        || !request.body().is_end_stream()
        || request.extensions().get::<campfire_kit::adapter::OriginalMethod>().is_some()
        || [header::RANGE, header::UPGRADE, header::IF_NONE_MATCH, header::IF_MODIFIED_SINCE]
            .iter()
            .any(|name| request.headers().contains_key(name))
        || directives(request.headers(), &["no-cache", "no-store"])
        || request.headers().contains_key(header::PRAGMA)
    {
        return None;
    }
    let mut digest = Sha256::new();
    let mut length = 0;
    let mut field = |value: &[u8]| {
        length += value.len() + 8;
        digest.update((value.len() as u64).to_le_bytes());
        digest.update(value);
    };
    field(request.uri().to_string().as_bytes());
    if let Some(peer) = request.extensions().get::<axum::extract::ConnectInfo<std::net::SocketAddr>>() {
        field(peer.0.ip().to_string().as_bytes());
    } else {
        field(b"");
    }
    let mut names: Vec<_> = request.headers().keys().collect();
    names.sort_unstable_by(|a, b| a.as_str().cmp(b.as_str()));
    for name in names {
        if matches!(name.as_str(), "x-request-id" | "x-request-start") {
            continue;
        }
        field(name.as_str().as_bytes());
        field(&(request.headers().get_all(name).iter().count() as u64).to_le_bytes());
        for value in request.headers().get_all(name) {
            field(value.as_bytes());
        }
    }
    (length <= MAX_KEY_INPUT).then(|| format!("early/{}", hex::encode(digest.finalize())))
}

pub struct Store {
    observer: Mutex<Observer>,
    responses: MemoryCache<Option<Authority>>,
    enabled: bool,
    #[cfg(test)]
    pub hits: std::sync::atomic::AtomicUsize,
    #[cfg(test)]
    pub early_hits: std::sync::atomic::AtomicUsize,
}

struct Observer {
    db: Connection,
    data_version: i64,
    generation: u64,
}

impl Observer {
    fn version(&mut self) -> rusqlite::Result<u64> {
        // Retain the statement, not its result: each capture/lookup/admission still observes
        // SQLite under the same mutex, including commits from other processes.
        let current = self.db.prepare_cached("PRAGMA data_version")?.query_row([], |row| row.get(0))?;
        if current != self.data_version {
            self.generation += 1;
            self.data_version = current;
        }
        Ok(self.generation)
    }
}

impl Store {
    pub fn open(path: &Path, capacity: usize) -> rusqlite::Result<Arc<Self>> {
        let db = Connection::open_with_flags(path, OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_NO_MUTEX)?;
        db.busy_timeout(Duration::from_millis(50))?;
        let data_version = db.query_row("PRAGMA data_version", [], |row| row.get(0))?;
        Ok(Arc::new(Self {
            observer: Mutex::new(Observer { db, data_version, generation: 0 }),
            enabled: capacity != 0,
            responses: MemoryCache::new(capacity.min(i64::MAX as usize) as i64, (MAX_BODY * 2) as i64),
            #[cfg(test)]
            hits: std::sync::atomic::AtomicUsize::new(0),
            #[cfg(test)]
            early_hits: std::sync::atomic::AtomicUsize::new(0),
        }))
    }

    pub fn version(&self) -> Option<u64> {
        self.observer.lock().unwrap().version().ok()
    }

    fn early_get(&self, key: &str, clock: &campfire_kit::SharedClock) -> Option<axum::response::Response> {
        let mut observer = self.observer.lock().unwrap();
        let generation = observer.version().ok()?;
        let key = format!("{generation}/{key}");
        let cached = self.responses.get(&key, Instant::now())?;
        drop(observer);
        let authority = cached.metadata?;
        let now = clock.now();
        if now < authority.not_before || now >= authority.until {
            return None;
        }
        let mut response = axum::response::Response::new(AxumBody::from(cached.body.clone()));
        *response.status_mut() = cached.status;
        *response.headers_mut() = cached.headers.clone();
        response.headers_mut().insert(header::DATE, campfire_kit::clock::httpdate(now).parse().ok()?);
        if let Some(room) = authority.last_room {
            let cookie = concerns::last_room_preference_cookie(room, now);
            response.headers_mut().append(header::SET_COOKIE, campfire_kit::cookies::set_cookie_header("last_room", &cookie).parse().ok()?);
        }
        #[cfg(test)]
        {
            self.hits.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            self.early_hits.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        }
        Some(response)
    }

    fn get(&self, ticket: &Ticket) -> Option<Arc<CachedResponse<Option<Authority>>>> {
        let mut observer = self.observer.lock().unwrap();
        if observer.version().ok()? != ticket.generation {
            return None;
        }
        let response = self.responses.get(&ticket.key, Instant::now())?;
        #[cfg(test)]
        self.hits.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        Some(response)
    }

    fn put(&self, ticket: &Ticket, response: CachedResponse<Option<Authority>>) {
        let mut observer = self.observer.lock().unwrap();
        if observer.version().ok() != Some(ticket.generation) {
            return;
        }
        // Keep version observation and admission together: an old render is never assigned the
        // next generation. A later commit makes this generation unreachable on the next lookup.
        let now = Instant::now();
        self.responses.set(ticket.key.clone(), response, now + TTL, now);
    }
}

#[derive(Clone)]
pub struct Snapshot {
    generation: Option<u64>,
    fragments: Arc<campfire_views::fragment_cache::FragmentCache>,
}

impl Snapshot {
    pub fn capture(c: &Ctx) -> Self {
        let generation = c.app().response_cache.version();
        let fragments = match generation {
            Some(generation) => {
                // Opengraph embeds filter links against request_host. Their nested fragments
                // must have the same origin isolation as the finished response.
                let origin = hex::encode(Sha256::digest(c.url_for("").as_bytes()));
                c.app().fragment_cache.namespace(generation).scoped(origin)
            }
            // An observer error bypasses both caches instead of falling back to an old namespace.
            None => campfire_views::fragment_cache::FragmentCache::new(0),
        };
        Self { generation, fragments }
    }
}

/// The request's pre-authentication fragment namespace, also on off-thread reader renders.
pub fn fragments(c: &Ctx) -> Arc<campfire_views::fragment_cache::FragmentCache> {
    if let Some(snapshot) = c.current::<Snapshot>() {
        return snapshot.fragments.clone();
    }
    Snapshot::capture(c).fragments
}

#[derive(Clone)]
struct Round {
    generation: u64,
    endpoint: &'static str,
}

#[derive(Clone)]
struct Ticket {
    store: Arc<Store>,
    generation: u64,
    key: String,
    authority: Option<Authority>,
}

/// Before authentication, so a session refresh/revocation during it also prevents a hit.
pub fn begin(c: &mut Ctx, endpoint: &'static str) {
    if !matches!(endpoint, "rooms#show" | "messages#index" | "users/sidebars#show" | "searches#index") || !eligible_request(c) {
        return;
    }
    if c.app().response_cache.enabled
        && let Some(generation) = c.current::<Snapshot>().and_then(|snapshot| snapshot.generation)
    {
        c.set_current(Round { generation, endpoint });
    }
}

/// Called after the action's fresh authentication, permission checks and request side effects.
pub fn lookup(c: &mut Ctx) -> Option<Response> {
    let round = c.current::<Round>()?.clone();
    if concerns::authenticated_by(c) != AuthenticatedBy::Session || !c.flash().is_empty() {
        return None;
    }
    let session = concerns::current_session(c)?;
    let user = concerns::current_user(c)?;
    let store = c.app().response_cache.clone();
    let key = request_key(c, &round, user.id, session.id)?;
    let authority = Authority::capture(c);
    let ticket = Ticket { store, generation: round.generation, key, authority };
    let cached = ticket.store.get(&ticket);
    if let Some(cached) = cached {
        let mut response = Response::new(cached.status).body(cached.body.clone());
        response.headers = cached.headers.clone();
        return Some(response);
    }
    // Even if a concurrent commit made `get` miss, `put` checks the original version again.
    c.set_current(ticket);
    None
}

pub fn prepare(c: &mut Ctx, response: &mut Response) {
    let Some(ticket) = c.current::<Ticket>().cloned() else { return };
    if c.request.method != Method::GET || response.status != StatusCode::OK || !c.flash().is_empty() {
        return;
    }
    let size = match &response.body {
        Body::Bytes(body) => body.len(),
        Body::Parts(parts) => parts.body_len(),
        _ => return,
    };
    if size <= MAX_BODY && cacheable_headers(&response.headers) {
        response.extensions.insert(ticket);
    }
}

#[derive(Clone)]
struct EarlyKey(String);

/// Inside SSL, method override and request-id middleware; outside the routed action pipeline.
/// The outer deflater sees the same already-negotiated representation as a normal late hit.
pub async fn early(State(app): State<App>, request: axum::extract::Request, next: Next) -> axum::response::Response {
    if request.extensions().get::<campfire_kit::OriginalMethod>().is_none()
        && let Some(EarlyKey(key)) = request.extensions().get::<EarlyKey>()
        && let Some(response) = app.response_cache.early_get(key, &app.clock)
    {
        return response;
    }
    next.run(request).await
}

/// Outside the kit and deflater: retain exactly the successfully finished representation.
pub async fn completed(State(app): State<App>, mut request: axum::extract::Request, next: Next) -> axum::response::Response {
    let early_key = if app.response_cache.enabled { early_key(&request) } else { None };
    if let Some(key) = &early_key {
        request.extensions_mut().insert(EarlyKey(key.clone()));
    }
    let mut response = next.run(request).await;
    let Some(ticket) = response.extensions_mut().remove::<Ticket>() else { return response };
    if response.status() != StatusCode::OK || !cacheable_headers(response.headers()) {
        return response;
    }
    let (mut parts, body) = response.into_parts();
    // `prepare` only marks finite in-memory bodies no larger than MAX_BODY. Deflater's output
    // is bounded too, so collecting here cannot turn an arbitrary stream into an allocation.
    let body = match body.collect().await {
        Ok(body) => body.to_bytes(),
        Err(error) => {
            return axum::response::Response::from_parts(
                parts,
                AxumBody::from_stream(futures_util::stream::once(async move { Err::<Bytes, _>(error) })),
            );
        }
    };
    parts.headers.insert(header::CONTENT_LENGTH, body.len().into());
    let mut headers = parts.headers.clone();
    for name in ["set-cookie", "date", "x-request-id", "x-runtime"] {
        headers.remove(name);
    }
    if let (Some(key), Some(authority)) = (early_key, ticket.authority) {
        // No session/flash/credential cookie may be replayed. The only allowed side effect is
        // a room preference, whose calendar expiry is generated anew on every hit.
        if permitted_cookie_effects(&parts.headers, authority.last_room) {
            let metadata = Some(authority);
            let fast = Ticket { key: format!("{}/{key}", ticket.generation), ..ticket.clone() };
            ticket.store.put(
                &fast,
                CachedResponse { status: parts.status, headers: headers.clone(), body: body.clone(), variant: Vec::new(), metadata },
            );
        }
    }
    ticket.store.put(&ticket, CachedResponse { status: parts.status, headers, body: body.clone(), variant: Vec::new(), metadata: None });
    axum::response::Response::from_parts(parts, AxumBody::from(body))
}

fn permitted_cookie_effects(headers: &HeaderMap, last_room: Option<i64>) -> bool {
    let mut cookies = headers.get_all(header::SET_COOKIE).iter();
    match (cookies.next(), last_room) {
        (None, None) => true,
        (Some(cookie), Some(room)) if cookies.next().is_none() => {
            // Validate the final header against the recorded effect; never decode authority
            // or a preference value from response headers. Any other cookie fails closed.
            cookie.as_bytes().starts_with(format!("last_room={room};").as_bytes())
        }
        _ => false,
    }
}

fn eligible_request(c: &Ctx) -> bool {
    matches!(c.request.method, Method::GET | Method::HEAD)
        && c.request.original_method == c.request.method
        && c.request.raw_post().is_empty()
        && !c.request.headers.contains_key(header::RANGE)
        && !c.request.headers.contains_key(header::UPGRADE)
        && !directives(&c.request.headers, &["no-cache", "no-store"])
        && !c.request.header("pragma").is_some_and(|value| value.eq_ignore_ascii_case("no-cache"))
}

fn cacheable_headers(headers: &HeaderMap) -> bool {
    !directives(headers, &["no-store", "no-transform"])
}

fn directives(headers: &HeaderMap, rejected: &[&str]) -> bool {
    headers.get_all(header::CACHE_CONTROL).iter().filter_map(|value| value.to_str().ok()).any(|value| {
        value.split(',').any(|directive| {
            let name = directive.trim().split('=').next().unwrap_or("");
            rejected.iter().any(|rejected| name.eq_ignore_ascii_case(rejected))
        })
    })
}

fn request_key(c: &Ctx, round: &Round, user: i64, session: i64) -> Option<String> {
    let mut digest = Sha256::new();
    let mut length = 0;
    let mut field = |value: &[u8]| {
        length += value.len() + 8;
        digest.update((value.len() as u64).to_le_bytes());
        digest.update(value);
    };
    field(&round.generation.to_le_bytes());
    field(round.endpoint.as_bytes());
    field(&user.to_le_bytes());
    field(&session.to_le_bytes());
    field(c.request.uri.to_string().as_bytes());
    field(c.url_for("").as_bytes());
    // Include all representation/session variants, including Cookie, UA, Turbo-Frame, Accept,
    // encoding and proxy/origin headers. Only validators and request tracing headers are omitted.
    let mut names: Vec<_> = c.request.headers.keys().collect();
    names.sort_unstable_by(|a, b| a.as_str().cmp(b.as_str()));
    for name in names {
        if matches!(name.as_str(), "if-none-match" | "if-modified-since" | "x-request-id" | "x-request-start") {
            continue;
        }
        field(name.as_str().as_bytes());
        field(&(c.request.headers.get_all(name).iter().count() as u64).to_le_bytes());
        for value in c.request.headers.get_all(name) {
            field(value.as_bytes());
        }
    }
    if length > MAX_KEY_INPUT {
        return None;
    }
    Some(hex::encode(digest.finalize()))
}

#[cfg(test)]
mod tests;
