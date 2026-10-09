//! Private, authenticated whole responses, following the C port's versioned response cache.
//!
//! Authentication and room access run before every lookup. A separate SQLite connection observes
//! commits from every writer; its version is captured before authentication and checked again at
//! lookup and admission. Thus a commit during authentication or rendering cannot populate the
//! new version with an old authorization snapshot. What authentication and room access read is
//! itself reused within a version ([`Reads`]). Cookies, flash, HEAD and conditional GET
//! still go through the kit on a hit. Only the completed, bounded identity/gzip representation is
//! retained; Rust's Sec-Fetch-Site protection does not put CSRF secrets in these pages.

use std::borrow::Borrow;
use std::collections::HashMap;
use std::hash::Hash;
use std::path::Path;
use std::sync::{Arc, Mutex, RwLock};
use std::time::{Duration, Instant};

use axum::body::{Body as AxumBody, Bytes};
use axum::http::{HeaderMap, Method, StatusCode, header};
use axum::middleware::Next;
use campfire_db::{Membership, Room, Session, User};
use campfire_kit::front::{CachedResponse, MemoryCache};
use campfire_kit::{Body, Ctx, Response};
use http_body_util::BodyExt;
use rusqlite::{Connection, OpenFlags};
use sha2::{Digest, Sha256};

use crate::app::AppCtx;
use crate::concerns::{self, AuthenticatedBy};

const MAX_BODY: usize = 1024 * 1024;
const MAX_KEY_INPUT: usize = 8192;
// Time-dependent links and dates still expire even without a database commit.
const TTL: Duration = Duration::from_secs(15);

/// Entries each [`GenerationCache`] keeps for its generation: well past the sessions and rooms in
/// use between two commits.
const READS_PER_GENERATION: usize = 4096;

pub struct Store {
    observer: Mutex<Observer>,
    responses: MemoryCache,
    enabled: bool,
    pub reads: Reads,
    #[cfg(test)]
    pub hits: std::sync::atomic::AtomicUsize,
}

struct Observer {
    db: Connection,
    data_version: i64,
    generation: u64,
}

impl Observer {
    fn version(&mut self) -> rusqlite::Result<u64> {
        // Every request runs this under the store's lock: reuse the prepared statement rather than
        // preparing and finalizing it each time.
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
            reads: Reads::default(),
            #[cfg(test)]
            hits: std::sync::atomic::AtomicUsize::new(0),
        }))
    }

    pub fn version(&self) -> Option<u64> {
        self.observer.lock().unwrap().version().ok()
    }

    fn get(&self, ticket: &Ticket) -> Option<Arc<CachedResponse>> {
        let mut observer = self.observer.lock().unwrap();
        if observer.version().ok()? != ticket.generation {
            return None;
        }
        let response = self.responses.get(&ticket.key, Instant::now())?;
        #[cfg(test)]
        self.hits.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        Some(response)
    }

    fn put(&self, ticket: &Ticket, response: CachedResponse) {
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

/// What authentication and room access read, kept for the version they were read in. Every commit,
/// another process's included, moves the version on, and a read is only kept under a version
/// observed before it was made, so a request reusing one sees what a read of its own would have
/// seen, or later. Kept only while the response cache is on, like the responses.
#[derive(Default)]
pub struct Reads {
    /// `session_token` → the session and its user.
    pub sessions: GenerationCache<String, (Session, Option<User>)>,
    /// (user, room) → `Room::find_for_user`.
    pub rooms: GenerationCache<(i64, i64), Room>,
    /// (room, user) → the membership and its room.
    pub memberships: GenerationCache<(i64, i64), (Membership, Room)>,
}

/// Values for one version at a time: storing under a later version drops the earlier one's.
pub struct GenerationCache<K, V> {
    entries: RwLock<(u64, HashMap<K, V>)>,
}

impl<K, V> Default for GenerationCache<K, V> {
    fn default() -> Self {
        Self { entries: RwLock::new((0, HashMap::new())) }
    }
}

impl<K: Eq + Hash, V: Clone> GenerationCache<K, V> {
    pub fn get<Q: Eq + Hash + ?Sized>(&self, generation: u64, key: &Q) -> Option<V>
    where
        K: Borrow<Q>,
    {
        let entries = self.entries.read().unwrap();
        if entries.0 != generation {
            return None;
        }
        entries.1.get(key).cloned()
    }

    pub fn insert(&self, generation: u64, key: K, value: V) {
        let mut entries = self.entries.write().unwrap();
        if generation > entries.0 {
            *entries = (generation, HashMap::new());
        }
        if generation == entries.0 && entries.1.len() < READS_PER_GENERATION {
            entries.1.insert(key, value);
        }
    }
}

/// The version this request's authentication and room access reads may be reused within: the one
/// captured before authentication, while the response cache is on.
pub fn reads_generation(c: &Ctx) -> Option<u64> {
    if !c.app().response_cache.enabled {
        return None;
    }
    c.current::<Snapshot>()?.generation
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
    let ticket = Ticket { store, generation: round.generation, key };
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

/// Outside the kit and deflater: retain exactly the successfully finished representation.
pub async fn completed(request: axum::extract::Request, next: Next) -> axum::response::Response {
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
    ticket.store.put(&ticket, CachedResponse { status: parts.status, headers, body: body.clone(), variant: Vec::new() });
    axum::response::Response::from_parts(parts, AxumBody::from(body))
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
        for value in c.request.headers.get_all(name) {
            field(value.as_bytes());
        }
        field(b"");
    }
    if length > MAX_KEY_INPUT {
        return None;
    }
    Some(hex::encode(digest.finalize()))
}

#[cfg(test)]
mod tests;
