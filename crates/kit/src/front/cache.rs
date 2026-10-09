//! Thruster's response cache: `internal/cache_handler.go`, `cacheable_response.go`,
//! `memory_cache.go` and `variant.go`.
//!
//! GET and HEAD responses that say `public` with a positive `s-max-age` (sic) or `max-age`, and no
//! `no-cache`, are kept in memory until they expire and served again with `X-Cache: hit`, to any
//! client whose request has the same method, path, query, host and `Vary`ing headers. Everything
//! else passes through with `X-Cache: miss`, or `X-Cache: bypass` for requests that can't be
//! cached at all.
//!
//! Unlike Thruster, an entry's size includes its key and bookkeeping, so CACHE_SIZE bounds the
//! memory the cache holds, and requests with very long URIs aren't cached at all: otherwise a
//! stream of cacheable URIs padded with junk query parameters would be charged for their small
//! responses while holding their large keys.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, LazyLock, LockResult, RwLock, RwLockReadGuard, RwLockWriteGuard};
use std::time::{Duration, Instant};

use axum::http::{HeaderMap, HeaderName, HeaderValue, Method, Request, StatusCode, header};
use bytes::Bytes;
use rand::Rng;
use regex::Regex;

/// The longest path and query a cacheable request may have. Campfire's own cacheable URLs
/// (assets, avatars, QR codes) are far shorter.
pub const MAX_CACHEABLE_URI: usize = 2048;

/// What an entry costs beyond its key and response: the map slot, the `Entry` and the
/// `CachedResponse`, and the key's place in the eviction list.
const ENTRY_OVERHEAD: usize = 256;

/// A response as the cache keeps it (`CacheableResponse`).
#[derive(Debug)]
pub struct CachedResponse {
    pub status: StatusCode,
    pub headers: HeaderMap,
    pub body: Bytes,
    /// The request's values of the headers the response `Vary`s on, when it was stored.
    pub variant: Vec<(String, String)>,
}

impl CachedResponse {
    /// How much of the cache it takes up stored under `key`.
    fn size(&self, key: &str) -> usize {
        let headers: usize = self.headers.iter().map(|(n, v)| n.as_str().len() + v.len()).sum();
        let variant: usize = self.variant.iter().map(|(n, v)| n.len() + v.len()).sum();
        self.body.len() + headers + variant + key.len() + ENTRY_OVERHEAD
    }
}

/// `MemoryCache`: a size-bounded map that evicts by sampling.
///
/// Lookups share a read lock. A hit records its access time in the entry, an atomic in whole
/// milliseconds since the cache was made, and only when the time moved on: sampling eviction
/// needs no finer recency, and hits on a hot entry in the same millisecond then don't all write
/// to it.
pub struct MemoryCache {
    epoch: Instant,
    inner: Shared,
}

/// The cache's state under a read-write lock: shared by lookups (`read`), exclusive for changes
/// (`lock`).
struct Shared(RwLock<Inner>);

impl Shared {
    fn read(&self) -> LockResult<RwLockReadGuard<'_, Inner>> {
        self.0.read()
    }

    fn lock(&self) -> LockResult<RwLockWriteGuard<'_, Inner>> {
        self.0.write()
    }
}

struct Inner {
    capacity: i64,
    max_item_size: i64,
    size: i64,
    /// The keys again, for sampling; each shares its string with the map.
    keys: Vec<Arc<str>>,
    items: HashMap<Arc<str>, Entry>,
}

struct Entry {
    /// Milliseconds since [`MemoryCache::epoch`].
    last_accessed_at: AtomicU64,
    expires_at: Instant,
    value: Arc<CachedResponse>,
    size: i64,
}

impl MemoryCache {
    pub fn new(capacity: i64, max_item_size: i64) -> Self {
        Self {
            epoch: Instant::now(),
            inner: Shared(RwLock::new(Inner { capacity, max_item_size, size: 0, keys: Vec::new(), items: HashMap::new() })),
        }
    }

    pub fn get(&self, key: &str, now: Instant) -> Option<Arc<CachedResponse>> {
        let accessed_at = self.millis(now);
        let inner = self.inner.read().unwrap();
        let item = inner.items.get(key)?;
        if item.expires_at < now {
            return None;
        }
        // `fetch_max`, so a hit that read the clock earlier never moves the time back.
        if item.last_accessed_at.load(Ordering::Relaxed) < accessed_at {
            item.last_accessed_at.fetch_max(accessed_at, Ordering::Relaxed);
        }
        Some(item.value.clone())
    }

    pub fn set(&self, key: String, value: CachedResponse, expires_at: Instant, now: Instant) {
        let item_size = value.size(&key) as i64;
        let entry = Entry { last_accessed_at: AtomicU64::new(self.millis(now)), expires_at, value: Arc::new(value), size: item_size };
        let mut inner = self.inner.lock().unwrap();
        if item_size > inner.max_item_size || item_size > inner.capacity {
            tracing::debug!(len = item_size, "Cache: item is too large to store");
            return;
        }
        let limit = inner.capacity - item_size;
        while inner.size > limit && !inner.keys.is_empty() {
            inner.evict_oldest_item(now);
        }
        let key: Arc<str> = key.into();
        match inner.items.get(&key) {
            Some(existing) => inner.size -= existing.size,
            None => inner.keys.push(key.clone()),
        }
        inner.items.insert(key, entry);
        inner.size += item_size;
    }

    /// `now` in whole milliseconds since the cache was made (zero for an earlier `now`).
    fn millis(&self, now: Instant) -> u64 {
        now.saturating_duration_since(self.epoch).as_millis() as u64
    }

    #[cfg(test)]
    fn size(&self) -> i64 {
        self.inner.lock().unwrap().size
    }
}

impl Inner {
    /// Samples 5 random items and evicts the least recently used, or the first expired one found.
    fn evict_oldest_item(&mut self, now: Instant) {
        let mut rng = rand::rng();
        let mut oldest: Option<(usize, u64)> = None;
        for _ in 0..5 {
            let index = rng.random_range(0..self.keys.len());
            let item = &self.items[&self.keys[index]];
            let accessed_at = item.last_accessed_at.load(Ordering::Relaxed);
            if item.expires_at < now {
                oldest = Some((index, accessed_at));
                break;
            }
            if oldest.is_none_or(|(_, at)| accessed_at < at) {
                oldest = Some((index, accessed_at));
            }
        }
        let (index, _) = oldest.expect("sampled at least one key");
        let key = self.keys.swap_remove(index);
        if let Some(item) = self.items.remove(&key) {
            self.size -= item.size;
        }
    }
}

/// `shouldCacheRequest`: GET or HEAD, not an upgrade, not a range; and (unlike Thruster) not a
/// very long URI.
pub fn should_cache_request<B>(request: &Request<B>) -> bool {
    let header = |name| request.headers().get(name).map(HeaderValue::as_bytes).unwrap_or_default();
    let allowed_method = request.method() == Method::GET || request.method() == Method::HEAD;
    let is_upgrade = header(header::CONNECTION) == b"Upgrade" || header(header::UPGRADE) == b"websocket";
    let is_range = !header(header::RANGE).is_empty();
    let uri_length = request.uri().path_and_query().map_or(0, |p| p.as_str().len());
    allowed_method && !is_upgrade && !is_range && uri_length <= MAX_CACHEABLE_URI
}

static PUBLIC: LazyLock<Regex> = LazyLock::new(|| Regex::new(r"(?-u)\bpublic\b").unwrap());
static NO_CACHE: LazyLock<Regex> = LazyLock::new(|| Regex::new(r"(?-u)\bno-cache\b").unwrap());
static S_MAX_AGE: LazyLock<Regex> = LazyLock::new(|| Regex::new(r"(?-u)\bs-max-age=(\d+)\b").unwrap());
static MAX_AGE: LazyLock<Regex> = LazyLock::new(|| Regex::new(r"(?-u)\bmax-age=(\d+)\b").unwrap());

/// `CacheStatus` (before the body's size is known): how long the response may be cached, if at all.
pub fn cache_lifetime(status: StatusCode, headers: &HeaderMap) -> Option<Duration> {
    let status = status.as_u16();
    if !(200..=399).contains(&status) || status == 304 {
        return None;
    }
    if first(headers, &header::VARY).contains('*') {
        return None;
    }
    let cache_control = first(headers, &header::CACHE_CONTROL);
    if !PUBLIC.is_match(cache_control) || NO_CACHE.is_match(cache_control) {
        return None;
    }
    let max_age = S_MAX_AGE.captures(cache_control).or_else(|| MAX_AGE.captures(cache_control))?;
    let seconds: i64 = max_age[1].parse().ok()?;
    (seconds > 0).then(|| Duration::from_secs(seconds as u64))
}

/// `Header.Get`: the first value, or "".
fn first<'a>(headers: &'a HeaderMap, name: &HeaderName) -> &'a str {
    headers.get(name).and_then(|v| v.to_str().ok()).unwrap_or("")
}

/// `Variant`: the cache key for a request, given the headers its response varies on.
pub struct Variant {
    base: String,
    request_headers: HeaderMap,
    names: Vec<String>,
}

impl Variant {
    pub fn new<B>(request: &Request<B>) -> Self {
        let uri = request.uri();
        let host = request
            .headers()
            .get(header::HOST)
            .and_then(|v| v.to_str().ok())
            .map(str::to_string)
            .or_else(|| uri.authority().map(|a| a.to_string()))
            .unwrap_or_default();
        // The raw path and query, as the router and the params parser see them. Thruster keys on the
        // decoded path, which would give `/a%2Fb` and `/a/b` one entry, and on the query re-encoded
        // by Go's `url.Values.Encode`, which drops any pair containing `;` that Rack keeps.
        let query = uri.query().unwrap_or("");
        let base = format!("{}\n{}\n{query}\n{host}", request.method(), uri.path());
        Self { base, request_headers: request.headers().clone(), names: Vec::new() }
    }

    /// `SetResponseHeader`: vary on the response's `Vary` names (canonical, sorted).
    pub fn set_response_headers(&mut self, headers: &HeaderMap) {
        let vary = first(headers, &header::VARY);
        self.names = if vary.is_empty() {
            Vec::new()
        } else {
            let mut names: Vec<String> = vary.split(',').map(|n| n.trim().to_ascii_lowercase()).collect();
            names.sort();
            names
        };
    }

    pub fn cache_key(&self) -> String {
        let mut key = self.base.clone();
        for name in &self.names {
            key.push('\n');
            key.push_str(name);
            key.push('=');
            key.push_str(self.request_value(name));
        }
        key
    }

    /// Whether a stored response's variant headers match this request's.
    pub fn matches(&self, variant: &[(String, String)]) -> bool {
        self.names.iter().all(|name| {
            let stored = variant.iter().find(|(n, _)| n == name).map(|(_, v)| v.as_str()).unwrap_or("");
            stored == self.request_value(name)
        })
    }

    pub fn variant_headers(&self) -> Vec<(String, String)> {
        self.names.iter().map(|name| (name.clone(), self.request_value(name).to_string())).collect()
    }

    fn request_value(&self, name: &str) -> &str {
        self.request_headers.get(name).and_then(|v| v.to_str().ok()).unwrap_or("")
    }
}

/// `wasNotModified`: the request's `If-None-Match` names the cached `ETag`.
pub fn was_not_modified<B>(cached: &CachedResponse, request: &Request<B>) -> bool {
    let etag = first(&cached.headers, &header::ETAG);
    if etag.is_empty() {
        return false;
    }
    let if_none_match = request.headers().get(header::IF_NONE_MATCH).and_then(|v| v.to_str().ok()).unwrap_or("");
    if_none_match.split(',').any(|candidate| candidate.trim() == etag)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn response(body: &str) -> CachedResponse {
        CachedResponse { status: StatusCode::OK, headers: HeaderMap::new(), body: Bytes::from(body.to_string()), variant: Vec::new() }
    }

    fn headers(pairs: &[(&str, &str)]) -> HeaderMap {
        let mut map = HeaderMap::new();
        for (name, value) in pairs {
            map.append(HeaderName::from_bytes(name.as_bytes()).unwrap(), HeaderValue::from_str(value).unwrap());
        }
        map
    }

    #[test]
    fn lifetime_needs_public_and_a_max_age() {
        let ok = StatusCode::OK;
        assert_eq!(cache_lifetime(ok, &headers(&[("cache-control", "public, max-age=2592000")])), Some(Duration::from_secs(2592000)));
        assert_eq!(
            cache_lifetime(ok, &headers(&[("cache-control", "max-age=300, public, stale-while-revalidate=604800")])),
            Some(Duration::from_secs(300))
        );
        assert_eq!(cache_lifetime(ok, &headers(&[("cache-control", "public, s-max-age=10, max-age=99")])), Some(Duration::from_secs(10)));
        assert_eq!(cache_lifetime(ok, &headers(&[("cache-control", "max-age=0, private, must-revalidate")])), None);
        assert_eq!(cache_lifetime(ok, &headers(&[("cache-control", "public")])), None);
        assert_eq!(cache_lifetime(ok, &headers(&[("cache-control", "public, max-age=0")])), None);
        assert_eq!(cache_lifetime(ok, &headers(&[("cache-control", "public, no-cache, max-age=60")])), None);
        assert_eq!(cache_lifetime(ok, &headers(&[("cache-control", "public, max-age=60"), ("vary", "*")])), None);
        let public = headers(&[("cache-control", "public, max-age=60")]);
        assert_eq!(cache_lifetime(StatusCode::NOT_MODIFIED, &public), None);
        assert_eq!(cache_lifetime(StatusCode::NOT_FOUND, &public), None);
        assert!(cache_lifetime(StatusCode::MOVED_PERMANENTLY, &public).is_some());
    }

    #[test]
    fn requests_that_bypass_the_cache() {
        let get = |pairs: &[(&str, &str)]| {
            let mut request = Request::get("/x").body(()).unwrap();
            *request.headers_mut() = headers(pairs);
            should_cache_request(&request)
        };
        assert!(get(&[]));
        assert!(!get(&[("upgrade", "websocket")]));
        assert!(!get(&[("connection", "Upgrade")]));
        assert!(get(&[("connection", "upgrade")])); // Thruster compares exactly
        assert!(!get(&[("range", "bytes=0-1")]));
        assert!(!should_cache_request(&Request::post("/x").body(()).unwrap()));
        assert!(should_cache_request(&Request::head("/x").body(()).unwrap()));
        let uri = |length: usize| format!("/qr_code/aGk?pad={}", "x".repeat(length - 17));
        assert!(should_cache_request(&Request::head(uri(MAX_CACHEABLE_URI)).body(()).unwrap()));
        assert!(!should_cache_request(&Request::head(uri(MAX_CACHEABLE_URI + 1)).body(()).unwrap()));
    }

    #[test]
    fn keys_use_the_raw_path_and_query_and_include_varying_headers() {
        let request = |uri: &str, ae: &str| Request::get(uri).header("host", "chat.test").header("accept-encoding", ae).body(()).unwrap();
        let key = |uri: &str| Variant::new(&request(uri, "gzip")).cache_key();
        assert_ne!(key("/a?a=1"), key("/a?a=2"));
        assert_ne!(key("/a%2Fb"), key("/a/b"));
        assert_ne!(key("/a?b=2&a=1"), key("/a?a=1&b=2"));
        assert_ne!(key("/a?q=a+b"), key("/a?q=a%20b"));
        // Rack reads `disposition=attachment;` where Go's query parser dropped the pair.
        assert_ne!(key("/a?disposition=attachment;"), key("/a"));
        assert_ne!(key("/a?disposition=inline&disposition=x;"), key("/a?disposition=inline"));
        assert_eq!(key("/a?"), key("/a"));

        let mut gzip = Variant::new(&request("/a", "gzip"));
        let mut plain = Variant::new(&request("/a", ""));
        assert_eq!(gzip.cache_key(), plain.cache_key());
        let vary = headers(&[("vary", "Accept-Encoding")]);
        gzip.set_response_headers(&vary);
        plain.set_response_headers(&vary);
        assert_ne!(gzip.cache_key(), plain.cache_key());
        assert!(gzip.matches(&gzip.variant_headers()));
        assert!(!plain.matches(&gzip.variant_headers()));
    }

    /// A body that makes an entry under a one-letter key take `size` bytes.
    fn response_of_size(size: usize) -> CachedResponse {
        response(&"x".repeat(size - 1 - ENTRY_OVERHEAD))
    }

    #[test]
    fn memory_cache_expires_and_evicts() {
        let now = Instant::now();
        let item = ENTRY_OVERHEAD as i64 + 50;
        let cache = MemoryCache::new(2 * item, item + 10);
        cache.set("a".into(), response_of_size(item as usize), now + Duration::from_secs(10), now);
        assert!(cache.get("a", now).is_some());
        assert!(cache.get("a", now + Duration::from_secs(11)).is_none());

        cache.set("big".into(), response_of_size(item as usize + 11), now + Duration::from_secs(10), now);
        assert!(cache.get("big", now).is_none(), "larger than the item limit");

        cache.set("b".into(), response_of_size(item as usize), now + Duration::from_secs(10), now);
        cache.set("c".into(), response_of_size(item as usize), now + Duration::from_secs(10), now);
        assert_eq!(cache.size(), 2 * item);
        let kept = ["a", "b", "c"].iter().filter(|k| cache.get(k, now).is_some()).count();
        assert_eq!(kept, 2);

        cache.set("c".into(), response_of_size(item as usize - 30), now + Duration::from_secs(10), now);
        assert!(cache.size() <= 2 * item);
    }

    #[test]
    fn memory_cache_charges_keys() {
        let now = Instant::now();
        let capacity = 64 * 1024;
        let cache = MemoryCache::new(capacity, 1024 * 1024);
        for n in 0..100 {
            let key = format!("HEAD\n/qr_code/aGk\npad={n}{}\nchat.test", "x".repeat(1000));
            cache.set(key, response(""), now + Duration::from_secs(60), now);
        }
        assert!(cache.size() <= capacity);
        assert!(cache.size() > capacity - 2000, "charged the keys: {}", cache.size());
        let inner = cache.inner.lock().unwrap();
        let held: usize = inner.keys.iter().map(|k| k.len() + ENTRY_OVERHEAD).sum();
        assert!(held as i64 <= capacity);
    }

    #[test]
    fn not_modified_compares_etags() {
        let mut cached = response("x");
        cached.headers.insert(header::ETAG, HeaderValue::from_static("\"abc\""));
        let request = |inm: &str| Request::get("/").header("if-none-match", inm).body(()).unwrap();
        assert!(was_not_modified(&cached, &request("\"abc\"")));
        assert!(was_not_modified(&cached, &request("\"x\", \"abc\"")));
        assert!(!was_not_modified(&cached, &request("W/\"abc\"")));
    }
}
