//! Fragment caching: `cache record do ... end` in ERB and `json.cache! record do ... end` in
//! Jbuilder. The first rendering of a record version is what every later render reuses, whoever
//! renders it: a broadcast renders without a request, and the pages that show the same message
//! afterwards repeat that rendering byte for byte. So nothing in a fragment may depend on the
//! request unless its key does (a message's "Copy link" carries a path, not the request's host).
//!
//! [`FragmentCache`] is the process's store. The reference keeps fragments in Redis
//! (`config.cache_store = :redis_cache_store`, `config/environments/production.rb`), whose
//! `config/redis.conf` sets no `maxmemory`, so it grows with every message. An in-process store
//! can't do that, so this one is bounded by bytes the way Rails bounds its in-process store,
//! `ActiveSupport::Cache::MemoryStore`: each entry counts its key, its payload and
//! [`PER_ENTRY_OVERHEAD`] bytes, and when a write takes the total past the limit, least recently
//! used entries go until it's back to three quarters of it (`MemoryStore#prune`). Reads count as
//! uses. An entry larger than a quarter of the limit is returned but not kept, so that one huge
//! fragment can't flush everything else. Templates reach the store that's current on this thread:
//! the app enters it for every request ([`Scoped`]) and for renders outside one ([`with`]).
//! Without a current store, fragments render uncached (`perform_caching = false`).
//!
//! Keys follow `ActionView::Helpers::CacheHelper#fragment_name_with_digest`:
//! `views/<template>:<digest>/<record cache_key_with_version>[/<extra>]`, where the digest covers
//! the template and the partials it renders (see [`digest`]).

use std::any::Any;
use std::cell::RefCell;
use std::collections::HashMap;
use std::future::Future;
use std::hash::{Hash, Hasher};
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, RwLock};
use std::task::{Context, Poll};

/// A rendered fragment as the store keeps it.
pub type Fragment = Arc<String>;

/// The store's default limit: `MemoryStore`'s default `size`, 32 MB.
pub const DEFAULT_MAX_BYTES: usize = 32 * 1024 * 1024;

/// What an entry costs beyond its key and payload (`MemoryStore::PER_ENTRY_OVERHEAD`).
pub const PER_ENTRY_OVERHEAD: usize = 240;

/// The bytes a cached value accounts for, like the `bytesize` of the payload `MemoryStore` and
/// Redis keep. Shared parts (an `Arc`) count in full: an entry is charged for what it holds alive.
pub trait CacheSize {
    fn cache_size(&self) -> usize;
}

/// A string holds its capacity, not just its length.
impl CacheSize for String {
    fn cache_size(&self) -> usize {
        self.capacity()
    }
}

impl<T: CacheSize + ?Sized> CacheSize for Arc<T> {
    fn cache_size(&self) -> usize {
        T::cache_size(self)
    }
}

/// The length of `value`'s JSON: the payload size of a Jbuilder value.
pub fn serialized_size(value: &impl serde::Serialize) -> usize {
    struct Count(usize);
    impl std::io::Write for Count {
        fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
            self.0 += bytes.len();
            Ok(bytes.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }
    let mut count = Count(0);
    serde_json::to_writer(&mut count, value).map_or(0, |()| count.0)
}

type Value = Arc<dyn Any + Send + Sync>;

/// `html` without the spare capacity rendering leaves (Askama reserves a size hint up front),
/// which a stored fragment would otherwise hold for as long as it's kept.
fn fitted(mut html: String) -> String {
    html.shrink_to_fit();
    html
}

/// A byte-bounded in-process fragment store.
///
/// Reads share the lock: each one stamps its entry from `clock` without taking the lock
/// exclusively, because a room page reads dozens of fragments per request on every worker. Writes
/// hold the lock exclusively, and an eviction orders the entries by their stamps.
pub struct FragmentCache {
    max_bytes: usize,
    clock: Arc<AtomicU64>,
    entries: Arc<RwLock<Entries>>,
    namespace: u64,
    scope: Option<Arc<str>>,
}

#[derive(Default)]
struct Entries {
    values: HashMap<Arc<str>, Entry>,
    /// The sum of every entry's `size`.
    bytes: usize,
}

struct Entry {
    value: Value,
    /// The `clock` value of the last use.
    used: AtomicU64,
    size: usize,
}

impl FragmentCache {
    /// A store that keeps at most `max_bytes` of entries (as [`CacheSize`] and
    /// [`PER_ENTRY_OVERHEAD`] count them).
    pub fn new(max_bytes: usize) -> Arc<Self> {
        Arc::new(Self { max_bytes, clock: Arc::default(), entries: Arc::default(), namespace: 0, scope: None })
    }

    /// A render snapshot sharing this store's byte budget, isolated from other database versions.
    /// An old in-flight render can populate its own namespace without poisoning the new one.
    pub fn namespace(&self, namespace: u64) -> Arc<Self> {
        Arc::new(Self { max_bytes: self.max_bytes, clock: self.clock.clone(), entries: self.entries.clone(), namespace, scope: None })
    }

    /// Isolates fragments whose rendering depends on an origin or another explicit context.
    /// The backing store and memory budget remain shared with every other scope.
    pub fn scoped(&self, scope: impl Into<Arc<str>>) -> Arc<Self> {
        Arc::new(Self {
            max_bytes: self.max_bytes,
            clock: self.clock.clone(),
            entries: self.entries.clone(),
            namespace: self.namespace,
            scope: Some(scope.into()),
        })
    }

    fn scoped_key<'a>(&self, key: &'a str) -> std::borrow::Cow<'a, str> {
        match &self.scope {
            Some(scope) => format!("\0{}:{}:{scope}/{key}", self.namespace, scope.len()).into(),
            None if self.namespace == 0 => key.into(),
            None => format!("\0{}/{key}", self.namespace).into(),
        }
    }

    /// `Rails.cache.fetch(key) { render }` for a rendered fragment, shared with the store.
    pub fn fetch(&self, key: &str, render: impl FnOnce() -> String) -> Fragment {
        self.fetch_value(key, || Fragment::new(fitted(render())))
    }

    /// `Rails.cache.fetch(key) { value }` for any cloneable value (Jbuilder caches the hash it
    /// built, not its JSON).
    pub fn fetch_value<T: CacheSize + Clone + Send + Sync + 'static>(&self, key: &str, compute: impl FnOnce() -> T) -> T {
        match self.try_fetch_value(key, || Ok::<T, std::convert::Infallible>(compute())) {
            Ok(value) => value,
            Err(never) => match never {},
        }
    }

    /// [`Self::fetch_value`] where computing can fail: nothing is stored then. When two renders
    /// of a key race, the first one stored is what both return.
    pub fn try_fetch_value<T: CacheSize + Clone + Send + Sync + 'static, E>(
        &self,
        key: &str,
        compute: impl FnOnce() -> Result<T, E>,
    ) -> Result<T, E> {
        let key = self.scoped_key(key);
        if let Some(value) = self.read::<T>(&key) {
            return Ok(value);
        }
        // Rendered unlocked: a fragment renders the fragments nested in it through this store.
        let value = compute()?;
        let size = key.len() + value.cache_size() + PER_ENTRY_OVERHEAD;
        Ok(self.write(&key, value, size))
    }

    /// The value `key` holds, if any (a use, for eviction).
    pub fn get<T: Clone + 'static>(&self, key: &str) -> Option<T> {
        self.read(&self.scoped_key(key))
    }

    pub fn len(&self) -> usize {
        self.shared().values.len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// The bytes the entries account for.
    pub fn bytes(&self) -> usize {
        self.shared().bytes
    }

    pub fn max_bytes(&self) -> usize {
        self.max_bytes
    }

    pub fn clear(&self) {
        *self.exclusive() = Entries::default();
    }

    fn tick(&self) -> u64 {
        self.clock.fetch_add(1, Ordering::Relaxed) + 1
    }

    fn read<T: Clone + 'static>(&self, key: &str) -> Option<T> {
        let entries = self.shared();
        let entry = entries.values.get(key)?;
        let value = entry.value.downcast_ref::<T>()?.clone();
        entry.used.fetch_max(self.tick(), Ordering::Relaxed);
        Some(value)
    }

    /// Stores `value` unless `key` already holds one of its type, and returns what `key` holds.
    fn write<T: Clone + Send + Sync + 'static>(&self, key: &str, value: T, size: usize) -> T {
        let mut entries = self.exclusive();
        let now = self.tick();
        let Entries { values, bytes } = &mut *entries;
        if let Some(entry) = values.get(key) {
            if let Some(stored) = entry.value.downcast_ref::<T>() {
                let stored = stored.clone();
                entry.used.fetch_max(now, Ordering::Relaxed);
                return stored;
            }
            if let Some(replaced) = values.remove(key) {
                *bytes -= replaced.size;
            }
        }
        if size > self.max_bytes / 4 {
            return value;
        }
        values.insert(key.into(), Entry { value: Arc::new(value.clone()), used: AtomicU64::new(now), size });
        *bytes += size;
        if *bytes > self.max_bytes {
            // `MemoryStore#prune(@max_size * 0.75)`: least recently used first.
            let target = self.max_bytes / 4 * 3;
            let mut by_use: Vec<(u64, Arc<str>)> =
                values.iter().map(|(key, entry)| (entry.used.load(Ordering::Relaxed), key.clone())).collect();
            by_use.sort_unstable_by_key(|(used, _)| *used);
            for (_, oldest) in by_use {
                if *bytes <= target {
                    break;
                }
                if let Some(entry) = values.remove(&oldest) {
                    *bytes -= entry.size;
                }
            }
        }
        value
    }

    fn shared(&self) -> std::sync::RwLockReadGuard<'_, Entries> {
        self.entries.read().unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    fn exclusive(&self) -> std::sync::RwLockWriteGuard<'_, Entries> {
        self.entries.write().unwrap_or_else(|poisoned| poisoned.into_inner())
    }
}

thread_local! {
    static CURRENT: RefCell<Option<Arc<FragmentCache>>> = const { RefCell::new(None) };
}

/// Runs `f` with `cache` as this thread's current store.
pub fn with<R>(cache: &Arc<FragmentCache>, f: impl FnOnce() -> R) -> R {
    struct Restore(Option<Arc<FragmentCache>>);
    impl Drop for Restore {
        fn drop(&mut self) {
            let previous = self.0.take();
            CURRENT.with(|current| *current.borrow_mut() = previous);
        }
    }
    let _restore = Restore(CURRENT.with(|current| current.borrow_mut().replace(cache.clone())));
    f()
}

/// This thread's current store, if any.
pub fn current() -> Option<Arc<FragmentCache>> {
    CURRENT.with(|current| current.borrow().clone())
}

/// Runs `f` on the key `write` puts in this thread's key buffer, which every key reuses: keys are
/// built for every cached record on every request, hit or miss. A key built while another is in
/// use (a fragment rendering the fragments nested in it) gets a buffer of its own.
pub fn with_key<R>(write: impl FnOnce(&mut String), f: impl FnOnce(&str) -> R) -> R {
    let mut key = KEY.take();
    key.clear();
    write(&mut key);
    let result = f(&key);
    KEY.set(key);
    result
}

thread_local! {
    static KEY: std::cell::Cell<String> = const { std::cell::Cell::new(String::new()) };
}

/// `cache key do render end` against the current store (uncached without one), for the key
/// `key` writes.
pub fn fetch(key: impl FnOnce(&mut String), render: impl FnOnce() -> String) -> Fragment {
    match current() {
        Some(cache) => with_key(key, |key| cache.fetch(key, render)),
        None => Fragment::new(render()),
    }
}

/// The fragment the key `key` writes holds in the current store, if any, shared rather than
/// copied. For callers that gather a fragment's inputs only on a miss, as `cache key do ... end`
/// evaluates its block only then.
pub fn read(key: impl FnOnce(&mut String)) -> Option<Fragment> {
    let cache = current()?;
    with_key(key, |key| cache.get(key))
}

/// `json.cache! key do ... end` against the current store (uncached without one), for the key
/// `key` writes.
pub fn try_fetch_value<T: CacheSize + Clone + Send + Sync + 'static, E>(
    key: impl FnOnce(&mut String),
    compute: impl FnOnce() -> Result<T, E>,
) -> Result<T, E> {
    match current() {
        Some(cache) => with_key(key, |key| cache.try_fetch_value(key, compute)),
        None => compute(),
    }
}

/// A future that has `cache` as the current store whenever it's polled: a request's handler,
/// whose synchronous renders (on whichever worker thread polls it) then see the store.
pub struct Scoped<F> {
    cache: Arc<FragmentCache>,
    future: Pin<Box<F>>,
}

impl<F: Future> Scoped<F> {
    pub fn new(cache: Arc<FragmentCache>, future: F) -> Self {
        Self { cache, future: Box::pin(future) }
    }
}

impl<F: Future> Future for Scoped<F> {
    type Output = F::Output;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<F::Output> {
        let this = &mut *self;
        with(&this.cache, || this.future.as_mut().poll(cx))
    }
}

/// The template digest part of a key: a stable hash of the template sources a fragment renders
/// (`ActionView::Digestor` digests the template and its dependency tree). Only its stability
/// within the process matters: the store doesn't outlive it, and ETags hash the fragments'
/// content, not their keys.
pub fn digest(sources: &[&str]) -> String {
    let mut hasher = std::hash::DefaultHasher::new();
    sources.hash(&mut hasher);
    format!("{:016x}", hasher.finish())
}

/// `Time#to_fs(:usec)` of a record's `updated_at`: its `cache_version`.
pub fn cache_version(updated_at: jiff::Timestamp) -> String {
    let mut version = String::with_capacity(20);
    push_cache_version(&mut version, updated_at);
    version
}

/// `record.cache_key_with_version`: `"messages/1-20240601120000000000"`.
pub fn cache_key_with_version(table: &str, id: i64, updated_at: jiff::Timestamp) -> String {
    let mut key = String::with_capacity(table.len() + 41);
    push_cache_key_with_version(&mut key, table, id, updated_at);
    key
}

/// Appends `record.cache_key_with_version` to `key`.
pub fn push_cache_key_with_version(key: &mut String, table: &str, id: i64, updated_at: jiff::Timestamp) {
    key.push_str(table);
    key.push('/');
    push_integer(key, id);
    key.push('-');
    push_cache_version(key, updated_at);
}

/// Appends a record fragment's key, `views/<template>:<digest>/<record cache_key_with_version>`,
/// to `key` (`CacheHelper#fragment_name_with_digest`).
pub fn push_record_fragment_key(key: &mut String, template: &str, digest: &str, table: &str, id: i64, updated_at: jiff::Timestamp) {
    key.push_str("views/");
    key.push_str(template);
    key.push(':');
    key.push_str(digest);
    key.push('/');
    push_cache_key_with_version(key, table, id, updated_at);
}

/// Appends [`cache_version`] to `version`: `%Y%m%d%H%M%S` and six digits of microseconds, in UTC.
/// Written digit by digit, as `strftime` and `format!` cost more than the rest of a cache hit.
fn push_cache_version(version: &mut String, updated_at: jiff::Timestamp) {
    if updated_at < jiff::Timestamp::UNIX_EPOCH {
        // Before 1970 `{:06}` prints the negative sub-second part with its sign; no record is that
        // old, so this just keeps those bytes the same.
        let (time, microseconds) = (updated_at.strftime("%Y%m%d%H%M%S"), updated_at.subsec_microsecond());
        std::fmt::Write::write_fmt(version, format_args!("{time}{microseconds:06}")).expect("a String takes any write");
        return;
    }
    // `Timestamp::MAX` is in 9999, so the year has four digits.
    let time = jiff::tz::Offset::UTC.to_datetime(updated_at);
    let fields = [
        (i64::from(time.year()), 4),
        (i64::from(time.month()), 2),
        (i64::from(time.day()), 2),
        (i64::from(time.hour()), 2),
        (i64::from(time.minute()), 2),
        (i64::from(time.second()), 2),
        (i64::from(updated_at.subsec_microsecond()), 6),
    ];
    for (value, width) in fields {
        push_padded(version, value.unsigned_abs(), width);
    }
}

/// Appends `value` in decimal, as `{value}` would.
fn push_integer(out: &mut String, value: i64) {
    if value < 0 {
        out.push('-');
    }
    let magnitude = value.unsigned_abs();
    push_padded(out, magnitude, magnitude.checked_ilog10().map_or(1, |log| log as usize + 1));
}

/// Appends the last `width` decimal digits of `value`, zero-padded.
fn push_padded(out: &mut String, mut value: u64, width: usize) {
    let mut digits = [b'0'; 20];
    let digits = &mut digits[..width];
    for digit in digits.iter_mut().rev() {
        *digit = b'0' + (value % 10) as u8;
        value /= 10;
    }
    out.push_str(std::str::from_utf8(digits).expect("ASCII digits"));
}

#[cfg(test)]
mod tests {
    use super::*;

    const BIG: usize = 1 << 20;

    impl CacheSize for i32 {
        fn cache_size(&self) -> usize {
            4
        }
    }

    #[test]
    fn namespaces_share_a_budget_but_cannot_repopulate_a_later_generation() {
        let store = FragmentCache::new(BIG);
        let old = store.namespace(1);
        let current = store.namespace(2);
        let barrier = Arc::new(std::sync::Barrier::new(2));
        let worker_barrier = barrier.clone();
        let renderer = std::thread::spawn(move || {
            old.fetch_value("message", || {
                worker_barrier.wait();
                worker_barrier.wait();
                "old creator/body/boost".to_string()
            })
        });
        barrier.wait();
        assert_eq!(current.fetch_value("message", || "fresh creator/body/boost".to_string()), "fresh creator/body/boost");
        barrier.wait();
        assert_eq!(renderer.join().unwrap(), "old creator/body/boost");
        assert_eq!(current.get::<String>("message").unwrap(), "fresh creator/body/boost");
        assert_eq!(store.len(), 2);
        assert_eq!(store.bytes(), current.bytes());
        assert!(store.bytes() <= store.max_bytes());
    }

    #[test]
    fn many_namespaces_keep_one_lru_and_memory_limit() {
        let store = FragmentCache::new(4096);
        for generation in 1..100 {
            store.namespace(generation).fetch("message", || "x".repeat(100));
            assert!(store.bytes() <= store.max_bytes());
        }
        assert!(store.len() < 100);
        assert!(store.namespace(99).get::<Fragment>("message").is_some());
        assert!(store.namespace(1).get::<Fragment>("message").is_none());
    }

    /// What a one-letter key holding `payload` bytes costs.
    fn entry(payload: usize) -> usize {
        1 + payload + PER_ENTRY_OVERHEAD
    }

    /// The keys built with `format!` and `strftime`, which the digit-by-digit ones must match byte
    /// for byte.
    mod formatted {
        pub fn cache_version(updated_at: jiff::Timestamp) -> String {
            format!("{}{:06}", updated_at.strftime("%Y%m%d%H%M%S"), updated_at.subsec_microsecond())
        }

        pub fn cache_key_with_version(table: &str, id: i64, updated_at: jiff::Timestamp) -> String {
            format!("{table}/{id}-{}", cache_version(updated_at))
        }
    }

    /// Timestamps with the calendar's edges and every sub-second precision.
    fn timestamps() -> Vec<jiff::Timestamp> {
        let mut times: Vec<jiff::Timestamp> = [
            "1970-01-01T00:00:00Z",
            "1970-01-01T00:00:00.000001Z",
            "1999-12-31T23:59:59.999999999Z",
            "2000-02-29T12:34:56.5Z",
            "2024-02-29T23:59:59.000999Z",
            "2024-06-01T12:00:00.000123Z",
            "2024-06-01T12:00:00.000123456Z",
            "2024-12-31T23:59:59.1Z",
            "2038-01-19T03:14:08Z",
            "2100-03-01T00:00:00.00001Z",
            "9999-12-30T21:59:59.999999999Z",
            "1969-12-31T23:59:59.5Z",
            "1900-01-01T00:00:00Z",
            "0001-01-01T00:00:00.25Z",
            "-000001-06-15T01:02:03Z",
        ]
        .iter()
        .map(|time| time.parse().unwrap())
        .collect();
        times.extend([jiff::Timestamp::MIN, jiff::Timestamp::MAX]);
        // An instant written with an offset still formats in UTC.
        times.push("2024-11-03T01:30:00.123456-07:00".parse().unwrap());
        times
    }

    #[test]
    fn keys_match_their_formatted_versions_byte_for_byte() {
        let ids = [0, 1, 7, 10, 99, 100, 12_345, 1_000_000_000, i64::MAX, -1, i64::MIN];
        for (i, time) in timestamps().into_iter().enumerate() {
            assert_eq!(cache_version(time), formatted::cache_version(time), "{time}");
            let id = ids[i % ids.len()];
            assert_eq!(cache_key_with_version("messages", id, time), formatted::cache_key_with_version("messages", id, time));
            let mut key = String::from("existing/");
            push_record_fragment_key(&mut key, "messages/_message", "0123456789abcdef", "messages", id, time);
            assert_eq!(
                key,
                format!("existing/views/messages/_message:0123456789abcdef/{}", formatted::cache_key_with_version("messages", id, time))
            );
        }
    }

    /// A key writer for the key `name`.
    fn named(name: &str) -> impl FnOnce(&mut String) + '_ {
        move |key| key.push_str(name)
    }

    #[test]
    fn the_first_rendering_is_reused() {
        let cache = FragmentCache::new(BIG);
        assert_eq!(*cache.fetch("a", || "first".into()), "first");
        assert_eq!(*cache.fetch("a", || "second".into()), "first");
        assert_eq!(*cache.fetch("b", || "other".into()), "other");
    }

    #[test]
    fn entries_count_their_key_payload_and_overhead() {
        let cache = FragmentCache::new(BIG);
        cache.fetch("a", || "x".repeat(100));
        cache.fetch("bb", || "y".repeat(10));
        assert_eq!(cache.bytes(), (1 + 100 + PER_ENTRY_OVERHEAD) + (2 + 10 + PER_ENTRY_OVERHEAD));
        cache.fetch("a", || unreachable!());
        assert_eq!(cache.bytes(), entry(100) + (2 + 10 + PER_ENTRY_OVERHEAD), "a hit costs nothing");
        cache.clear();
        assert_eq!(cache.bytes(), 0);
    }

    #[test]
    fn the_byte_limit_is_enforced() {
        let max = 10 * entry(1000);
        let cache = FragmentCache::new(max);
        for i in 0..1000 {
            cache.fetch(&format!("{}", i % 3), || "x".repeat(1000));
            cache.fetch(&format!("k{i}"), || "x".repeat(1000));
            assert!(cache.bytes() <= max, "{} bytes after {i} writes", cache.bytes());
        }
        assert!(cache.len() < 20);
        assert!(cache.bytes() > max / 2, "pruning stops at three quarters, not empty");
        for hot in 0..3 {
            assert!(cache.get::<Fragment>(&hot.to_string()).is_some(), "entry {hot} is used every sixth write");
        }
    }

    #[test]
    fn going_over_prunes_to_three_quarters_least_recently_used_first() {
        let cache = FragmentCache::new(4 * entry(100));
        for key in ["a", "b", "c", "d"] {
            cache.fetch(key, || "x".repeat(100));
        }
        cache.fetch("a", || unreachable!("a is still stored"));
        assert_eq!(cache.len(), 4);
        // Five entries is over; three quarters of the limit holds three.
        cache.fetch("e", || "x".repeat(100));
        assert_eq!(cache.len(), 3);
        assert!(cache.bytes() <= 3 * entry(100));
        assert_eq!(cache.get::<Fragment>("b"), None, "b was least recently used");
        assert_eq!(cache.get::<Fragment>("c"), None, "c went next");
        for key in ["a", "d", "e"] {
            assert!(cache.get::<Fragment>(key).is_some(), "{key} survives (a was used after d)");
        }
    }

    #[test]
    fn stored_fragments_hold_no_spare_capacity() {
        let cache = FragmentCache::new(BIG);
        cache.fetch("a", || {
            let mut html = String::with_capacity(16 * 1024);
            html.push_str("<p>hi</p>");
            html
        });
        assert_eq!(cache.get::<Fragment>("a").unwrap().capacity(), "<p>hi</p>".len());
        assert_eq!(cache.bytes(), entry("<p>hi</p>".len()));
    }

    #[test]
    fn a_value_larger_than_the_store_is_returned_but_not_kept() {
        let cache = FragmentCache::new(entry(10));
        assert_eq!(*cache.fetch("a", || "x".repeat(100)), "x".repeat(100));
        assert_eq!(cache.len(), 0);
        assert_eq!(cache.bytes(), 0);
    }

    #[test]
    fn a_value_larger_than_a_quarter_of_the_store_doesnt_evict_the_rest() {
        let cache = FragmentCache::new(8 * entry(100));
        for key in ["a", "b", "c"] {
            cache.fetch(key, || "x".repeat(100));
        }
        let big = "y".repeat(3 * entry(100));
        assert_eq!(*cache.fetch("big", || big.clone()), big);
        assert_eq!(cache.len(), 3, "the big value isn't kept");
        assert_eq!(cache.get::<Fragment>("big"), None);
        for key in ["a", "b", "c"] {
            assert!(cache.get::<Fragment>(key).is_some(), "{key} is still stored");
        }
        assert_eq!(cache.bytes(), 3 * entry(100));
    }

    #[test]
    fn concurrent_use_stays_within_the_limit() {
        let max = 64 * entry(200);
        let cache = FragmentCache::new(max);
        std::thread::scope(|scope| {
            for t in 0..8 {
                let cache = &cache;
                scope.spawn(move || {
                    for i in 0..5000 {
                        let hot = cache.fetch(&format!("{}", i % 8), || "x".repeat(200));
                        assert_eq!(*hot, "x".repeat(200));
                        cache.fetch(&format!("cold/{t}/{i}"), || "y".repeat(200));
                        assert!(cache.bytes() <= max);
                    }
                });
            }
        });
        let len = cache.len();
        assert!(len > 0 && cache.bytes() <= max);
        assert_eq!(cache.bytes(), cache.shared().values.values().map(|entry| entry.size).sum::<usize>());
    }

    #[test]
    fn racing_renders_all_return_the_first_stored_fragment() {
        let cache = FragmentCache::new(BIG);
        let barrier = std::sync::Barrier::new(8);
        let values: Vec<Fragment> = std::thread::scope(|scope| {
            let handles: Vec<_> = (0..8)
                .map(|t| {
                    let (cache, barrier) = (&cache, &barrier);
                    scope.spawn(move || {
                        cache.fetch_value("k", || {
                            barrier.wait();
                            Fragment::new(t.to_string())
                        })
                    })
                })
                .collect();
            handles.into_iter().map(|handle| handle.join().unwrap()).collect()
        });
        assert!(values.iter().all(|value| Arc::ptr_eq(value, &values[0])), "{values:?}");
        assert!(Arc::ptr_eq(&cache.get::<Fragment>("k").unwrap(), &values[0]));
        assert_eq!(cache.len(), 1);
    }

    #[test]
    fn jbuilder_values_count_their_json() {
        assert_eq!(serialized_size(&vec!["ab", "c"]), r#"["ab","c"]"#.len());
    }

    #[test]
    fn nested_fragments_use_the_same_store() {
        let cache = FragmentCache::new(BIG);
        let outer = with(&cache, || fetch(named("outer"), || format!("[{}]", fetch(named("inner"), || "x".into()))));
        assert_eq!(*outer, "[x]");
        assert_eq!(cache.len(), 2);
        assert_eq!(cache.get::<Fragment>("outer").as_deref().map(String::as_str), Some("[x]"), "the inner key has its own buffer");
        assert_eq!(cache.get::<Fragment>("inner").as_deref().map(String::as_str), Some("x"));
        assert!(current().is_none(), "the store is only current inside `with`");
        assert_eq!(*fetch(named("outer"), || "uncached".into()), "uncached");
    }

    #[test]
    fn fragments_can_be_looked_up_before_rendering() {
        let cache = FragmentCache::new(BIG);
        assert_eq!(with(&cache, || read(named("a"))), None);
        with(&cache, || fetch(named("ab"), || "rendered".into()));
        assert_eq!(with(&cache, || read(named("ab"))).as_deref().map(String::as_str), Some("rendered"));
        assert_eq!(with(&cache, || read(named("a"))), None, "each key starts from an empty buffer");
        assert_eq!(read(named("ab")), None, "no store, no fragments");
    }

    #[test]
    fn failures_are_not_stored() {
        let cache = FragmentCache::new(BIG);
        assert!(cache.try_fetch_value::<i32, _>("k", || Err("boom")).is_err());
        assert_eq!(cache.try_fetch_value::<i32, &str>("k", || Ok(1)), Ok(1));
        assert_eq!(cache.try_fetch_value::<i32, &str>("k", || Ok(2)), Ok(1));
    }

    #[test]
    fn keys_use_usec_versions() {
        let time: jiff::Timestamp = "2024-06-01T12:00:00.000123Z".parse().unwrap();
        assert_eq!(cache_key_with_version("messages", 1, time), "messages/1-20240601120000000123");
    }
}
