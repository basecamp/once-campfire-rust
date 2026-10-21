use super::*;

fn store(capacity: usize) -> (tempfile::TempDir, Connection, Arc<Store>) {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("cache.sqlite3");
    let writer = Connection::open(&path).unwrap();
    writer.execute_batch("PRAGMA journal_mode=WAL; CREATE TABLE records (value TEXT); INSERT INTO records VALUES ('before');").unwrap();
    let cache = Store::open(&path, capacity).unwrap();
    (dir, writer, cache)
}

fn ticket(store: &Arc<Store>, key: &str) -> Ticket {
    let generation = store.version().unwrap();
    Ticket { store: store.clone(), generation, key: format!("{generation}/{key}"), authority: None }
}

fn response(body: impl Into<Bytes>) -> CachedResponse<Option<Authority>> {
    CachedResponse { status: StatusCode::OK, headers: HeaderMap::new(), body: body.into(), variant: Vec::new(), metadata: None }
}

#[test]
fn committed_external_writes_invalidate_without_timestamp_changes() {
    let (_dir, writer, cache) = store(1024);
    let old = ticket(&cache, "room");
    cache.put(&old, response("before"));
    assert_eq!(cache.get(&old).unwrap().body, "before");
    writer.execute_batch("BEGIN; UPDATE records SET value = 'after';").unwrap();
    assert!(cache.get(&old).is_some(), "an uncommitted transaction is invisible");
    writer.execute_batch("COMMIT;").unwrap();
    assert!(cache.get(&old).is_none());
    assert!(cache.get(&ticket(&cache, "room")).is_none());
    let current = ticket(&cache, "room");
    cache.put(&current, response("after"));
    assert_eq!(cache.get(&current).unwrap().body, "after");
}

#[test]
fn retained_observer_statement_resets_and_observes_later_commits() {
    let (_dir, writer, cache) = store(1024);
    let first = cache.version().unwrap();
    for _ in 0..32 {
        assert_eq!(cache.version(), Some(first));
    }
    writer.execute_batch("ALTER TABLE records ADD COLUMN extra TEXT; UPDATE records SET value = 'after';").unwrap();
    let second = cache.version().unwrap();
    assert_ne!(second, first);
    for _ in 0..32 {
        assert_eq!(cache.version(), Some(second));
    }
    // An unreset observer would retain its read transaction and block this checkpoint.
    let busy: i64 = writer.query_row("PRAGMA wal_checkpoint(TRUNCATE)", [], |row| row.get(0)).unwrap();
    assert_eq!(busy, 0);
    let checkpointed = cache.version().unwrap();
    writer.execute_batch("BEGIN; UPDATE records SET value = 'rolled back'; ROLLBACK;").unwrap();
    assert_eq!(cache.version(), Some(checkpointed));
    writer.execute("UPDATE records SET value = 'committed again'", []).unwrap();
    assert_ne!(cache.version(), Some(checkpointed));
}

#[test]
fn commit_during_render_cannot_admit_old_authorization_or_content() {
    let (_dir, writer, cache) = store(1024);
    let before_authentication = ticket(&cache, "sidebar");
    writer.execute("UPDATE records SET value = 'revoked'", []).unwrap();
    let after_authentication = ticket(&cache, "sidebar");
    cache.put(&before_authentication, response("old private sidebar"));
    assert!(cache.get(&before_authentication).is_none());
    assert!(cache.get(&after_authentication).is_none());
    cache.put(&after_authentication, response("fresh sidebar"));
    assert_eq!(cache.get(&after_authentication).unwrap().body, "fresh sidebar");
    // A second delayed render may finish after the new generation has already been populated.
    cache.put(&before_authentication, response("late old private sidebar"));
    assert_eq!(cache.get(&after_authentication).unwrap().body, "fresh sidebar");
}

#[test]
fn replacement_and_eviction_stay_within_the_budget() {
    let (_dir, _writer, cache) = store(1024);
    let room = ticket(&cache, "room");
    cache.put(&room, response("first"));
    cache.put(&room, response("replacement"));
    assert_eq!(cache.get(&room).unwrap().body, "replacement");
    let too_large = ticket(&cache, "too-large");
    cache.put(&too_large, response(vec![0; 1024]));
    assert!(cache.get(&too_large).is_none());
    let second = ticket(&cache, "second");
    let third = ticket(&cache, "third");
    cache.put(&second, response(vec![1; 350]));
    cache.put(&third, response(vec![2; 350]));
    let retained = [&room, &second, &third].into_iter().filter(|ticket| cache.get(ticket).is_some()).count();
    assert!(retained < 3);
}

#[test]
fn disabled_response_cache_keeps_zero_bytes_and_still_observes_fragment_changes() {
    let (_dir, writer, cache) = store(0);
    assert!(!cache.enabled);
    let old = ticket(&cache, "room");
    cache.put(&old, response("not retained"));
    assert!(cache.get(&old).is_none());
    writer.execute("UPDATE records SET value = 'changed'", []).unwrap();
    assert_ne!(ticket(&cache, "room").generation, old.generation);
}

#[test]
fn delayed_render_racing_a_foreign_commit_never_repopulates_the_cache() {
    let (_dir, writer, cache) = store(1024);
    let before_authentication = ticket(&cache, "private-room");
    let barrier = Arc::new(std::sync::Barrier::new(2));
    let worker_barrier = barrier.clone();
    let worker_cache = cache.clone();
    let renderer = std::thread::spawn(move || {
        // Authorization/rendering captured the old state, then pauses before final admission.
        worker_barrier.wait();
        worker_barrier.wait();
        worker_cache.put(&before_authentication, response("private content before revocation"));
    });
    barrier.wait();
    writer.execute("UPDATE records SET value = 'revoked'", []).unwrap();
    let current = ticket(&cache, "private-room");
    cache.put(&current, response("current public response"));
    barrier.wait();
    renderer.join().unwrap();
    assert_eq!(cache.get(&current).unwrap().body, "current public response");
}

#[test]
fn early_authority_observes_external_commits_clock_boundaries_and_fresh_cookie_dates() {
    let (_dir, writer, cache) = store(4096);
    let now: jiff::Timestamp = "2024-06-01T12:00:00Z".parse().unwrap();
    let clock = Arc::new(campfire_kit::clock::TestClock::frozen_at(now));
    let shared: campfire_kit::SharedClock = clock.clone();
    let authority = Authority { not_before: now, until: now + jiff::SignedDuration::from_secs(2), last_room: Some(42) };
    let ticket = ticket(&cache, "early/room");
    let mut body = response("authorized body");
    body.metadata = Some(authority);
    cache.put(&ticket, body);
    let first = cache.early_get("early/room", &shared).unwrap();
    clock.travel(jiff::SignedDuration::from_secs(1));
    let next = cache.early_get("early/room", &shared).unwrap();
    assert_ne!(first.headers()[header::DATE], next.headers()[header::DATE]);
    assert_ne!(first.headers()[header::SET_COOKIE], next.headers()[header::SET_COOKIE]);
    clock.travel(jiff::SignedDuration::from_secs(1));
    assert!(cache.early_get("early/room", &shared).is_none(), "the exact deadline falls back for activity/credential refresh");
    clock.travel_to(now - jiff::SignedDuration::from_secs(1));
    assert!(cache.early_get("early/room", &shared).is_none(), "clock rollback cannot replay future authority");
    clock.travel_to(now);
    assert!(cache.early_get("early/room", &shared).is_some());
    writer.execute("UPDATE records SET value = 'revoked'", []).unwrap();
    assert!(cache.early_get("early/room", &shared).is_none());
    let mut late = response("late old authorization");
    late.metadata = Some(authority);
    cache.put(&ticket, late);
    assert!(cache.early_get("early/room", &shared).is_none(), "a delayed response cannot authorize the new generation");
}

#[test]
fn repeated_header_values_cannot_impersonate_header_boundaries() {
    let mut separate = axum::http::Request::get("/rooms/1")
        .header("host", "campfire.test")
        .header("t", "value")
        .header("turbo-frame", "room")
        .body(AxumBody::empty())
        .unwrap();
    let mut embedded =
        axum::http::Request::get("/rooms/1").header("host", "campfire.test").header("t", "value").body(AxumBody::empty()).unwrap();
    for value in ["", "turbo-frame", "room"] {
        embedded.headers_mut().append("t", value.parse().unwrap());
    }
    assert_ne!(early_key(&separate), early_key(&embedded));
    let before = early_key(&separate);
    separate.headers_mut().insert("x-request-id", "trace-one".parse().unwrap());
    assert_eq!(before, early_key(&separate), "only tracing is ignored");
}

#[test]
fn early_admission_requires_recorded_cookie_effects_and_rejects_other_cookies() {
    let mut headers = HeaderMap::new();
    assert!(permitted_cookie_effects(&headers, None));
    assert!(!permitted_cookie_effects(&headers, Some(42)), "a missing side effect is not silently recreated");
    headers.append(header::SET_COOKIE, "last_room=42; path=/; samesite=lax".parse().unwrap());
    assert!(permitted_cookie_effects(&headers, Some(42)));
    assert!(!permitted_cookie_effects(&headers, None), "a cookie cannot invent an unrecorded effect");
    assert!(!permitted_cookie_effects(&headers, Some(4)), "a value prefix is not a matching preference");
    headers.append(header::SET_COOKIE, "session_token=refresh; path=/".parse().unwrap());
    assert!(!permitted_cookie_effects(&headers, Some(42)), "credential cookies cannot be replayed or dropped");
    headers.remove(header::SET_COOKIE);
    headers.append(header::SET_COOKIE, "_campfire_session=flash; path=/".parse().unwrap());
    assert!(!permitted_cookie_effects(&headers, None));
    assert!(!permitted_cookie_effects(&headers, Some(42)));
}
