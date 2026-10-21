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
    Ticket { store: store.clone(), generation, key: format!("{generation}/{key}") }
}

fn response(body: impl Into<Bytes>) -> CachedResponse {
    CachedResponse { status: StatusCode::OK, headers: HeaderMap::new(), body: body.into(), variant: Vec::new() }
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
