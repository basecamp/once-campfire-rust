//! One writer thread that owns the write connection and takes a bounded queue of work, plus
//! reader threads that take reads from one queue, each with a free reader connection. Each write
//! runs in `BEGIN IMMEDIATE` (`default_transaction_mode: immediate` in
//! `reference/config/database.yml`), then its after-commit work runs in order, outside the
//! transaction, the way Active Record runs `after_commit` callbacks.
//!
//! Reads run on their own threads rather than tokio's blocking pool, where a read that found
//! every connection busy parked a blocking thread until one came free (99–136 threads for 5
//! readers under load, in the pool bcrypt, storage and uploads share). Here a waiting read costs
//! a queue entry, reads leave the queue in the order they were queued (with several readers, two
//! reads taken one after the other may still start in either order), and a reader that finishes
//! a read takes the next one without a hand-off to another thread, which is what makes it cheaper
//! than waiting for a connection on an async semaphore (bench/results/db-hops-20260930).
//!
//! A read that finds a connection free, with no read queued ahead of it, skips the queue and runs
//! on the calling task's thread instead (see [`Database::read`]).
//!
//! WAL checkpoints run on a checkpointer thread with a connection of its own, not on the writer.
//! Rails keeps SQLite's auto-checkpoint: once a commit leaves the WAL at 1,000 pages or more,
//! that commit checkpoints (PASSIVE) before returning, fsyncing the WAL and then the database
//! (with the WAL header's fsync when the next write restarts the WAL, ~12 ms here, every ~64
//! message posts), and every write queued behind it waits. Here the writer's commits only note
//! the WAL's size, and every 1,000 pages it grows wake the checkpointer, which runs the same
//! PASSIVE checkpoint while writes carry on appending to the WAL.
//!
//! Durability is the same as Rails': `journal_mode=wal` with `synchronous=normal`, so a commit
//! doesn't fsync, and what was committed since the WAL was last synced can be lost to a power
//! failure (never to a crash of the process); every checkpoint syncs the WAL, and one runs for
//! every 1,000 pages written, as in Rails.
//!
//! The trade-off is WAL size. SQLite only restarts the WAL from its beginning once a checkpoint
//! has caught up with it entirely, which a background checkpoint never does while writes keep
//! coming. So the WAL grows past 1,000 pages under sustained writes (it restarts in the first
//! lull), and at [`WAL_LIMIT_PAGES`] (~40 MB) the writer checkpoints it itself with RESTART,
//! stalling writes as Rails' commits do, but once per 10,000 pages instead of per 1,000.

use std::cell::Cell;
use std::collections::VecDeque;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Condvar, Mutex, MutexGuard, TryLockError};

use rails_compat::clock::SharedClock;
use rusqlite::{Connection, OpenFlags, Transaction, TransactionBehavior};
use tokio::sync::{mpsc, oneshot};

use crate::error::{Error, Result};
use crate::events::{Event, EventSink};
use crate::rich_text::RichText;
use crate::schema;
use crate::time::Timestamp;

/// Everything models need besides the connection: the clock, where side effects go, and
/// the Action Text adapter.
#[derive(Clone)]
pub struct Env {
    pub clock: SharedClock,
    pub sink: Arc<dyn EventSink>,
    pub rich_text: Arc<dyn RichText>,
    /// BCrypt cost for `has_secure_password`. Rails uses `BCrypt::Engine.cost` (12), or
    /// `MIN_COST` (4) in the test environment.
    pub bcrypt_cost: u32,
}

impl Env {
    /// `Time.current`, at the microseconds a `datetime(6)` column keeps.
    pub fn now(&self) -> Timestamp {
        Timestamp::from_jiff(self.clock.now())
    }
}

type AfterCommitHook = Box<dyn FnOnce(&mut Tx<'_>) -> Result<()> + Send>;

enum AfterCommit {
    Hook(AfterCommitHook),
    Event(Event),
}

/// A write in progress: the writer connection inside a transaction (or, while after-commit
/// work runs, outside one), the environment, and the queued after-commit work.
pub struct Tx<'c> {
    conn: &'c Connection,
    env: &'c Env,
    in_transaction: bool,
    after_commit: Vec<AfterCommit>,
}

impl<'c> Tx<'c> {
    pub fn conn(&self) -> &'c Connection {
        self.conn
    }

    pub fn env(&self) -> &'c Env {
        self.env
    }

    /// `Time.current`
    pub fn now(&self) -> Timestamp {
        self.env.now()
    }

    pub fn rich_text(&self) -> &'c dyn RichText {
        &*self.env.rich_text
    }

    /// Emits an event right away, even though the transaction may still roll back, for the
    /// side effects Rails performs mid-transaction.
    pub fn emit_now(&self, event: Event) {
        self.env.sink.emit(event);
    }

    /// Emits an event once the transaction commits (`after_commit`), or right away when
    /// already running after commit.
    pub fn emit_after_commit(&mut self, event: Event) {
        if self.in_transaction {
            self.after_commit.push(AfterCommit::Event(event));
        } else {
            self.env.sink.emit(event);
        }
    }

    /// Queues database work to run after commit, in its own implicit transaction.
    pub fn after_commit(&mut self, hook: impl FnOnce(&mut Tx<'_>) -> Result<()> + Send + 'static) {
        if self.in_transaction {
            self.after_commit.push(AfterCommit::Hook(Box::new(hook)));
        } else {
            let mut tx = Tx { conn: self.conn, env: self.env, in_transaction: false, after_commit: Vec::new() };
            if let Err(error) = hook(&mut tx) {
                tracing::error!(%error, "after_commit hook failed");
            }
        }
    }

    pub fn in_transaction(&self) -> bool {
        self.in_transaction
    }
}

/// Runs `f` in `BEGIN IMMEDIATE`, commits, then runs the after-commit queue. An error from
/// `f`, a panic in it, or a failed commit rolls back and discards the queue. An error from an
/// after-commit hook is returned after the rest of the queue has run (Rails raises it from the
/// save that committed).
pub fn run_write<T>(conn: &Connection, env: &Env, f: impl FnOnce(&mut Tx<'_>) -> Result<T>) -> Result<T> {
    // Rolls back when dropped without committing.
    let transaction = Transaction::new_unchecked(conn, TransactionBehavior::Immediate)?;
    let mut tx = Tx { conn, env, in_transaction: true, after_commit: Vec::new() };
    let value = f(&mut tx)?;
    transaction.commit()?;

    let mut queue = std::mem::take(&mut tx.after_commit);
    let mut first_error = None;
    let mut after = Tx { conn, env, in_transaction: false, after_commit: Vec::new() };
    for item in queue.drain(..) {
        match item {
            AfterCommit::Event(event) => env.sink.emit(event),
            AfterCommit::Hook(hook) => {
                if let Err(error) = hook(&mut after) {
                    tracing::error!(%error, "after_commit hook failed");
                    first_error.get_or_insert(error);
                }
            }
        }
    }
    match first_error {
        Some(error) => Err(error),
        None => Ok(value),
    }
}

/// Runs `f`'s reads in one read transaction: one snapshot for all of them, and the WAL read lock
/// taken and released once, where each statement on its own takes and releases its own (in one
/// process, often an `fcntl` each way). The transaction ends however `f` does, panics included,
/// so the connection never goes back to the readers inside it.
pub fn in_read_transaction<T>(conn: &Connection, f: impl FnOnce(&Connection) -> Result<T>) -> Result<T> {
    struct End<'c>(&'c Connection);
    impl Drop for End<'_> {
        fn drop(&mut self) {
            if !self.0.is_autocommit() {
                let _ = self.0.prepare_cached("ROLLBACK").and_then(|mut rollback| rollback.execute([]));
            }
        }
    }
    conn.prepare_cached("BEGIN")?.execute([])?;
    let end = End(conn);
    let value = f(conn)?;
    // Ending a read transaction releases its snapshot: COMMIT and ROLLBACK do the same.
    conn.prepare_cached("COMMIT")?.execute([])?;
    drop(end);
    Ok(value)
}

#[derive(Debug, Clone)]
pub struct Config {
    pub path: PathBuf,
    pub readers: usize,
    /// Bound on queued writes; senders wait when it's full.
    pub write_queue: usize,
    /// Load the schema into an empty database (`db:prepare`).
    pub prepare: bool,
    /// `ar_internal_metadata.environment` when preparing.
    pub environment: String,
}

impl Config {
    pub fn new(path: impl Into<PathBuf>) -> Self {
        Self { path: path.into(), readers: 8, write_queue: 256, prepare: true, environment: "production".into() }
    }
}

type Job = Box<dyn FnOnce(&Connection, &Env) + Send>;
type Read = Box<dyn FnOnce(&Connection) + Send>;

/// The database handle. Cheap to clone.
#[derive(Clone)]
pub struct Database {
    writer: mpsc::Sender<Job>,
    readers: Arc<Readers>,
    /// [`Database::read_blocking`]'s connection, opened on first use.
    blocking_reader: Arc<Mutex<Option<Connection>>>,
    env: Env,
    path: PathBuf,
}

impl Database {
    pub fn open(config: Config, env: Env) -> Result<Self> {
        let mut conn = open_connection(&config.path, false)?;
        if config.prepare {
            schema::prepare(&mut conn, &config.environment, &*env.clock)?;
        }
        let mut checkpoints = Checkpoints::spawn(&config.path)?;
        // In place of the auto-checkpoint, which is itself a WAL hook (`sqlite3_wal_autocheckpoint`).
        conn.wal_hook(Some(note_wal_size));

        let (sender, mut receiver) = mpsc::channel::<Job>(config.write_queue.max(1));
        let writer_env = env.clone();
        std::thread::Builder::new()
            .name("campfire-db-writer".into())
            .spawn(move || {
                while let Some(job) = receiver.blocking_recv() {
                    // A panicking write must not take the writer down with it. `run_write`'s
                    // transaction rolls back as the panic unwinds; the rollback here is a
                    // backstop for a job that panics some other way.
                    let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| job(&conn, &writer_env)));
                    if outcome.is_err() && !conn.is_autocommit() {
                        let _ = conn.execute_batch("ROLLBACK TRANSACTION");
                    }
                    match WAL_PAGES.replace(0) {
                        0 => {}
                        pages if pages >= WAL_LIMIT_PAGES => restart_wal(&conn, &checkpoints),
                        pages => checkpoints.wal_grew_to(pages),
                    }
                }
            })
            .map_err(Error::other)?;

        let readers = Readers::start(&config.path, config.readers.max(1))?;

        Ok(Self { writer: sender, readers: Arc::new(readers), blocking_reader: Arc::default(), env, path: config.path })
    }

    pub fn env(&self) -> &Env {
        &self.env
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Runs `f` as one immediate transaction on the writer thread.
    pub async fn write<T, F>(&self, f: F) -> Result<T>
    where
        T: Send + 'static,
        F: FnOnce(&mut Tx<'_>) -> Result<T> + Send + 'static,
    {
        let (reply, response) = oneshot::channel();
        self.writer
            .send(Box::new(move |conn, env| {
                let _ = reply.send(run_write(conn, env, f));
            }))
            .await
            .map_err(|_| Error::WriterGone)?;
        response.await.map_err(|_| Error::WriterGone)?
    }

    /// [`Database::write`] for synchronous callers (not from inside an async task).
    pub fn write_blocking<T, F>(&self, f: F) -> Result<T>
    where
        T: Send + 'static,
        F: FnOnce(&mut Tx<'_>) -> Result<T> + Send + 'static,
    {
        let (reply, response) = oneshot::channel();
        self.writer
            .blocking_send(Box::new(move |conn, env| {
                let _ = reply.send(run_write(conn, env, f));
            }))
            .map_err(|_| Error::WriterGone)?;
        response.blocking_recv().map_err(|_| Error::WriterGone)?
    }

    /// Runs `f` on a reader connection: right here, on the calling task's thread, when one is free
    /// and no read is queued for one, since a read on a warm page cache takes less time than the
    /// hop to a reader thread and back; otherwise as [`Database::read_offloaded`] does. At most as
    /// many runtime workers as there are readers are ever inside `f`.
    ///
    /// Reads whose cost grows with the whole database rather than with a page (search, every
    /// user, all of a user's messages) use [`Database::read_offloaded`], to keep them off the
    /// runtime's workers.
    pub async fn read<T, F>(&self, f: F) -> Result<T>
    where
        T: Send + 'static,
        F: FnOnce(&Connection) -> Result<T> + Send + 'static,
    {
        let Some(conn) = self.readers.queue.take_connection() else {
            return self.read_offloaded(f).await;
        };
        // A panicking read fails its caller's read, as on a reader thread, and gives its connection back.
        let result =
            std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| f(&conn))).unwrap_or_else(|_| Err(Error::other("the read panicked")));
        self.readers.queue.give_back(conn);
        // Give the worker's other tasks their turn, as the hop to another thread did.
        tokio::task::yield_now().await;
        result
    }

    /// Runs `f` on a reader thread, the next one free. Once queued, `f` runs even if its caller
    /// stops waiting (a request dropped when its client goes away), as it did on the blocking
    /// pool: some reads broadcast what a write committed, like messages#create's.
    pub async fn read_offloaded<T, F>(&self, f: F) -> Result<T>
    where
        T: Send + 'static,
        F: FnOnce(&Connection) -> Result<T> + Send + 'static,
    {
        let (reply, response) = oneshot::channel();
        self.readers.queue.push(Box::new(move |conn| {
            let _ = reply.send(f(conn));
        }));
        // A read that panics drops its reply as it unwinds.
        response.await.map_err(|_| Error::other("the read panicked"))?
    }

    /// [`Database::read`] for synchronous callers (tests), on the calling thread, so that `f` may
    /// borrow. It keeps a reader connection for these reads, and opens another for one that finds
    /// it in use: a `read_blocking` inside another would otherwise wait for itself.
    pub fn read_blocking<T>(&self, f: impl FnOnce(&Connection) -> Result<T>) -> Result<T> {
        let mut kept = match self.blocking_reader.try_lock() {
            Ok(kept) => kept,
            Err(TryLockError::Poisoned(poisoned)) => poisoned.into_inner(),
            Err(TryLockError::WouldBlock) => return f(&open_connection(&self.path, true)?),
        };
        if kept.is_none() {
            *kept = Some(open_connection(&self.path, true)?);
        }
        f(kept.as_ref().expect("opened"))
    }
}

/// Prepared statements each connection keeps.
const STATEMENT_CACHE_CAPACITY: usize = 256;

/// SQLite's default `wal_autocheckpoint`, which Rails keeps: a checkpoint per 1,000 WAL pages.
const AUTOCHECKPOINT_PAGES: i32 = 1000;

/// The WAL size at which the writer checkpoints and restarts the WAL itself, because writes never
/// paused long enough for a background checkpoint to catch up. Below `journal_size_limit`.
const WAL_LIMIT_PAGES: i32 = 10_000;

thread_local! {
    /// The WAL's size in pages after the writer thread's latest commit.
    static WAL_PAGES: Cell<i32> = const { Cell::new(0) };
}

/// The writer connection's WAL hook: runs on the writer thread after each commit.
fn note_wal_size(_: &rusqlite::hooks::Wal, pages: std::os::raw::c_int) -> rusqlite::Result<()> {
    WAL_PAGES.set(pages);
    Ok(())
}

/// The writer's side of the checkpointer thread, which runs a PASSIVE checkpoint on its own
/// connection each time it's woken. It stops with the writer (the sender's owner).
struct Checkpoints {
    wake: std::sync::mpsc::SyncSender<()>,
    /// Held while the checkpointer runs, so the writer's RESTART waits for it rather than being
    /// refused (SQLite runs one checkpoint at a time) and letting the WAL grow past its limit.
    running: Arc<Mutex<()>>,
    /// The WAL's size in pages when the checkpointer was last woken.
    woken_at: i32,
}

impl Checkpoints {
    fn spawn(path: &Path) -> Result<Self> {
        let conn = open_connection(path, false)?;
        let (wake, woken) = std::sync::mpsc::sync_channel::<()>(1);
        let running = Arc::new(Mutex::new(()));
        let checkpointer_running = running.clone();
        std::thread::Builder::new()
            .name("campfire-db-checkpointer".into())
            .spawn(move || {
                while woken.recv().is_ok() {
                    let _running = checkpointer_running.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
                    checkpoint(&conn, "PASSIVE");
                }
            })
            .map_err(Error::other)?;
        Ok(Self { wake, running, woken_at: 0 })
    }

    /// Wakes the checkpointer for every [`AUTOCHECKPOINT_PAGES`] the WAL grows.
    fn wal_grew_to(&mut self, pages: i32) {
        if pages < self.woken_at {
            self.woken_at = 0; // the WAL restarted
        }
        // While a checkpoint is still due (the channel is full), the next commit tries again.
        if pages - self.woken_at >= AUTOCHECKPOINT_PAGES && self.wake.try_send(()).is_ok() {
            self.woken_at = pages;
        }
    }
}

/// A RESTART checkpoint on the writer connection, between writes: it copies what the
/// checkpointer hasn't, and waits for readers so that the next write restarts the WAL. It waits
/// for a running PASSIVE checkpoint first, which would otherwise make SQLite refuse it.
fn restart_wal(conn: &Connection, checkpoints: &Checkpoints) {
    let _running = checkpoints.running.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
    checkpoint(conn, "RESTART");
}

/// `PRAGMA wal_checkpoint`, which reports a checkpoint it couldn't finish (another checkpoint
/// running, or readers still on old frames past the busy timeout) in its `busy` column, not as an
/// error.
fn checkpoint(conn: &Connection, mode: &str) {
    match conn.query_row(&format!("PRAGMA wal_checkpoint({mode})"), [], |row| row.get::<_, i64>(0)) {
        Ok(0) => {}
        Ok(_) => tracing::warn!(mode, "WAL checkpoint couldn't finish"),
        Err(error) => tracing::warn!(%error, mode, "WAL checkpoint failed"),
    }
}

fn open_connection(path: &Path, reader: bool) -> Result<Connection> {
    let flags =
        OpenFlags::SQLITE_OPEN_READ_WRITE | OpenFlags::SQLITE_OPEN_CREATE | OpenFlags::SQLITE_OPEN_NO_MUTEX | OpenFlags::SQLITE_OPEN_URI;
    let conn = Connection::open_with_flags(path, flags)?;
    // rusqlite's default of 16 is fewer statements than a page like the room show runs.
    conn.set_prepared_statement_cache_capacity(STATEMENT_CACHE_CAPACITY);
    schema::configure_connection(&conn)?;
    if reader {
        conn.pragma_update(None, "query_only", true)?;
    }
    Ok(conn)
}

/// The reader threads, one per reader connection. They stop once the last [`Database`] handle,
/// and so this, is gone.
struct Readers {
    queue: Arc<ReadQueue>,
}

impl Readers {
    fn start(path: &Path, count: usize) -> Result<Self> {
        // Built first, so that a connection failing to open drops it, which stops the threads
        // started before.
        let readers = Self { queue: Arc::default() };
        for _ in 0..count {
            readers.queue.lock().connections.push(open_connection(path, true)?);
            let queue = readers.queue.clone();
            std::thread::Builder::new()
                .name("campfire-db-reader".into())
                .spawn(move || {
                    let mut finished = None;
                    while let Some((read, conn)) = queue.next(finished.take()) {
                        // A panicking read fails its caller's read, not the reader.
                        let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| read(&conn)));
                        finished = Some(conn);
                    }
                })
                .map_err(Error::other)?;
        }
        Ok(readers)
    }
}

impl Drop for Readers {
    fn drop(&mut self) {
        self.queue.lock().closed = true;
        self.queue.ready.notify_all();
    }
}

/// The reads waiting for a reader thread, in order, and the reader connections not in use.
#[derive(Default)]
struct ReadQueue {
    state: Mutex<Queued>,
    ready: Condvar,
}

#[derive(Default)]
struct Queued {
    reads: VecDeque<Read>,
    connections: Vec<Connection>,
    /// Reader threads waiting for a read and a connection to run it on.
    idle: usize,
    closed: bool,
}

impl ReadQueue {
    fn push(&self, read: Read) {
        let mut state = self.lock();
        state.reads.push_back(read);
        self.wake_a_reader(state);
    }

    /// A connection for a read on the calling thread, unless none is free or reads are queued
    /// (they go first).
    fn take_connection(&self) -> Option<Connection> {
        let mut state = self.lock();
        if state.reads.is_empty() { state.connections.pop() } else { None }
    }

    /// Returns a connection taken by [`ReadQueue::take_connection`].
    fn give_back(&self, conn: Connection) {
        let mut state = self.lock();
        state.connections.push(conn);
        self.wake_a_reader(state);
    }

    /// Wakes a reader thread when one is waiting and there's a read and a connection for it. Only
    /// then: std's Condvar makes a futex syscall for every notify, waiter or not.
    fn wake_a_reader(&self, state: MutexGuard<'_, Queued>) {
        let wake = state.idle > 0 && !state.reads.is_empty() && !state.connections.is_empty();
        drop(state);
        if wake {
            self.ready.notify_one();
        }
    }

    /// For a reader thread, which gives back the connection of the read it `finished`: the next
    /// read and a connection to run it on, once there are both; `None` once the queue is closed
    /// and empty.
    fn next(&self, finished: Option<Connection>) -> Option<(Read, Connection)> {
        let mut state = self.lock();
        state.connections.extend(finished);
        loop {
            if !state.reads.is_empty() && !state.connections.is_empty() {
                return state.reads.pop_front().zip(state.connections.pop());
            }
            if state.closed && state.reads.is_empty() {
                return None;
            }
            state.idle += 1;
            state = self.ready.wait(state).unwrap_or_else(|poisoned| poisoned.into_inner());
            state.idle -= 1;
        }
    }

    fn lock(&self) -> MutexGuard<'_, Queued> {
        self.state.lock().unwrap_or_else(|poisoned| poisoned.into_inner())
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Duration;

    use tokio::task::JoinHandle;

    use super::*;

    fn main_file_len(path: &Path) -> u64 {
        std::fs::metadata(path).unwrap().len()
    }

    #[test]
    fn now_is_truncated_to_microseconds() {
        let at = jiff::Timestamp::new(1_700_000_000, 123_456_999).unwrap();
        let env = Env { clock: Arc::new(rails_compat::clock::TestClock::frozen_at(at)), ..Env::default() };
        assert_eq!(env.now().to_db(), "2023-11-14 22:13:20.123456");
    }

    #[test]
    fn a_panicking_blocking_read_leaves_its_connection_usable() {
        let dir = tempfile::tempdir().unwrap();
        let db = open_with_readers(&dir, 1);
        for _ in 0..3 {
            let panicked = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                db.read_blocking(|_| -> Result<()> { panic!("a bug in a read") })
            }));
            assert!(panicked.is_err());
        }
        assert_eq!(db.read_blocking(select_one).unwrap(), 1);
    }

    /// A `read_blocking` inside another opens a connection rather than waiting for its caller's.
    #[test]
    fn blocking_reads_nest() {
        let dir = tempfile::tempdir().unwrap();
        let db = open_with_readers(&dir, 1);
        let (done, nested) = std::sync::mpsc::channel();
        std::thread::spawn(move || {
            let _ = done.send(db.read_blocking(|outer| Ok(select_one(outer)? + db.read_blocking(select_one)?)));
        });
        let sum = nested.recv_timeout(Duration::from_secs(10)).expect("the inner read waited for the outer read's connection");
        assert_eq!(sum.unwrap(), 2);
    }

    fn select_one(conn: &Connection) -> Result<i64> {
        Ok(conn.query_row("SELECT 1", [], |r| r.get(0))?)
    }

    fn open_with_readers(dir: &tempfile::TempDir, readers: usize) -> Database {
        let mut config = Config::new(dir.path().join("test.sqlite3"));
        config.readers = readers;
        Database::open(config, Env::default()).unwrap()
    }

    /// Awaits `future` for up to 10 seconds, so that reads which never run fail these tests
    /// (saying `what` was awaited) rather than hang them.
    async fn within<T>(what: &str, future: impl Future<Output = T>) -> T {
        tokio::time::timeout(Duration::from_secs(10), future).await.unwrap_or_else(|_| panic!("timed out after 10 s waiting for: {what}"))
    }

    fn queued(db: &Database) -> usize {
        db.readers.queue.lock().reads.len()
    }

    /// Waits until `count` reads are queued for a reader.
    async fn until_queued(db: &Database, count: usize) {
        within(&format!("{count} reads queued"), async {
            while queued(db) < count {
                tokio::task::yield_now().await;
            }
        })
        .await;
    }

    /// Awaits reads spawned as tasks, each of which must succeed.
    async fn all_succeed(what: &str, reads: impl IntoIterator<Item = JoinHandle<Result<()>>>) {
        within(what, async {
            for read in reads {
                read.await.unwrap().unwrap();
            }
        })
        .await;
    }

    /// Occupies every reader thread until the returned senders are dropped.
    async fn hold_every_reader(db: &Database, readers: usize) -> (Vec<std::sync::mpsc::Sender<()>>, Vec<JoinHandle<Result<()>>>) {
        let (started, mut holding) = mpsc::unbounded_channel();
        let (releases, holders) = (0..readers)
            .map(|_| {
                let (release, released) = std::sync::mpsc::channel::<()>();
                let (db, started) = (db.clone(), started.clone());
                let holder = tokio::spawn(async move {
                    db.read_offloaded(move |_| {
                        started.send(()).unwrap();
                        let _ = released.recv();
                        Ok(())
                    })
                    .await
                });
                (release, holder)
            })
            .unzip();
        within(&format!("{readers} readers each started a read"), async {
            for _ in 0..readers {
                holding.recv().await.unwrap();
            }
        })
        .await;
        (releases, holders)
    }

    /// Reads that wait for a reader take no thread: with the blocking pool capped at one thread,
    /// other blocking work still gets it while ten reads wait for busy readers.
    #[test]
    fn waiting_reads_hold_no_threads() {
        const READERS: usize = 2;
        let runtime = tokio::runtime::Builder::new_current_thread().max_blocking_threads(1).enable_all().build().unwrap();
        runtime.block_on(async {
            let dir = tempfile::tempdir().unwrap();
            let db = open_with_readers(&dir, READERS);
            let (releases, holders) = hold_every_reader(&db, READERS).await;

            let ran = Arc::new(AtomicUsize::new(0));
            let waiting: Vec<_> = (0..10)
                .map(|_| {
                    let (db, ran) = (db.clone(), ran.clone());
                    tokio::spawn(async move {
                        db.read(move |_| {
                            ran.fetch_add(1, Ordering::SeqCst);
                            Ok(())
                        })
                        .await
                    })
                })
                .collect();
            until_queued(&db, 10).await;

            let other_work = within("other blocking work while reads waited", tokio::task::spawn_blocking(|| "done")).await;
            assert_eq!(other_work.unwrap(), "done");
            assert_eq!(ran.load(Ordering::SeqCst), 0, "no reader came free");

            drop(releases);
            all_succeed("the waiting reads ran once the readers came free", holders.into_iter().chain(waiting)).await;
            assert_eq!(ran.load(Ordering::SeqCst), 10);
        });
    }

    #[tokio::test]
    async fn reads_run_in_the_order_they_were_queued() {
        let dir = tempfile::tempdir().unwrap();
        let db = open_with_readers(&dir, 1);
        let (releases, holders) = hold_every_reader(&db, 1).await;

        let order = Arc::new(Mutex::new(Vec::new()));
        let mut reads = Vec::new();
        for n in 0..10 {
            let (reader, order) = (db.clone(), order.clone());
            reads.push(tokio::spawn(async move {
                reader
                    .read(move |_| {
                        order.lock().unwrap().push(n);
                        Ok(())
                    })
                    .await
            }));
            until_queued(&db, n + 1).await;
        }

        drop(releases);
        all_succeed("the queued reads ran once the reader came free", holders.into_iter().chain(reads)).await;
        assert_eq!(*order.lock().unwrap(), (0..10).collect::<Vec<_>>());
    }

    /// A queued read runs even when its caller stops waiting for it, because some reads broadcast
    /// what a write committed (messages#create's): a request dropped as its client goes away must
    /// not lose the broadcast.
    #[tokio::test]
    async fn a_read_given_up_while_it_waits_still_runs() {
        let dir = tempfile::tempdir().unwrap();
        let db = open_with_readers(&dir, 1);
        let (releases, holders) = hold_every_reader(&db, 1).await;

        let (ran, runs) = oneshot::channel();
        let abandoned = db.read(move |_| {
            let _ = ran.send(());
            Ok(())
        });
        assert!(tokio::time::timeout(Duration::from_millis(50), abandoned).await.is_err(), "the reader is busy");
        assert_eq!(queued(&db), 1, "the abandoned read is still queued");

        drop(releases);
        all_succeed("the holding read finished", holders).await;
        within("the abandoned read ran", runs).await.expect("the abandoned read was dropped without running");
    }

    #[tokio::test]
    async fn a_panicking_read_fails_and_leaves_its_reader_running() {
        let dir = tempfile::tempdir().unwrap();
        let db = open_with_readers(&dir, 1);
        for _ in 0..3 {
            assert!(within("the panicking read failed", db.read(|_| -> Result<()> { panic!("a bug in a read") })).await.is_err());
            let offloaded = db.read_offloaded(|_| -> Result<()> { panic!("a bug in a read") });
            assert!(within("the panicking offloaded read failed", offloaded).await.is_err());
        }
        assert_eq!(within("a read after the panics", db.read(select_one)).await.unwrap(), 1);
    }

    /// No readers configured still starts one.
    #[tokio::test]
    async fn zero_configured_readers_still_read() {
        let dir = tempfile::tempdir().unwrap();
        let db = open_with_readers(&dir, 0);
        assert_eq!(within("the read", db.read(select_one)).await.unwrap(), 1);
    }

    /// Each reader thread holds the queue, so the queue going means they've all stopped.
    #[test]
    fn the_reader_threads_stop_with_the_last_handle() {
        let dir = tempfile::tempdir().unwrap();
        let db = open_with_readers(&dir, 3);
        let other = db.clone();
        let queue = Arc::downgrade(&db.readers.queue);
        drop(db);
        assert_eq!(queue.strong_count(), 4, "the other handle and the three readers");
        drop(other);
        let deadline = std::time::Instant::now() + Duration::from_secs(10);
        while queue.strong_count() > 0 {
            assert!(std::time::Instant::now() < deadline, "the reader threads are still running");
            std::thread::sleep(Duration::from_millis(1));
        }
    }

    #[test]
    fn a_panicking_write_rolls_back() {
        let conn = Connection::open_in_memory().unwrap();
        conn.execute_batch("CREATE TABLE things (id INTEGER)").unwrap();
        let panicked = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            run_write(&conn, &Env::default(), |tx| -> Result<()> {
                tx.conn().execute("INSERT INTO things VALUES (1)", [])?;
                panic!("a bug in a write")
            })
        }));
        assert!(panicked.is_err());
        assert!(conn.is_autocommit(), "the transaction was left open");
        assert_eq!(conn.query_row("SELECT COUNT(*) FROM things", [], |r| r.get::<_, i64>(0)).unwrap(), 0);
    }

    #[test]
    fn a_panicking_write_leaves_the_writer_usable() {
        let dir = tempfile::tempdir().unwrap();
        let mut config = Config::new(dir.path().join("test.sqlite3"));
        config.readers = 1;
        let db = Database::open(config, Env::default()).unwrap();
        db.write_blocking(|tx| Ok(tx.conn().execute_batch("CREATE TABLE things (id INTEGER)")?)).unwrap();

        let panicked = db.write_blocking(|tx| -> Result<()> {
            tx.conn().execute("INSERT INTO things VALUES (1)", [])?;
            panic!("a bug in a write")
        });
        assert!(matches!(panicked, Err(Error::WriterGone)), "{panicked:?}");

        db.write_blocking(|tx| Ok(tx.conn().execute("INSERT INTO things VALUES (2)", [])?)).unwrap();
        let ids = db.read_blocking(|conn| Ok(conn.query_row("SELECT group_concat(id) FROM things", [], |r| r.get::<_, String>(0))?));
        assert_eq!(ids.unwrap(), "2");
    }

    #[tokio::test]
    async fn a_read_runs_on_the_calling_thread_while_a_reader_is_free() {
        let dir = tempfile::tempdir().unwrap();
        let db = open_with_readers(&dir, 1);
        let caller = std::thread::current().id();
        assert_eq!(db.read(|_| Ok(std::thread::current().id())).await.unwrap(), caller);
        assert_ne!(db.read_offloaded(|_| Ok(std::thread::current().id())).await.unwrap(), caller);
    }

    /// A connection that comes free while reads are queued is theirs, so a read on the calling
    /// thread queues behind them instead of taking it.
    #[test]
    fn queued_reads_get_a_free_connection_first() {
        let queue = ReadQueue::default();
        queue.give_back(Connection::open_in_memory().unwrap());
        queue.push(Box::new(|_| {}));
        assert!(queue.take_connection().is_none());
        let (_, conn) = queue.next(None).expect("the queued read, with the connection");
        queue.give_back(conn);
        assert!(queue.take_connection().is_some());
    }

    /// A read queued while the only connection is in use on the calling thread gets it back.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_read_waits_for_a_connection_in_use_on_the_calling_thread() {
        let dir = tempfile::tempdir().unwrap();
        let db = open_with_readers(&dir, 1);
        let (release, released) = std::sync::mpsc::channel::<()>();
        let (started, starting) = oneshot::channel();
        let holder = tokio::spawn({
            let db = db.clone();
            async move {
                db.read(move |_| {
                    started.send(()).unwrap();
                    let _ = released.recv();
                    Ok(())
                })
                .await
            }
        });
        within("the inline read started", starting).await.unwrap();
        let waiting = tokio::spawn({
            let db = db.clone();
            async move { db.read(select_one).await }
        });
        until_queued(&db, 1).await;
        drop(release);
        within("the inline read finished", holder).await.unwrap().unwrap();
        assert_eq!(within("the queued read ran", waiting).await.unwrap().unwrap(), 1);
    }

    /// Commits never checkpoint on the writer: the WAL reaching the auto-checkpoint threshold
    /// wakes the checkpointer, which copies it into the database file on its own.
    #[test]
    fn the_checkpointer_copies_the_wal_into_the_database() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.sqlite3");
        let mut config = Config::new(&path);
        config.readers = 1;
        let db = Database::open(config, Env::default()).unwrap();
        db.write_blocking(|tx| Ok(tx.conn().execute_batch("CREATE TABLE filler (data BLOB)")?)).unwrap();
        let before = main_file_len(&path);

        // ~1,200 pages of 4 KiB, over a few commits.
        for _ in 0..6 {
            db.write_blocking(|tx| {
                Ok(tx.conn().execute_batch("WITH RECURSIVE n(i) AS (SELECT 1 UNION ALL SELECT i + 1 FROM n WHERE i < 200) INSERT INTO filler SELECT randomblob(3900) FROM n")?)
            })
            .unwrap();
        }

        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
        while main_file_len(&path) < before + 1000 * 4096 {
            assert!(std::time::Instant::now() < deadline, "the WAL was never checkpointed");
            std::thread::sleep(std::time::Duration::from_millis(10));
        }
    }

    /// Before 3.51.3, a checkpoint that starts just as another connection's commit restarts the
    /// WAL can leave that commit out of the database (https://sqlite.org/wal.html#walresetbug).
    /// The checkpointer and the writer are two such connections, on separate threads.
    #[test]
    fn the_bundled_sqlite_has_the_wal_reset_fix() {
        assert!(rusqlite::version_number() >= 3_051_003, "SQLite {}", rusqlite::version());
    }

    /// Writes that never pause still get the WAL restarted, at WAL_LIMIT_PAGES.
    #[test]
    fn the_wal_stays_bounded_under_sustained_writes() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.sqlite3");
        let mut config = Config::new(&path);
        config.readers = 1;
        let db = Database::open(config, Env::default()).unwrap();
        db.write_blocking(|tx| Ok(tx.conn().execute_batch("CREATE TABLE filler (data BLOB)")?)).unwrap();

        // ~25,000 pages, 500 per commit.
        for _ in 0..50 {
            db.write_blocking(|tx| {
                Ok(tx.conn().execute_batch(
                    "WITH RECURSIVE n(i) AS (SELECT 1 UNION ALL SELECT i + 1 FROM n WHERE i < 500) INSERT INTO filler SELECT randomblob(3900) FROM n",
                )?)
            })
            .unwrap();
        }
        let wal = std::fs::metadata(path.with_extension("sqlite3-wal")).unwrap().len();
        assert!(wal < (WAL_LIMIT_PAGES as u64 + 1000) * 4200, "WAL of {wal} bytes");
    }
}
