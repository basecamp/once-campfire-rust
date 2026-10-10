//! Boot: configuration, the database (`db:prepare`), storage, the cable server, jobs and the HTTP
//! stack, wired the way the reference's middleware and initializers are.
//!
//! [`AppState`] is what controllers, channels, jobs and integrations share. Actions reach it with
//! `c.app()` ([`AppCtx`]).
//!
//! Request flow (mirroring the Rails middleware order): `Rack::Deflater` (config.ru) → kit's
//! pre-routing middleware (`ActionDispatch::SSL`, request id, `_method` override) → public files
//! (`ActionDispatch::Static`, from `campfire_assets`) → `/cable` (Action Cable) or the Rails route
//! table (`controllers::dispatch`).

use std::sync::Arc;
use std::time::Duration;

use axum::Router;
use axum::middleware::Next;
use campfire_db::{Connection, Database, Tx};
use campfire_kit::exceptions::ErrorPages;
use campfire_kit::{Ctx, Kit, KitConfig, SharedClock};
use campfire_storage::{DiskService, Storage};
use campfire_views::fragment_cache::{FragmentCache, Scoped};
use rails_compat::Secrets;

use crate::config::Config;
use crate::rich_text::AppRichText;
use crate::{channels, controllers, jobs};

pub use crate::channels::Cable;

/// Everything that outlives a request. Cheap to share as [`App`].
pub struct AppState {
    pub config: Config,
    pub secrets: Arc<Secrets>,
    pub clock: SharedClock,
    pub db: Database,
    pub storage: Arc<Storage>,
    pub cable: Cable,
    pub broadcasts: channels::Broadcasts,
    pub jobs: jobs::Jobs,
    /// `config.x.web_push_pool`; `None` when Web Push is off (no valid VAPID keys).
    pub web_push: Option<crate::integrations::web_push::Pool>,
    /// `Rails.cache` for view fragments (`cache message do`), current during every request
    /// and every render outside one.
    pub fragment_cache: Arc<FragmentCache>,
    pub response_cache: Arc<crate::response_cache::Store>,
}

impl AppState {
    /// Current database namespace for request-less broadcasts and jobs.
    pub fn fragments(&self) -> Arc<FragmentCache> {
        match self.response_cache.version() {
            Some(generation) => self.fragment_cache.namespace(generation).scoped("detached"),
            None => FragmentCache::new(0),
        }
    }

    /// The key pages offer browsers to subscribe with: none while Web Push is off, so that browsers
    /// don't subscribe to notifications that would never be sent.
    pub fn vapid_public_key(&self) -> Option<String> {
        self.web_push.as_ref().and(self.config.vapid_public_key.clone())
    }

    /// `db.read` for actions, whose errors become responses as [`db_error`] maps them. (Jobs and
    /// channels use `db.read`, for a `campfire_db::Result`.)
    pub async fn read<T, F>(&self, f: F) -> campfire_kit::Result<T>
    where
        T: Send + 'static,
        F: FnOnce(&Connection) -> campfire_db::Result<T> + Send + 'static,
    {
        self.db.read(f).await.map_err(db_error)
    }

    /// `db.read_offloaded` for actions (reads whose cost grows with the whole database), with
    /// errors mapped as [`AppState::read`] maps them.
    pub async fn read_offloaded<T, F>(&self, f: F) -> campfire_kit::Result<T>
    where
        T: Send + 'static,
        F: FnOnce(&Connection) -> campfire_db::Result<T> + Send + 'static,
    {
        self.db.read_offloaded(f).await.map_err(db_error)
    }

    /// `db.write` for actions, whose errors become responses as [`db_error`] maps them.
    pub async fn write<T, F>(&self, f: F) -> campfire_kit::Result<T>
    where
        T: Send + 'static,
        F: FnOnce(&mut Tx<'_>) -> campfire_db::Result<T> + Send + 'static,
    {
        self.db.write(f).await.map_err(db_error)
    }
}

/// A database error raised in an action, as Active Record's `rescue_responses` answer it
/// (activerecord/lib/active_record/railtie.rb): `RecordNotFound` is a 404, `RecordInvalid` a 422,
/// and anything else a 500.
pub fn db_error(error: campfire_db::Error) -> campfire_kit::Error {
    match error {
        campfire_db::Error::RecordNotFound(_) => campfire_kit::Error::NotFound,
        invalid @ campfire_db::Error::RecordInvalid(_) => {
            campfire_kit::Error::with_status(campfire_kit::StatusCode::UNPROCESSABLE_ENTITY, invalid)
        }
        other => campfire_kit::Error::internal(other),
    }
}

pub type App = Arc<AppState>;

/// `c.app()` in actions.
pub trait AppCtx {
    fn app(&self) -> &App;
}

impl AppCtx for Ctx {
    fn app(&self) -> &App {
        self.state::<App>()
    }
}

/// A booted app: its state, the HTTP service, and the job runner.
pub struct Booted {
    pub app: App,
    pub router: Router,
    pub jobs: jobs::Runner,
}

/// Boots the app from `config`: prepares the database, restores the reference's boot-time
/// side effects, and builds the HTTP stack. Must run inside a Tokio runtime.
pub async fn boot(config: Config) -> anyhow::Result<Booted> {
    config.storage.create_dirs()?;
    let secrets = Arc::new(Secrets::new(&config.secret_key_base));
    let clock = campfire_kit::clock::from_env()?;

    let (jobs, queue) = jobs::Jobs::new(jobs::QUEUE_CAPACITY);
    let rich_text = Arc::new(AppRichText::new(secrets.clone(), clock.clone()));
    let db = open_database(&config, clock.clone(), jobs.clone(), rich_text.clone()).await?;

    // config/puma.rb: `Membership.disconnect_all` when the server boots.
    db.write(|tx| campfire_db::Membership::disconnect_all(tx).map(|_| ())).await?;

    let storage =
        Arc::new(Storage::new(DiskService::new(&config.storage.files, "local"), rails_compat::app_verifier(&secrets, "ActiveStorage")));

    let cable_config = campfire_cable::Config { assume_ssl: !config.disable_ssl, ..campfire_cable::Config::default() };
    let deps = channels::Deps { db: db.clone(), secrets: secrets.clone(), clock: clock.clone() };
    let cable = channels::server(deps, cable_config);

    let mut kit_config = KitConfig::production(config.disable_ssl);
    kit_config.error_pages = error_pages();

    let fragment_cache = FragmentCache::new(config.fragment_cache_bytes);
    let response_cache = crate::response_cache::Store::open(db.path(), config.response_cache_bytes)?;
    let web_push = crate::integrations::web_push_pool(&config, &db, jobs.backlog().clone());
    let app = Arc::new(AppState {
        config,
        secrets,
        clock: clock.clone(),
        db,
        storage,
        broadcasts: channels::Broadcasts::new(cable.clone()),
        cable,
        jobs,
        web_push,
        fragment_cache,
        response_cache,
    });

    let mut registry = jobs::Registry::with_core_jobs();
    // Room::PushMessageJob and Bot::WebhookJob
    crate::integrations::register_jobs(&mut registry);
    let runner = jobs::start(queue, app.clone(), registry, app.config.job_concurrency);

    let kit = Kit::new(kit_config, app.secrets.clone(), clock, app.clone());
    let router = router(&app, kit);
    Ok(Booted { app, router, jobs: runner })
}

async fn open_database(config: &Config, clock: SharedClock, jobs: jobs::Jobs, rich_text: Arc<AppRichText>) -> anyhow::Result<Database> {
    let mut db_config = campfire_db::Config::new(&config.storage.database);
    db_config.readers = config.db_readers;
    db_config.environment = config.environment.clone();
    let env = campfire_db::Env { clock, sink: Arc::new(jobs), rich_text, bcrypt_cost: rails_compat::password::COST };
    Ok(tokio::task::spawn_blocking(move || Database::open(db_config, env)).await??)
}

/// The HTTP service: public files, then `/cable`, then the Rails route table.
fn router(app: &App, kit: Kit) -> Router {
    let dispatch = || axum::routing::any(campfire_kit::action(dispatch_with_fragment_cache));
    let routes = Router::new()
        .merge(app.cable.router::<Kit>(campfire_cable::protocol::DEFAULT_MOUNT_PATH))
        .route("/", dispatch())
        .route("/{*path}", dispatch())
        .layer(axum::middleware::from_fn(public_files));
    // config.ru: `use Rack::Deflater` around the whole app.
    campfire_kit::app(routes, kit)
        .layer(axum::middleware::from_fn(campfire_kit::deflater::deflater))
        .layer(axum::middleware::from_fn(crate::response_cache::completed))
}

/// The Rails route table, with the app's fragment cache current while the action runs. A request
/// that can write waits first while the job backlog is over its mark (see `campfire_jobs::Backlog`).
async fn dispatch_with_fragment_cache(c: &mut Ctx) -> campfire_kit::Result {
    if !matches!(c.request.method, axum::http::Method::GET | axum::http::Method::HEAD) {
        c.app().jobs.backlog().admit().await;
    }
    let snapshot = crate::response_cache::Snapshot::capture(c);
    c.set_current(snapshot);
    let cache = crate::response_cache::fragments(c);
    Scoped::new(cache, controllers::dispatch(c)).await
}

/// `ActionDispatch::Static`: serve `public/` (including digested `/assets`) before routing.
async fn public_files(request: axum::extract::Request, next: Next) -> axum::response::Response {
    match static_response(&request) {
        Some(response) => response,
        None => next.run(request).await,
    }
}

fn static_response(request: &axum::extract::Request) -> Option<axum::response::Response> {
    let header = |name| request.headers().get(name).and_then(|v| v.to_str().ok());
    let served = campfire_assets::serve(&campfire_assets::StaticRequest {
        method: request.method().as_str(),
        path: request.uri().path(),
        accept_encoding: header(axum::http::header::ACCEPT_ENCODING),
        range: header(axum::http::header::RANGE),
        if_modified_since: header(axum::http::header::IF_MODIFIED_SINCE),
    })?;
    // A borrowed body is the embedded file (or a range of it), sent as `Bytes::from_static`
    // without a copy; only a multipart range body is owned.
    let mut response = axum::response::Response::new(axum::body::Body::from(served.body));
    *response.status_mut() = axum::http::StatusCode::from_u16(served.status).unwrap_or(axum::http::StatusCode::OK);
    response.extensions_mut().insert(campfire_kit::deflater::StaticFile);
    // `try_from` a String keeps its buffer (and checks it as `from_str` does, which copies).
    for (name, value) in served.headers {
        if let (Ok(name), Ok(value)) = (axum::http::HeaderName::from_bytes(name.as_bytes()), axum::http::HeaderValue::try_from(value)) {
            response.headers_mut().append(name, value);
        }
    }
    Some(response)
}

/// The error pages kit renders (`ActionDispatch::PublicExceptions`), from the embedded `public/`.
fn error_pages() -> ErrorPages {
    ErrorPages::new([404, 422, 500, 502].into_iter().filter_map(|status| {
        let path = format!("/{status}.html");
        let request = campfire_assets::StaticRequest { method: "GET", path: &path, ..Default::default() };
        campfire_assets::serve(&request).map(|page| (status, page.body.into_owned().into()))
    }))
}

// --- Commands --------------------------------------------------------------------------------------

const USAGE: &str = "usage: campfire [server|backup]";

/// The binary's entry point.
///
/// - `campfire` / `campfire server`: serve the app behind the front server, as `bin/boot` did
///   with Thruster in front of `bin/start-app` (see `campfire_kit::front`).
/// - `campfire backup`: the ONCE `pre-backup` hook (`script/admin/prepare-backup`): snapshot the
///   live database into `storage/backups/` with SQLite's online backup API.
///
/// The ONCE `post-restore` hook stays the reference's shell script (`hooks/post-restore`): copy
/// `storage/backups/<env>.sqlite3` over `storage/db/<env>.sqlite3` and delete its `-wal` and
/// `-shm` files; the next boot's `db:prepare` picks it up.
pub fn run() -> anyhow::Result<()> {
    let command = std::env::args().nth(1);
    if matches!(command.as_deref(), Some("-h" | "--help")) {
        println!("{USAGE}");
        return Ok(());
    }
    let config = Config::from_env()?;
    init_logging(&config);
    match command.as_deref() {
        None | Some("server") => {
            if let Some(limit) = campfire_kit::server::raise_open_file_limit() {
                tracing::info!(limit, "open files");
            }
            tokio::runtime::Runtime::new()?.block_on(serve(config))
        }
        Some("backup") => backup(&config),
        Some(other) => anyhow::bail!("unknown command {other:?}\n{USAGE}"),
    }
}

fn init_logging(config: &Config) {
    let level = match config.log_level.to_ascii_lowercase().as_str() {
        "debug" => "debug",
        "warn" => "warn",
        "error" | "fatal" | "unknown" => "error",
        _ => "info",
    };
    // The front server logs on its own terms, as Thruster did: requests at info, more with DEBUG.
    let front = if campfire_kit::front::FrontConfig::from_env().debug { "debug" } else { "info" };
    let default = format!("{level},thruster={front},campfire_kit::front={front}");
    let filter =
        tracing_subscriber::EnvFilter::try_from_env("CAMPFIRE_LOG").unwrap_or_else(|_| tracing_subscriber::EnvFilter::new(default));
    let _ = tracing_subscriber::fmt().with_env_filter(filter).try_init();
}

/// How long in-flight requests and queued jobs get after SIGTERM/SIGINT.
const SHUTDOWN_GRACE: Duration = Duration::from_secs(10);

/// `bin/boot`'s `thrust bin/start-app`: the front server (kit's Thruster) on HTTP_PORT and, with
/// TLS_DOMAIN, HTTPS_PORT, and the app itself on TARGET_PORT.
async fn serve(config: Config) -> anyhow::Result<()> {
    let front = campfire_kit::front::FrontConfig::from_env();
    let Booted { app, router, jobs } = boot(config).await?;

    let (stopping_tx, stopping) = tokio::sync::watch::channel(false);
    let cable = app.cable.clone();
    let signal = async move {
        campfire_kit::server::shutdown_signal().await;
        tracing::info!("shutting down");
        // Close every WebSocket (`server_restart`, so clients reconnect) or they'd hold the
        // graceful shutdown open.
        cable.restart();
        let _ = stopping_tx.send(true);
    };
    let server = campfire_kit::front::serve(front, router, signal);
    let deadline = async move {
        let mut stopping = stopping;
        let _ = stopping.wait_for(|stopping| *stopping).await;
        tokio::time::sleep(SHUTDOWN_GRACE).await;
    };
    tokio::select! {
        result = server => result?,
        _ = deadline => tracing::warn!("requests still running at shutdown were abandoned"),
    }
    // Jobs first: pushing a message queues its notifications on the Web Push pool.
    jobs.shutdown(SHUTDOWN_GRACE).await;
    if let Some(web_push) = &app.web_push {
        web_push.shutdown().await;
    }
    Ok(())
}

/// `script/admin/prepare-backup`: `SQLite3::Backup` of the live database, all pages in one step,
/// into `storage/backups/<database file name>`.
pub fn backup(config: &Config) -> anyhow::Result<()> {
    let destination = config.storage.backup_file();
    if let Some(dir) = destination.parent() {
        std::fs::create_dir_all(dir)?;
    }
    // Written to a file of its own beside the destination and renamed over it, so a failed,
    // interrupted or concurrent backup never leaves a torn file where ONCE (and `post-restore`)
    // expect the last good one. A failed one's file is deleted when `partial` drops.
    let dir = destination.parent().unwrap_or(std::path::Path::new("."));
    let partial = tempfile::Builder::new().prefix(".backup-").suffix(".sqlite3").tempfile_in(dir)?;
    copy_database(&config.storage.database, partial.path())?;
    partial.persist(&destination)?;
    tracing::info!(path = %destination.display(), "backup written");
    Ok(())
}

/// SQLite's online backup of the live database at `source` into a new file at `target`.
fn copy_database(source: &std::path::Path, target: &std::path::Path) -> anyhow::Result<()> {
    let source = rusqlite::Connection::open_with_flags(source, rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY)?;
    source.busy_timeout(Duration::from_secs(5))?;
    let mut target = rusqlite::Connection::open(target)?;
    let backup = rusqlite::backup::Backup::new(&source, &mut target)?;
    // `backup.step(-1)`: every page in one step; a busy or locked source is retried.
    let mut attempts = 0;
    loop {
        match backup.step(-1)? {
            rusqlite::backup::StepResult::Done => return Ok(()),
            _ if attempts < 50 => {
                attempts += 1;
                std::thread::sleep(Duration::from_millis(100));
            }
            other => anyhow::bail!("backup did not finish: {other:?}"),
        }
    }
}

#[cfg(test)]
mod tests;
