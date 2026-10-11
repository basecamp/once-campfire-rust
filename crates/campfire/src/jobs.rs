//! The in-process job runner that replaces Resque (see plans/rust-conversion.md, "Jobs").
//!
//! Models emit [`Event`]s at the point Rails would `perform_later` (the database's
//! [`EventSink`]); [`Jobs`] puts each on its kind's lane without blocking the writer thread, and
//! each kind has its own workers, so a kind that's slow (webhooks to a slow bot) can't hold up the
//! others (pushes, purges). A lane never drops a job, as Resque doesn't. Its bound is the
//! [`Backlog`]: while too many jobs wait, requests that write wait before they run. Nothing
//! retries (`retry_on` is commented out in `reference/app/jobs/application_job.rb`); a failure or
//! panic is logged. Queued work is lost if the process crashes, which the plan accepts. On
//! shutdown the runner stops taking new work, performs what's queued and waits for it up to a
//! deadline.
//!
//! Handlers are looked up in a [`Registry`]. Core registers `RemoveBannedContent` and
//! `PurgeBlob`; integrations register `PushMessage` and `DeliverWebhook` through
//! `crate::integrations::register_jobs`. `DisconnectUser` is not a job in Rails (it's a
//! synchronous Action Cable broadcast), so it goes straight to the cable server.

use std::collections::HashMap;
use std::future::Future;
use std::panic::AssertUnwindSafe;
use std::sync::{Arc, OnceLock};
use std::time::Duration;

use anyhow::anyhow;
use campfire_db::{Event, EventSink};
use campfire_jobs::{Backlog, Lane};
use futures_util::FutureExt as _;
use futures_util::future::BoxFuture;
use tokio::task::JoinHandle;

use crate::app::{App, Cable};

/// How many jobs and Web Push deliveries may wait before requests that write wait for them.
pub const QUEUE_CAPACITY: usize = 4096;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum JobKind {
    PushMessage,
    DeliverWebhook,
    RemoveBannedContent,
    PurgeBlob,
    /// Work enqueued with [`Jobs::perform_later`].
    AdHoc,
}

impl JobKind {
    const ALL: [JobKind; 5] =
        [JobKind::PushMessage, JobKind::DeliverWebhook, JobKind::RemoveBannedContent, JobKind::PurgeBlob, JobKind::AdHoc];

    /// `None` for an event that isn't a job.
    pub fn of(event: &Event) -> Option<Self> {
        match event {
            Event::PushMessage { .. } => Some(JobKind::PushMessage),
            Event::DeliverWebhook { .. } => Some(JobKind::DeliverWebhook),
            Event::RemoveBannedContent { .. } => Some(JobKind::RemoveBannedContent),
            Event::PurgeBlob { .. } => Some(JobKind::PurgeBlob),
            Event::DisconnectUser { .. } => None,
        }
    }

    /// The Rails job class, for logs.
    pub fn name(self) -> &'static str {
        match self {
            JobKind::PushMessage => "Room::PushMessageJob",
            JobKind::DeliverWebhook => "Bot::WebhookJob",
            JobKind::RemoveBannedContent => "RemoveBannedContentJob",
            JobKind::PurgeBlob => "ActiveStorage::PurgeJob",
            JobKind::AdHoc => "AdHoc",
        }
    }
}

/// Performs one kind of job.
type Handler = Box<dyn Fn(App, Event) -> BoxFuture<'static, anyhow::Result<()>> + Send + Sync>;

/// Which handler performs which kind of job.
#[derive(Default)]
pub struct Registry {
    handlers: HashMap<JobKind, Handler>,
}

impl Registry {
    /// The jobs the app core performs itself.
    pub fn with_core_jobs() -> Self {
        let mut registry = Self::default();
        registry.handle(JobKind::RemoveBannedContent, remove_banned_content);
        registry.handle(JobKind::PurgeBlob, purge_blob);
        registry
    }

    pub fn handle<F, Fut>(&mut self, kind: JobKind, handler: F)
    where
        F: Fn(App, Event) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = anyhow::Result<()>> + Send + 'static,
    {
        self.handlers.insert(kind, Box::new(move |app, event| Box::pin(handler(app, event))));
    }

    fn get(&self, kind: JobKind) -> Option<&Handler> {
        self.handlers.get(&kind)
    }
}

enum Work {
    Event(JobKind, Event),
    AdHoc(&'static str, BoxFuture<'static, anyhow::Result<()>>),
}

impl Work {
    fn kind(&self) -> JobKind {
        match self {
            Work::Event(kind, _) => *kind,
            Work::AdHoc(..) => JobKind::AdHoc,
        }
    }

    fn name(&self) -> &'static str {
        match self {
            Work::Event(kind, _) => kind.name(),
            Work::AdHoc(name, _) => name,
        }
    }
}

/// The enqueueing side: the database's event sink, and `perform_later` for everything else.
/// Cheap to clone.
#[derive(Clone)]
pub struct Jobs {
    lanes: Arc<HashMap<JobKind, Arc<Lane<Work>>>>,
    backlog: Arc<Backlog>,
    cable: Arc<OnceLock<Cable>>,
}

impl Jobs {
    /// A lane for each kind, sharing a backlog whose high-water mark is `capacity`, and the
    /// lanes' receiving side, which [`start`] turns into the runner.
    pub fn new(capacity: usize) -> (Self, Queue) {
        let lanes: HashMap<JobKind, Arc<Lane<Work>>> = JobKind::ALL.into_iter().map(|kind| (kind, Arc::default())).collect();
        let lanes = Arc::new(lanes);
        let backlog = Arc::new(Backlog::new(capacity));
        (Self { lanes: lanes.clone(), backlog: backlog.clone(), cable: Arc::new(OnceLock::new()) }, Queue { lanes, backlog })
    }

    /// Enqueues work (`SomeJob.perform_later`).
    pub fn perform_later(&self, name: &'static str, work: impl Future<Output = anyhow::Result<()>> + Send + 'static) {
        self.enqueue(Work::AdHoc(name, Box::pin(work)));
    }

    /// The jobs and deliveries waiting, which requests that write wait for.
    pub fn backlog(&self) -> &Arc<Backlog> {
        &self.backlog
    }

    fn enqueue(&self, work: Work) {
        let name = work.name();
        self.backlog.added();
        match self.lanes[&work.kind()].push(work) {
            Ok(()) => tracing::debug!(job = name, "enqueued"),
            Err(_) => {
                self.backlog.finished();
                tracing::warn!(job = name, "job runner stopped, dropping job");
            }
        }
    }

    fn set_cable(&self, cable: Cable) {
        let _ = self.cable.set(cable);
    }
}

impl EventSink for Jobs {
    fn emit(&self, event: Event) {
        match (JobKind::of(&event), event) {
            (Some(kind), event) => self.enqueue(Work::Event(kind, event)),
            // `ActionCable.server.remote_connections.where(current_user: user).disconnect`: a
            // pub/sub broadcast in Rails, done right away. Before boot finishes there are no
            // connections to disconnect.
            (None, Event::DisconnectUser { user_id, reconnect }) => {
                if let Some(cable) = self.cable.get() {
                    crate::channels::revocation::disconnect_user(cable, user_id, reconnect);
                }
            }
            (None, event) => tracing::warn!(?event, "not a job, dropping event"),
        }
    }
}

/// The lanes' receiving side, until the runner starts.
pub struct Queue {
    lanes: Arc<HashMap<JobKind, Arc<Lane<Work>>>>,
    backlog: Arc<Backlog>,
}

/// The running job runner.
pub struct Runner {
    lanes: Arc<HashMap<JobKind, Arc<Lane<Work>>>>,
    workers: Vec<JoinHandle<()>>,
}

impl Runner {
    /// Stops taking new work, performs what's already queued, and waits for running jobs until
    /// `deadline`, after which they're abandoned (logged).
    pub async fn shutdown(self, deadline: Duration) {
        for lane in self.lanes.values() {
            lane.close();
        }
        if tokio::time::timeout(deadline, futures_util::future::join_all(self.workers)).await.is_err() {
            tracing::warn!("jobs still running at shutdown were abandoned");
        }
    }
}

/// Starts performing queued jobs: `concurrency` workers for each kind of job.
pub fn start(queue: Queue, app: App, registry: Registry, concurrency: usize) -> Runner {
    app.jobs.set_cable(app.cable.clone());
    let registry = Arc::new(registry);
    let mut workers = Vec::new();
    for lane in queue.lanes.values() {
        for _ in 0..concurrency.max(1) {
            workers.push(tokio::spawn(work(lane.clone(), queue.backlog.clone(), app.clone(), registry.clone())));
        }
    }
    Runner { lanes: queue.lanes, workers }
}

/// One of a kind's workers: performs that kind's jobs, one at a time, until its lane has closed
/// and drained.
async fn work(lane: Arc<Lane<Work>>, backlog: Arc<Backlog>, app: App, registry: Arc<Registry>) {
    while let Some(work) = lane.next().await {
        perform(app.clone(), &registry, work).await;
        backlog.finished();
    }
}

async fn perform(app: App, registry: &Registry, work: Work) {
    let name = work.name();
    let job = async move {
        match work {
            Work::AdHoc(_, future) => future.await,
            Work::Event(kind, event) => match registry.get(kind) {
                Some(handler) => handler(app, event).await,
                None => Err(anyhow!("no handler registered for {event:?}")),
            },
        }
    };
    match AssertUnwindSafe(job).catch_unwind().await {
        Ok(Ok(())) => tracing::info!(job = name, "performed"),
        Ok(Err(error)) => tracing::error!(job = name, error = %format_args!("{error:#}"), "job failed"),
        Err(panic) => tracing::error!(job = name, panic = panic_message(&*panic), "job panicked"),
    }
}

fn panic_message(panic: &(dyn std::any::Any + Send)) -> &str {
    panic.downcast_ref::<&str>().copied().or_else(|| panic.downcast_ref::<String>().map(String::as_str)).unwrap_or("Box<dyn Any>")
}

/// `RemoveBannedContentJob`: `user.remove_banned_content`, which destroys each of the user's
/// messages (each in its own transaction) and broadcasts its removal
/// (`reference/app/models/user/bannable.rb`, `Message::Broadcasts#broadcast_remove`).
async fn remove_banned_content(app: App, event: Event) -> anyhow::Result<()> {
    let Event::RemoveBannedContent { user_id } = event else { return Ok(()) };
    // Offloaded: it reads every message the user wrote.
    let messages = app.db.read_offloaded(move |conn| campfire_db::Message::by_creator(conn, user_id)).await?;
    for message in messages {
        let (removed, room_id) = (message.clone(), message.room_id);
        app.db.write(move |tx| message.destroy(tx)).await?;
        let room = app.db.read(move |conn| campfire_db::Room::find(conn, room_id)).await?;
        app.broadcasts.message_remove(&room, &removed);
    }
    Ok(())
}

/// `ActiveStorage::PurgeJob`
async fn purge_blob(app: App, event: Event) -> anyhow::Result<()> {
    let Event::PurgeBlob { blob_id } = event else { return Ok(()) };
    crate::active_storage::purge(&app, blob_id).await
}

#[cfg(test)]
mod tests;
