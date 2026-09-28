//! The in-process job runner that replaces Resque (see plans/rust-conversion.md, "Jobs").
//!
//! Models emit [`Event`]s at the point Rails would `perform_later` (the database's
//! [`EventSink`]); [`Jobs`] puts each on its kind's **bounded** queue without blocking the writer
//! thread, and each kind has its own workers, so a kind that's slow (webhooks to a slow bot) or
//! full can't hold up the others (pushes, purges). Nothing retries (`retry_on` is commented out
//! in `reference/app/jobs/application_job.rb`); a failure or panic is logged. Queued work is lost
//! if the process crashes, which the plan accepts. On shutdown the runner stops taking new work,
//! performs what's queued and waits for it up to a deadline.
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
use futures_util::FutureExt as _;
use futures_util::future::BoxFuture;
use tokio::sync::{Mutex, mpsc, watch};
use tokio::task::JoinHandle;

use crate::app::{App, Cable};

/// How many jobs of each kind may wait before new ones of that kind are dropped (and logged).
pub const QUEUE_CAPACITY: usize = 1024;

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
type Handler = Arc<dyn Fn(App, Event) -> BoxFuture<'static, anyhow::Result<()>> + Send + Sync>;

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
        self.handlers.insert(kind, Arc::new(move |app, event| handler(app, event).boxed()));
    }

    fn get(&self, kind: JobKind) -> Option<Handler> {
        self.handlers.get(&kind).cloned()
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
    queues: Arc<HashMap<JobKind, mpsc::Sender<Work>>>,
    cable: Arc<OnceLock<Cable>>,
}

impl Jobs {
    /// A queue of `capacity` for each kind, and their receiving ends, which [`start`] turns into
    /// the runner.
    pub fn new(capacity: usize) -> (Self, Queue) {
        let (queues, receivers) = JobKind::ALL
            .into_iter()
            .map(|kind| {
                let (sender, receiver) = mpsc::channel(capacity.max(1));
                ((kind, sender), (kind, receiver))
            })
            .unzip();
        (Self { queues: Arc::new(queues), cable: Arc::new(OnceLock::new()) }, Queue { receivers })
    }

    /// Enqueues best-effort work (`SomeJob.perform_later`). Dropped with an error log when its
    /// queue is full.
    pub fn perform_later(&self, name: &'static str, work: impl Future<Output = anyhow::Result<()>> + Send + 'static) {
        self.enqueue(Work::AdHoc(name, Box::pin(work)));
    }

    fn enqueue(&self, work: Work) {
        let name = work.name();
        match self.queues[&work.kind()].try_send(work) {
            Ok(()) => tracing::debug!(job = name, "enqueued"),
            Err(mpsc::error::TrySendError::Full(_)) => tracing::error!(job = name, "job queue is full, dropping job"),
            Err(mpsc::error::TrySendError::Closed(_)) => tracing::warn!(job = name, "job runner stopped, dropping job"),
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

/// The receiving ends of the queues, until the runner starts.
pub struct Queue {
    receivers: Vec<(JobKind, mpsc::Receiver<Work>)>,
}

/// The running job runner.
pub struct Runner {
    stopping: watch::Sender<bool>,
    workers: Vec<JoinHandle<()>>,
}

impl Runner {
    /// Stops taking new work, performs what's already queued, and waits for running jobs until
    /// `deadline`, after which they're abandoned (logged).
    pub async fn shutdown(self, deadline: Duration) {
        let _ = self.stopping.send(true);
        if tokio::time::timeout(deadline, futures_util::future::join_all(self.workers)).await.is_err() {
            tracing::warn!("jobs still running at shutdown were abandoned");
        }
    }
}

/// Starts performing queued jobs: `concurrency` workers for each kind of job.
pub fn start(queue: Queue, app: App, registry: Registry, concurrency: usize) -> Runner {
    app.jobs.set_cable(app.cable.clone());
    let (stopping, _) = watch::channel(false);
    let registry = Arc::new(registry);
    let mut workers = Vec::new();
    for (_, receiver) in queue.receivers {
        let receiver = Arc::new(Mutex::new(receiver));
        for _ in 0..concurrency.max(1) {
            workers.push(tokio::spawn(work(receiver.clone(), app.clone(), registry.clone(), stopping.subscribe())));
        }
    }
    Runner { stopping, workers }
}

/// One of a kind's workers: performs that kind's jobs, one at a time, until its queue has closed
/// and drained.
async fn work(queue: Arc<Mutex<mpsc::Receiver<Work>>>, app: App, registry: Arc<Registry>, mut stopping: watch::Receiver<bool>) {
    while let Some(work) = next(&queue, &mut stopping).await {
        perform(app.clone(), &registry, work).await;
    }
}

/// The next job from the queue; once the runner is stopping, the queue closes and what's left in
/// it drains.
async fn next(queue: &Mutex<mpsc::Receiver<Work>>, stopping: &mut watch::Receiver<bool>) -> Option<Work> {
    let mut queue = queue.lock().await;
    tokio::select! {
        work = queue.recv() => work,
        () = stopped(stopping) => {
            queue.close();
            queue.recv().await
        }
    }
}

/// Resolves once shutdown starts. A runner dropped without a shutdown leaves its workers running.
async fn stopped(stopping: &mut watch::Receiver<bool>) {
    if stopping.wait_for(|stopping| *stopping).await.is_err() {
        std::future::pending::<()>().await;
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
        Ok(Err(error)) => tracing::error!(job = name, %error, "job failed"),
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
    let messages = app.db.read(move |conn| campfire_db::Message::by_creator(conn, user_id)).await?;
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
