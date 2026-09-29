use std::sync::Mutex as StdMutex;

use tokio::sync::{Notify, mpsc::UnboundedSender};

use super::*;
use crate::app::{Booted, boot};
use crate::config::Config;

/// An app booted over an empty storage directory, for its runner's handlers to receive.
async fn app() -> (Booted, tempfile::TempDir) {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().to_string_lossy().into_owned();
    let config = Config::from_lookup(|name| match name {
        "SECRET_KEY_BASE_DUMMY" => Some("1".into()),
        "CAMPFIRE_STORAGE_PATH" => Some(root.clone()),
        _ => None,
    })
    .unwrap();
    (boot(config).await.unwrap(), dir)
}

/// A handler that reports each event it performs, after `gate` lets it through (when given).
fn reporting(
    performed: UnboundedSender<Event>,
    gate: Option<Arc<Notify>>,
) -> impl Fn(App, Event) -> BoxFuture<'static, anyhow::Result<()>> + Send + Sync + 'static {
    move |_app: App, event: Event| {
        let (performed, gate) = (performed.clone(), gate.clone());
        Box::pin(async move {
            if let Some(gate) = gate {
                gate.notified().await;
            }
            let _ = performed.send(event);
            Ok(())
        })
    }
}

async fn next_performed(performed: &mut mpsc::UnboundedReceiver<Event>) -> Event {
    tokio::time::timeout(Duration::from_secs(5), performed.recv()).await.expect("a job was performed").unwrap()
}

fn webhook(message_id: i64) -> Event {
    Event::DeliverWebhook { bot_id: 1, message_id }
}

/// Log lines written while `f` runs on this thread (the test runtime's only thread).
struct Logs(Arc<StdMutex<Vec<u8>>>);

impl Logs {
    fn capture() -> (Self, tracing::subscriber::DefaultGuard) {
        let buffer = Arc::new(StdMutex::new(Vec::new()));
        let writer = buffer.clone();
        let subscriber = tracing_subscriber::fmt()
            .with_ansi(false)
            .with_writer(move || LogWriter(writer.clone()))
            .finish();
        (Self(buffer), tracing::subscriber::set_default(subscriber))
    }

    fn text(&self) -> String {
        String::from_utf8_lossy(&self.0.lock().unwrap()).into_owned()
    }
}

struct LogWriter(Arc<StdMutex<Vec<u8>>>);

impl std::io::Write for LogWriter {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        self.0.lock().unwrap().extend_from_slice(bytes);
        Ok(bytes.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

#[tokio::test]
async fn a_busy_or_full_kind_doesnt_hold_up_the_others() {
    let (booted, _dir) = app().await;
    let (logs, _guard) = Logs::capture();
    let (performed, mut performed_rx) = mpsc::unbounded_channel();
    let slow_bot = Arc::new(Notify::new());
    let mut registry = Registry::default();
    registry.handle(JobKind::DeliverWebhook, reporting(performed.clone(), Some(slow_bot.clone())));
    registry.handle(JobKind::PurgeBlob, reporting(performed, None));
    let (jobs, queue) = Jobs::new(2);
    let runner = start(queue, booted.app.clone(), registry, 1);

    // One webhook runs (stuck on the slow bot), two wait, and the rest are dropped.
    jobs.emit(webhook(1));
    while jobs.queues[&JobKind::DeliverWebhook].capacity() < 2 {
        tokio::task::yield_now().await;
    }
    for message_id in 2..=10 {
        jobs.emit(webhook(message_id));
    }
    jobs.emit(Event::PurgeBlob { blob_id: 7 });
    assert_eq!(next_performed(&mut performed_rx).await, Event::PurgeBlob { blob_id: 7 });
    assert!(logs.text().contains("job queue is full, dropping job job=\"Bot::WebhookJob\""), "{}", logs.text());

    for _ in 0..3 {
        slow_bot.notify_one();
        assert!(matches!(next_performed(&mut performed_rx).await, Event::DeliverWebhook { .. }));
    }
    runner.shutdown(Duration::from_secs(5)).await;
    assert!(performed_rx.try_recv().is_err());
}

#[tokio::test]
async fn a_panicking_job_is_logged_and_its_worker_carries_on() {
    let (booted, _dir) = app().await;
    let (logs, _guard) = Logs::capture();
    let (jobs, queue) = Jobs::new(QUEUE_CAPACITY);
    let runner = start(queue, booted.app.clone(), Registry::default(), 1);

    let (done, finished) = tokio::sync::oneshot::channel();
    jobs.perform_later("Exploding", async { panic!("kaboom") });
    jobs.perform_later("After", async move {
        let _ = done.send(());
        Ok(())
    });
    tokio::time::timeout(Duration::from_secs(5), finished).await.unwrap().unwrap();
    runner.shutdown(Duration::from_secs(5)).await;

    let logs = logs.text();
    assert!(logs.contains("job panicked job=\"Exploding\" panic=\"kaboom\""), "{logs}");
}

#[tokio::test]
async fn shutdown_performs_what_is_queued_and_then_takes_no_more() {
    let (booted, _dir) = app().await;
    let (performed, mut performed_rx) = mpsc::unbounded_channel();
    let mut registry = Registry::default();
    registry.handle(JobKind::DeliverWebhook, reporting(performed, None));
    let (jobs, queue) = Jobs::new(QUEUE_CAPACITY);
    let runner = start(queue, booted.app.clone(), registry, 2);

    for message_id in 1..=5 {
        jobs.emit(webhook(message_id));
    }
    runner.shutdown(Duration::from_secs(5)).await;
    jobs.emit(webhook(6));

    let mut ids: Vec<i64> = std::iter::from_fn(|| performed_rx.try_recv().ok())
        .map(|event| match event {
            Event::DeliverWebhook { message_id, .. } => message_id,
            event => panic!("{event:?}"),
        })
        .collect();
    ids.sort();
    assert_eq!(ids, [1, 2, 3, 4, 5]);
}
