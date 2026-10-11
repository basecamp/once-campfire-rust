//! `WebPush::Pool` (reference/lib/web_push/pool.rb) with the invalid-subscription handler from
//! reference/config/initializers/web_push.rb: up to 50 deliveries at once, and one worker that
//! destroys expired or unusable subscriptions in order.
//!
//! Rails drops deliveries beyond 10,000 waiting (`Concurrent::RejectedExecutionError`). Here they
//! wait in a lane and are never dropped. Each one waiting counts in the app's job backlog, so a
//! flood of deliveries holds back the requests that write, not the deliveries.

use std::fmt::Display;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, mpsc};
use std::time::Duration;

use campfire_db::{Connection, PushPayload, PushSubscription};
use campfire_jobs::{Backlog, Lane};
use tokio::task::JoinHandle;

use super::{Notification, VapidConfig};
use crate::integrations::net::Network;

/// `Concurrent::ThreadPoolExecutor.new(max_threads: 50, max_queue: 10000)`
const MAX_THREADS: usize = 50;
#[cfg(test)]
const MAX_QUEUE: usize = 10_000;

type Handler = Box<dyn Fn(i64) -> Result<(), String> + Send>;

#[derive(Clone)]
pub struct Pool {
    inner: Arc<Inner>,
}

struct Inner {
    net: Network,
    vapid: VapidConfig,
    deliveries: Lane<Notification>,
    /// Deliveries waiting or running.
    pending: AtomicUsize,
    backlog: Arc<Backlog>,
    workers: Mutex<Vec<JoinHandle<()>>>,
    invalidations: Mutex<Option<mpsc::Sender<i64>>>,
    invalidator: Mutex<Option<std::thread::JoinHandle<()>>>,
}

impl Pool {
    /// Call from inside the Tokio runtime deliveries should run on. `invalid_subscription_handler`
    /// is `Push::Subscription.find_by(id:)&.destroy`; it runs on the pool's own thread, so it may
    /// block (e.g. `Database::write_blocking`).
    #[cfg(test)]
    pub fn new<F, E>(net: Network, vapid: VapidConfig, invalid_subscription_handler: F) -> Self
    where
        F: Fn(i64) -> Result<(), E> + Send + 'static,
        E: Display,
    {
        Self::counting(net, vapid, invalid_subscription_handler, Arc::new(Backlog::new(MAX_QUEUE)))
    }

    /// [`Pool::new`], with each waiting delivery counted in `backlog`.
    pub fn counting<F, E>(net: Network, vapid: VapidConfig, invalid_subscription_handler: F, backlog: Arc<Backlog>) -> Self
    where
        F: Fn(i64) -> Result<(), E> + Send + 'static,
        E: Display,
    {
        let handler: Handler = Box::new(move |id| invalid_subscription_handler(id).map_err(|e| e.to_string()));
        let (sender, receiver) = mpsc::channel::<i64>();
        let invalidator = std::thread::Builder::new()
            .name("web_push-invalidation".into())
            .spawn(move || {
                for id in receiver {
                    tracing::info!("Destroying push subscription: {id}");
                    match std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| handler(id))) {
                        Ok(Ok(())) => {}
                        Ok(Err(message)) => {
                            tracing::error!("Error in WebPush::Pool.invalid_subscription_handler: {message}")
                        }
                        Err(_) => tracing::error!("Error in WebPush::Pool.invalid_subscription_handler: panic"),
                    }
                }
            })
            .expect("spawn the web push invalidation thread");

        let inner = Arc::new(Inner {
            net,
            vapid,
            deliveries: Lane::default(),
            pending: AtomicUsize::new(0),
            backlog,
            workers: Mutex::new(Vec::new()),
            invalidations: Mutex::new(Some(sender)),
            invalidator: Mutex::new(Some(invalidator)),
        });
        let workers = (0..MAX_THREADS).map(|_| tokio::spawn(deliver_queued(inner.clone()))).collect();
        *inner.workers.lock().unwrap() = workers;
        Self { inner }
    }

    /// `queue(payload, subscriptions)`: in id order (`find_each`), each subscription's
    /// notification is built here (counting its badge) and delivered on the pool.
    pub fn queue(&self, conn: &Connection, payload: &PushPayload, mut subscriptions: Vec<PushSubscription>) -> campfire_db::Result<()> {
        subscriptions.sort_by_key(|s| s.id);
        for subscription in &subscriptions {
            self.deliver_later(Notification::build(conn, subscription, payload)?);
        }
        Ok(())
    }

    pub fn deliver_later(&self, notification: Notification) {
        let inner = &self.inner;
        inner.pending.fetch_add(1, Ordering::AcqRel);
        inner.backlog.added();
        if inner.deliveries.push(notification).is_err() {
            inner.delivered();
            tracing::warn!("WebPush::Pool is shut down, dropping a notification");
        }
    }

    /// Waits (up to a second, like `wait_for_termination(1)`) for queued deliveries, then stops
    /// the invalidation worker once it has drained.
    pub async fn shutdown(&self) {
        let inner = &self.inner;
        inner.deliveries.close();
        let workers = std::mem::take(&mut *inner.workers.lock().unwrap());
        let _ = tokio::time::timeout(Duration::from_secs(1), futures_util::future::join_all(workers)).await;
        inner.invalidations.lock().unwrap().take();
        let worker = inner.invalidator.lock().unwrap().take();
        if let Some(worker) = worker {
            let _ = tokio::task::spawn_blocking(move || worker.join()).await;
        }
    }

    pub fn vapid(&self) -> &VapidConfig {
        &self.inner.vapid
    }

    /// Queued or running deliveries.
    #[cfg(test)]
    pub fn pending(&self) -> usize {
        self.inner.pending.load(Ordering::Acquire)
    }
}

/// One of the pool's 50 workers. A delivery that panics is counted as done, and the worker
/// carries on.
async fn deliver_queued(inner: Arc<Inner>) {
    while let Some(notification) = inner.deliveries.next().await {
        let delivery = std::panic::AssertUnwindSafe(inner.deliver(&notification));
        if futures_util::FutureExt::catch_unwind(delivery).await.is_err() {
            tracing::error!("Error in WebPush::Pool.deliver: panic");
        }
        inner.delivered();
    }
}

impl Inner {
    fn delivered(&self) {
        self.pending.fetch_sub(1, Ordering::AcqRel);
        self.backlog.finished();
    }

    async fn deliver(&self, notification: &Notification) {
        match notification.deliver(&self.net, &self.vapid).await {
            Ok(_) => {}
            Err(error) if error.invalidates_subscription() => self.invalidate_subscription_later(notification.subscription.id),
            Err(error) => tracing::error!("Error in WebPush::Pool.deliver: {} {}", error.class_name(), error),
        }
    }

    fn invalidate_subscription_later(&self, id: i64) {
        if let Some(sender) = self.invalidations.lock().unwrap().as_ref() {
            let _ = sender.send(id);
        }
    }
}
