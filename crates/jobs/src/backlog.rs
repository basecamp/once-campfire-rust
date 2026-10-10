//! The background work that requests leave behind (jobs, Web Push deliveries), and admission for
//! the requests that add to it.
//!
//! Queues of background work never drop work. Their bound is at the edge instead: while more
//! than the high-water mark of work waits, a request that writes waits before it runs, until the
//! backlog falls to half of the mark. Requests that read are never held. Admission follows the
//! wavey request limiter: arm the notification, check, then wait, so a drain between the check and
//! the wait is not missed.
use std::sync::atomic::{AtomicUsize, Ordering};

use tokio::sync::Notify;

pub struct Backlog {
    depth: AtomicUsize,
    high: usize,
    low: usize,
    drained: Notify,
}

impl Backlog {
    /// A backlog that holds writes back while more than `high` items wait.
    pub fn new(high: usize) -> Self {
        let high = high.max(1);
        Self { depth: AtomicUsize::new(0), high, low: high / 2, drained: Notify::new() }
    }

    /// Items waiting or running.
    pub fn depth(&self) -> usize {
        self.depth.load(Ordering::Acquire)
    }

    pub fn added(&self) {
        self.depth.fetch_add(1, Ordering::AcqRel);
    }

    pub fn finished(&self) {
        let before = self.depth.fetch_sub(1, Ordering::AcqRel);
        if before == self.low + 1 {
            self.drained.notify_waiters();
        }
    }

    /// Waits while the backlog is above its high-water mark.
    pub async fn admit(&self) {
        loop {
            let drained = self.drained.notified();
            tokio::pin!(drained);
            drained.as_mut().enable();
            if self.depth() <= self.high {
                return;
            }
            drained.await;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::time::Duration;

    #[tokio::test]
    async fn holds_writes_above_the_mark_until_half_has_drained() {
        let backlog = Arc::new(Backlog::new(4));
        for _ in 0..5 {
            backlog.added();
        }
        let waiting = tokio::spawn({
            let backlog = backlog.clone();
            async move { backlog.admit().await }
        });
        tokio::time::sleep(Duration::from_millis(20)).await;
        assert!(!waiting.is_finished());
        backlog.finished();
        backlog.finished();
        tokio::time::sleep(Duration::from_millis(20)).await;
        assert!(!waiting.is_finished(), "three wait: above half the mark");
        backlog.finished();
        tokio::time::timeout(Duration::from_secs(1), waiting).await.unwrap().unwrap();
        backlog.admit().await;
    }
}
