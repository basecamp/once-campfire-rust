use std::collections::VecDeque;
use std::sync::Mutex;

use tokio::sync::Notify;

/// One kind of work, in order. Any number of workers take from it.
pub struct Lane<W> {
    work: Mutex<Queued<W>>,
    ready: Notify,
}

struct Queued<W> {
    items: VecDeque<W>,
    closed: bool,
}

impl<W> Default for Lane<W> {
    fn default() -> Self {
        Self { work: Mutex::new(Queued { items: VecDeque::new(), closed: false }), ready: Notify::new() }
    }
}

impl<W> Lane<W> {
    /// Queues `work`, or gives it back once the lane has closed.
    pub fn push(&self, work: W) -> Result<(), W> {
        let mut queued = self.work.lock().unwrap();
        if queued.closed {
            return Err(work);
        }
        queued.items.push_back(work);
        drop(queued);
        self.ready.notify_one();
        Ok(())
    }

    /// The next work; `None` once the lane has closed and drained.
    pub async fn next(&self) -> Option<W> {
        loop {
            let ready = self.ready.notified();
            tokio::pin!(ready);
            ready.as_mut().enable();
            {
                let mut queued = self.work.lock().unwrap();
                if let Some(work) = queued.items.pop_front() {
                    // More work may wait for another worker.
                    if !queued.items.is_empty() {
                        self.ready.notify_one();
                    }
                    return Some(work);
                }
                if queued.closed {
                    return None;
                }
            }
            ready.await;
        }
    }

    /// Takes no more work. Workers drain what is queued, then stop.
    pub fn close(&self) {
        self.work.lock().unwrap().closed = true;
        self.ready.notify_waiters();
    }

    /// Work waiting.
    pub fn len(&self) -> usize {
        self.work.lock().unwrap().items.len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    #[tokio::test]
    async fn workers_take_everything_in_order_then_stop_after_close() {
        let lane = Arc::new(Lane::default());
        for i in 0..100 {
            lane.push(i).unwrap();
        }
        let workers: Vec<_> = (0..4)
            .map(|_| {
                let lane = lane.clone();
                tokio::spawn(async move {
                    let mut got = Vec::new();
                    while let Some(i) = lane.next().await {
                        got.push(i);
                    }
                    got
                })
            })
            .collect();
        tokio::task::yield_now().await;
        lane.close();
        assert_eq!(lane.push(100), Err(100));
        let mut all = Vec::new();
        for worker in workers {
            let got = worker.await.unwrap();
            assert!(got.windows(2).all(|pair| pair[0] < pair[1]), "each worker takes in order");
            all.extend(got);
        }
        all.sort();
        assert_eq!(all, (0..100).collect::<Vec<_>>());
    }
}
