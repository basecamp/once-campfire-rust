use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, Ordering};
use std::task::{Context, Poll, Waker};

/// One task's wake-up: rung by any number of publishers, answered by one waiting task. Only the
/// ring that changes it from quiet to rung wakes the task, so a task that already has work
/// pending costs a publisher one atomic swap.
#[derive(Default)]
pub struct Bell {
    rung: AtomicBool,
    waker: Mutex<Option<Waker>>,
}

impl Bell {
    pub fn ring(&self) {
        if !self.rung.swap(true, Ordering::AcqRel)
            && let Some(waker) = self.waker.lock().unwrap().take()
        {
            waker.wake();
        }
    }

    /// Marks the bell rung without a wake: the owner's next [`Bell::wait`] returns at once.
    pub fn set(&self) {
        self.rung.store(true, Ordering::Release);
    }

    /// Completes when the bell has been rung since the last wait completed.
    pub fn wait(&self) -> impl Future<Output = ()> + '_ {
        std::future::poll_fn(move |cx| self.poll_wait(cx))
    }

    fn poll_wait(&self, cx: &mut Context<'_>) -> Poll<()> {
        if self.rung.swap(false, Ordering::AcqRel) {
            return Poll::Ready(());
        }
        {
            let mut waker = self.waker.lock().unwrap();
            match &mut *waker {
                Some(current) if current.will_wake(cx.waker()) => {}
                slot => *slot = Some(cx.waker().clone()),
            }
        }
        // A ring between the first check and the registration found no waker to wake.
        if self.rung.swap(false, Ordering::AcqRel) { Poll::Ready(()) } else { Poll::Pending }
    }
}

/// Registered entries with stable tokens, for removal without a search.
pub(crate) struct Slab<T> {
    slots: Vec<Option<T>>,
    free: Vec<usize>,
}

impl<T> Default for Slab<T> {
    fn default() -> Self {
        Self { slots: Vec::new(), free: Vec::new() }
    }
}

impl<T> Slab<T> {
    pub(crate) fn insert(&mut self, entry: T) -> usize {
        match self.free.pop() {
            Some(token) => {
                self.slots[token] = Some(entry);
                token
            }
            None => {
                self.slots.push(Some(entry));
                self.slots.len() - 1
            }
        }
    }

    pub(crate) fn remove(&mut self, token: usize) {
        self.slots[token] = None;
        self.free.push(token);
        if self.free.len() == self.slots.len() {
            self.slots = Vec::new();
            self.free = Vec::new();
        }
    }

    pub(crate) fn iter(&self) -> impl Iterator<Item = &T> {
        self.slots.iter().flatten()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    #[tokio::test]
    async fn bells_wake_once_until_waited() {
        let bell = Arc::new(Bell::default());
        let waiting = tokio::spawn({
            let bell = bell.clone();
            async move { bell.wait().await }
        });
        tokio::task::yield_now().await;
        bell.ring();
        bell.ring();
        waiting.await.unwrap();
        bell.set();
        bell.wait().await;
    }
}
