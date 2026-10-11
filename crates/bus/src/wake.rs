//! Wake shards: one thread with a current-thread runtime for each core, for the tasks that read
//! lanes. A publisher hands a shard a [`Ring`] and rings the shard's own bell once; the shard's
//! dispatcher then rings the subscribers' bells on its thread, so each of their wakes goes to that
//! runtime's local queue. A broadcast to a large room costs one cross-thread wake for each shard.
//! Tasks on the shards also run apart from the runtime that serves HTTP requests, so the requests
//! that arrive during a large broadcast don't queue behind its wakes.
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, OnceLock};

use crate::Bell;

/// Something that rings its subscribers on one shard: a lane with new frames, or every
/// connection of a hub.
pub trait Ring: Send + Sync + 'static {
    fn ring(&self, shard: usize);
}

struct Shard {
    handle: tokio::runtime::Handle,
    inbox: Mutex<Vec<Arc<dyn Ring>>>,
    bell: Bell,
}

fn shards() -> &'static [Shard] {
    static SHARDS: OnceLock<Box<[Shard]>> = OnceLock::new();
    SHARDS.get_or_init(|| {
        let count = std::thread::available_parallelism().map_or(4, |n| n.get());
        (0..count)
            .map(|index| {
                let runtime = tokio::runtime::Builder::new_current_thread().enable_all().build().expect("a wake shard starts");
                let handle = runtime.handle().clone();
                std::thread::Builder::new()
                    .name(format!("wake-{index}"))
                    .spawn(move || runtime.block_on(dispatch(index)))
                    .expect("a wake shard thread starts");
                Shard { handle, inbox: Mutex::new(Vec::new()), bell: Bell::default() }
            })
            .collect()
    })
}

/// The number of shards.
pub fn count() -> usize {
    shards().len()
}

/// The shard for the next task, in turn.
pub fn next() -> usize {
    static NEXT: AtomicUsize = AtomicUsize::new(0);
    NEXT.fetch_add(1, Ordering::Relaxed) % count()
}

/// The runtime of shard `index`.
pub fn handle(index: usize) -> &'static tokio::runtime::Handle {
    &shards()[index].handle
}

/// Asks shard `index` to ring `ring`'s subscribers there. The caller makes sure a ring is queued
/// once until the shard has run it.
pub(crate) fn queue(index: usize, ring: Arc<dyn Ring>) {
    let shard = &shards()[index];
    shard.inbox.lock().unwrap().push(ring);
    shard.bell.ring();
}

/// One value for each shard.
pub(crate) fn per_shard<T>(make: impl Fn() -> T) -> Box<[T]> {
    (0..count()).map(|_| make()).collect()
}

async fn dispatch(index: usize) -> ! {
    let shard = &shards()[index];
    let mut rings = Vec::new();
    loop {
        shard.bell.wait().await;
        std::mem::swap(&mut rings, &mut *shard.inbox.lock().unwrap());
        for ring in rings.drain(..) {
            ring.ring(index);
        }
    }
}
