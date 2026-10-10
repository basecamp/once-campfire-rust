use std::sync::atomic::{AtomicBool, AtomicI64, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::Duration;

use tokio::sync::Notify;

use crate::bell::Slab;
use crate::hub::{Hub, Limits};
use crate::wake::{self, Ring};
use crate::{Bell, Frame};

/// Frames in one log segment.
pub(crate) const SEGMENT: usize = 32;

pub(crate) struct Segment {
    /// The sequence number of `slots[0]`.
    base: u64,
    slots: [OnceLock<Frame>; SEGMENT],
    next: OnceLock<Arc<Segment>>,
}

impl Segment {
    fn new(base: u64) -> Arc<Self> {
        Arc::new(Self { base, slots: std::array::from_fn(|_| OnceLock::new()), next: OnceLock::new() })
    }

    fn frame(&self, index: usize) -> &Frame {
        self.slots[index].get().expect("a slot below the head is written")
    }

    fn successor(&self) -> &Arc<Segment> {
        self.next.get().expect("a segment below the head has its successor")
    }
}

impl Drop for Segment {
    /// Frees a chain of segments that nothing else holds one at a time, not recursively.
    fn drop(&mut self) {
        let mut next = self.next.take();
        while let Some(segment) = next {
            next = match Arc::try_unwrap(segment) {
                Ok(mut segment) => segment.next.take(),
                Err(_) => None,
            };
        }
    }
}

/// A subscriber's published position in its lane, and its task's bell.
pub(crate) struct Mark {
    bell: Arc<Bell>,
    /// The sequence number of the next frame the subscriber reads.
    position: AtomicU64,
    lagged: AtomicBool,
}

/// The subscribers of one broadcasting that receive identical frames: the payload wrapped for
/// the same variant (for Action Cable, the encoded channel identifier), or the raw payload when
/// `variant` is `None`.
pub(crate) struct Lane {
    pub(crate) broadcasting: String,
    pub(crate) variant: Option<Arc<str>>,
    /// The sequence number of the next frame.
    head: AtomicU64,
    /// Payload bytes published so far.
    head_bytes: AtomicU64,
    /// The segment the next frame goes in. Publishers hold the lock while they write a frame.
    tail: Mutex<Arc<Segment>>,
    /// A lower bound of the positions of the subscribers that haven't lagged.
    floor: AtomicU64,
    limits: Limits,
    readers: Box<[Mutex<Slab<Arc<Mark>>>]>,
    /// Subscribers on each shard.
    counts: Box<[AtomicUsize]>,
    /// A ring of the lane is queued on that shard.
    queued: Box<[AtomicBool]>,
    pub(crate) subscribers: AtomicUsize,
    /// Publishers waiting for subscribers to catch up ([`Lane::settle`]): the position the
    /// subscribers must reach (0 when none wait), how many are still below it, and the wake.
    target: AtomicU64,
    behind: AtomicI64,
    caught_up: Notify,
}

impl Lane {
    pub(crate) fn new(broadcasting: &str, variant: Option<Arc<str>>, limits: Limits) -> Arc<Self> {
        Arc::new(Self {
            broadcasting: broadcasting.to_string(),
            variant,
            head: AtomicU64::new(0),
            head_bytes: AtomicU64::new(0),
            tail: Mutex::new(Segment::new(0)),
            floor: AtomicU64::new(0),
            limits: Limits { frames: limits.frames.max(1), ..limits },
            readers: wake::per_shard(Mutex::default),
            counts: wake::per_shard(AtomicUsize::default),
            queued: wake::per_shard(AtomicBool::default),
            subscribers: AtomicUsize::new(0),
            target: AtomicU64::new(0),
            behind: AtomicI64::new(0),
            caught_up: Notify::new(),
        })
    }

    pub(crate) fn publish(self: &Arc<Self>, frame: Frame) {
        let len = frame.len() as u64;
        let head = {
            let mut tail = self.tail.lock().unwrap();
            let seq = self.head.load(Ordering::Relaxed);
            let mut index = (seq - tail.base) as usize;
            if index == SEGMENT {
                let next = Segment::new(seq);
                let _ = tail.next.set(next.clone());
                *tail = next;
                index = 0;
            }
            let _ = tail.slots[index].set(frame);
            self.head_bytes.fetch_add(len, Ordering::Relaxed);
            self.head.store(seq + 1, Ordering::Release);
            seq + 1
        };
        if head - self.floor.load(Ordering::Acquire) > self.limits.frames {
            self.mark_lagging(head - self.limits.frames);
        }
        for index in 0..self.counts.len() {
            if self.counts[index].load(Ordering::Acquire) > 0 && !self.queued[index].swap(true, Ordering::AcqRel) {
                wake::queue(index, self.clone());
            }
        }
    }

    /// Marks the subscribers below `limit` lagged and rings them, and raises the floor to the
    /// lowest position of the others. It runs once the floor falls the capacity behind the head,
    /// so its scan of the subscribers is shared by the publishes in between.
    fn mark_lagging(&self, limit: u64) {
        let mut floor = self.head.load(Ordering::Acquire);
        for readers in &self.readers {
            for mark in readers.lock().unwrap().iter() {
                if mark.lagged.load(Ordering::Acquire) {
                    continue;
                }
                let position = mark.position.load(Ordering::Acquire);
                if position < limit {
                    mark.lagged.store(true, Ordering::Release);
                    mark.bell.ring();
                    self.advanced(position, u64::MAX);
                } else {
                    floor = floor.min(position);
                }
            }
        }
        self.floor.fetch_max(floor, Ordering::AcqRel);
    }

    /// Waits while the lane's slowest live subscriber is more than `lag` frames behind the head as
    /// it is now, for at most `limit`. This is the wavey writer's wait for reader capacity, done
    /// after the publish: publishers slow to the pace of their subscribers instead of outrunning
    /// them. A subscriber that doesn't catch up within `limit` doesn't hold the publisher longer;
    /// the lane's capacity still applies to it.
    pub(crate) async fn settle(&self, lag: u64, limit: Duration) {
        let head = self.head.load(Ordering::Acquire);
        if head <= lag {
            return;
        }
        let target = head - lag;
        let deadline = tokio::time::Instant::now() + limit;
        loop {
            let caught_up = self.caught_up.notified();
            tokio::pin!(caught_up);
            caught_up.as_mut().enable();
            // Raise the shared target to this publisher's, then count the subscribers below it.
            // Subscribers that cross the target from now on count themselves down.
            let shared = self.target.fetch_max(target, Ordering::AcqRel).max(target);
            let behind = self.count_below(shared);
            self.behind.store(behind as i64, Ordering::Release);
            if self.count_below(target) == 0 {
                return;
            }
            if tokio::time::timeout_at(deadline, caught_up).await.is_err() {
                return;
            }
        }
    }

    fn count_below(&self, position: u64) -> usize {
        let mut below = 0;
        for readers in &self.readers {
            for mark in readers.lock().unwrap().iter() {
                if !mark.lagged.load(Ordering::Acquire) && mark.position.load(Ordering::Acquire) < position {
                    below += 1;
                }
            }
        }
        below
    }

    /// A subscriber moved from `from` to `to`: if it crossed the waiting publishers' target and
    /// was the last one below it, wake them.
    fn advanced(&self, from: u64, to: u64) {
        let target = self.target.load(Ordering::Acquire);
        if target != 0 && from < target && to >= target && self.behind.fetch_sub(1, Ordering::AcqRel) <= 1 {
            self.caught_up.notify_waiters();
        }
    }

    /// Registers a subscriber from the next frame on.
    pub(crate) fn register(self: &Arc<Self>, hub: Arc<Hub>, bell: &Arc<Bell>, shard: usize) -> Cursor {
        // The position is read under the tail's lock, so it matches the segment, and the mark is
        // registered before a later publish can look for lagging subscribers.
        let tail = self.tail.lock().unwrap();
        let (segment, seq) = (tail.clone(), self.head.load(Ordering::Acquire));
        let mark = Arc::new(Mark { bell: bell.clone(), position: AtomicU64::new(seq), lagged: AtomicBool::new(false) });
        let token = self.readers[shard].lock().unwrap().insert(mark.clone());
        drop(tail);
        self.counts[shard].fetch_add(1, Ordering::Release);
        let bytes = self.head_bytes.load(Ordering::Acquire);
        Cursor { hub, lane: self.clone(), segment, seq, bytes, mark, shard, token }
    }
}

impl Ring for Lane {
    fn ring(&self, shard: usize) {
        // Cleared before the ring: a publish from now on queues the lane again.
        self.queued[shard].store(false, Ordering::Release);
        for mark in self.readers[shard].lock().unwrap().iter() {
            mark.bell.ring();
        }
    }
}

#[derive(Debug, PartialEq, Eq)]
pub enum RecvError {
    /// The subscriber fell too far behind; its task must stop reading.
    Lagged,
}

/// What [`Cursor::peek`] lent: the number of frames and their payload bytes.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct Peeked {
    pub frames: usize,
    bytes: u64,
}

/// One subscription's position in its lane. It holds the subscription until it drops.
///
/// Reading is in two steps. [`Cursor::peek`] lends the ready frames out of the cursor's own chain
/// of segments, so a write can send them without a reference count for each frame. Once they're
/// written, [`Cursor::advance`] moves past them and publishes the new position.
pub struct Cursor {
    hub: Arc<Hub>,
    lane: Arc<Lane>,
    segment: Arc<Segment>,
    seq: u64,
    bytes: u64,
    mark: Arc<Mark>,
    shard: usize,
    token: usize,
}

impl Cursor {
    pub fn broadcasting(&self) -> &str {
        &self.lane.broadcasting
    }

    /// Appends the ready frames, at most `max`, to `out`, without moving the cursor.
    pub fn peek<'a>(&'a self, max: usize, out: &mut Vec<&'a Frame>) -> Result<Peeked, RecvError> {
        if self.lagged() {
            return Err(RecvError::Lagged);
        }
        let head = self.lane.head.load(Ordering::Acquire);
        if head == self.seq {
            return Ok(Peeked::default());
        }
        let limits = self.lane.limits;
        if head - self.seq > limits.frames || self.lane.head_bytes.load(Ordering::Relaxed) - self.bytes > limits.bytes {
            self.mark.lagged.store(true, Ordering::Release);
            return Err(RecvError::Lagged);
        }
        let (mut segment, mut seq, mut peeked) = (&*self.segment, self.seq, Peeked::default());
        while seq < head && peeked.frames < max {
            let mut index = (seq - segment.base) as usize;
            if index == SEGMENT {
                segment = segment.successor();
                index = 0;
            }
            let frame = segment.frame(index);
            peeked.bytes += frame.len() as u64;
            peeked.frames += 1;
            seq += 1;
            out.push(frame);
        }
        Ok(peeked)
    }

    /// Moves past frames that [`Cursor::peek`] lent.
    pub fn advance(&mut self, peeked: Peeked) {
        let from = self.seq;
        self.seq += peeked.frames as u64;
        self.bytes += peeked.bytes;
        self.mark.position.store(self.seq, Ordering::Release);
        self.lane.advanced(from, self.seq);
        while self.seq - self.segment.base > SEGMENT as u64 {
            let next = self.segment.successor().clone();
            self.segment = next;
        }
    }

    /// Whether frames are ready to read.
    pub fn ready(&self) -> bool {
        self.lane.head.load(Ordering::Acquire) != self.seq
    }

    /// Whether the subscriber fell too far behind, as a publisher or the cursor itself found.
    pub fn lagged(&self) -> bool {
        self.mark.lagged.load(Ordering::Acquire)
    }

    #[cfg(test)]
    pub(crate) fn segment(&self) -> &Arc<Segment> {
        &self.segment
    }
}

impl Drop for Cursor {
    fn drop(&mut self) {
        self.lane.counts[self.shard].fetch_sub(1, Ordering::Release);
        self.lane.readers[self.shard].lock().unwrap().remove(self.token);
        // A subscriber that leaves no longer holds publishers back.
        self.lane.advanced(self.seq, u64::MAX);
        if self.lane.subscribers.fetch_sub(1, Ordering::AcqRel) == 1 {
            self.hub.release(&self.lane);
        }
    }
}
