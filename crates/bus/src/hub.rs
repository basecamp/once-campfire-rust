use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

use crate::bell::Slab;
use crate::lane::Lane;
use crate::wake::{self, Ring};
use crate::{Bell, Cursor};

/// How far a subscriber may fall behind its lane before it is lagged.
#[derive(Debug, Clone, Copy)]
pub struct Limits {
    pub frames: u64,
    pub bytes: u64,
}

/// Wraps a payload for the subscribers of one variant.
pub type Wrap = fn(variant: &str, payload: &str) -> String;

pub struct Hub {
    limits: Limits,
    wrap: Wrap,
    lanes: Mutex<HashMap<String, Vec<Arc<Lane>>>>,
    tasks: Arc<Tasks>,
}

/// Every registered task's bell, by shard, for rings of all of them (heartbeats, restarts).
struct Tasks {
    bells: Box<[Mutex<Slab<Arc<Bell>>>]>,
    queued: Box<[AtomicBool]>,
}

impl Ring for Tasks {
    fn ring(&self, shard: usize) {
        self.queued[shard].store(false, Ordering::Release);
        for bell in self.bells[shard].lock().unwrap().iter() {
            bell.ring();
        }
    }
}

impl Hub {
    pub fn new(limits: Limits, wrap: Wrap) -> Arc<Self> {
        Arc::new(Self {
            limits,
            wrap,
            lanes: Mutex::new(HashMap::new()),
            tasks: Arc::new(Tasks { bells: wake::per_shard(Mutex::default), queued: wake::per_shard(AtomicBool::default) }),
        })
    }

    /// Publishes to every subscriber of `broadcasting`. Returns how many subscribers receive it.
    pub fn broadcast(&self, broadcasting: &str, payload: &str) -> usize {
        let lanes = match self.lanes.lock().unwrap().get(broadcasting) {
            Some(lanes) => lanes.clone(),
            None => return 0,
        };
        let mut receivers = 0;
        for lane in &lanes {
            receivers += lane.subscribers.load(Ordering::Relaxed);
            let frame = match &lane.variant {
                Some(variant) => (self.wrap)(variant, payload).into(),
                None => payload.into(),
            };
            lane.publish(frame);
        }
        receivers
    }

    /// Waits while a live subscriber of `broadcasting` is more than `lag` frames behind its lane,
    /// for at most `limit` (see `Lane::settle`).
    pub async fn settle(&self, broadcasting: &str, lag: u64, limit: std::time::Duration) {
        let lanes = match self.lanes.lock().unwrap().get(broadcasting) {
            Some(lanes) => lanes.clone(),
            None => return,
        };
        for lane in lanes {
            lane.settle(lag, limit).await;
        }
    }

    /// Frames a subscriber may fall behind before it lags.
    pub fn limits(&self) -> Limits {
        self.limits
    }

    /// Subscribes `bell`, on `shard`, to `broadcasting` from its next frame on, receiving each
    /// payload wrapped for `variant`, or raw when it's `None`.
    pub fn subscribe(self: &Arc<Self>, broadcasting: &str, variant: Option<Arc<str>>, bell: &Arc<Bell>, shard: usize) -> Cursor {
        let mut lanes = self.lanes.lock().unwrap();
        let group = lanes.entry(broadcasting.to_string()).or_default();
        let lane = match group.iter().find(|lane| lane.variant == variant) {
            Some(lane) => lane.clone(),
            None => {
                let lane = Lane::new(broadcasting, variant, self.limits);
                group.push(lane.clone());
                lane
            }
        };
        lane.subscribers.fetch_add(1, Ordering::Relaxed);
        drop(lanes);
        lane.register(self.clone(), bell, shard)
    }

    /// Registers a task's bell for [`Hub::ring_all`] until the returned guard drops.
    pub fn register(self: &Arc<Self>, bell: &Arc<Bell>, shard: usize) -> Registration {
        let token = self.tasks.bells[shard].lock().unwrap().insert(bell.clone());
        Registration { hub: self.clone(), shard, token }
    }

    /// Rings every registered task.
    pub fn ring_all(&self) {
        for index in 0..self.tasks.queued.len() {
            if !self.tasks.queued[index].swap(true, Ordering::AcqRel) {
                wake::queue(index, self.tasks.clone());
            }
        }
    }

    /// Number of broadcastings with at least one subscriber here.
    pub fn stream_count(&self) -> usize {
        self.lanes.lock().unwrap().len()
    }

    pub(crate) fn release(&self, lane: &Arc<Lane>) {
        let mut lanes = self.lanes.lock().unwrap();
        if lane.subscribers.load(Ordering::Relaxed) > 0 {
            return;
        }
        if let Some(group) = lanes.get_mut(&lane.broadcasting) {
            group.retain(|other| !Arc::ptr_eq(other, lane));
            if group.is_empty() {
                lanes.remove(&lane.broadcasting);
            }
        }
    }
}

pub struct Registration {
    hub: Arc<Hub>,
    shard: usize,
    token: usize,
}

impl Drop for Registration {
    fn drop(&mut self) {
        self.hub.tasks.bells[self.shard].lock().unwrap().remove(self.token);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::RecvError;
    use crate::lane::SEGMENT;

    const LIMITS: Limits = Limits { frames: 1 << 20, bytes: 1 << 30 };

    fn wrap(variant: &str, payload: &str) -> String {
        format!(r#"{{"identifier":{variant},"message":{payload}}}"#)
    }

    fn hub(limits: Limits) -> Arc<Hub> {
        Hub::new(limits, wrap)
    }

    fn texts(cursor: &mut Cursor, max: usize) -> Vec<String> {
        let mut out = Vec::new();
        let peeked = cursor.peek(max, &mut out).unwrap();
        let texts = out.iter().map(|frame| frame.as_str().to_string()).collect();
        cursor.advance(peeked);
        texts
    }

    #[test]
    fn delivers_to_every_subscriber_and_cleans_up() {
        let hub = hub(LIMITS);
        let bell = Arc::new(Bell::default());
        let mut a = hub.subscribe("room", None, &bell, 0);
        let mut b = hub.subscribe("room", None, &bell, 0);
        assert_eq!(hub.broadcast("room", "1"), 2);
        assert_eq!(texts(&mut a, 8), ["1"]);
        assert_eq!(texts(&mut b, 8), ["1"]);
        assert!(texts(&mut a, 8).is_empty());
        drop(a);
        assert_eq!(hub.stream_count(), 1);
        drop(b);
        assert_eq!(hub.stream_count(), 0);
        assert_eq!(hub.broadcast("room", "2"), 0);
    }

    #[test]
    fn wraps_payloads_once_per_variant() {
        let hub = hub(LIMITS);
        let bell = Arc::new(Bell::default());
        let variant: Arc<str> = r#""{\"channel\":\"RoomChannel\"}""#.into();
        let a = hub.subscribe("room", Some(variant.clone()), &bell, 0);
        let b = hub.subscribe("room", Some(variant.clone()), &bell, 0);
        let mut other = hub.subscribe("room", Some(r#""other""#.into()), &bell, 0);
        let mut raw = hub.subscribe("room", None, &bell, 0);
        assert_eq!(hub.broadcast("room", r#"{"id":1}"#), 4);

        let (mut first, mut second) = (Vec::new(), Vec::new());
        a.peek(8, &mut first).unwrap();
        b.peek(8, &mut second).unwrap();
        assert_eq!(*first[0], r#"{"identifier":"{\"channel\":\"RoomChannel\"}","message":{"id":1}}"#);
        assert!(std::ptr::eq(first[0].as_str(), second[0].as_str()), "one frame shared by both subscribers");
        assert_eq!(texts(&mut other, 8), [r#"{"identifier":"other","message":{"id":1}}"#]);
        assert_eq!(texts(&mut raw, 8), [r#"{"id":1}"#]);

        drop((first, second));
        drop(other);
        drop(raw);
        assert_eq!(hub.lanes.lock().unwrap()["room"].len(), 1);
    }

    #[test]
    fn reads_across_segments_in_order_and_in_bounded_batches() {
        let hub = hub(LIMITS);
        let bell = Arc::new(Bell::default());
        let mut cursor = hub.subscribe("room", None, &bell, 0);
        let sent: Vec<String> = (0..SEGMENT * 3 + 5).map(|i| i.to_string()).collect();
        for payload in &sent {
            hub.broadcast("room", payload);
        }
        let mut got = Vec::new();
        loop {
            let batch = texts(&mut cursor, 7);
            if batch.is_empty() {
                break;
            }
            assert!(batch.len() <= 7);
            got.extend(batch);
        }
        assert_eq!(got, sent);
    }

    #[test]
    fn a_new_subscriber_starts_at_the_next_frame() {
        let hub = hub(LIMITS);
        let bell = Arc::new(Bell::default());
        let _early = hub.subscribe("room", None, &bell, 0);
        for i in 0..SEGMENT {
            hub.broadcast("room", &i.to_string());
        }
        let mut late = hub.subscribe("room", None, &bell, 0);
        assert!(!late.ready());
        hub.broadcast("room", "next");
        assert_eq!(texts(&mut late, 8), ["next"]);
    }

    #[test]
    fn subscribers_behind_the_limits_lag_instead_of_skipping() {
        let hub = hub(Limits { frames: 2, bytes: 1 << 30 });
        let bell = Arc::new(Bell::default());
        let slow = hub.subscribe("room", None, &bell, 0);
        for i in 0..5 {
            hub.broadcast("room", &i.to_string());
        }
        assert_eq!(slow.peek(8, &mut Vec::new()).err(), Some(RecvError::Lagged));

        let hub = self::hub(Limits { frames: 1 << 20, bytes: 10 });
        let slow = hub.subscribe("room", None, &bell, 0);
        hub.broadcast("room", "0123456789a");
        assert_eq!(slow.peek(8, &mut Vec::new()).err(), Some(RecvError::Lagged));
    }

    #[test]
    fn publishers_mark_subscribers_behind_the_capacity_lagged_and_spare_the_others() {
        let hub = hub(Limits { frames: 4, bytes: 1 << 30 });
        let bell = Arc::new(Bell::default());
        let slow = hub.subscribe("room", None, &bell, 0);
        let mut fast = hub.subscribe("room", None, &bell, 0);
        for i in 0..4 {
            hub.broadcast("room", &i.to_string());
        }
        assert!(!slow.lagged(), "four behind is within the capacity");
        assert_eq!(texts(&mut fast, 8).len(), 4);
        hub.broadcast("room", "4");
        assert!(slow.lagged(), "the publish found it five behind");
        assert!(!fast.lagged());
        assert_eq!(texts(&mut fast, 8), ["4"]);
    }

    #[tokio::test]
    async fn publishers_wait_for_subscribers_to_catch_up() {
        let hub = hub(Limits { frames: 64, bytes: 1 << 30 });
        let bell = Arc::new(Bell::default());
        let mut slow = hub.subscribe("room", None, &bell, 0);
        for i in 0..40 {
            hub.broadcast("room", &i.to_string());
        }
        let settled = tokio::spawn({
            let hub = hub.clone();
            async move { hub.settle("room", 16, std::time::Duration::from_secs(5)).await }
        });
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        assert!(!settled.is_finished(), "the subscriber is 40 behind");
        assert_eq!(texts(&mut slow, 10).len(), 10);
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        assert!(!settled.is_finished(), "30 behind is still over 16");
        assert_eq!(texts(&mut slow, 20).len(), 20);
        tokio::time::timeout(std::time::Duration::from_secs(1), settled).await.unwrap().unwrap();
        hub.settle("room", 16, std::time::Duration::from_secs(5)).await;
    }

    #[test]
    fn segments_are_freed_once_every_cursor_has_passed_them() {
        let hub = hub(LIMITS);
        let bell = Arc::new(Bell::default());
        let mut cursor = hub.subscribe("room", None, &bell, 0);
        let first = Arc::downgrade(cursor.segment());
        for i in 0..SEGMENT * 2 {
            hub.broadcast("room", &i.to_string());
        }
        assert!(first.upgrade().is_some());
        while !texts(&mut cursor, 64).is_empty() {}
        assert!(first.upgrade().is_none());
    }
}
