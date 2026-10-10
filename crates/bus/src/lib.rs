//! Broadcast lanes for Campfire, in the wavey lane model.
//!
//! A [`Hub`] keeps one lane for each broadcasting and each way its subscribers wrap a payload. A
//! lane is an append-only log of shared [`Frame`]s in fixed segments, numbered by sequence. A
//! subscriber is a [`Cursor`] that reads every frame between its position and the lane's head, by
//! reference, with no lock and no reference count. Each subscriber publishes its position, and a
//! publish that would leave a subscriber more than the lane's capacity behind marks it lagged.
//!
//! Subscribers live on [`wake`] shards: one thread with a current-thread runtime for each core. A
//! publish rings each shard that has subscribers once, and the shard rings its subscribers' bells
//! on its own thread.
mod bell;
mod frame;
mod hub;
mod lane;
pub mod wake;

pub use bell::Bell;
pub use frame::Frame;
pub use hub::{Hub, Limits, Registration};
pub use lane::{Cursor, Peeked, RecvError};
