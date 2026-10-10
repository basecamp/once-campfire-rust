//! Background work for Campfire, in the wavey lane model.
//!
//! A [`Lane`] holds one kind of work in order and never drops it. Workers wait for it with the
//! wavey notification pattern: arm, check, then wait. The bound is a [`Backlog`] that several lanes
//! share: while too much work waits, requests that write wait at the edge before they run.
mod backlog;
mod lane;

pub use backlog::Backlog;
pub use lane::Lane;
