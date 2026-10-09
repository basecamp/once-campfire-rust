//! Campfire's data layer: rusqlite over the Rails schema, with models that write rows
//! exactly as Active Record does and perform the same callback side effects in the same
//! transactions. See `plans/rust-conversion.md`, "Persisted data".
//!
//! Reads take a `&Connection` (a reader, or the writer inside a write). Writes take a
//! [`Tx`], which carries the clock, the [`EventSink`] for side effects that leave the
//! database, and the after-commit queue.

pub mod database;
pub mod error;
pub mod events;
#[cfg(feature = "test-support")]
pub mod fixtures;
pub mod models;
pub mod rich_text;
pub mod schema;
#[cfg(feature = "test-support")]
pub mod testing;
pub mod time;

mod sql;

pub use sql::{CachedStatements, placeholders, query_all, query_one};

pub use database::{Config, Database, Env, Tx, in_read_transaction, run_write};
pub use error::{Error, Errors, Result};
pub use events::{Event, EventSink};
pub use models::*;
pub use rich_text::RichText;
pub use rusqlite::Connection;
pub use time::Timestamp;

#[cfg(test)]
mod tests;
