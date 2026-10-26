//! SqLive: live queries for SQLite.
//!
//! Every [`LiveDb::write`] publishes a [`CommitEvent`] with the net row
//! changes of its transaction. Queries return their rows with a read set: the
//! tables, columns, and key ranges the result depends on. An app re-runs a
//! query when its read set intersects an event.

mod capture;
mod compare;
mod constraints;
mod db;
mod deps;
mod error;
mod event;
mod hash;
mod index;
mod log;
mod params;
mod read;
mod read_set;
mod schema;

pub use compare::{Affinity, Collation};
pub use constraints::{Condition, Conjunction, Constraint, Filter};
pub use db::{LiveDb, Options};
pub use error::{Error, Result};
pub use event::{CommitEvent, Op, Row, RowChange, Version};
pub use hash::ResultHash;
pub use index::ReadSetIndex;
pub use log::Changes;
pub use params::{ParamValues, QueryParams};
pub use read::{QueryResult, Snapshot};
pub use read_set::{ReadSet, TableRead};
pub use rusqlite;
pub use rusqlite::types::Value;

use std::sync::{Mutex, MutexGuard};

/// Locks a mutex, carrying on past a panic in another holder: every
/// structure behind these locks stays consistent between statements.
pub(crate) fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}
