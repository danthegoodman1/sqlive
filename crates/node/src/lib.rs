//! Node.js bindings for SqLive. `index.js` wraps these classes in the
//! package's public API.

mod db;
mod errors;
mod events;
mod query;
mod read_set;
pub mod rows;
pub mod values;

pub use db::{NativeDb, OpenOptions};
pub use events::{ChangeStream, CommitEvent};
pub use query::NativeSnapshot;
pub use read_set::{
    NativeIndex, TableRead, read_set_display, read_set_every_commit, read_set_intersects,
    read_set_reads_rows_of, read_set_tables, read_set_version,
};

use std::sync::{Mutex, MutexGuard};

/// Locks a mutex, carrying on past a panic in another holder.
pub(crate) fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}
