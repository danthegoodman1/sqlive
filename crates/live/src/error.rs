//! Errors from SqLive.

use crate::event::Version;
use std::fmt;

#[derive(Debug)]
pub enum Error {
    Sqlite(rusqlite::Error),
    /// A change stream fell behind the retained log, or the log lost track
    /// of the database. Query again and open a new stream from the result's
    /// version.
    Lagged {
        /// The last version the stream had delivered.
        requested: Version,
        /// The log holds events after this version.
        oldest: Version,
    },
    /// [`crate::LiveDb::changes_since`] was asked for a version that has not
    /// committed.
    FutureVersion {
        requested: Version,
        latest: Version,
    },
    /// SQLite rolled the transaction back after an error inside the `write`
    /// closure, such as an I/O error, so nothing committed.
    RolledBack,
    /// The database uses something SqLive does not support.
    Unsupported(String),
}

pub type Result<T> = std::result::Result<T, Error>;

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Error::Sqlite(e) => write!(f, "{e}"),
            Error::Lagged { requested, oldest } => write!(
                f,
                "the change stream lagged: it needs events after version {requested}, \
                 and the log holds events after {oldest}"
            ),
            Error::FutureVersion { requested, latest } => write!(
                f,
                "version {requested} has not committed; the latest is {latest}"
            ),
            Error::RolledBack => f.write_str(
                "SQLite rolled the transaction back after an error inside the write closure",
            ),
            Error::Unsupported(what) => write!(f, "unsupported: {what}"),
        }
    }
}

impl std::error::Error for Error {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Error::Sqlite(e) => Some(e),
            _ => None,
        }
    }
}

impl From<rusqlite::Error> for Error {
    fn from(e: rusqlite::Error) -> Error {
        Error::Sqlite(e)
    }
}
