//! Queries on read-only connections, each inside a snapshot whose version
//! the read set records.

use crate::db::{LiveDb, open_connection};
use crate::deps::{self, Reads, Slot};
use crate::error::{Error, Result};
use crate::event::Version;
use crate::params::{ParamValues, QueryParams};
use crate::read_set::ReadSet;
use rusqlite::{Connection, Statement};
use std::cell::RefCell;
use std::collections::BTreeMap;

/// A query's rows, the version of the snapshot it read, and what the rows
/// depend on.
#[derive(Clone, Debug)]
pub struct QueryResult<T> {
    pub rows: Vec<T>,
    pub version: Version,
    pub read_set: ReadSet,
}

/// Distinct query texts whose reads a reader remembers.
const CACHED_QUERIES: usize = 256;

pub(crate) struct Reader {
    conn: Connection,
    slot: Slot,
    /// The schema cookie the cached reads were recorded under.
    schema: i64,
    /// The reads of each query text prepared under `schema`.
    cache: RefCell<BTreeMap<String, Reads>>,
}

impl Reader {
    fn open(db: &LiveDb) -> rusqlite::Result<Reader> {
        let mut conn = open_connection(db.path(), db.options(), true)?;
        let slot = deps::install(&mut conn)?;
        Ok(Reader {
            conn,
            slot,
            schema: -1,
            cache: RefCell::new(BTreeMap::new()),
        })
    }

    /// Starts a read transaction and returns its version and schema cookie.
    fn begin(&self) -> Result<(Version, i64)> {
        self.conn.execute_batch("BEGIN")?;
        let version: i64 =
            self.conn
                .query_row("SELECT version FROM _sqlive_meta WHERE id = 0", [], |r| {
                    r.get(0)
                })?;
        let cookie: i64 = self
            .conn
            .query_row("PRAGMA schema_version", [], |r| r.get(0))?;
        self.still_open()?;
        Ok((version as Version, cookie))
    }

    /// Checks that the snapshot's transaction is still open. After an I/O
    /// error SQLite can end it even when the statement reports success, and
    /// later statements would then read the latest version.
    fn still_open(&self) -> Result<()> {
        if self.conn.is_autocommit() {
            return Err(Error::SnapshotEnded);
        }
        Ok(())
    }

    /// The reads of `sql`, recorded while preparing it the first time this
    /// reader sees it under the current schema.
    fn reads(&self, sql: &str) -> Result<(Option<Statement<'_>>, Reads)> {
        if let Some(reads) = self.cache.borrow().get(sql) {
            return Ok((None, reads.clone()));
        }
        let (prepared, mut reads) = deps::collect(&self.slot, || self.conn.prepare(sql));
        let statement = prepared?;
        deps::resolve(&self.conn, &mut reads)?;
        let mut cache = self.cache.borrow_mut();
        if cache.len() >= CACHED_QUERIES {
            cache.clear();
        }
        cache.insert(sql.to_owned(), reads.clone());
        Ok((Some(statement), reads))
    }
}

/// A read transaction on one version of the database. Every query in it
/// sees the same rows and reports the same version.
pub struct Snapshot<'db> {
    db: &'db LiveDb,
    reader: Option<Reader>,
    version: Version,
}

impl LiveDb {
    /// Starts a snapshot at the latest published version.
    pub fn snapshot(&self) -> Result<Snapshot<'_>> {
        let popped = crate::lock(&self.readers).pop();
        let mut reader = match popped {
            Some(reader) => reader,
            None => Reader::open(self)?,
        };
        match reader.begin() {
            Ok((version, schema)) => {
                // The snapshot may see a commit before its event is
                // published; a result's version must be one the log has
                // reached, so `changes_since` can follow it.
                self.log.catch_up(version);
                if schema != reader.schema {
                    reader.cache.get_mut().clear();
                    reader.schema = schema;
                }
                Ok(Snapshot {
                    db: self,
                    reader: Some(reader),
                    version,
                })
            }
            Err(e) => {
                self.release(reader);
                Err(e)
            }
        }
    }

    /// Runs one query in a fresh snapshot.
    pub fn query<T>(
        &self,
        sql: &str,
        params: impl QueryParams,
        map: impl FnMut(&rusqlite::Row<'_>) -> rusqlite::Result<T>,
    ) -> Result<QueryResult<T>> {
        self.snapshot()?.query(sql, params, map)
    }

    /// Returns a reader to the pool once its transaction has ended.
    fn release(&self, reader: Reader) {
        if !reader.conn.is_autocommit() && reader.conn.execute_batch("ROLLBACK").is_err() {
            return;
        }
        let mut readers = crate::lock(&self.readers);
        if readers.len() < self.options().readers {
            readers.push(reader);
        }
    }
}

impl Snapshot<'_> {
    pub fn version(&self) -> Version {
        self.version
    }

    /// Runs a read-only query. `params` binds `?N` and named parameters, as
    /// rusqlite's do. A query may not run `PRAGMA`, `ATTACH`, `DETACH`, or
    /// transaction statements.
    pub fn query<T>(
        &self,
        sql: &str,
        params: impl QueryParams,
        mut map: impl FnMut(&rusqlite::Row<'_>) -> rusqlite::Result<T>,
    ) -> Result<QueryResult<T>> {
        let reader = self
            .reader
            .as_ref()
            .expect("a snapshot holds its reader until it drops");
        reader.still_open()?;
        let values = params.into_values()?;
        let (fresh, reads) = reader.reads(sql)?;
        let mut cached;
        let mut fresh = fresh;
        let statement: &mut Statement<'_> = match fresh.as_mut() {
            Some(statement) => statement,
            None => {
                cached = reader.conn.prepare_cached(sql)?;
                &mut cached
            }
        };
        if !statement.readonly() {
            return Err(Error::Unsupported(format!("a query that writes: {sql}")));
        }
        bind(statement, &values)?;
        let mut rows = statement.raw_query();
        let mut out = Vec::new();
        while let Some(row) = rows.next()? {
            out.push(map(row)?);
        }
        drop(rows);
        reader.still_open()?;
        Ok(QueryResult {
            rows: out,
            version: self.version,
            read_set: ReadSet::new(self.version, reads),
        })
    }
}

impl Drop for Snapshot<'_> {
    fn drop(&mut self) {
        if let Some(reader) = self.reader.take() {
            self.db.release(reader);
        }
    }
}

/// Binds every parameter. Positional values must fill the statement's
/// parameters exactly, as rusqlite requires.
fn bind(statement: &mut Statement<'_>, values: &ParamValues) -> Result<()> {
    let expected = statement.parameter_count();
    if values.positional.len() > expected
        || (values.named.is_empty() && values.positional.len() != expected)
    {
        return Err(
            rusqlite::Error::InvalidParameterCount(values.positional.len(), expected).into(),
        );
    }
    for (i, value) in values.positional.iter().enumerate() {
        statement.raw_bind_parameter(i + 1, value)?;
    }
    for (name, value) in &values.named {
        let index = statement
            .parameter_index(name)?
            .ok_or_else(|| rusqlite::Error::InvalidParameterName(name.clone()))?;
        statement.raw_bind_parameter(index, value)?;
    }
    Ok(())
}
