//! Queries on read-only connections, each inside a snapshot whose version
//! the read set records.

use crate::capture::owned;
use crate::constraints::{self, Analysis, Condition, Constraint, Filter, Numbered};
use crate::db::{LiveDb, open_connection};
use crate::deps::{self, Reads, Slot};
use crate::error::{Error, Result};
use crate::event::Version;
use crate::hash::{ResultHash, RowHasher};
use crate::params::{ParamValues, QueryParams};
use crate::read_set::ReadSet;
use crate::schema::Schema;
use rusqlite::types::Value;
use rusqlite::{Connection, Statement};
use std::cell::RefCell;
use std::collections::BTreeMap;
use std::sync::Arc;

/// A query's rows, the version of the snapshot it read, what the rows
/// depend on, and a hash of the rows.
#[derive(Clone, Debug)]
pub struct QueryResult<T> {
    /// Empty when the result is `unchanged`.
    pub rows: Vec<T>,
    pub version: Version,
    pub read_set: ReadSet,
    pub hash: ResultHash,
    /// Set by `rerun` when the rows hash the same as the previous result's.
    pub unchanged: bool,
}

/// Distinct query texts whose reads a reader remembers.
const CACHED_QUERIES: usize = 256;

pub(crate) struct Reader {
    conn: Connection,
    slot: Slot,
    /// The schema cookie `schema` and the cache were loaded under.
    cookie: i64,
    schema: Arc<Schema>,
    /// What each query text reads, recorded under `cookie`.
    cache: RefCell<BTreeMap<String, Cached>>,
    /// The buffer each result's hash gathers its bytes in.
    hash_buffer: RefCell<Vec<u8>>,
}

/// What a reader remembers about a query text.
#[derive(Clone)]
struct Cached {
    reads: Reads,
    /// The text with its parameters numbered, for the constraint analysis;
    /// `None` when SQLite numbers them differently.
    numbered: Option<Numbered>,
}

impl Reader {
    fn open(db: &LiveDb) -> rusqlite::Result<Reader> {
        let mut conn = open_connection(db.path(), db.options(), true)?;
        let slot = deps::install(&mut conn)?;
        Ok(Reader {
            conn,
            slot,
            cookie: -1,
            schema: Arc::new(Schema::default()),
            cache: RefCell::new(BTreeMap::new()),
            hash_buffer: RefCell::new(Vec::new()),
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

    /// What `sql` reads, recorded while preparing it the first time this
    /// reader sees it under the current schema.
    fn reads(&self, sql: &str) -> Result<(Option<Statement<'_>>, Cached)> {
        if let Some(cached) = self.cache.borrow().get(sql) {
            return Ok((None, cached.clone()));
        }
        let (prepared, mut reads) = deps::collect(&self.slot, || self.conn.prepare(sql));
        let statement = prepared?;
        deps::resolve(&self.conn, &mut reads)?;
        let numbered = constraints::number_parameters(sql).filter(|n| n.agrees(&statement));
        let cached = Cached { reads, numbered };
        let mut cache = self.cache.borrow_mut();
        if cache.len() >= CACHED_QUERIES {
            cache.clear();
        }
        cache.insert(sql.to_owned(), cached.clone());
        Ok((Some(statement), cached))
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
                if schema != reader.cookie {
                    match Schema::load(&reader.conn) {
                        Ok(loaded) => reader.schema = Arc::new(loaded),
                        Err(e) => {
                            self.release(reader);
                            return Err(e.into());
                        }
                    }
                    // Statements prepared under the old schema report its
                    // columns until their first step re-prepares them.
                    reader.cache.get_mut().clear();
                    reader.conn.flush_prepared_statement_cache();
                    reader.cookie = schema;
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

    /// Runs a query again in a fresh snapshot and compares its rows with a
    /// previous result's hash. See [`Snapshot::rerun`].
    pub fn rerun<T>(
        &self,
        sql: &str,
        params: impl QueryParams,
        previous: ResultHash,
        map: impl FnMut(&rusqlite::Row<'_>) -> rusqlite::Result<T>,
    ) -> Result<QueryResult<T>> {
        self.snapshot()?.rerun(sql, params, previous, map)
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
        map: impl FnMut(&rusqlite::Row<'_>) -> rusqlite::Result<T>,
    ) -> Result<QueryResult<T>> {
        self.run(sql, params, None, map)
    }

    /// Runs a query again and compares its rows with `previous`, the hash
    /// of an earlier result. When they hash the same, the result is
    /// `unchanged` and carries no rows, so the app has nothing to push. It
    /// still carries this snapshot's version, read set and hash, which
    /// replace the earlier result's. The rows are mapped as they are read,
    /// then dropped if unchanged.
    pub fn rerun<T>(
        &self,
        sql: &str,
        params: impl QueryParams,
        previous: ResultHash,
        map: impl FnMut(&rusqlite::Row<'_>) -> rusqlite::Result<T>,
    ) -> Result<QueryResult<T>> {
        self.run(sql, params, Some(previous), map)
    }

    fn run<T>(
        &self,
        sql: &str,
        params: impl QueryParams,
        previous: Option<ResultHash>,
        mut map: impl FnMut(&rusqlite::Row<'_>) -> rusqlite::Result<T>,
    ) -> Result<QueryResult<T>> {
        let reader = self
            .reader
            .as_ref()
            .expect("a snapshot holds its reader until it drops");
        reader.still_open()?;
        let values = params.into_values()?;
        let (fresh, cached) = reader.reads(sql)?;
        let mut reused;
        let mut fresh = fresh;
        let statement: &mut Statement<'_> = match fresh.as_mut() {
            Some(statement) => statement,
            None => {
                reused = reader.conn.prepare_cached(sql)?;
                &mut reused
            }
        };
        if !statement.readonly() {
            return Err(Error::Unsupported(format!("a query that writes: {sql}")));
        }
        bind(statement, &values)?;
        let analysis = match &cached.numbered {
            Some(numbered) => constraints::analyze(
                numbered,
                &numbered.values(&values),
                &reader.schema,
                &reader.conn,
            ),
            None => Analysis::default(),
        };
        // The result column that holds the window's sort key, if any.
        let boundary_column = analysis.window.as_ref().and_then(|plan| {
            statement.columns_with_metadata().position(|column| {
                column.database_name() == Some("main")
                    && column
                        .table_name()
                        .is_some_and(|t| t.eq_ignore_ascii_case(&plan.table))
                    && column
                        .origin_name()
                        .is_some_and(|c| c.eq_ignore_ascii_case(&plan.column))
            })
        });
        // Only the row that fills the window bounds it.
        let filling = analysis.window.as_ref().map(|plan| plan.limit);
        let mut boundary = None;
        let mut hasher = RowHasher::new(reader.hash_buffer.take());
        let mut rows = statement.raw_query();
        let mut out = Vec::new();
        while let Some(row) = rows.next()? {
            if let Some(i) = boundary_column
                && filling == Some(out.len() as u64 + 1)
            {
                boundary = Some(owned(row.get_ref(i)?));
            }
            hasher.row(row)?;
            out.push(map(row)?);
        }
        drop(rows);
        reader.still_open()?;
        let mut reads = cached.reads;
        constrain(&mut reads, analysis, boundary);
        let (hash, buffer) = hasher.finish();
        *reader.hash_buffer.borrow_mut() = buffer;
        let unchanged = previous == Some(hash);
        if unchanged {
            out = Vec::new();
        }
        Ok(QueryResult {
            rows: out,
            version: self.version,
            read_set: ReadSet::new(self.version, reads),
            hash,
            unchanged,
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

/// Narrows each analyzed table to the rows its constraints allow. A `LIMIT`
/// window also bounds the rows at `boundary`, the sort key of the row that
/// filled it, if one did; a `LIMIT 0` result depends on no rows at all.
fn constrain(reads: &mut Reads, analysis: Analysis, boundary: Option<Value>) {
    let mut tables = analysis.tables;
    if let Some(plan) = analysis.window
        && let Some(conjunctions) = tables.get_mut(&plan.table)
        && let [conjunction] = conjunctions.as_mut_slice()
    {
        let condition = match boundary {
            _ if plan.limit == 0 => Some(Condition::Never),
            // Text that converted lossily cannot bound the window exactly,
            // and a last row of `NULL`s that sort last bounds nothing.
            Some(Value::Text(text)) if text.contains('\u{FFFD}') => None,
            Some(Value::Null) if !plan.nulls_first => None,
            Some(boundary) => Some(Condition::Window {
                boundary,
                descending: plan.descending,
                nulls_first: plan.nulls_first,
            }),
            None => None,
        };
        if let Some(condition) = condition {
            conjunction.constraints.push(Constraint {
                column: plan.column,
                collation: plan.collation,
                condition,
            });
        }
    }
    for (name, conjunctions) in tables {
        if let Some(table) = reads.get_mut(&name) {
            table.set_filter(Filter::any_of(conjunctions));
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
