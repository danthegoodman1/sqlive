//! Queries on read-only connections, each inside a snapshot whose version
//! the read set records.

use crate::capture::owned;
use crate::constraints::{self, Analysis, Condition, Constraint, Filter, Parameters, Plan};
use crate::db::{LiveDb, open_connection};
use crate::deps::{self, Slot};
use crate::error::{Error, Result};
use crate::event::Version;
use crate::hash::{ResultHash, RowHasher};
use crate::params::{ParamValues, QueryParams};
use crate::read_set::{ColumnsRead, ReadSet, TableRead};
use crate::schema::Schema;
use rusqlite::types::Value;
use rusqlite::{Connection, Statement};
use std::cell::RefCell;
use std::collections::BTreeMap;
use std::ptr::NonNull;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

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

/// Distinct query texts whose reads and statements a reader remembers.
const CACHED_QUERIES: usize = 256;

pub(crate) struct Reader {
    /// The connection, allocated by `Box::leak` so its address stays put
    /// while `statements` borrow it. `Drop` frees it.
    conn: NonNull<Connection>,
    slot: Slot,
    /// The schema cookie `schema` and the caches were loaded under.
    cookie: i64,
    schema: Arc<Schema>,
    /// What each query text reads, recorded under `cookie`.
    cache: RefCell<BTreeMap<String, Arc<Cached>>>,
    /// A slot for each query text in `cache`, holding its statement,
    /// prepared under `cookie`, while no query runs it. rusqlite's own cache
    /// clears a statement's bindings when it takes the statement back, and
    /// SQLite then prepares the statement again if planning looked at any
    /// parameter's value; binding a parameter prepares it again only if
    /// planning looked at that one.
    statements: RefCell<BTreeMap<String, Option<Statement<'static>>>>,
    /// The buffer each result's hash gathers its bytes in.
    hash_buffer: RefCell<Vec<u8>>,
}

// SAFETY: a reader moves between threads together with its connection and
// the statements that borrow it, and only the thread holding the reader
// uses them, as SQLite's multi-thread mode allows.
unsafe impl Send for Reader {}

impl Drop for Reader {
    fn drop(&mut self) {
        drop(std::mem::take(self.statements.get_mut()));
        // SAFETY: `conn` came from `Box::leak`, and no statement borrows it
        // any more.
        drop(unsafe { Box::from_raw(self.conn.as_ptr()) });
    }
}

/// What a reader remembers about a query text.
struct Cached {
    /// The tables the text reads, in order of their lowercase names.
    reads: Box<[Arc<ColumnsRead>]>,
    /// How the text numbers its parameters, and its constraint plan;
    /// `None` when SQLite numbers the parameters differently.
    planned: Option<(Parameters, Plan)>,
    /// The result column that holds the window's sort key, if any.
    boundary_column: Option<usize>,
}

impl Reader {
    fn open(db: &LiveDb) -> rusqlite::Result<Reader> {
        let mut conn = open_connection(db.path(), db.options(), true)?;
        let slot = deps::install(&mut conn)?;
        Ok(Reader {
            conn: NonNull::from(Box::leak(Box::new(conn))),
            slot,
            cookie: -1,
            schema: Arc::new(Schema::default()),
            cache: RefCell::new(BTreeMap::new()),
            statements: RefCell::new(BTreeMap::new()),
            hash_buffer: RefCell::new(Vec::new()),
        })
    }

    fn conn(&self) -> &Connection {
        // SAFETY: `conn` stays valid until `Drop`.
        unsafe { self.conn.as_ref() }
    }

    /// Forgets each query text's reads and statement. Statements prepared
    /// under an older schema report its columns until their first step
    /// prepares them again.
    fn forget(&mut self) {
        self.cache.get_mut().clear();
        self.statements.get_mut().clear();
    }

    /// Starts a read transaction and returns its version and schema cookie.
    /// Its statements stay prepared, since every query runs them.
    fn begin(&self) -> Result<(Version, i64)> {
        self.conn().prepare_cached("BEGIN")?.execute([])?;
        let (version, cookie) = self
            .conn()
            .prepare_cached(
                "SELECT version, (SELECT schema_version FROM pragma_schema_version)
                 FROM _sqlive_meta WHERE id = 0",
            )?
            .query_row([], |r| Ok((r.get::<_, i64>(0)?, r.get::<_, i64>(1)?)))?;
        self.still_open()?;
        Ok((version as Version, cookie))
    }

    /// Checks that the snapshot's transaction is still open. After an I/O
    /// error SQLite can end it even when the statement reports success, and
    /// later statements would then read the latest version.
    fn still_open(&self) -> Result<()> {
        if self.conn().is_autocommit() {
            return Err(Error::SnapshotEnded);
        }
        Ok(())
    }

    /// The statement for `sql`, taken from the reader until `keep` returns
    /// it, and what `sql` reads with its constraint plan, recorded while
    /// preparing it the first time this reader sees it under the current
    /// schema.
    fn statement(&self, sql: &str) -> Result<(Statement<'static>, Arc<Cached>)> {
        // SAFETY: a statement prepared here lives in `statements` or in the
        // query running it, both of which end before `Drop` frees the
        // connection.
        let conn: &'static Connection = unsafe { self.conn.as_ref() };
        if let Some(cached) = self.cache.borrow().get(sql).cloned() {
            let kept = self
                .statements
                .borrow_mut()
                .get_mut(sql)
                .and_then(Option::take);
            let statement = match kept {
                Some(statement) => statement,
                // A query runs inside another's row mapping, or the
                // statement was dropped after an error.
                None => conn.prepare(sql)?,
            };
            return Ok((statement, cached));
        }
        let (prepared, mut reads) = deps::collect(&self.slot, || conn.prepare(sql));
        let statement = prepared?;
        deps::resolve(conn, &mut reads)?;
        let planned = constraints::number_parameters(sql)
            .filter(|n| n.agrees(&statement))
            .map(|numbered| {
                let plan = constraints::plan(&numbered, &self.schema);
                (numbered.parameters, plan)
            });
        let boundary_column = planned
            .as_ref()
            .and_then(|(_, plan)| plan.window_column())
            .and_then(|(table, column)| {
                statement.columns_with_metadata().position(|c| {
                    c.database_name() == Some("main")
                        && c.table_name()
                            .is_some_and(|t| t.eq_ignore_ascii_case(table))
                        && c.origin_name()
                            .is_some_and(|c| c.eq_ignore_ascii_case(column))
                })
            });
        let cached = Arc::new(Cached {
            reads: reads.into_values().map(|read| read.finish()).collect(),
            planned,
            boundary_column,
        });
        let mut cache = self.cache.borrow_mut();
        let mut statements = self.statements.borrow_mut();
        if cache.len() >= CACHED_QUERIES {
            cache.clear();
            statements.clear();
        }
        cache.insert(sql.to_owned(), cached.clone());
        statements.insert(sql.to_owned(), None);
        Ok((statement, cached))
    }

    /// Keeps a statement `statement` took, in its text's empty slot, once it
    /// lets go of its values, which a large text or blob would otherwise
    /// hold until the text runs again. A text the reader has forgotten has
    /// no slot, and its statement closes.
    fn keep(&self, sql: &str, mut statement: Statement<'static>) {
        let released = (1..=statement.parameter_count())
            .all(|i| statement.raw_bind_parameter(i, Value::Null).is_ok());
        if released && let Some(slot @ None) = self.statements.borrow_mut().get_mut(sql) {
            *slot = Some(statement);
        }
    }
}

/// Idle read-only connections, at most one per slot. Each thread visits
/// the slots starting from the one its number picks, so a connection tends
/// to stay with the thread that last used it, and threads seldom wait on
/// one another. Every visit from one thread runs the slots in the same
/// cyclic order, so the readers a thread gets depend on its order of takes
/// and returns alone, whatever its number: a seed replays the same way on
/// any thread.
pub(crate) struct Readers {
    slots: Box<[Mutex<Option<Reader>>]>,
}

/// Numbers threads in the order they first visit the slots.
static THREADS: AtomicUsize = AtomicUsize::new(0);

thread_local! {
    static THREAD: usize = THREADS.fetch_add(1, Ordering::Relaxed);
}

impl Readers {
    pub fn new(slots: usize) -> Readers {
        Readers {
            slots: (0..slots).map(|_| Mutex::new(None)).collect(),
        }
    }

    /// The slots in the order this thread visits them. A thread whose
    /// number is gone, as while its thread-locals drop, starts at the first.
    fn visit(&self) -> impl Iterator<Item = &Mutex<Option<Reader>>> {
        let first = THREAD.try_with(|thread| *thread).unwrap_or(0);
        let count = self.slots.len();
        (0..count).map(move |i| &self.slots[(first + i) % count])
    }

    fn take(&self) -> Option<Reader> {
        self.visit().find_map(|slot| crate::lock(slot).take())
    }

    /// Closes every idle reader.
    pub fn close(&self) {
        for slot in &self.slots {
            drop(crate::lock(slot).take());
        }
    }

    /// Keeps `reader` in the first empty slot, or closes it when every slot
    /// holds one.
    fn keep(&self, reader: Reader) {
        let mut reader = Some(reader);
        for slot in self.visit() {
            let mut slot = crate::lock(slot);
            if slot.is_none() {
                *slot = reader.take();
                return;
            }
        }
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
        let mut reader = match self.readers.take() {
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
                    match Schema::load(reader.conn()) {
                        Ok(loaded) => reader.schema = Arc::new(loaded),
                        Err(e) => {
                            self.release(reader);
                            return Err(e.into());
                        }
                    }
                    reader.forget();
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
        if !reader.conn().is_autocommit()
            && reader
                .conn()
                .prepare_cached("ROLLBACK")
                .and_then(|mut rollback| rollback.execute([]))
                .is_err()
        {
            return;
        }
        self.readers.keep(reader);
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
        let (mut statement, cached) = reader.statement(sql)?;
        if !statement.readonly() {
            return Err(Error::Unsupported(format!("a query that writes: {sql}")));
        }
        bind(&mut statement, &values)?;
        let analysis = match &cached.planned {
            Some((numbered, plan)) => plan.bind(&numbered.values(&values), reader.conn()),
            None => Analysis::default(),
        };
        let boundary_column = analysis.window.as_ref().and(cached.boundary_column);
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
        let tables = constrain(&cached.reads, analysis, boundary);
        let (hash, buffer) = hasher.finish(&statement)?;
        *reader.hash_buffer.borrow_mut() = buffer;
        reader.keep(sql, statement);
        let unchanged = previous == Some(hash);
        if unchanged {
            out = Vec::new();
        }
        Ok(QueryResult {
            rows: out,
            version: self.version,
            read_set: ReadSet::new(self.version, tables),
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

/// Each table a query reads, with each analyzed one narrowed to the rows
/// its constraints allow. A `LIMIT` window also bounds the rows at
/// `boundary`, the sort key of the row that filled it, if one did; a
/// `LIMIT 0` result depends on no rows at all.
fn constrain(
    reads: &[Arc<ColumnsRead>],
    analysis: Analysis,
    boundary: Option<Value>,
) -> Box<[TableRead]> {
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
    reads
        .iter()
        .map(|read| {
            let filter = tables
                .remove(read.key())
                .map_or(Filter::All, Filter::any_of);
            TableRead::new(read.clone(), filter)
        })
        .collect()
}

/// Binds every parameter `values` holds; a statement starts each run with
/// every parameter `NULL`. Positional values must fill the statement's
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
