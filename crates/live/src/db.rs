//! The live database: one writer connection that records every committed
//! transaction's net row changes, and the log those changes go to.

use crate::capture::{self, Capture, Captured, Key, Raw, Touch, owned};
use crate::error::{Error, Result};
use crate::event::{CommitEvent, Op, Row, RowChange, Version};
use crate::log::{Changes, Log};
use crate::read::Readers;
use crate::schema::{META_TABLE, Schema, Table};
use rusqlite::hooks::{AuthAction, AuthContext, Authorization};
use rusqlite::{
    CachedStatement, Connection, OpenFlags, Transaction, TransactionBehavior, params_from_iter,
};
use std::collections::btree_map::{BTreeMap, Entry};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

/// How [`LiveDb::open_with`] opens a database.
#[derive(Clone, Debug)]
pub struct Options {
    /// The SQLite VFS to open the database with; `None` for the default.
    pub vfs: Option<String>,
    /// Read-only connections kept open for queries.
    pub readers: usize,
    /// Commit events the log retains for [`LiveDb::changes_since`].
    pub log_capacity: usize,
    pub busy_timeout: Duration,
    /// WAL pages after which a commit checkpoints (`wal_autocheckpoint`).
    pub checkpoint_pages: u32,
}

impl Default for Options {
    fn default() -> Options {
        Options {
            vfs: None,
            readers: 4,
            log_capacity: 4096,
            busy_timeout: Duration::from_secs(5),
            checkpoint_pages: 1000,
        }
    }
}

/// A SQLite database whose writes publish commit events.
pub struct LiveDb {
    path: String,
    options: Options,
    writer: Mutex<Writer>,
    /// Idle read-only connections, at most `options.readers`.
    pub(crate) readers: Readers,
    pub(crate) log: Arc<Log>,
}

struct Writer {
    conn: Connection,
    capture: Arc<Mutex<Capture>>,
    /// True while SqLive runs its own statements; false while the caller's
    /// closure runs, when the authorizer guards the transaction.
    internal: Arc<AtomicBool>,
    /// True only while SqLive runs its own `COMMIT` or `VACUUM`; the commit
    /// hook refuses every other commit.
    committing: Arc<AtomicBool>,
    /// Set whenever SQLite rolls a transaction back; cleared as each write
    /// begins.
    rolled_back: Arc<AtomicBool>,
    schema: Arc<Schema>,
    /// A failed commit or vacuum left this process unsure of the database's
    /// state; the next write reads the version back, resets the log, and
    /// sets `schema_changed`.
    unsure: bool,
}

/// Statements a `write` closure may not run: they would end the transaction
/// early, change durability, or reach another database.
const GUARDED_PRAGMAS: [&str; 4] = ["journal_mode", "synchronous", "locking_mode", "query_only"];

impl LiveDb {
    /// Opens or creates a database with the default [`Options`].
    pub fn open(path: impl AsRef<str>) -> Result<LiveDb> {
        LiveDb::open_with(path, Options::default())
    }

    pub fn open_with(path: impl AsRef<str>, options: Options) -> Result<LiveDb> {
        let path = path.as_ref().to_owned();
        let mut conn = open_connection(&path, &options, false)?;
        let mode: String = conn.query_row("PRAGMA journal_mode = WAL", [], |r| r.get(0))?;
        if !mode.eq_ignore_ascii_case("wal") {
            return Err(Error::Unsupported(format!(
                "journal mode {mode}; SqLive needs WAL"
            )));
        }
        conn.pragma_update(None, "synchronous", "FULL")?;
        conn.pragma_update(None, "foreign_keys", true)?;
        conn.pragma_update(None, "wal_autocheckpoint", options.checkpoint_pages)?;
        // `schema` holds the schema cookie as of the last commit, so a cookie
        // that moves without a commit, as `VACUUM` moves it, shows.
        conn.execute_batch(&format!(
            "BEGIN IMMEDIATE;
             CREATE TABLE IF NOT EXISTS {META_TABLE} (
                 id INTEGER PRIMARY KEY CHECK (id = 0),
                 version INTEGER NOT NULL,
                 schema INTEGER NOT NULL
             );
             INSERT OR IGNORE INTO {META_TABLE}
                 VALUES (0, 0, (SELECT schema_version FROM pragma_schema_version));
             COMMIT;"
        ))?;
        let meta = read_meta(&conn)?;
        let schema = Arc::new(Schema::load(&conn)?);
        let capture = Arc::new(Mutex::new(Capture::default()));
        capture::install(&mut conn, capture.clone())?;
        let internal = Arc::new(AtomicBool::new(true));
        install_guard(&mut conn, internal.clone())?;
        // After an error, SQLite can roll a transaction back, sometimes
        // without failing the statement, and run later statements each in a
        // transaction of its own. Refusing every commit but SqLive's own
        // keeps a write atomic, and the rollback hook records the rollback.
        let committing = Arc::new(AtomicBool::new(false));
        let allowed = committing.clone();
        conn.commit_hook(Some(move || !allowed.load(Ordering::Relaxed)))?;
        let rolled_back = Arc::new(AtomicBool::new(false));
        let flag = rolled_back.clone();
        conn.rollback_hook(Some(move || flag.store(true, Ordering::Relaxed)))?;
        let moved = schema.version != meta.schema;
        let log = Arc::new(Log::new(meta.version, options.log_capacity));
        let readers = Readers::new(options.readers);
        let db = LiveDb {
            path,
            options,
            writer: Mutex::new(Writer {
                conn,
                capture,
                internal,
                committing,
                rolled_back,
                schema,
                unsure: false,
            }),
            readers,
            log,
        };
        if moved {
            // The schema moved after the last commit, as when a `VACUUM`
            // ran and its event never committed: publish the change now, so
            // a consumer that resumes from before it reloads.
            let mut writer = crate::lock(&db.writer);
            db.commit(&mut writer, |_| Ok(()), true)?;
        }
        Ok(db)
    }

    /// The path the database was opened with.
    pub fn path(&self) -> &str {
        &self.path
    }

    pub fn options(&self) -> &Options {
        &self.options
    }

    /// The latest version the log has reached: the newest published commit,
    /// or a newer one a snapshot found the database already held, as after a
    /// commit whose outcome was uncertain.
    pub fn version(&self) -> Version {
        self.log.latest()
    }

    /// Runs `f` inside a transaction and commits it, publishing one commit
    /// event. Returns `f`'s value and the commit's version.
    ///
    /// `f` may run any SQL, savepoints included, but not `BEGIN`, `COMMIT`
    /// or `ROLLBACK`. If `f` fails, the transaction rolls back and nothing is
    /// published. If `COMMIT` reports an error but the transaction took
    /// effect, `write` publishes the event and succeeds.
    pub fn write<T>(
        &self,
        f: impl FnOnce(&Transaction<'_>) -> rusqlite::Result<T>,
    ) -> Result<(T, Version)> {
        let mut writer = crate::lock(&self.writer);
        self.commit(&mut writer, f, false)
    }

    /// Runs `VACUUM`, then publishes an event with `schema_changed` set,
    /// since vacuuming can renumber rowids. If either step fails, `VACUUM`
    /// may still have taken effect, so every open stream lags, and the next
    /// write, or the next open, publishes `schema_changed`.
    pub fn vacuum(&self) -> Result<Version> {
        let mut writer = crate::lock(&self.writer);
        let vacuumed = {
            let _allowed = Allowed::enter(&writer.committing);
            writer.conn.execute_batch("VACUUM")
        };
        let result = match vacuumed {
            Ok(()) => self
                .commit(&mut writer, |_| Ok(()), true)
                .map(|((), version)| version),
            Err(error) => Err(error.into()),
        };
        if result.is_err() {
            self.doubt(&mut writer);
        }
        result
    }

    /// Records that the database may have changed without an event: every
    /// open stream lags, and the next write sets `schema_changed`.
    fn doubt(&self, writer: &mut Writer) {
        writer.unsure = true;
        self.log.reset(self.log.latest());
    }

    /// A stream of the events committed after now.
    pub fn changes(&self) -> Result<Changes> {
        Ok(self.log.from_latest())
    }

    /// A stream of the events after `version`, starting with any the log
    /// still retains. A `version` older than the log yields
    /// [`Error::Lagged`].
    pub fn changes_since(&self, version: Version) -> Result<Changes> {
        self.log.since(version)
    }

    fn commit<T>(
        &self,
        writer: &mut Writer,
        f: impl FnOnce(&Transaction<'_>) -> rusqlite::Result<T>,
        schema_changed: bool,
    ) -> Result<(T, Version)> {
        let result = self.attempt(writer, f, schema_changed);
        if !writer.conn.is_autocommit()
            && writer.conn.execute_batch("ROLLBACK").is_err()
            && !writer.conn.is_autocommit()
        {
            self.doubt(writer);
        }
        result
    }

    fn attempt<T>(
        &self,
        writer: &mut Writer,
        f: impl FnOnce(&Transaction<'_>) -> rusqlite::Result<T>,
        force_schema_changed: bool,
    ) -> Result<(T, Version)> {
        let Writer {
            conn,
            capture,
            internal,
            committing,
            rolled_back,
            schema,
            unsure,
        } = writer;
        let tx = Transaction::new_unchecked(conn, TransactionBehavior::Immediate)?;
        rolled_back.store(false, Ordering::Relaxed);
        let meta = read_meta(&tx)?;
        if meta.version != self.log.latest() {
            self.log.reset(meta.version);
        }
        // A failed write may have changed the database without an event, so
        // this one tells every consumer to reload.
        let suspect = *unsure;
        let before = schema_version(&tx)?;
        // A schema that moved since the last commit moved outside any write,
        // as it does when a `VACUUM` runs and its event fails to commit.
        let moved_outside = before != meta.schema;
        if before != schema.version {
            *schema = Arc::new(Schema::load(&tx)?);
        }
        let recording = Recording::begin(capture, schema.clone());
        let value = {
            let _caller = CallerScope::enter(internal);
            f(&tx)
        }?;
        if ended(rolled_back, &tx) {
            return Err(Error::RolledBack);
        }
        let after = schema_version(&tx)?;
        let schema_changed = force_schema_changed || suspect || moved_outside || after != before;
        // Recording continues through the version bump, so nothing a write
        // sets off goes unrecorded.
        let version = bump_version(&tx, after)?;
        let captured = recording.finish();
        let schema_changed = schema_changed || captured.unaccounted;
        let (changes, reloaded) = if schema_changed {
            (Vec::new(), Some(Arc::new(Schema::load(&tx)?)))
        } else {
            (net_changes(&tx, captured.touched)?, None)
        };
        let event = CommitEvent {
            version,
            schema_changed,
            changes,
        };
        if ended(rolled_back, &tx) {
            return Err(Error::RolledBack);
        }
        // Readers may see this version before it is published; they wait
        // until the commit's outcome is settled.
        let _in_flight = self.log.begin_commit();
        let committed = {
            let _allowed = Allowed::enter(committing);
            tx.commit()
        };
        match committed {
            Ok(()) => {
                if let Some(reloaded) = reloaded {
                    *schema = reloaded;
                }
                *unsure = false;
                self.log.publish(event);
                Ok((value, version))
            }
            Err(error) => {
                // The transaction may have taken effect anyway, as when the
                // commit succeeded and a later step failed.
                if !conn.is_autocommit() {
                    let _ = conn.execute_batch("ROLLBACK");
                }
                match read_version(conn) {
                    Ok(seen) if seen == version => {
                        if let Some(reloaded) = reloaded {
                            *schema = reloaded;
                        }
                        *unsure = false;
                        self.log.publish(event);
                        Ok((value, version))
                    }
                    Ok(seen) if seen + 1 == version => Err(error.into()),
                    _ => {
                        *unsure = true;
                        self.log.reset(self.log.latest());
                        Err(error.into())
                    }
                }
            }
        }
    }
}

/// Whether SQLite ended a write's transaction on its own. A `SAVEPOINT`
/// after an automatic rollback opens a new transaction, so the rollback
/// hook's flag tells where the autocommit state cannot.
fn ended(rolled_back: &AtomicBool, tx: &Transaction<'_>) -> bool {
    rolled_back.load(Ordering::Relaxed) || tx.is_autocommit()
}

/// Lets SqLive's own commit through the commit hook until it drops.
struct Allowed<'a>(&'a AtomicBool);

impl<'a> Allowed<'a> {
    fn enter(committing: &'a AtomicBool) -> Allowed<'a> {
        committing.store(true, Ordering::Relaxed);
        Allowed(committing)
    }
}

impl Drop for Allowed<'_> {
    fn drop(&mut self) {
        self.0.store(false, Ordering::Relaxed);
    }
}

impl Drop for LiveDb {
    fn drop(&mut self) {
        self.log.close();
    }
}

/// Records the preupdate hook's changes until finished or dropped.
struct Recording<'a>(&'a Mutex<Capture>);

impl<'a> Recording<'a> {
    fn begin(capture: &'a Mutex<Capture>, schema: Arc<Schema>) -> Recording<'a> {
        crate::lock(capture).begin(schema);
        Recording(capture)
    }

    fn finish(self) -> Captured {
        let captured = crate::lock(self.0).finish();
        std::mem::forget(self);
        captured
    }
}

impl Drop for Recording<'_> {
    fn drop(&mut self) {
        crate::lock(self.0).finish();
    }
}

/// Marks the span where the caller's closure runs.
struct CallerScope<'a>(&'a AtomicBool);

impl<'a> CallerScope<'a> {
    fn enter(internal: &'a AtomicBool) -> CallerScope<'a> {
        internal.store(false, Ordering::Relaxed);
        CallerScope(internal)
    }
}

impl Drop for CallerScope<'_> {
    fn drop(&mut self) {
        self.0.store(true, Ordering::Relaxed);
    }
}

pub(crate) fn open_connection(
    path: &str,
    options: &Options,
    read_only: bool,
) -> rusqlite::Result<Connection> {
    let flags = if read_only {
        OpenFlags::SQLITE_OPEN_READ_ONLY
            | OpenFlags::SQLITE_OPEN_URI
            | OpenFlags::SQLITE_OPEN_NO_MUTEX
    } else {
        OpenFlags::SQLITE_OPEN_READ_WRITE
            | OpenFlags::SQLITE_OPEN_CREATE
            | OpenFlags::SQLITE_OPEN_URI
            | OpenFlags::SQLITE_OPEN_NO_MUTEX
    };
    let conn = match &options.vfs {
        Some(vfs) => Connection::open_with_flags_and_vfs(path, flags, vfs.as_str())?,
        None => Connection::open_with_flags(path, flags)?,
    };
    conn.busy_timeout(options.busy_timeout)?;
    Ok(conn)
}

fn install_guard(conn: &mut Connection, internal: Arc<AtomicBool>) -> rusqlite::Result<()> {
    conn.authorizer(Some(move |context: AuthContext<'_>| {
        if internal.load(Ordering::Relaxed) {
            return Authorization::Allow;
        }
        let meta = |table: &str| table.eq_ignore_ascii_case(META_TABLE);
        match context.action {
            AuthAction::Transaction { .. }
            | AuthAction::Attach { .. }
            | AuthAction::Detach { .. } => Authorization::Deny,
            AuthAction::Insert { table_name }
            | AuthAction::Delete { table_name }
            | AuthAction::Update { table_name, .. }
            | AuthAction::DropTable { table_name }
            | AuthAction::AlterTable { table_name, .. }
            | AuthAction::CreateTrigger { table_name, .. }
            | AuthAction::CreateTempTrigger { table_name, .. }
            | AuthAction::CreateIndex { table_name, .. }
                if meta(table_name) =>
            {
                Authorization::Deny
            }
            AuthAction::Pragma {
                pragma_name,
                pragma_value: Some(_),
            } if GUARDED_PRAGMAS
                .iter()
                .any(|p| p.eq_ignore_ascii_case(pragma_name)) =>
            {
                Authorization::Deny
            }
            _ => Authorization::Allow,
        }
    }))
}

pub(crate) fn read_version(conn: &Connection) -> rusqlite::Result<Version> {
    conn.prepare_cached(&format!("SELECT version FROM {META_TABLE} WHERE id = 0"))?
        .query_row([], |r| r.get::<_, i64>(0))
        .map(|v| v as Version)
}

/// What `_sqlive_meta` records about the last commit.
struct Meta {
    version: Version,
    /// The schema cookie the last commit left.
    schema: i64,
}

fn read_meta(conn: &Connection) -> rusqlite::Result<Meta> {
    conn.prepare_cached(&format!(
        "SELECT version, schema FROM {META_TABLE} WHERE id = 0"
    ))?
    .query_row([], |r| {
        Ok(Meta {
            version: r.get::<_, i64>(0)? as Version,
            schema: r.get(1)?,
        })
    })
}

/// Advances the version, recording the schema cookie the commit leaves.
fn bump_version(conn: &Connection, schema: i64) -> rusqlite::Result<Version> {
    conn.prepare_cached(&format!(
        "UPDATE {META_TABLE} SET version = version + 1, schema = ?1 WHERE id = 0 RETURNING version"
    ))?
    .query_row([schema], |r| r.get::<_, i64>(0))
    .map(|v| v as Version)
}

fn schema_version(conn: &Connection) -> rusqlite::Result<i64> {
    conn.prepare_cached("PRAGMA schema_version")?
        .query_row([], |r| r.get(0))
}

/// Reads each touched row back and compares it with its first old value.
/// A run of touches with consecutive rowids in one table, as a bulk insert
/// or a range update makes, reads back in one scan.
fn net_changes(conn: &Connection, mut touched: Vec<Touch>) -> Result<Vec<RowChange>> {
    // Each table's read-back statements, by the table's index in the schema.
    let mut by_key: BTreeMap<usize, CachedStatement<'_>> = BTreeMap::new();
    let mut by_range: BTreeMap<usize, CachedStatement<'_>> = BTreeMap::new();
    let mut changes = Vec::new();
    let mut start = 0;
    while start < touched.len() {
        let end = run_end(&touched, start);
        let table = touched[start].table.clone();
        let run = &mut touched[start..end];
        if let (true, Key::Rowid(first), Key::Rowid(last)) =
            (run.len() > 1, &run[0].key, &run[run.len() - 1].key)
        {
            let statement = read_back(&mut by_range, conn, &table, &table.select_range)?;
            let mut rows = statement.query([*first, *last])?;
            let mut pending = run.iter_mut().peekable();
            while let Some(row) = rows.next()? {
                let rowid: i64 = row.get(table.stored.len())?;
                while let Some(touch) =
                    pending.next_if(|touch| matches!(touch.key, Key::Rowid(r) if r < rowid))
                {
                    changes.extend(change(touch, None)?);
                }
                if let Some(touch) =
                    pending.next_if(|touch| matches!(touch.key, Key::Rowid(r) if r == rowid))
                {
                    changes.extend(change(touch, Some(row))?);
                }
            }
            for touch in pending {
                changes.extend(change(touch, None)?);
            }
        } else {
            for touch in run {
                let statement = read_back(&mut by_key, conn, &table, &table.select_by_key)?;
                let mut rows = match &touch.key {
                    Key::Rowid(rowid) => statement.query([rowid])?,
                    Key::PrimaryKey(values) => statement.query(params_from_iter(values))?,
                };
                changes.extend(change(touch, rows.next()?)?);
            }
        }
        start = end;
    }
    Ok(changes)
}

/// The end of the run of touches from `start` whose rowids in one table
/// count up by one.
fn run_end(touched: &[Touch], start: usize) -> usize {
    let mut end = start + 1;
    while let Some(next) = touched.get(end)
        && let (Key::Rowid(previous), Key::Rowid(rowid)) = (&touched[end - 1].key, &next.key)
        && next.table.index == touched[start].table.index
        && previous.checked_add(1) == Some(*rowid)
    {
        end += 1;
    }
    end
}

/// A statement that reads `table`'s rows back, prepared once per commit.
fn read_back<'m, 'c>(
    statements: &'m mut BTreeMap<usize, CachedStatement<'c>>,
    conn: &'c Connection,
    table: &Table,
    sql: &str,
) -> Result<&'m mut CachedStatement<'c>> {
    if !table.without_rowid && table.rowid_name.is_none() {
        return Err(Error::Unsupported(format!(
            "table {} has columns named rowid, _rowid_ and oid, so its rowid cannot be read",
            table.name
        )));
    }
    Ok(match statements.entry(table.index) {
        Entry::Occupied(entry) => entry.into_mut(),
        Entry::Vacant(entry) => entry.insert(conn.prepare_cached(sql)?),
    })
}

/// A touched row's net change, given the row as it now stands: `None` if
/// the transaction left it as it was.
fn change(
    touch: &mut Touch,
    current: Option<&rusqlite::Row<'_>>,
) -> rusqlite::Result<Option<RowChange>> {
    let table = &touch.table;
    let old_row = |values: Vec<Raw>| {
        Row::new(
            table.stored.clone(),
            values.into_iter().map(Raw::into_value).collect(),
        )
    };
    let new_row = |row: &rusqlite::Row<'_>| -> rusqlite::Result<Row> {
        let values = (0..table.stored.len())
            .map(|i| row.get_ref(i).map(owned))
            .collect::<rusqlite::Result<_>>()?;
        Ok(Row::new(table.stored.clone(), values))
    };
    let (op, old, new) = match (touch.first_old.take(), current) {
        (None, None) => return Ok(None),
        (Some(old), Some(new)) if unchanged(&old, new)? => return Ok(None),
        (None, Some(new)) => (Op::Insert, None, Some(new_row(new)?)),
        (Some(old), None) => (Op::Delete, Some(old_row(old)), None),
        (Some(old), Some(new)) => (Op::Update, Some(old_row(old)), Some(new_row(new)?)),
    };
    Ok(Some(RowChange {
        table: table.name.clone(),
        op,
        rowid: match touch.key {
            Key::Rowid(rowid) => Some(rowid),
            Key::PrimaryKey(_) => None,
        },
        old,
        new,
    }))
}

/// Whether a row read back holds exactly its old stored values.
fn unchanged(old: &[Raw], row: &rusqlite::Row<'_>) -> rusqlite::Result<bool> {
    for (i, value) in old.iter().enumerate() {
        if !value.same(row.get_ref(i)?) {
            return Ok(false);
        }
    }
    Ok(true)
}
