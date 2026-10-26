//! The live database: one writer connection that records every committed
//! transaction's net row changes, and the log those changes go to.

use crate::capture::{self, Capture, Captured, Key, Raw, Touch, same_raws};
use crate::error::{Error, Result};
use crate::event::{CommitEvent, Op, Row, RowChange, Version};
use crate::log::{Changes, Log};
use crate::schema::{META_TABLE, Schema, Table};
use rusqlite::hooks::{AuthAction, AuthContext, Authorization};
use rusqlite::{Connection, OpenFlags, Transaction, TransactionBehavior, params_from_iter};
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
    log: Arc<Log>,
}

struct Writer {
    conn: Connection,
    capture: Arc<Mutex<Capture>>,
    /// True while SqLive runs its own statements; false while the caller's
    /// closure runs, when the authorizer guards the transaction.
    internal: Arc<AtomicBool>,
    schema: Arc<Schema>,
    /// A failed commit left this process unsure of the version; the next
    /// write reads it back and resets the log.
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
        conn.execute_batch(&format!(
            "BEGIN IMMEDIATE;
             CREATE TABLE IF NOT EXISTS {META_TABLE} (
                 id INTEGER PRIMARY KEY CHECK (id = 0),
                 version INTEGER NOT NULL
             );
             INSERT OR IGNORE INTO {META_TABLE} VALUES (0, 0);
             COMMIT;"
        ))?;
        let version = read_version(&conn)?;
        let schema = Arc::new(Schema::load(&conn)?);
        let capture = Arc::new(Mutex::new(Capture::default()));
        capture::install(&mut conn, capture.clone())?;
        let internal = Arc::new(AtomicBool::new(true));
        install_guard(&mut conn, internal.clone())?;
        let log = Arc::new(Log::new(version, options.log_capacity));
        Ok(LiveDb {
            path,
            options,
            writer: Mutex::new(Writer {
                conn,
                capture,
                internal,
                schema,
                unsure: false,
            }),
            log,
        })
    }

    /// The path the database was opened with.
    pub fn path(&self) -> &str {
        &self.path
    }

    pub fn options(&self) -> &Options {
        &self.options
    }

    /// The version of the latest commit this process has published.
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
    /// since vacuuming can renumber rowids.
    pub fn vacuum(&self) -> Result<Version> {
        let mut writer = crate::lock(&self.writer);
        writer.conn.execute_batch("VACUUM")?;
        self.commit(&mut writer, |_| Ok(()), true)
            .map(|((), version)| version)
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
            writer.unsure = true;
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
            schema,
            unsure,
        } = writer;
        let tx = Transaction::new_unchecked(conn, TransactionBehavior::Immediate)?;
        let stored = read_version(&tx)?;
        if *unsure || stored != self.log.latest() {
            self.log.reset(stored);
            *unsure = false;
        }
        let before = schema_version(&tx)?;
        // A schema that moved since the last write moved outside any write,
        // as a `VACUUM` whose event failed to commit does.
        let moved_outside = before != schema.version;
        if moved_outside {
            *schema = Arc::new(Schema::load(&tx)?);
        }
        let recording = Recording::begin(capture, schema.clone());
        let value = {
            let _caller = CallerScope::enter(internal);
            f(&tx)
        }?;
        let schema_changed =
            force_schema_changed || moved_outside || schema_version(&tx)? != before;
        // Recording continues through the version bump, so nothing a write
        // sets off goes unrecorded.
        let version = bump_version(&tx)?;
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
        match tx.commit() {
            Ok(()) => {
                if let Some(reloaded) = reloaded {
                    *schema = reloaded;
                }
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
                        self.log.publish(event);
                        Ok((value, version))
                    }
                    Ok(seen) if seen + 1 == version => Err(error.into()),
                    _ => {
                        *unsure = true;
                        Err(error.into())
                    }
                }
            }
        }
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

fn read_version(conn: &Connection) -> rusqlite::Result<Version> {
    conn.prepare_cached(&format!("SELECT version FROM {META_TABLE} WHERE id = 0"))?
        .query_row([], |r| r.get::<_, i64>(0))
        .map(|v| v as Version)
}

fn bump_version(conn: &Connection) -> rusqlite::Result<Version> {
    conn.prepare_cached(&format!(
        "UPDATE {META_TABLE} SET version = version + 1 WHERE id = 0 RETURNING version"
    ))?
    .query_row([], |r| r.get::<_, i64>(0))
    .map(|v| v as Version)
}

fn schema_version(conn: &Connection) -> rusqlite::Result<i64> {
    conn.query_row("PRAGMA schema_version", [], |r| r.get(0))
}

/// Reads each touched row back and compares it with its first old value.
fn net_changes(conn: &Connection, touched: Vec<Touch>) -> Result<Vec<RowChange>> {
    let mut changes = Vec::new();
    for touch in touched {
        let table = &touch.table;
        let current = read_back(conn, table, &touch.key)?;
        let row = |values: Vec<Raw>| {
            Row::new(
                table.stored.clone(),
                values.iter().map(Raw::to_value).collect(),
            )
        };
        let (op, old, new) = match (touch.first_old, current) {
            (None, None) => continue,
            (Some(old), Some(new)) if same_raws(&old, &new) => continue,
            (None, Some(new)) => (Op::Insert, None, Some(row(new))),
            (Some(old), None) => (Op::Delete, Some(row(old)), None),
            (Some(old), Some(new)) => (Op::Update, Some(row(old)), Some(row(new))),
        };
        changes.push(RowChange {
            table: table.name.clone(),
            op,
            rowid: match touch.key {
                Key::Rowid(rowid) => Some(rowid),
                Key::PrimaryKey(_) => None,
            },
            old,
            new,
        });
    }
    Ok(changes)
}

fn read_back(conn: &Connection, table: &Table, key: &Key) -> Result<Option<Vec<Raw>>> {
    if !table.without_rowid && table.rowid_name.is_none() {
        return Err(Error::Unsupported(format!(
            "table {} has columns named rowid, _rowid_ and oid, so its rowid cannot be read",
            table.name
        )));
    }
    let mut statement = conn.prepare_cached(&table.select_by_key)?;
    let mut rows = match key {
        Key::Rowid(rowid) => statement.query([rowid])?,
        Key::PrimaryKey(values) => statement.query(params_from_iter(values))?,
    };
    let Some(row) = rows.next()? else {
        return Ok(None);
    };
    let values = (0..table.stored.len())
        .map(|i| row.get_ref(i).map(Raw::from_ref))
        .collect::<rusqlite::Result<Vec<_>>>()?;
    Ok(Some(values))
}
