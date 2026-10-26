//! The storage harness: plain SQLite on the simulated VFS, in WAL mode with
//! one writer and several readers, checked against a row model through
//! crashes, clean reopens and injected faults.
//!
//! Every transaction bumps a version row, so the database names the commit
//! it holds. What the model checks:
//! - a reader's snapshot answers every read with the rows of the version it
//!   started at, whatever commits since;
//! - after a crash or reopen, the database holds either the last
//!   acknowledged commit or a commit whose `COMMIT` failed after it, exactly;
//! - `PRAGMA integrity_check` passes after every restart.
//!
//! A failed `COMMIT` leaves its outcome open: SQLite hides it from this
//! process, but its frames may have reached the WAL and come back after a
//! crash. The harness reads the version back to learn what the process sees,
//! and keeps the hidden commit as a candidate for recovery.

use crate::prng::Prng;
use crate::trace::Trace;
use crate::vfs::{FaultConfig, IoStats, SimVfs};
use rusqlite::{Connection, OpenFlags, OptionalExtension, params};
use std::collections::BTreeMap;
use std::fmt;
use std::rc::Rc;

type Rows = BTreeMap<i64, Vec<u8>>;

/// A write's planned puts (`Some`) and deletes (`None`), in order.
type Plan = Vec<(i64, Option<Vec<u8>>)>;

#[derive(Clone, Debug)]
pub struct StorageOptions {
    pub steps: u64,
    pub page_size: u32,
    pub readers: usize,
    pub autocheckpoint: u32,
    pub cache_pages: i64,
    pub keys: i64,
    pub max_value: usize,
    pub crash_chance: f64,
    pub reopen_chance: f64,
    pub faults: FaultConfig,
}

impl StorageOptions {
    pub fn from_seed(seed: u64) -> StorageOptions {
        let mut prng = Prng::stream(seed, "storage options");
        StorageOptions {
            steps: prng.range(100..=400),
            page_size: *prng.pick(&[512, 1024, 4096, 8192]),
            readers: prng.range(1..=3) as usize,
            autocheckpoint: *prng.pick(&[0, 4, 32, 1000]),
            cache_pages: *prng.pick(&[10, 100, 2000]),
            keys: prng.range(4..=300) as i64,
            max_value: *prng.pick(&[0, 64, 600, 5000]),
            crash_chance: *prng.pick(&[0.0, 0.01, 0.05]),
            reopen_chance: *prng.pick(&[0.0, 0.01]),
            faults: FaultConfig::from_prng(&mut prng),
        }
    }
}

#[derive(Debug, Default)]
pub struct StorageSummary {
    pub commits: u64,
    pub failed_commits: u64,
    pub recovered_hidden: u64,
    pub rollbacks: u64,
    pub reads: u64,
    pub read_errors: u64,
    pub restarts: u64,
    pub io: IoStats,
    pub digest: u64,
}

impl fmt::Display for StorageSummary {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{} commits ({} failed, {} recovered after failing), {} rollbacks, {} reads ({} failed), \
             {} restarts, {} crashes, {} sectors lost, I/O: {} reads {} writes {} syncs, \
             I/O errors: {} read {} write {} sync {} busy, digest {:016x}",
            self.commits,
            self.failed_commits,
            self.recovered_hidden,
            self.rollbacks,
            self.reads,
            self.read_errors,
            self.restarts,
            self.io.crashes,
            self.io.sectors_lost,
            self.io.reads,
            self.io.writes,
            self.io.syncs,
            self.io.read_errors,
            self.io.write_errors,
            self.io.sync_errors,
            self.io.busy,
            self.digest
        )
    }
}

struct Snapshot {
    version: u64,
    rows: Rc<Rows>,
}

struct Reader {
    conn: Connection,
    snapshot: Option<Snapshot>,
}

struct Harness {
    options: StorageOptions,
    prng: Prng,
    trace: Trace,
    writer: Option<Connection>,
    readers: Vec<Reader>,
    /// The last commit this process saw take effect, and its rows.
    acked: u64,
    rows: Rc<Rows>,
    /// Commits whose `COMMIT` failed and that this process cannot see. A
    /// restart may recover one of them.
    hidden: Vec<(u64, Rows)>,
    /// A failure left the process unsure of the database's state.
    must_restart: bool,
    faults: bool,
    summary: StorageSummary,
    /// Declared last so it drops last: every connection closes before the
    /// VFS unregisters.
    vfs: SimVfs,
}

const DB: &str = "storage.db";

pub fn run(seed: u64, verbose: bool) -> Result<StorageSummary, String> {
    let options = StorageOptions::from_seed(seed);
    if verbose {
        eprintln!("storage options: {options:?}");
    }
    let vfs = SimVfs::new(seed, options.faults.clone());
    let mut harness = Harness {
        prng: Prng::stream(seed, "storage steps"),
        trace: Trace::new(verbose),
        writer: None,
        readers: Vec::new(),
        acked: 0,
        rows: Rc::new(Rows::new()),
        hidden: Vec::new(),
        must_restart: false,
        faults: false,
        summary: StorageSummary::default(),
        options,
        vfs,
    };
    harness.create()?;
    harness.set_faults(true);
    for step in 0..harness.options.steps {
        harness
            .step()
            .map_err(|message| format!("step {step}: {message}"))?;
    }
    harness.set_faults(false);
    harness
        .restart(false)
        .map_err(|message| format!("final reopen: {message}"))?;
    harness.close();
    harness.summary.io = harness.vfs.stats();
    harness.summary.digest = harness.trace.digest();
    Ok(harness.summary)
}

impl Harness {
    fn open(&self, read_only: bool) -> rusqlite::Result<Connection> {
        let flags = if read_only {
            OpenFlags::SQLITE_OPEN_READ_ONLY
        } else {
            OpenFlags::SQLITE_OPEN_READ_WRITE | OpenFlags::SQLITE_OPEN_CREATE
        };
        let conn = Connection::open_with_flags_and_vfs(DB, flags, self.vfs.name())?;
        conn.busy_timeout(std::time::Duration::from_secs(5))?;
        conn.pragma_update(None, "cache_size", self.options.cache_pages)?;
        if !read_only {
            conn.pragma_update(None, "synchronous", "FULL")?;
            conn.pragma_update(None, "wal_autocheckpoint", self.options.autocheckpoint)?;
        }
        Ok(conn)
    }

    fn create(&mut self) -> Result<(), String> {
        let conn = self.open(false).map_err(|e| format!("create: {e}"))?;
        conn.pragma_update(None, "page_size", self.options.page_size)
            .and_then(|()| conn.pragma_update(None, "journal_mode", "WAL"))
            .and_then(|()| {
                conn.execute_batch(
                    "CREATE TABLE kv (k INTEGER PRIMARY KEY, v BLOB NOT NULL);
                     CREATE INDEX kv_by_value ON kv (v);
                     CREATE TABLE meta (id INTEGER PRIMARY KEY, version INTEGER NOT NULL);
                     INSERT INTO meta VALUES (0, 0);",
                )
            })
            .map_err(|e| format!("create: {e}"))?;
        self.writer = Some(conn);
        self.open_readers()
            .map_err(|e| format!("open readers: {e}"))
    }

    fn open_readers(&mut self) -> rusqlite::Result<()> {
        for _ in 0..self.options.readers {
            let conn = self.open(true)?;
            self.readers.push(Reader {
                conn,
                snapshot: None,
            });
        }
        Ok(())
    }

    fn close(&mut self) {
        self.readers.clear();
        self.writer = None;
    }

    fn set_faults(&mut self, on: bool) {
        self.faults = on;
        self.vfs.set_faults(on);
    }

    fn writer(&self) -> &Connection {
        self.writer.as_ref().expect("the writer is open")
    }

    fn step(&mut self) -> Result<(), String> {
        if self.must_restart || self.prng.chance(self.options.crash_chance) {
            return self.restart(true);
        }
        if self.prng.chance(self.options.reopen_chance) {
            return self.restart(false);
        }
        match self.prng.below(10) {
            0..=3 => self.write(),
            4..=8 => self.read(),
            _ => {
                self.checkpoint();
                Ok(())
            }
        }
    }

    fn value(&mut self) -> Vec<u8> {
        let len = self.prng.range(0..=self.options.max_value as u64) as usize;
        let mut value = vec![0u8; len];
        self.prng.fill(&mut value);
        value
    }

    fn write(&mut self) -> Result<(), String> {
        let mut plan = Plan::new();
        for _ in 0..self.prng.range(1..=12) {
            let key = self.prng.range(0..=self.options.keys as u64) as i64;
            let value = (!self.prng.chance(0.3)).then(|| self.value());
            plan.push((key, value));
        }
        let version = self.acked + 1;
        let writer = self.writer();
        let body = (|| -> rusqlite::Result<()> {
            writer.execute_batch("BEGIN IMMEDIATE")?;
            for (key, value) in &plan {
                match value {
                    Some(value) => writer.execute(
                        "INSERT INTO kv (k, v) VALUES (?1, ?2) ON CONFLICT (k) DO UPDATE SET v = excluded.v",
                        params![key, value],
                    )?,
                    None => writer.execute("DELETE FROM kv WHERE k = ?1", [key])?,
                };
            }
            writer.execute(
                "UPDATE meta SET version = ?1 WHERE id = 0",
                [version as i64],
            )?;
            Ok(())
        })();
        if let Err(e) = body {
            self.trace
                .record(format_args!("write {version} rolled back: {e}"));
            self.summary.rollbacks += 1;
            self.rollback();
            return Ok(());
        }
        match writer.execute_batch("COMMIT") {
            Ok(()) => {
                self.trace.record(format_args!("commit {version}"));
                self.summary.commits += 1;
                self.acked = version;
                apply(Rc::make_mut(&mut self.rows), plan);
                self.hidden.clear();
            }
            Err(e) => {
                self.trace
                    .record(format_args!("commit {version} failed: {e}"));
                self.summary.failed_commits += 1;
                self.rollback();
                let mut next = (*self.rows).clone();
                apply(&mut next, plan);
                // What does this process see now?
                match self
                    .writer()
                    .query_row("SELECT version FROM meta WHERE id = 0", [], |r| {
                        r.get::<_, i64>(0)
                    }) {
                    Ok(seen) if seen as u64 == version => {
                        self.acked = version;
                        self.rows = Rc::new(next);
                        self.hidden.clear();
                    }
                    Ok(seen) if seen as u64 == self.acked => self.hidden.push((version, next)),
                    Ok(seen) => {
                        return Err(format!(
                            "after a failed commit {version}, the writer sees version {seen}"
                        ));
                    }
                    Err(_) => {
                        self.hidden.push((version, next));
                        self.must_restart = true;
                    }
                }
            }
        }
        Ok(())
    }

    fn rollback(&mut self) {
        let writer = self.writer();
        if !writer.is_autocommit()
            && writer.execute_batch("ROLLBACK").is_err()
            && !writer.is_autocommit()
        {
            self.must_restart = true;
        }
    }

    fn read(&mut self) -> Result<(), String> {
        let index = self.prng.index(self.readers.len());
        let action = self.prng.below(6);
        let keys = self.options.keys as u64;
        let probe: Vec<i64> = (0..self.prng.range(1..=5))
            .map(|_| self.prng.range(0..=keys) as i64)
            .collect();
        self.summary.reads += 1;
        match self.read_with(index, action, &probe) {
            Ok(verdict) => {
                self.trace
                    .record(format_args!("reader {index} read: {verdict:?}"));
                verdict
            }
            Err(e) => {
                self.trace
                    .record(format_args!("reader {index} failed: {e}"));
                self.summary.read_errors += 1;
                let reader = &mut self.readers[index];
                reader.snapshot = None;
                if !reader.conn.is_autocommit() {
                    let _ = reader.conn.execute_batch("ROLLBACK");
                    if !reader.conn.is_autocommit() {
                        self.must_restart = true;
                    }
                }
                Ok(())
            }
        }
    }

    /// One reader step: start a snapshot, end it, scan, or look keys up.
    /// The outer result is SQLite's; the inner one is the model's verdict.
    fn read_with(
        &mut self,
        index: usize,
        action: u64,
        probe: &[i64],
    ) -> rusqlite::Result<Result<(), String>> {
        let reader = &mut self.readers[index];
        let Some(snapshot) = &reader.snapshot else {
            reader.conn.execute_batch("BEGIN")?;
            let version: i64 =
                reader
                    .conn
                    .query_row("SELECT version FROM meta WHERE id = 0", [], |r| r.get(0))?;
            let version = version as u64;
            if version != self.acked {
                return Ok(Err(format!(
                    "reader {index} started at version {version} but {} is the latest",
                    self.acked
                )));
            }
            reader.snapshot = Some(Snapshot {
                version,
                rows: self.rows.clone(),
            });
            return Ok(Ok(()));
        };
        let version = snapshot.version;
        if action == 0 {
            reader.snapshot = None;
            reader.conn.execute_batch("COMMIT")?;
            return Ok(Ok(()));
        }
        if action == 1 {
            let found = scan(&reader.conn)?;
            return Ok(if found == *snapshot.rows {
                Ok(())
            } else {
                Err(format!(
                    "reader {index} at version {version} scanned {} rows, expected {}",
                    found.len(),
                    snapshot.rows.len()
                ))
            });
        }
        for key in probe {
            let found = lookup(&reader.conn, *key)?;
            let expected = snapshot.rows.get(key);
            if found.as_ref() != expected {
                return Ok(Err(format!(
                    "reader {index} at version {version} read key {key} as {:?} bytes, expected {:?}",
                    found.map(|v| v.len()),
                    expected.map(Vec::len)
                )));
            }
        }
        Ok(Ok(()))
    }

    fn checkpoint(&mut self) {
        let mode = *self.prng.pick(&["PASSIVE", "FULL", "RESTART", "TRUNCATE"]);
        let result = self
            .writer()
            .query_row(&format!("PRAGMA wal_checkpoint({mode})"), [], |r| {
                Ok((
                    r.get::<_, i64>(0)?,
                    r.get::<_, i64>(1)?,
                    r.get::<_, i64>(2)?,
                ))
            });
        self.trace
            .record(format_args!("checkpoint {mode}: {}", result.is_ok()));
    }

    /// Closes every connection, cleanly or by crashing, reopens, and checks
    /// what the database recovered. Leaves fault injection as it found it.
    fn restart(&mut self, crash: bool) -> Result<(), String> {
        self.summary.restarts += 1;
        let faults = self.faults;
        self.set_faults(false);
        if crash {
            self.vfs.begin_crash();
            self.close();
            self.vfs.finish_crash();
            let lost = self.vfs.stats().sectors_lost;
            self.trace
                .record(format_args!("crash: {lost} sectors lost so far"));
        } else {
            self.close();
        }
        self.must_restart = false;
        let conn = self.open(false).map_err(|e| format!("reopen: {e}"))?;
        let check: String = conn
            .query_row("PRAGMA integrity_check", [], |r| r.get(0))
            .map_err(|e| format!("integrity check: {e}"))?;
        if check != "ok" {
            return Err(format!("integrity check after restart: {check}"));
        }
        let version: i64 = conn
            .query_row("SELECT version FROM meta WHERE id = 0", [], |r| r.get(0))
            .map_err(|e| format!("version after restart: {e}"))?;
        let version = version as u64;
        let rows = scan(&conn).map_err(|e| format!("scan after restart: {e}"))?;
        if version == self.acked && rows == *self.rows {
            self.trace
                .record(format_args!("restart (crash {crash}) recovered {version}"));
        } else if version > self.acked
            && self.hidden.iter().any(|(v, r)| *v == version && *r == rows)
        {
            self.trace.record(format_args!(
                "restart (crash {crash}) recovered failed commit {version}"
            ));
            self.summary.recovered_hidden += 1;
            self.acked = version;
            self.rows = Rc::new(rows);
        } else if version < self.acked {
            return Err(format!(
                "restart (crash {crash}) lost acknowledged commits: recovered {version}, acknowledged {}",
                self.acked
            ));
        } else {
            return Err(format!(
                "restart (crash {crash}) recovered version {version} with {} rows; expected {} with {} rows{}",
                rows.len(),
                self.acked,
                self.rows.len(),
                if self.hidden.is_empty() {
                    ""
                } else {
                    " or a failed commit"
                }
            ));
        }
        self.hidden.clear();
        self.writer = Some(conn);
        self.open_readers()
            .map_err(|e| format!("open readers: {e}"))?;
        self.set_faults(faults);
        Ok(())
    }
}

fn apply(rows: &mut Rows, plan: Plan) {
    for (key, value) in plan {
        match value {
            Some(value) => rows.insert(key, value),
            None => rows.remove(&key),
        };
    }
}

fn scan(conn: &Connection) -> rusqlite::Result<Rows> {
    let mut statement = conn.prepare("SELECT k, v FROM kv ORDER BY k")?;
    let rows = statement.query_map([], |r| Ok((r.get(0)?, r.get(1)?)))?;
    rows.collect()
}

fn lookup(conn: &Connection, key: i64) -> rusqlite::Result<Option<Vec<u8>>> {
    conn.query_row("SELECT v FROM kv WHERE k = ?1", [key], |r| r.get(0))
        .optional()
}
