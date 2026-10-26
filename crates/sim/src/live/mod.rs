//! The live harness: SqLive on the simulated VFS, driven by random schemas
//! and write transactions, checked against SQLite itself.
//!
//! What it checks:
//! - each successful write returns the next version, and a failed write
//!   leaves the version where it was;
//! - replicas built only from commit events match a dump of the database
//!   whenever they have caught up, whether they follow every event at once
//!   or fall behind, lag, and rebuild.

mod replica;
mod schema;
mod writes;

use crate::prng::Prng;
use crate::trace::Trace;
use crate::vfs::{FaultConfig, IoStats, SimVfs};
use replica::{Replica, difference, dump, shapes};
use rusqlite::{Connection, OpenFlags, params_from_iter};
use schema::SchemaSpec;
use sqlive::{LiveDb, Options, Version};
use std::fmt;
use std::time::Duration;

const DB: &str = "live.db";

#[derive(Clone, Debug)]
pub struct LiveOptions {
    pub steps: u64,
    pub log_capacity: usize,
    pub checkpoint_pages: u32,
    /// How often the lagging replica catches up, per step.
    pub lazy_drain: f64,
    pub schema_change: f64,
    pub vacuum: f64,
    pub schema: SchemaSpec,
}

impl LiveOptions {
    pub fn from_seed(seed: u64) -> LiveOptions {
        let mut prng = Prng::stream(seed, "live options");
        LiveOptions {
            steps: prng.range(50..=250),
            log_capacity: *prng.pick(&[1, 4, 32, 4096]),
            checkpoint_pages: *prng.pick(&[2, 50, 1000]),
            lazy_drain: *prng.pick(&[0.02, 0.1, 0.5]),
            schema_change: *prng.pick(&[0.0, 0.01, 0.03]),
            vacuum: *prng.pick(&[0.0, 0.01]),
            schema: schema::generate(&mut Prng::stream(seed, "live schema")),
        }
    }
}

#[derive(Debug, Default)]
pub struct LiveSummary {
    pub writes: u64,
    pub failed_writes: u64,
    pub statements: u64,
    pub statement_errors: u64,
    pub events: u64,
    pub changes: u64,
    pub schema_events: u64,
    pub lagged: u64,
    pub rebuilds: u64,
    pub checks: u64,
    pub io: IoStats,
    pub digest: u64,
}

impl fmt::Display for LiveSummary {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{} writes ({} failed), {} statements ({} failed), {} events with {} changes ({} schema), \
             {} lags, {} rebuilds, {} replica checks, digest {:016x}",
            self.writes,
            self.failed_writes,
            self.statements,
            self.statement_errors,
            self.events,
            self.changes,
            self.schema_events,
            self.lagged,
            self.rebuilds,
            self.checks,
            self.digest
        )
    }
}

struct Live {
    options: LiveOptions,
    schema: SchemaSpec,
    prng: Prng,
    trace: Trace,
    serial: u64,
    summary: LiveSummary,
    replicas: Vec<Replica>,
    oracle: Option<Connection>,
    db: Option<LiveDb>,
    /// Declared last so it drops last: every connection closes before the
    /// VFS unregisters.
    vfs: SimVfs,
}

pub fn run(seed: u64, verbose: bool) -> Result<LiveSummary, String> {
    let options = LiveOptions::from_seed(seed);
    if verbose {
        eprintln!(
            "live options: {options:?}\nschema:\n{}",
            options.schema.ddl()
        );
    }
    let vfs = SimVfs::new(seed, FaultConfig::none());
    let mut live = Live {
        schema: options.schema.clone(),
        options,
        prng: Prng::stream(seed, "live steps"),
        trace: Trace::new(verbose),
        serial: 0,
        summary: LiveSummary::default(),
        replicas: vec![Replica::new("eager"), Replica::new("lazy")],
        oracle: None,
        db: None,
        vfs,
    };
    live.start()?;
    for step in 0..live.options.steps {
        live.step()
            .map_err(|message| format!("step {step}: {message}"))?;
    }
    for i in 0..live.replicas.len() {
        live.catch_up(i)
            .map_err(|message| format!("final check: {message}"))?;
    }
    live.summary.io = live.vfs.stats();
    live.summary.digest = live.trace.digest();
    Ok(live.summary)
}

impl Live {
    fn db(&self) -> &LiveDb {
        self.db.as_ref().expect("the database is open")
    }

    fn oracle(&self) -> &Connection {
        self.oracle.as_ref().expect("the oracle is open")
    }

    fn start(&mut self) -> Result<(), String> {
        let options = Options {
            vfs: Some(self.vfs.name().to_owned()),
            readers: 1,
            log_capacity: self.options.log_capacity,
            busy_timeout: Duration::from_secs(5),
            checkpoint_pages: self.options.checkpoint_pages,
        };
        let db = LiveDb::open_with(DB, options).map_err(|e| format!("open: {e}"))?;
        let ddl = self.schema.ddl();
        db.write(|tx| tx.execute_batch(&ddl))
            .map_err(|e| format!("schema: {e}\n{ddl}"))?;
        let oracle = Connection::open_with_flags_and_vfs(
            DB,
            OpenFlags::SQLITE_OPEN_READ_ONLY,
            self.vfs.name(),
        )
        .map_err(|e| format!("oracle: {e}"))?;
        self.db = Some(db);
        self.oracle = Some(oracle);
        for i in 0..self.replicas.len() {
            self.rebuild(i)?;
        }
        Ok(())
    }

    fn step(&mut self) -> Result<(), String> {
        if self.prng.chance(self.options.schema_change) {
            self.change_schema()?;
        } else if self.prng.chance(self.options.vacuum) {
            self.vacuum()?;
        } else {
            self.write()?;
        }
        self.catch_up(0)?;
        if self.prng.chance(self.options.lazy_drain) {
            self.catch_up(1)?;
        }
        Ok(())
    }

    fn write(&mut self) -> Result<(), String> {
        let txn = writes::transaction(&mut self.prng, &self.schema);
        let before = self.db().version();
        let mut outcomes = Vec::new();
        let result = self.db().write(|tx| {
            for statement in &txn.statements {
                match tx.execute(&statement.sql, params_from_iter(&statement.params)) {
                    Ok(n) => outcomes.push(format!("{n}")),
                    Err(e) => {
                        outcomes.push(format!("error {:?}", e.sqlite_error_code()));
                        if txn.on_error == writes::OnError::Abort {
                            return Err(e);
                        }
                    }
                }
            }
            if txn.fail_at_end {
                return Err(rusqlite::Error::InvalidQuery);
            }
            Ok(())
        });
        self.summary.statements += outcomes.len() as u64;
        self.summary.statement_errors +=
            outcomes.iter().filter(|o| o.starts_with("error")).count() as u64;
        for (statement, outcome) in txn.statements.iter().zip(&outcomes) {
            self.trace.record(format_args!(
                "  {} {:?} -> {outcome}",
                statement.sql, statement.params
            ));
        }
        self.finish_write(
            before,
            result
                .map(|((), version)| version)
                .map_err(|e| e.to_string()),
        )
    }

    /// Checks a write's version against the one before it.
    fn finish_write(
        &mut self,
        before: Version,
        result: Result<Version, String>,
    ) -> Result<(), String> {
        let now = self.db().version();
        match result {
            Ok(version) => {
                self.trace
                    .record(format_args!("write -> version {version}"));
                self.summary.writes += 1;
                if version != before + 1 || now != version {
                    return Err(format!(
                        "a write after version {before} returned {version}; the database reports {now}"
                    ));
                }
            }
            Err(e) => {
                self.trace.record(format_args!("write failed: {e}"));
                self.summary.failed_writes += 1;
                if now != before {
                    return Err(format!(
                        "a failed write moved the version from {before} to {now}"
                    ));
                }
            }
        }
        Ok(())
    }

    fn change_schema(&mut self) -> Result<(), String> {
        self.serial += 1;
        let statement = writes::schema_change(&mut self.prng, &mut self.schema, self.serial);
        self.trace
            .record(format_args!("schema change: {}", statement.sql));
        let before = self.db().version();
        let result = self
            .db()
            .write(|tx| tx.execute_batch(&statement.sql))
            .map(|((), version)| version)
            .map_err(|e| e.to_string());
        self.finish_write(before, result)
    }

    fn vacuum(&mut self) -> Result<(), String> {
        self.trace.record("vacuum");
        let before = self.db().version();
        let result = self.db().vacuum().map_err(|e| e.to_string());
        self.finish_write(before, result)
    }

    fn rebuild(&mut self, i: usize) -> Result<(), String> {
        let (db, oracle) = (
            self.db.as_ref().expect("open"),
            self.oracle.as_ref().expect("open"),
        );
        self.replicas[i].rebuild(oracle, db)?;
        self.summary.rebuilds += 1;
        self.trace.record(format_args!(
            "{} replica rebuilt at {}",
            self.replicas[i].name, self.replicas[i].version
        ));
        Ok(())
    }

    /// Applies every committed event to replica `i`, rebuilding it when it
    /// lags or sees a schema change, then compares it with the database.
    fn catch_up(&mut self, i: usize) -> Result<(), String> {
        loop {
            if self.replicas[i].stale {
                self.rebuild(i)?;
            }
            let drained = self.replicas[i].drain(usize::MAX)?;
            self.summary.events += drained.events;
            self.summary.changes += drained.changes;
            self.summary.schema_events += u64::from(drained.schema_changed);
            if drained.lagged {
                self.summary.lagged += 1;
                self.trace
                    .record(format_args!("{} replica lagged", self.replicas[i].name));
            }
            if !self.replicas[i].stale {
                break;
            }
        }
        self.check(i)
    }

    fn check(&mut self, i: usize) -> Result<(), String> {
        let replica = &self.replicas[i];
        let latest = self.db().version();
        if replica.version != latest {
            return Err(format!(
                "{} replica drained to version {} but the database is at {latest}",
                replica.name, replica.version
            ));
        }
        let oracle = self.oracle();
        let read = (|| -> rusqlite::Result<(i64, replica::Dump)> {
            oracle.execute_batch("BEGIN")?;
            let version: i64 =
                oracle.query_row("SELECT version FROM _sqlive_meta WHERE id = 0", [], |r| {
                    r.get(0)
                })?;
            let shapes = shapes(oracle)?;
            let dump = dump(oracle, &shapes, "rowid")?;
            oracle.execute_batch("COMMIT")?;
            Ok((version, dump))
        })();
        let (version, expected) = read.map_err(|e| format!("oracle read: {e}"))?;
        if version as Version != latest {
            return Err(format!(
                "the oracle read version {version}, the log reports {latest}"
            ));
        }
        let actual = replica.dump().map_err(|e| format!("replica dump: {e}"))?;
        if let Some(diff) = difference(&expected, &actual) {
            return Err(format!(
                "{} replica at version {latest} differs: {diff}",
                replica.name
            ));
        }
        self.summary.checks += 1;
        Ok(())
    }
}
