//! The live harness: SqLive on the simulated VFS, driven by random schemas
//! and write transactions, checked against SQLite itself.
//!
//! What it checks:
//! - each successful write returns the next version, and a failed write
//!   leaves the version where it was, unless an earlier failure left the
//!   outcome unknown;
//! - nothing a write does is visible to other connections before it commits;
//! - replicas built only from commit events match a dump of the database
//!   whenever they have caught up, whether they follow every event at once
//!   or fall behind, lag, and rebuild; a replica reloads each table an
//!   event names whole, at whatever version the database has reached, and
//!   applies the rows the event lists for the rest; no event lists more
//!   rows of a table than the cap allows;
//! - a replica that holds every commit receives the next one, whatever the
//!   log's bounds;
//! - every published commit survives a crash, and after a restart each
//!   replica either resumes or lags and rebuilds;
//! - a clean close leaves no WAL behind;
//! - a subscription that re-runs its query whenever its read set intersects
//!   an event holds the same rows as the query run fresh, once it has
//!   processed every event, whether it processes them at once or late and
//!   out of order, and whether it started from a snapshot held across
//!   writes;
//! - a snapshot held across writes returns the rows of its version;
//! - an index of every subscription's read set finds, for each event,
//!   exactly the read sets that intersect it, both before the subscriptions
//!   process the event and once their newer read sets are indexed.
//!
//! The oracle, a plain read-only connection, reads with fault injection
//! suspended, so the checks themselves never fail on an injected error.
//! Peeks and restarts draw from their own PRNG streams, so tuning them leaves
//! the workload unchanged.

mod queries;
mod replica;
mod schema;
mod subscriptions;
mod writes;

use crate::prng::Prng;
use crate::trace::Trace;
use crate::vfs::{FaultConfig, IoStats, SimVfs};
use replica::{Dump, Replica, difference};
use rusqlite::{Connection, OpenFlags, params_from_iter};
use schema::SchemaSpec;
use sqlive::{Changes, CommitEvent, LiveDb, Options, ReadSetIndex, Version};
use std::collections::BTreeMap;
use std::fmt;
use std::sync::Arc;
use std::time::Duration;
use subscriptions::{Held, Kind, Subscription, Tally, fresh, same_rows};

const DB: &str = "live.db";

#[derive(Clone, Debug)]
pub struct LiveOptions {
    pub steps: u64,
    pub log_capacity: usize,
    /// The bytes of events the log retains.
    pub log_bytes: usize,
    /// The rows of one table an event lists.
    pub table_changes: usize,
    pub checkpoint_pages: u32,
    /// How often the lagging replica catches up, per step.
    pub lazy_drain: f64,
    pub schema_change: f64,
    pub vacuum: f64,
    pub crash: f64,
    pub reopen: f64,
    /// How often a write's closure checks that its changes stay invisible.
    pub peek: f64,
    /// Subscriptions created at the start.
    pub subscriptions: u64,
    /// How often a step holds a snapshot across its writes.
    pub snapshot: f64,
    pub faults: FaultConfig,
    pub schema: SchemaSpec,
}

impl LiveOptions {
    pub fn from_seed(seed: u64) -> LiveOptions {
        let mut prng = Prng::stream(seed, "live options");
        let mut subscriptions = Prng::stream(seed, "live subscriptions");
        let mut bounds = Prng::stream(seed, "live event bounds");
        LiveOptions {
            steps: prng.range(50..=250),
            log_capacity: *prng.pick(&[1, 4, 32, 4096]),
            log_bytes: *bounds.pick(&[1_500, 30_000, 64 << 20, 64 << 20]),
            table_changes: *bounds.pick(&[3, 20, 10_000, 10_000]),
            checkpoint_pages: *prng.pick(&[2, 50, 1000]),
            lazy_drain: *prng.pick(&[0.02, 0.1, 0.5]),
            schema_change: *prng.pick(&[0.0, 0.01, 0.03]),
            vacuum: *prng.pick(&[0.0, 0.01]),
            crash: *prng.pick(&[0.0, 0.01, 0.03]),
            reopen: *prng.pick(&[0.0, 0.01]),
            peek: *prng.pick(&[0.05, 0.3]),
            faults: FaultConfig::from_prng(&mut prng),
            schema: {
                let mut schema = schema::generate(&mut Prng::stream(seed, "live schema"));
                schema.join_views = schema::join_views(
                    &mut Prng::stream(seed, "live schema join views"),
                    &schema.tables,
                );
                schema.full_text = schema::full_text(
                    &mut Prng::stream(seed, "live schema full text"),
                    &schema.tables,
                );
                schema
            },
            subscriptions: subscriptions.range(1..=8),
            snapshot: *subscriptions.pick(&[0.02, 0.1]),
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
    /// Tables that events named whole, as the replicas received them.
    pub whole_tables: u64,
    /// Times a replica reloaded the tables an event named whole.
    pub reloads: u64,
    pub checks: u64,
    pub skipped_checks: u64,
    pub peeks: u64,
    pub restarts: u64,
    /// Writes whose transaction SQLite rolled back after an error.
    pub rolled_back: u64,
    /// Restarts that recovered a commit this process never published.
    pub recovered_unpublished: u64,
    pub subscriptions: Tally,
    pub subscription_checks: u64,
    pub snapshot_checks: u64,
    /// Events whose affected read sets the index found.
    pub index_checks: u64,
    /// Read sets the index found across those events.
    pub index_hits: u64,
    /// Times the index was checked against an event again, after read sets
    /// that already reflect it joined the index.
    pub index_rechecks: u64,
    pub io: IoStats,
    pub digest: u64,
}

impl fmt::Display for LiveSummary {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{} writes ({} failed), {} statements ({} failed), {} events with {} changes ({} schema), \
             {} lags, {} rebuilds, {} whole tables with {} reloads, {} replica checks ({} skipped), {} peeks, {} restarts ({} crashes, {} recovered unpublished commits), \
             {} rolled back, \
             {} subscription checks, {} queries ({} constrained, {} windowed, {} of every commit; by shape {:?}), {} of {} events intersected, {} re-runs ({} unchanged), {} subscription lags, {} failed queries ({} snapshots ended), {} snapshot checks, {} index checks ({} hits, {} of an earlier event), \
             I/O errors: {} read {} write {} sync {} busy, digest {:016x}",
            self.writes,
            self.failed_writes,
            self.statements,
            self.statement_errors,
            self.events,
            self.changes,
            self.schema_events,
            self.lagged,
            self.rebuilds,
            self.whole_tables,
            self.reloads,
            self.checks,
            self.skipped_checks,
            self.peeks,
            self.restarts,
            self.io.crashes,
            self.recovered_unpublished,
            self.rolled_back,
            self.subscription_checks,
            self.subscriptions.queries,
            self.subscriptions.constrained,
            self.subscriptions.windowed,
            self.subscriptions.every_commit,
            self.subscriptions.shapes,
            self.subscriptions.intersected,
            self.subscriptions.tested,
            self.subscriptions.reruns,
            self.subscriptions.unchanged,
            self.subscriptions.lagged,
            self.subscriptions.failed_queries,
            self.subscriptions.ended,
            self.snapshot_checks,
            self.index_checks,
            self.index_hits,
            self.index_rechecks,
            self.io.read_errors,
            self.io.write_errors,
            self.io.sync_errors,
            self.io.busy,
            self.digest
        )
    }
}

/// A snapshot held across a step's write, with the query it will run.
struct Hold<'d> {
    snapshot: sqlive::Snapshot<'d>,
    query: queries::QuerySpec,
}

struct Live {
    options: LiveOptions,
    schema: SchemaSpec,
    prng: Prng,
    peeks: Prng,
    restarts: Prng,
    queries: Prng,
    /// Query shapes added after the first, drawn apart so existing seeds
    /// keep their queries.
    query_shapes: Prng,
    /// The shapes added after those, each from its own stream.
    more_shapes: queries::Shapes,
    /// Writes to the full-text tables.
    full_text: Prng,
    trace: Trace,
    serial: u64,
    summary: LiveSummary,
    /// A write failed in a way that may have committed: the next write may
    /// skip versions, and the database may hold a commit the log lacks.
    unknown: bool,
    /// The newest version this process published.
    published: Version,
    replicas: Vec<Replica>,
    subscriptions: Vec<Subscription>,
    next_subscription: u64,
    /// Every held read set, by subscription id.
    index: ReadSetIndex<u64>,
    /// The harness's own copy of each indexed read set, which the index's
    /// answers are checked against.
    indexed: BTreeMap<u64, sqlive::ReadSet>,
    /// Every event, for checking the index.
    index_stream: Option<Changes>,
    /// The last event the index was checked against.
    last_indexed: Option<Arc<CommitEvent>>,
    oracle: Option<Connection>,
    /// A second read-only connection, which holds a read transaction beside
    /// a snapshot.
    witness: Option<Connection>,
    db: Option<Arc<LiveDb>>,
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
    let vfs = SimVfs::new(seed, options.faults.clone());
    let mut live = Live {
        schema: options.schema.clone(),
        options,
        prng: Prng::stream(seed, "live steps"),
        peeks: Prng::stream(seed, "live peeks"),
        restarts: Prng::stream(seed, "live restarts"),
        queries: Prng::stream(seed, "live queries"),
        query_shapes: Prng::stream(seed, "live query shapes"),
        more_shapes: queries::Shapes::from_seed(seed),
        full_text: Prng::stream(seed, "live full text writes"),
        trace: Trace::new(verbose),
        serial: 0,
        summary: LiveSummary::default(),
        unknown: false,
        published: 0,
        replicas: Vec::new(),
        subscriptions: Vec::new(),
        next_subscription: 0,
        index: ReadSetIndex::new(),
        indexed: BTreeMap::new(),
        index_stream: None,
        last_indexed: None,
        oracle: None,
        witness: None,
        db: None,
        vfs,
    };
    let cap = live.options.table_changes;
    live.replicas = vec![Replica::new("eager", cap), Replica::new("lazy", cap)];
    live.start()?;
    live.vfs.set_faults(true);
    for step in 0..live.options.steps {
        live.step()
            .map_err(|message| format!("step {step}: {message}"))?;
    }
    live.vfs.set_faults(false);
    for i in 0..live.replicas.len() {
        live.catch_up(i)
            .map_err(|message| format!("final check: {message}"))?;
    }
    live.settle()
        .map_err(|message| format!("final check: {message}"))?;
    live.close()
        .map_err(|message| format!("final close: {message}"))?;
    live.summary.io = live.vfs.stats();
    live.summary.digest = live.trace.digest();
    Ok(live.summary)
}

impl Live {
    fn db(&self) -> &LiveDb {
        self.db.as_deref().expect("the database is open")
    }

    fn oracle(&self) -> &Connection {
        self.oracle.as_ref().expect("the oracle is open")
    }

    /// The database's version and rows, read on the oracle.
    fn read(&self) -> Result<(Version, Dump), String> {
        self.vfs
            .quietly(|| replica::read(self.oracle()))
            .map(|(version, _, dump)| (version, dump))
            .map_err(|e| format!("oracle read: {e}"))
    }

    fn open(&mut self) -> Result<(), String> {
        let options = Options {
            vfs: Some(self.vfs.name().to_owned()),
            readers: 2,
            log_capacity: self.options.log_capacity,
            log_bytes: self.options.log_bytes,
            table_changes: self.options.table_changes,
            busy_timeout: Duration::from_secs(5),
            checkpoint_pages: self.options.checkpoint_pages,
        };
        let db = LiveDb::open_with(DB, options).map_err(|e| format!("open: {e}"))?;
        let reader = || {
            Connection::open_with_flags_and_vfs(
                DB,
                OpenFlags::SQLITE_OPEN_READ_ONLY,
                self.vfs.name(),
            )
            .map_err(|e| format!("oracle: {e}"))
        };
        self.oracle = Some(reader()?);
        self.witness = Some(reader()?);
        self.index_stream = Some(db.changes().map_err(|e| format!("index stream: {e}"))?);
        self.db = Some(Arc::new(db));
        Ok(())
    }

    fn start(&mut self) -> Result<(), String> {
        self.open()?;
        let ddl = self.schema.ddl();
        let ((), version) = self
            .db()
            .write(|tx| tx.execute_batch(&ddl))
            .map_err(|e| format!("schema: {e}\n{ddl}"))?;
        self.published = version;
        for i in 0..self.replicas.len() {
            self.rebuild(i)?;
        }
        for _ in 0..self.options.subscriptions {
            self.subscribe(None)?;
        }
        Ok(())
    }

    /// Closes every connection, the read-only ones first. Fails if a handle
    /// to the database outlives it.
    fn drop_connections(&mut self) -> Result<(), String> {
        self.oracle = None;
        self.witness = None;
        match self.db.take().map(Arc::into_inner) {
            Some(None) => Err("a handle to the database outlived its close".into()),
            _ => Ok(()),
        }
    }

    /// Closes every connection and checks that the WAL is gone: the last
    /// connection to close checkpoints it into the database and deletes it.
    fn close(&mut self) -> Result<(), String> {
        self.drop_connections()?;
        let wal = format!("{DB}-wal");
        if self.vfs.file_names().contains(&wal) {
            return Err(format!("a clean close left {wal} behind"));
        }
        Ok(())
    }

    /// Closes the database, cleanly or by crashing, reopens it, and checks
    /// that it is intact and that every published commit survived. Replicas
    /// keep their state and follow the reopened database from their version.
    fn restart(&mut self, crash: bool) -> Result<(), String> {
        self.summary.restarts += 1;
        let faults = self.vfs.faults();
        self.vfs.set_faults(false);
        if crash {
            self.vfs.begin_crash();
            self.drop_connections()?;
            self.vfs.finish_crash();
        } else {
            self.close()?;
        }
        self.open()?;
        let integrity: String = self
            .oracle()
            .query_row("PRAGMA integrity_check", [], |r| r.get(0))
            .map_err(|e| format!("integrity check: {e}"))?;
        if integrity != "ok" {
            return Err(format!(
                "restart (crash {crash}) left the database corrupt: {integrity}"
            ));
        }
        let recovered = self.db().version();
        self.trace.record(format_args!(
            "restart (crash {crash}) recovered {recovered}"
        ));
        if recovered < self.published {
            return Err(format!(
                "restart (crash {crash}) recovered version {recovered}, but version {} was published",
                self.published
            ));
        }
        if recovered > self.published {
            self.summary.recovered_unpublished += 1;
        }
        self.unknown = false;
        let db = self.db.as_deref().expect("just opened");
        for replica in &mut self.replicas {
            replica.resume(db)?;
        }
        for subscription in &mut self.subscriptions {
            subscription.resume(db)?;
        }
        self.vfs.set_faults(faults);
        Ok(())
    }

    fn step(&mut self) -> Result<(), String> {
        let mut restarted = true;
        if self.restarts.chance(self.options.crash) {
            self.restart(true)?;
        } else if self.restarts.chance(self.options.reopen) {
            self.restart(false)?;
        } else {
            restarted = false;
        }
        let db = self.db.clone().expect("open");
        // The log keeps its newest event, so a replica that holds every
        // commit so far receives this step's.
        let before = db.version();
        let eager = &self.replicas[0];
        let current = !restarted && !self.unknown && !eager.stale && eager.version == before;
        let hold = if self.queries.chance(self.options.snapshot) {
            self.hold(&db)?
        } else {
            None
        };
        if self.prng.chance(self.options.schema_change) {
            self.change_schema()?;
        } else if self.prng.chance(self.options.vacuum) {
            self.vacuum()?;
        } else {
            self.write()?;
        }
        if let Some(hold) = hold {
            self.check_hold(hold)?;
        }
        let lagged = self.catch_up(0)?;
        if lagged && current && !self.unknown && db.version() == before + 1 {
            return Err(format!(
                "the eager replica held version {before} and lagged on the next commit"
            ));
        }
        if self.prng.chance(self.options.lazy_drain) {
            self.catch_up(1)?;
        }
        self.follow(false)
    }

    /// Adds a subscription to a random query, which queries on its first
    /// advance, or one that starts from a result read earlier.
    fn subscribe(&mut self, held: Option<(Kind, queries::QuerySpec, Held)>) -> Result<(), String> {
        self.next_subscription += 1;
        let id = self.next_subscription;
        let subscription = match held {
            Some((kind, query, held)) => {
                let mut subscription = Subscription::new(id, query, kind);
                subscription.adopt(self.db(), held)?;
                subscription
            }
            None => {
                let query = queries::query(
                    &mut self.queries,
                    &mut self.query_shapes,
                    &mut self.more_shapes,
                    &self.schema,
                );
                let kind = *self.queries.pick(&[Kind::Eager, Kind::Lagging]);
                Subscription::new(id, query, kind)
            }
        };
        self.trace.record(format_args!(
            "subscription {id} ({:?}): {} {:?}",
            subscription.kind, subscription.query.sql, subscription.query.params
        ));
        self.subscriptions.push(subscription);
        if self.subscriptions.len() > 12 {
            self.subscriptions.remove(0);
        }
        Ok(())
    }

    /// Starts a snapshot, and a witness transaction beside it, to hold
    /// across this step's write. `None` if an injected fault stopped the
    /// snapshot.
    fn hold<'d>(&mut self, db: &'d LiveDb) -> Result<Option<Hold<'d>>, String> {
        let query = queries::query(
            &mut self.queries,
            &mut self.query_shapes,
            &mut self.more_shapes,
            &self.schema,
        );
        let snapshot = match db.snapshot() {
            Ok(snapshot) => snapshot,
            Err(e) => {
                self.summary.subscriptions.failed(&e);
                return Ok(None);
            }
        };
        let witness = self.witness.as_ref().expect("open");
        let version = self
            .vfs
            .quietly(|| replica::begin_read(witness))
            .map_err(|e| format!("witness: {e}"))?;
        if version != snapshot.version() {
            return Err(format!(
                "a snapshot reports version {}, but began at version {version}",
                snapshot.version()
            ));
        }
        self.trace.record(format_args!(
            "snapshot at {version}: {} {:?}",
            query.sql, query.params
        ));
        Ok(Some(Hold { snapshot, query }))
    }

    /// Checks that a held snapshot returns its version's rows, as the
    /// witness reads them. A subscription may start from the result; it must
    /// then catch up on the commits made since.
    fn check_hold(&mut self, hold: Hold<'_>) -> Result<(), String> {
        let Hold { snapshot, query } = hold;
        let version = snapshot.version();
        let mut columns = None;
        let held = snapshot.query(
            &query.sql,
            query.params.clone(),
            subscriptions::naming(&mut columns),
        );
        drop(snapshot);
        let witness = self.witness.as_ref().expect("open");
        let expected = self
            .vfs
            .quietly(|| replica::finish_read(witness, || fresh(witness, &query)))
            .map_err(|e| format!("witness: {e} in {}", query.sql))?;
        let held = match held {
            Ok(held) => held,
            Err(e) => {
                self.summary.subscriptions.failed(&e);
                return Ok(());
            }
        };
        if held.version != version || !same_rows(&held.rows, &expected, query.ordered, query.open) {
            return Err(format!(
                "a snapshot at version {version} returned version {} and rows {:?} for {} {:?}; \
                 the witness read {expected:?}",
                held.version, held.rows, query.sql, query.params
            ));
        }
        self.summary.snapshot_checks += 1;
        if self.queries.chance(0.5) {
            let kind = *self.queries.pick(&[Kind::Eager, Kind::Lagging]);
            let held = Held {
                rows: held.rows,
                columns,
                version: held.version,
                read_set: held.read_set,
                hash: held.hash,
            };
            self.subscribe(Some((kind, query, held)))?;
        }
        Ok(())
    }

    /// Advances every subscription, then checks each one that has processed
    /// every event against its query run fresh. With `settle`, every
    /// subscription processes everything and must catch up.
    fn follow(&mut self, settle: bool) -> Result<(), String> {
        let db = self.db.clone().expect("open");
        self.check_index(&db)?;
        for subscription in &mut self.subscriptions {
            let process = settle
                || subscription.kind == Kind::Eager
                || self.queries.chance(self.options.lazy_drain);
            subscription.advance(
                &db,
                &mut self.queries,
                process,
                &mut self.summary.subscriptions,
            )?;
        }
        self.sync_index();
        self.check_reflected()?;
        let latest = db.version();
        let oracle = self.oracle.as_ref().expect("open");
        let subscriptions = &self.subscriptions;
        let read = self.vfs.quietly(|| {
            replica::read_at(oracle, |version| {
                if version != latest {
                    return Ok(None);
                }
                let mut found = Vec::new();
                for (i, subscription) in subscriptions.iter().enumerate() {
                    if subscription.caught_up(latest) {
                        found.push((i, fresh(oracle, &subscription.query)?));
                    }
                }
                Ok(Some(found))
            })
        });
        let read = read.map_err(|e| format!("oracle: {e}"))?;
        let Some(found) = read else {
            if self.unknown {
                return Ok(());
            }
            return Err(format!(
                "the oracle and the log disagree on version {latest}"
            ));
        };
        if settle && found.len() != self.subscriptions.len() {
            let behind: Vec<String> = self
                .subscriptions
                .iter()
                .filter(|subscription| !subscription.caught_up(latest))
                .map(|subscription| {
                    format!(
                        "subscription {}: {} {:?}, whose last failure was {:?}",
                        subscription.id,
                        subscription.query.sql,
                        subscription.query.params,
                        subscription.last_error
                    )
                })
                .collect();
            return Err(format!(
                "{} of {} subscriptions caught up with faults off; behind: {}",
                found.len(),
                self.subscriptions.len(),
                behind.join("; ")
            ));
        }
        for (i, expected) in found {
            let subscription = &self.subscriptions[i];
            let held = subscription.held.as_ref().expect("caught up");
            let query = &subscription.query;
            if !same_rows(&held.rows, &expected, query.ordered, query.open) {
                return Err(format!(
                    "subscription {} ({:?}) at version {latest} holds {:?} from version {}, \
                     but {} {:?} returns {expected:?}; read set {}",
                    subscription.id,
                    subscription.kind,
                    held.rows,
                    held.version,
                    subscription.query.sql,
                    subscription.query.params,
                    held.read_set
                ));
            }
            self.summary.subscription_checks += 1;
        }
        Ok(())
    }

    /// Checks every event since the last check against the index. Runs
    /// before subscriptions process the events, while their read sets still
    /// predate them.
    fn check_index(&mut self, db: &LiveDb) -> Result<(), String> {
        let stream = self.index_stream.as_mut().expect("open");
        let mut events = Vec::new();
        while let Some(item) = stream.try_next() {
            match item {
                Ok(event) => events.push(event),
                Err(sqlive::Error::Lagged { .. }) => {
                    *stream = db.changes().map_err(|e| format!("index stream: {e}"))?;
                    break;
                }
                Err(e) => return Err(format!("index stream: {e}")),
            }
        }
        for event in events {
            self.summary.index_hits += self.check_affected(&event)? as u64;
            self.summary.index_checks += 1;
            self.last_indexed = Some(event);
        }
        Ok(())
    }

    /// Checks the last event again, once read sets that already reflect it
    /// are indexed: the index must leave those out.
    fn check_reflected(&mut self) -> Result<(), String> {
        if let Some(event) = self.last_indexed.clone() {
            self.check_affected(&event)?;
            self.summary.index_rechecks += 1;
        }
        Ok(())
    }

    /// Checks that the index finds exactly the read sets, as last indexed,
    /// that `event` intersects, and returns how many.
    fn check_affected(&self, event: &CommitEvent) -> Result<usize, String> {
        let found: Vec<u64> = self.index.affected(event).collect();
        let expected: Vec<u64> = self
            .indexed
            .iter()
            .filter(|(_, read_set)| read_set.intersects(event))
            .map(|(id, _)| *id)
            .collect();
        if found != expected {
            return Err(format!(
                "the index found {found:?} for event {}, but {expected:?} intersect it: {event:?}",
                event.version
            ));
        }
        Ok(found.len())
    }

    /// Indexes each subscription's held read set, replacing older ones and
    /// removing those no subscription holds.
    fn sync_index(&mut self) {
        let mut held: BTreeMap<u64, &sqlive::ReadSet> = BTreeMap::new();
        for subscription in &self.subscriptions {
            if let Some(h) = &subscription.held {
                held.insert(subscription.id, &h.read_set);
            }
        }
        let gone: Vec<u64> = self
            .indexed
            .keys()
            .filter(|id| !held.contains_key(id))
            .copied()
            .collect();
        for id in gone {
            self.indexed.remove(&id);
            self.index.remove(&id);
        }
        // A newer read set replaces the indexed one in place.
        for (id, read_set) in held {
            if self.indexed.get(&id).map(sqlive::ReadSet::version) != Some(read_set.version()) {
                self.index.insert(id, read_set.clone());
                self.indexed.insert(id, read_set.clone());
            }
        }
    }

    /// With faults off, brings every subscription up to date and checks it.
    fn settle(&mut self) -> Result<(), String> {
        // A subscription that lags or re-runs mid-batch needs another pass.
        for _ in 0..3 {
            self.follow(false)?;
        }
        self.follow(true)
    }

    fn write(&mut self) -> Result<(), String> {
        let mut txn = writes::transaction(&mut self.prng, &self.schema);
        // A statement that writes a full-text table runs with fault
        // injection suspended. After a read error inside an insert, FTS5
        // keeps the row and size totals it failed to load, and stores them
        // at the next commit, where the integrity check finds them wrong;
        // plain SQLite does the same.
        let mut full_text_at = None;
        if self.schema.full_text.is_some() && self.full_text.chance(0.4) {
            let at = self.full_text.index(txn.statements.len() + 1);
            txn.statements
                .insert(at, writes::full_text_statement(&mut self.full_text));
            full_text_at = Some(at);
        }
        let before = self.db().version();
        let peeks: Vec<bool> = txn
            .statements
            .iter()
            .map(|_| self.peeks.chance(self.options.peek))
            .collect();
        // A caught-up eager replica holds the committed rows, so the first
        // peek dumps it; otherwise the oracle reads them before the write.
        let eager = &self.replicas[0];
        let mut baseline = if eager.version == before && !eager.stale && !self.unknown {
            None
        } else {
            Some(self.read()?)
        };
        let mut outcomes = Vec::new();
        let mut problem = None;
        let mut rolled_back = false;
        let mut finished = false;
        let (oracle, vfs) = (self.oracle(), &self.vfs);
        let result = self.db().write(|tx| {
            for (i, (statement, peek)) in txn.statements.iter().zip(&peeks).enumerate() {
                let execute = || tx.execute(&statement.sql, params_from_iter(&statement.params));
                let outcome = if full_text_at == Some(i) {
                    vfs.quietly(execute)
                } else {
                    execute()
                };
                let failed = outcome.is_err();
                // Checked after each statement, before a later `SAVEPOINT`
                // can open a new transaction.
                rolled_back |= tx.is_autocommit();
                if (*peek || failed) && problem.is_none() {
                    problem = peek_at(oracle, vfs, eager, before, &mut baseline, &statement.sql);
                }
                match outcome {
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
            finished = true;
            Ok(())
        });
        self.summary.peeks += peeks.iter().filter(|p| **p).count() as u64;
        if rolled_back && result.is_ok() {
            return Err("SQLite rolled the transaction back, but write reported success".into());
        }
        let rolled_back = matches!(result, Err(sqlive::Error::RolledBack));
        self.summary.rolled_back += u64::from(rolled_back);
        if let Some(problem) = problem {
            return Err(problem);
        }
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
            finished && !rolled_back,
        )
    }

    /// Checks a write's version against the one before it. A failed write
    /// that got as far as committing leaves the outcome unknown.
    fn finish_write(
        &mut self,
        before: Version,
        result: Result<Version, String>,
        reached_commit: bool,
    ) -> Result<(), String> {
        let now = self.db().version();
        match result {
            Ok(version) => {
                self.trace
                    .record(format_args!("write -> version {version}"));
                self.summary.writes += 1;
                let next = if self.unknown {
                    version > before
                } else {
                    version == before + 1
                };
                if !next || now != version {
                    return Err(format!(
                        "a write after version {before} returned {version}; the log reports {now}"
                    ));
                }
                self.unknown = false;
                self.published = version;
            }
            Err(e) => {
                self.trace.record(format_args!("write failed: {e}"));
                self.summary.failed_writes += 1;
                if now != before && !self.unknown {
                    return Err(format!(
                        "a failed write moved the version from {before} to {now}"
                    ));
                }
                self.unknown |= reached_commit;
            }
        }
        Ok(())
    }

    fn change_schema(&mut self) -> Result<(), String> {
        self.serial += 1;
        let (statement, added) =
            writes::schema_change(&mut self.prng, &mut self.schema, self.serial);
        self.trace
            .record(format_args!("schema change: {}", statement.sql));
        let before = self.db().version();
        let mut finished = false;
        let result = self
            .db()
            .write(|tx| {
                tx.execute_batch(&statement.sql)?;
                finished = true;
                Ok(())
            })
            .map(|((), version)| version)
            .map_err(|e| e.to_string());
        if let Some((table, column)) = added {
            // A failed `ALTER TABLE` may or may not have taken effect.
            let name = self.schema.tables[table].name.clone();
            let exists = self.vfs.quietly(|| {
                self.oracle().query_row(
                    "SELECT count(*) FROM pragma_table_xinfo(?1) WHERE name = ?2",
                    [&name, &column],
                    |r| r.get::<_, i64>(0),
                )
            });
            if exists.map_err(|e| format!("oracle read: {e}"))? == 0 {
                self.schema.tables[table].columns.pop();
            }
        }
        self.finish_write(before, result, finished)
    }

    fn vacuum(&mut self) -> Result<(), String> {
        self.trace.record("vacuum");
        let before = self.db().version();
        let result = self.db().vacuum().map_err(|e| e.to_string());
        self.finish_write(before, result, true)
    }

    fn rebuild(&mut self, i: usize) -> Result<(), String> {
        let (db, oracle) = (
            self.db.as_ref().expect("open"),
            self.oracle.as_ref().expect("open"),
        );
        self.vfs.quietly(|| self.replicas[i].rebuild(oracle, db))?;
        self.summary.rebuilds += 1;
        self.trace.record(format_args!(
            "{} replica rebuilt at {}",
            self.replicas[i].name, self.replicas[i].version
        ));
        Ok(())
    }

    /// Applies every committed event to replica `i`, rebuilding it when it
    /// lags or sees a schema change and reloading the tables an event names
    /// whole, then compares it with the database. Returns whether it lagged.
    fn catch_up(&mut self, i: usize) -> Result<bool, String> {
        let mut lagged = false;
        loop {
            if self.replicas[i].stale {
                self.rebuild(i)?;
            }
            let drained = self.replicas[i].drain(usize::MAX)?;
            self.summary.events += drained.events;
            self.summary.changes += drained.changes;
            self.summary.schema_events += u64::from(drained.schema_changed);
            self.summary.whole_tables += drained.whole_tables;
            if drained.lagged {
                lagged = true;
                self.summary.lagged += 1;
                self.trace
                    .record(format_args!("{} replica lagged", self.replicas[i].name));
            }
            if !self.replicas[i].reload.is_empty() {
                self.reload(i)?;
            } else if !self.replicas[i].stale {
                break;
            }
        }
        self.check(i)?;
        Ok(lagged)
    }

    /// Reloads the tables the replica's last event named whole, at the
    /// version the database has reached. A table whose shape has changed
    /// since the event waits for the schema change ahead in the stream, and
    /// the replica rebuilds.
    fn reload(&mut self, i: usize) -> Result<(), String> {
        let (oracle, replica) = (self.oracle.as_ref().expect("open"), &mut self.replicas[i]);
        let (version, shapes, dump) = self
            .vfs
            .quietly(|| replica::read_tables(oracle, &replica.reload))
            .map_err(|e| format!("oracle read: {e}"))?;
        self.trace.record(format_args!(
            "{} replica at {} reloads {:?} at {version}",
            replica.name, replica.version, replica.reload
        ));
        if replica.reload_from(version, &shapes, &dump)? {
            self.summary.reloads += 1;
        } else {
            replica.reload.clear();
            replica.stale = true;
        }
        Ok(())
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
        let (version, expected) = self.read()?;
        if version != latest {
            if self.unknown {
                // A failed write took effect without reaching the log; the
                // next write resets the log and the replicas rebuild.
                self.summary.skipped_checks += 1;
                return Ok(());
            }
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

/// Checks, mid-write, that another connection still sees the rows as they
/// were before the write, and describes any difference.
fn peek_at(
    oracle: &Connection,
    vfs: &SimVfs,
    eager: &Replica,
    before: Version,
    baseline: &mut Option<(Version, Dump)>,
    after: &str,
) -> Option<String> {
    if baseline.is_none() {
        match eager.dump() {
            Ok(dump) => *baseline = Some((before, dump)),
            Err(e) => return Some(format!("the eager replica's dump failed: {e}")),
        }
    }
    let (version, rows) = baseline.as_ref().expect("just set");
    match vfs.quietly(|| replica::read(oracle)) {
        Ok((seen, _, dump)) => {
            let changed = difference(rows, &dump);
            (seen != *version || changed.is_some()).then(|| {
                format!(
                    "a write's changes showed before it committed: after {after}, another \
                     connection sees version {seen} and {}",
                    changed.unwrap_or_else(|| "the same rows".into())
                )
            })
        }
        Err(e) => Some(format!("the oracle failed mid-write: {e}")),
    }
}
