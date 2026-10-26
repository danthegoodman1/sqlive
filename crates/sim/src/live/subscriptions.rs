//! Live queries as an app would run them: query, follow the change stream,
//! and re-run against the held result's hash whenever the read set
//! intersects an event. The harness checks each caught-up subscription
//! against the same query run fresh on the oracle.

use super::queries::QuerySpec;
use crate::prng::Prng;
use rusqlite::types::Value;
use rusqlite::{Connection, params_from_iter};
use sqlive::{
    Changes, CommitEvent, Condition, Error, Filter, LiveDb, ReadSet, ResultHash, Version,
};
use std::cmp::Ordering;
use std::sync::Arc;

pub type Rows = Vec<Vec<Value>>;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Kind {
    /// Processes every event as soon as it commits.
    Eager,
    /// Collects events and processes them later, in shuffled order.
    Lagging,
}

pub struct Held {
    pub rows: Rows,
    pub version: Version,
    pub read_set: ReadSet,
    pub hash: ResultHash,
}

pub struct Subscription {
    pub id: u64,
    pub query: QuerySpec,
    pub kind: Kind,
    /// `None` when the subscription must query again.
    pub held: Option<Held>,
    pub stream: Option<Changes>,
    pub buffer: Vec<Arc<CommitEvent>>,
}

#[derive(Debug, Default)]
pub struct Tally {
    pub tested: u64,
    pub intersected: u64,
    pub reruns: u64,
    /// Re-runs whose rows hashed the same as the held result's.
    pub unchanged: u64,
    pub failed_queries: u64,
    /// Failed queries whose snapshot SQLite had ended.
    pub ended: u64,
    pub lagged: u64,
    pub queries: u64,
    /// Results whose read set narrows some table to the rows it needs.
    pub constrained: u64,
    /// Results whose read set bounds a `LIMIT` window.
    pub windowed: u64,
}

impl Tally {
    /// Counts a query an injected fault stopped.
    pub fn failed(&mut self, error: &Error) {
        self.failed_queries += 1;
        self.ended += u64::from(matches!(error, Error::SnapshotEnded));
    }

    fn count(&mut self, read_set: &ReadSet) {
        let conditions = || {
            read_set.tables().flat_map(|table| match table.filter() {
                Filter::All => &[][..],
                Filter::AnyOf(conjunctions) => conjunctions.as_slice(),
            })
        };
        self.queries += 1;
        self.constrained += u64::from(conditions().next().is_some());
        self.windowed += u64::from(conditions().any(|conjunction| {
            conjunction
                .constraints
                .iter()
                .any(|c| matches!(c.condition, Condition::Window { .. }))
        }));
    }
}

/// Every column of every row.
pub fn rows(row: &rusqlite::Row<'_>) -> rusqlite::Result<Vec<Value>> {
    (0..row.as_ref().column_count())
        .map(|i| row.get::<_, Value>(i))
        .collect()
}

impl Subscription {
    pub fn new(id: u64, query: QuerySpec, kind: Kind) -> Subscription {
        Subscription {
            id,
            query,
            kind,
            held: None,
            stream: None,
            buffer: Vec::new(),
        }
    }

    /// Queries afresh and follows the stream from the result's version.
    /// Returns false if an injected fault made the query fail.
    pub fn requery(&mut self, db: &LiveDb, tally: &mut Tally) -> Result<bool, String> {
        self.held = None;
        self.stream = None;
        self.buffer.clear();
        let result = match db.query(&self.query.sql, self.query.params.clone(), rows) {
            Ok(result) => result,
            Err(e) => {
                tally.failed(&e);
                return Ok(false);
            }
        };
        self.follow_from(db, result.version)?;
        tally.count(&result.read_set);
        self.held = Some(Held {
            rows: result.rows,
            version: result.version,
            read_set: result.read_set,
            hash: result.hash,
        });
        Ok(true)
    }

    /// Re-runs the query against the held result's hash and follows the
    /// stream from the new version. An unchanged result keeps the held rows,
    /// which the oracle checks once the subscription catches up; it must
    /// carry no rows. A changed result of a completely ordered query must
    /// differ from the held rows. Returns false if an injected fault made
    /// the query fail.
    fn rerun(&mut self, db: &LiveDb, tally: &mut Tally) -> Result<bool, String> {
        let held = self.held.take().expect("a held result to re-run");
        self.stream = None;
        self.buffer.clear();
        let params = self.query.params.clone();
        let result = match db.rerun(&self.query.sql, params, held.hash, rows) {
            Ok(result) => result,
            Err(e) => {
                tally.failed(&e);
                return Ok(false);
            }
        };
        tally.reruns += 1;
        let rows = if result.unchanged {
            tally.unchanged += 1;
            if !result.rows.is_empty() {
                return Err(format!(
                    "subscription {}: an unchanged re-run at version {} returned rows {:?}",
                    self.id, result.version, result.rows
                ));
            }
            held.rows
        } else {
            if self.query.ordered && same_rows(&held.rows, &result.rows, true) {
                return Err(format!(
                    "subscription {}: a re-run at version {} reported a change but returned \
                     the rows it held, {:?}, for {} {:?}",
                    self.id, result.version, result.rows, self.query.sql, self.query.params
                ));
            }
            result.rows
        };
        self.follow_from(db, result.version)?;
        tally.count(&result.read_set);
        self.held = Some(Held {
            rows,
            version: result.version,
            read_set: result.read_set,
            hash: result.hash,
        });
        Ok(true)
    }

    fn follow_from(&mut self, db: &LiveDb, version: Version) -> Result<(), String> {
        self.stream = Some(db.changes_since(version).map_err(|e| {
            format!(
                "subscription {} cannot follow version {version}: {e}",
                self.id
            )
        })?);
        Ok(())
    }

    /// Adopts a result read elsewhere, as a snapshot query is, and follows
    /// the stream from its version.
    pub fn adopt(&mut self, db: &LiveDb, held: Held) -> Result<(), String> {
        self.stream = Some(db.changes_since(held.version).map_err(|e| {
            format!(
                "subscription {} cannot follow version {}: {e}",
                self.id, held.version
            )
        })?);
        self.buffer.clear();
        self.held = Some(held);
        Ok(())
    }

    /// Follows a reopened database from the held version.
    pub fn resume(&mut self, db: &LiveDb) -> Result<(), String> {
        self.buffer.clear();
        self.stream = None;
        if let Some(held) = &self.held {
            self.stream = Some(db.changes_since(held.version).map_err(|e| {
                format!(
                    "subscription {} at version {} cannot follow the reopened database: {e}",
                    self.id, held.version
                )
            })?);
        }
        Ok(())
    }

    /// Moves committed events into the buffer. Returns false when the stream
    /// lagged and the subscription must query again.
    fn pull(&mut self, tally: &mut Tally) -> bool {
        let Some(stream) = self.stream.as_mut() else {
            return false;
        };
        while let Some(item) = stream.try_next() {
            match item {
                Ok(event) => self.buffer.push(event),
                Err(Error::Lagged { .. }) => {
                    tally.lagged += 1;
                    self.held = None;
                    return false;
                }
                Err(_) => return false,
            }
        }
        true
    }

    /// Pulls and processes events, re-running the query for each one its
    /// read set intersects.
    pub fn advance(
        &mut self,
        db: &LiveDb,
        prng: &mut Prng,
        process: bool,
        tally: &mut Tally,
    ) -> Result<(), String> {
        if self.held.is_none() || !self.pull(tally) {
            self.requery(db, tally)?;
            return Ok(());
        }
        if !process {
            return Ok(());
        }
        let mut events = std::mem::take(&mut self.buffer);
        if self.kind == Kind::Lagging {
            for i in (1..events.len()).rev() {
                events.swap(i, prng.index(i + 1));
            }
        }
        for event in events {
            let Some(held) = &self.held else {
                break;
            };
            tally.tested += 1;
            if held.read_set.intersects(&event) {
                tally.intersected += 1;
                if !self.rerun(db, tally)? {
                    break;
                }
            }
        }
        Ok(())
    }

    /// Whether the subscription has processed every event through `latest`.
    pub fn caught_up(&self, latest: Version) -> bool {
        self.held.is_some()
            && self.buffer.is_empty()
            && self
                .stream
                .as_ref()
                .is_some_and(|stream| stream.next_version() == latest + 1)
    }
}

/// The query's rows, run on the oracle.
pub fn fresh(oracle: &Connection, query: &QuerySpec) -> rusqlite::Result<Rows> {
    let mut statement = oracle.prepare(&query.sql)?;
    let found = statement
        .query_map(params_from_iter(&query.params), rows)?
        .collect::<rusqlite::Result<Rows>>()?;
    Ok(found)
}

/// Orders values totally: by storage class, then by value, with reals by
/// `total_cmp`.
fn order(a: &Value, b: &Value) -> Ordering {
    let class = |v: &Value| match v {
        Value::Null => 0,
        Value::Integer(_) => 1,
        Value::Real(_) => 2,
        Value::Text(_) => 3,
        Value::Blob(_) => 4,
    };
    match (a, b) {
        (Value::Integer(x), Value::Integer(y)) => x.cmp(y),
        (Value::Real(x), Value::Real(y)) => x.total_cmp(y),
        (Value::Text(x), Value::Text(y)) => x.cmp(y),
        (Value::Blob(x), Value::Blob(y)) => x.cmp(y),
        _ => class(a).cmp(&class(b)),
    }
}

fn row_order(a: &[Value], b: &[Value]) -> Ordering {
    a.iter()
        .zip(b)
        .map(|(x, y)| order(x, y))
        .find(|o| o.is_ne())
        .unwrap_or_else(|| a.len().cmp(&b.len()))
}

/// Whether two results hold the same rows: in the same order when the query
/// orders them completely, as multisets otherwise.
pub fn same_rows(a: &Rows, b: &Rows, ordered: bool) -> bool {
    if a.len() != b.len() {
        return false;
    }
    if ordered {
        return a.iter().zip(b).all(|(x, y)| row_order(x, y).is_eq());
    }
    let mut a: Vec<&Vec<Value>> = a.iter().collect();
    let mut b: Vec<&Vec<Value>> = b.iter().collect();
    a.sort_by(|x, y| row_order(x, y));
    b.sort_by(|x, y| row_order(x, y));
    a.iter().zip(&b).all(|(x, y)| row_order(x, y).is_eq())
}
