//! Read sets: what a query result depends on, and whether a commit can
//! have changed it.

use crate::deps::Reads;
use crate::event::{CommitEvent, Op, RowChange, Version};
use crate::schema::META_TABLE;
use rusqlite::types::Value;
use std::collections::BTreeSet;
use std::fmt;

/// The tables and columns a query result depends on, as of the snapshot
/// version the query read.
#[derive(Clone, Debug, PartialEq)]
pub struct ReadSet {
    version: Version,
    tables: Reads,
    /// The query reads a table that changes without row events, so every
    /// commit counts.
    every_commit: bool,
}

/// One table a query reads.
#[derive(Clone, Debug, PartialEq)]
pub struct TableRead {
    name: String,
    /// Lowercase names of the columns read.
    columns: BTreeSet<String>,
    /// Every change to the table counts, as for a virtual table's shadow
    /// tables.
    every_column: bool,
}

impl TableRead {
    pub(crate) fn new(name: &str) -> TableRead {
        TableRead {
            name: name.to_owned(),
            columns: BTreeSet::new(),
            every_column: false,
        }
    }

    /// Records a column read. A statement that reads a table without
    /// reading a column, as `count(*)` does, records an empty name.
    pub(crate) fn record(&mut self, column: &str) {
        if !column.is_empty() {
            self.columns.insert(column.to_ascii_lowercase());
        }
    }

    pub(crate) fn record_every_column(&mut self) {
        self.every_column = true;
    }

    /// The table's name, as SQLite spells it.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// The columns the query reads, in lowercase.
    pub fn columns(&self) -> impl Iterator<Item = &str> {
        self.columns.iter().map(String::as_str)
    }

    /// Whether the query depends on every column, so any change counts.
    pub fn every_column(&self) -> bool {
        self.every_column
    }

    /// Whether an update changed a column the query reads. A column the rows
    /// leave out, such as a `VIRTUAL` generated one, counts as changed; the
    /// rowid stays the same in an update, since SqLive reports a row that
    /// moves to a new rowid as a delete and an insert.
    fn reads_changed_column(&self, change: &RowChange) -> bool {
        let (Some(old), Some(new)) = (&change.old, &change.new) else {
            return true;
        };
        self.every_column
            || self
                .columns
                .iter()
                .any(|column| match (old.get(column), new.get(column)) {
                    (Some(a), Some(b)) => !same(a, b),
                    _ => !is_rowid(column),
                })
    }
}

/// Whether two values are the same: same storage class and same bytes, so
/// `-0.0` differs from `0.0`. Text that holds U+FFFD may have come from
/// bytes that were not UTF-8, whose differences the conversion erased, so
/// it never counts as the same.
fn same(a: &Value, b: &Value) -> bool {
    match (a, b) {
        (Value::Real(x), Value::Real(y)) => x.to_bits() == y.to_bits(),
        (Value::Text(x), Value::Text(y)) => x == y && !x.contains('\u{FFFD}'),
        (x, y) => x == y,
    }
}

/// Whether a table changes without row events: SqLive's version table,
/// which every commit updates, and SQLite's internal tables, such as
/// `sqlite_sequence`, which the preupdate hook never reports. A schema
/// change already flags its event.
fn changes_without_events(table: &str) -> bool {
    table == META_TABLE
        || (table.starts_with("sqlite_") && table != "sqlite_schema" && table != "sqlite_master")
}

fn is_rowid(column: &str) -> bool {
    ["rowid", "_rowid_", "oid"]
        .iter()
        .any(|alias| alias.eq_ignore_ascii_case(column))
}

impl ReadSet {
    pub(crate) fn new(version: Version, tables: Reads) -> ReadSet {
        let every_commit = tables.keys().any(|table| changes_without_events(table));
        ReadSet {
            version,
            tables,
            every_commit,
        }
    }

    /// The snapshot version the query read.
    pub fn version(&self) -> Version {
        self.version
    }

    pub fn tables(&self) -> impl Iterator<Item = &TableRead> {
        self.tables.values()
    }

    /// Whether `event` may have changed the query's result. False for an
    /// event at or below the read set's version, which the result already
    /// reflects; true for a schema change, and for every commit when the
    /// query reads SqLive's version table or one of SQLite's internal
    /// tables.
    pub fn intersects(&self, event: &CommitEvent) -> bool {
        if event.version <= self.version {
            return false;
        }
        event.schema_changed
            || self.every_commit
            || event.changes.iter().any(|change| self.affected_by(change))
    }

    /// Whether one row change may have changed the query's result.
    pub fn affected_by(&self, change: &RowChange) -> bool {
        let Some(table) = self.tables.get(&change.table.to_ascii_lowercase()) else {
            return false;
        };
        change.op != Op::Update || table.reads_changed_column(change)
    }
}

impl fmt::Display for ReadSet {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        for (i, table) in self.tables.values().enumerate() {
            if i > 0 {
                f.write_str("; ")?;
            }
            if table.every_column {
                write!(f, "{}[*]", table.name)?;
            } else {
                let columns: Vec<&str> = table.columns().collect();
                write!(f, "{}[{}]", table.name, columns.join(", "))?;
            }
        }
        Ok(())
    }
}
