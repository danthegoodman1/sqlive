//! Read sets: what a query result depends on, and whether a commit can
//! have changed it.

use crate::constraints::Filter;
use crate::event::{CommitEvent, Op, RowChange, Version};
use crate::schema::META_TABLE;
use rusqlite::types::Value;
use std::cmp::Ordering;
use std::collections::BTreeSet;
use std::fmt;
use std::sync::Arc;

/// The tables, columns and rows a query result depends on, as of the
/// snapshot version the query read.
#[derive(Clone, Debug, PartialEq)]
pub struct ReadSet {
    version: Version,
    /// In order of their lowercase names.
    tables: Box<[TableRead]>,
    /// The query reads a table that changes without row events, so every
    /// commit counts.
    every_commit: bool,
}

/// One table a query reads.
#[derive(Clone, Debug, PartialEq)]
pub struct TableRead {
    columns: Arc<ColumnsRead>,
    /// The rows the result depends on.
    filter: Filter,
}

/// The columns a query text reads from one table. They stay the same from
/// run to run, so every result of the text shares one copy.
#[derive(Debug, PartialEq)]
pub(crate) struct ColumnsRead {
    name: String,
    /// The name in lowercase, which orders a read set's tables.
    key: String,
    /// Lowercase names of the columns read, in order.
    columns: Box<[String]>,
    /// Every change to the table counts, as for a virtual table's shadow
    /// tables.
    every_column: bool,
}

/// The columns a statement reads from one table, as its preparation
/// reports them.
#[derive(Debug)]
pub(crate) struct Recorded {
    name: String,
    columns: BTreeSet<String>,
    every_column: bool,
}

impl Recorded {
    pub(crate) fn new(name: &str) -> Recorded {
        Recorded {
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
    pub(crate) fn name(&self) -> &str {
        &self.name
    }

    pub(crate) fn finish(self) -> Arc<ColumnsRead> {
        Arc::new(ColumnsRead {
            key: self.name.to_ascii_lowercase(),
            name: self.name,
            columns: self.columns.into_iter().collect(),
            every_column: self.every_column,
        })
    }
}

impl ColumnsRead {
    /// The table's name in lowercase.
    pub(crate) fn key(&self) -> &str {
        &self.key
    }
}

impl TableRead {
    /// The table's reads, narrowed to the rows `filter` allows; a table
    /// every change counts for keeps them all.
    pub(crate) fn new(columns: Arc<ColumnsRead>, filter: Filter) -> TableRead {
        let filter = if columns.every_column {
            Filter::All
        } else {
            filter
        };
        TableRead { columns, filter }
    }

    /// The table's name, as SQLite spells it.
    pub fn name(&self) -> &str {
        &self.columns.name
    }

    /// The columns the query reads, in lowercase.
    pub fn columns(&self) -> impl Iterator<Item = &str> {
        self.columns.columns.iter().map(String::as_str)
    }

    /// Whether the query depends on every column, so any change counts.
    pub fn every_column(&self) -> bool {
        self.columns.every_column
    }

    /// The rows the result depends on: a change counts only when its old or
    /// new row matches.
    pub fn filter(&self) -> &Filter {
        &self.filter
    }

    /// Whether an update changed a column the query reads. A column the rows
    /// leave out, such as a `VIRTUAL` generated one, counts as changed; the
    /// rowid stays the same in an update, since SqLive reports a row that
    /// moves to a new rowid as a delete and an insert.
    fn reads_changed_column(&self, change: &RowChange) -> bool {
        let (Some(old), Some(new)) = (&change.old, &change.new) else {
            return true;
        };
        self.every_column()
            || self
                .columns()
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

/// Orders names as their lowercase forms order.
fn cmp_ignoring_case(a: &str, b: &str) -> Ordering {
    a.bytes()
        .map(|b| b.to_ascii_lowercase())
        .cmp(b.bytes().map(|b| b.to_ascii_lowercase()))
}

impl ReadSet {
    /// A read set of `tables`, which come in order of their lowercase
    /// names.
    pub(crate) fn new(version: Version, tables: Box<[TableRead]>) -> ReadSet {
        debug_assert!(
            tables
                .windows(2)
                .all(|pair| pair[0].columns.key < pair[1].columns.key)
        );
        let every_commit = tables
            .iter()
            .any(|table| changes_without_events(&table.columns.key));
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

    /// Whether every commit after the version intersects, as when the query
    /// reads SqLive's version table or one of SQLite's internal tables.
    pub fn every_commit(&self) -> bool {
        self.every_commit
    }

    pub fn tables(&self) -> impl Iterator<Item = &TableRead> {
        self.tables.iter()
    }

    /// The table named `name`, matched without regard to ASCII case.
    fn table(&self, name: &str) -> Option<&TableRead> {
        self.tables
            .binary_search_by(|table| cmp_ignoring_case(&table.columns.key, name))
            .ok()
            .map(|i| &self.tables[i])
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
        let Some(table) = self.table(&change.table) else {
            return false;
        };
        if change.op == Op::Update && !table.reads_changed_column(change) {
            return false;
        }
        [&change.old, &change.new]
            .into_iter()
            .flatten()
            .any(|row| table.filter.matches(row))
    }
}

impl fmt::Display for ReadSet {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        for (i, table) in self.tables.iter().enumerate() {
            if i > 0 {
                f.write_str("; ")?;
            }
            if table.every_column() {
                write!(f, "{}[*]", table.name())?;
            } else {
                let columns: Vec<&str> = table.columns().collect();
                write!(f, "{}[{}]", table.name(), columns.join(", "))?;
            }
            if let Filter::AnyOf(_) = table.filter {
                write!(f, " where {}", table.filter)?;
            }
        }
        Ok(())
    }
}
