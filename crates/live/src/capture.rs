//! Records the rows a transaction touches, through the preupdate hook.
//!
//! The hook reports every change SQLite makes to a table, including those
//! from triggers, foreign-key cascades and `REPLACE` conflict resolution. For
//! each row, keyed by rowid or by primary key under the key's collations,
//! the capture keeps the stored values the row held before the transaction,
//! read the first time the transaction touches it, up to a limit of rows
//! for each table. `write` reads the touched rows back before committing to
//! find what changed, and names each table that passed the limit.
//!
//! Values stay exactly as SQLite holds them, text bytes included, so change
//! detection and row identity never depend on how text converts to a Rust
//! string.

use crate::compare::{EqKey, eq_key};
use crate::schema::{META_TABLE, Schema, Table};
use rusqlite::hooks::{PreUpdateCase, PreUpdateNewValueAccessor, PreUpdateOldValueAccessor};
use rusqlite::types::{ToSqlOutput, Value, ValueRef};
use rusqlite::{Connection, ToSql};
use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, Mutex};

/// A value exactly as SQLite stores it.
#[derive(Clone, Debug)]
pub(crate) enum Raw {
    Null,
    Integer(i64),
    Real(f64),
    Text(Vec<u8>),
    Blob(Vec<u8>),
}

impl Raw {
    pub fn from_ref(value: ValueRef<'_>) -> Raw {
        match value {
            ValueRef::Null => Raw::Null,
            ValueRef::Integer(i) => Raw::Integer(i),
            ValueRef::Real(r) => Raw::Real(r),
            ValueRef::Text(t) => Raw::Text(t.to_vec()),
            ValueRef::Blob(b) => Raw::Blob(b.to_vec()),
        }
    }

    pub fn as_ref(&self) -> ValueRef<'_> {
        match self {
            Raw::Null => ValueRef::Null,
            Raw::Integer(i) => ValueRef::Integer(*i),
            Raw::Real(r) => ValueRef::Real(*r),
            Raw::Text(t) => ValueRef::Text(t),
            Raw::Blob(b) => ValueRef::Blob(b),
        }
    }

    /// The value for a [`crate::Row`], keeping the bytes. Text that is not
    /// UTF-8 converts lossily, the one place SqLive's rows differ from what
    /// SQLite holds.
    pub fn into_value(self) -> Value {
        match self {
            Raw::Null => Value::Null,
            Raw::Integer(i) => Value::Integer(i),
            Raw::Real(r) => Value::Real(r),
            Raw::Text(bytes) => Value::Text(
                String::from_utf8(bytes)
                    .unwrap_or_else(|error| String::from_utf8_lossy(error.as_bytes()).into_owned()),
            ),
            Raw::Blob(bytes) => Value::Blob(bytes),
        }
    }

    /// Same storage class and same bytes, so `-0.0` differs from `0.0`.
    pub fn same(&self, other: ValueRef<'_>) -> bool {
        match (self, other) {
            (Raw::Null, ValueRef::Null) => true,
            (Raw::Integer(a), ValueRef::Integer(b)) => *a == b,
            (Raw::Real(a), ValueRef::Real(b)) => a.to_bits() == b.to_bits(),
            (Raw::Text(a), ValueRef::Text(b)) | (Raw::Blob(a), ValueRef::Blob(b)) => a == b,
            _ => false,
        }
    }
}

impl ToSql for Raw {
    fn to_sql(&self) -> rusqlite::Result<ToSqlOutput<'_>> {
        Ok(ToSqlOutput::Borrowed(self.as_ref()))
    }
}

/// Copies a value out of SQLite. Text that is not UTF-8 converts lossily.
pub(crate) fn owned(value: ValueRef<'_>) -> Value {
    match value {
        ValueRef::Null => Value::Null,
        ValueRef::Integer(i) => Value::Integer(i),
        ValueRef::Real(r) => Value::Real(r),
        ValueRef::Text(t) => Value::Text(String::from_utf8_lossy(t).into_owned()),
        ValueRef::Blob(b) => Value::Blob(b.to_vec()),
    }
}

/// How `write` reads a touched row back.
#[derive(Clone, Debug)]
pub(crate) enum Key {
    Rowid(i64),
    PrimaryKey(Vec<Raw>),
}

#[derive(Debug)]
pub(crate) struct Touch {
    pub table: Arc<Table>,
    pub key: Key,
    /// The row's stored columns before the transaction; `None` if it did
    /// not exist.
    pub first_old: Option<Vec<Raw>>,
}

#[derive(Default)]
pub(crate) struct Capture {
    schema: Option<Arc<Schema>>,
    touched: Vec<Touch>,
    /// Each touched row of a rowid table, by the table's index in `schema`.
    seen_rowids: BTreeSet<(usize, i64)>,
    /// Each touched row of a `WITHOUT ROWID` table, by the table's index in
    /// `schema` and the primary key's equality keys.
    seen_keys: BTreeSet<(usize, Vec<EqKey>)>,
    /// The most rows of one table a transaction records.
    limit: usize,
    /// Each touched table with the rows the transaction has touched in it,
    /// by the table's index in `schema`. A table with more rows than
    /// `limit` is whole: its rows are released and no longer recorded.
    counts: BTreeMap<usize, (Arc<Table>, usize)>,
    /// Touches of whole tables that `touched` still holds.
    released: usize,
    /// A change the schema could not account for, such as one to a table
    /// created inside the transaction.
    unaccounted: bool,
}

pub(crate) struct Captured {
    pub touched: Vec<Touch>,
    /// The tables that passed the limit, in name order.
    pub whole_tables: Vec<String>,
    pub unaccounted: bool,
}

impl Capture {
    /// Starts recording a transaction's changes against `schema`, up to
    /// `limit` rows of each table.
    pub fn begin(&mut self, schema: Arc<Schema>, limit: usize) {
        self.schema = Some(schema);
        self.limit = limit;
        self.clear();
        self.touched.clear();
        self.unaccounted = false;
    }

    /// Stops recording and hands over what was recorded.
    pub fn finish(&mut self) -> Captured {
        self.schema = None;
        if self.released > 0 {
            self.compact();
        }
        let whole_tables = self
            .counts
            .values()
            .filter(|(_, rows)| *rows > self.limit)
            .map(|(table, _)| table.name.clone())
            .collect();
        self.clear();
        Captured {
            touched: std::mem::take(&mut self.touched),
            whole_tables,
            unaccounted: std::mem::take(&mut self.unaccounted),
        }
    }

    /// Forgets which rows and tables the transaction touched.
    fn clear(&mut self) {
        self.seen_rowids.clear();
        self.seen_keys.clear();
        self.counts.clear();
        self.released = 0;
    }

    fn on_change(&mut self, db: &str, table: &str, case: &PreUpdateCase) {
        let Some(schema) = &self.schema else {
            return;
        };
        if db != "main" || table.eq_ignore_ascii_case(META_TABLE) {
            return;
        }
        let Some(info) = schema.table(table).cloned() else {
            self.unaccounted = true;
            return;
        };
        let whole = |(_, rows): &(Arc<Table>, usize)| *rows > self.limit;
        if self.counts.get(&info.index).is_some_and(whole) {
            return;
        }
        let recorded = match case {
            PreUpdateCase::Insert(new) => self.written(&info, new),
            PreUpdateCase::Delete(old) => self.removed(&info, old),
            // An update that keeps its rowid touches one row.
            PreUpdateCase::Update {
                old_value_accessor: old,
                new_value_accessor: new,
            } if !info.without_rowid && old.get_old_row_id() == new.get_new_row_id() => {
                self.removed(&info, old)
            }
            PreUpdateCase::Update {
                old_value_accessor: old,
                new_value_accessor: new,
            } => self
                .removed(&info, old)
                .and_then(|()| self.written(&info, new)),
            PreUpdateCase::Unknown => None,
        };
        if recorded.is_none() {
            self.unaccounted = true;
        }
    }

    /// Records the row an insert, or an update's new side, writes. `None`
    /// if the hook's row does not match the schema.
    fn written(&mut self, table: &Arc<Table>, new: &PreUpdateNewValueAccessor) -> Option<()> {
        if new.get_column_count() as usize != table.columns.len() {
            return None;
        }
        let key = key(table, new.get_new_row_id(), |cid| {
            new.get_new_column_value(cid)
        })?;
        self.touch(table, key, || Some(None))
    }

    /// Records the row a delete, or an update's old side, removes, with its
    /// stored values the first time the transaction touches it. `None` if
    /// the hook's row does not match the schema.
    fn removed(&mut self, table: &Arc<Table>, old: &PreUpdateOldValueAccessor) -> Option<()> {
        if old.get_column_count() as usize != table.columns.len() {
            return None;
        }
        let key = key(table, old.get_old_row_id(), |cid| {
            old.get_old_column_value(cid)
        })?;
        self.touch(table, key, || {
            table
                .stored_cids
                .iter()
                .map(|&cid| old.get_old_column_value(cid as i32).ok().map(Raw::from_ref))
                .collect::<Option<Vec<Raw>>>()
                .map(Some)
        })
    }

    /// Records a row the first time the transaction touches it, with
    /// `first_old` reading its stored values from before the transaction:
    /// `Some(None)` for a row that did not exist, `None` if they cannot be
    /// read.
    fn touch(
        &mut self,
        table: &Arc<Table>,
        key: Key,
        first_old: impl FnOnce() -> Option<Option<Vec<Raw>>>,
    ) -> Option<()> {
        let first_touch = match &key {
            Key::Rowid(rowid) => self.seen_rowids.insert((table.index, *rowid)),
            Key::PrimaryKey(values) => self.seen_keys.insert((
                table.index,
                values
                    .iter()
                    .zip(&table.pk_collations)
                    .map(|(value, collation)| eq_key(value.as_ref(), collation))
                    .collect(),
            )),
        };
        if first_touch {
            let (_, rows) = self
                .counts
                .entry(table.index)
                .or_insert_with(|| (table.clone(), 0));
            *rows += 1;
            if *rows > self.limit {
                // The table is whole from here on, and the rows recorded
                // for it, one for each touch up to the limit, go.
                self.released += self.limit;
                if self.released > self.touched.len() / 2 {
                    self.compact();
                }
                return Some(());
            }
            self.touched.push(Touch {
                table: table.clone(),
                key,
                first_old: first_old()?,
            });
        }
        Some(())
    }

    /// Drops the touches of whole tables. It runs once they make up half of
    /// `touched`, so its passes cost in proportion to the touches recorded.
    fn compact(&mut self) {
        let (counts, limit) = (&self.counts, self.limit);
        self.touched.retain(|touch| {
            counts
                .get(&touch.table.index)
                .is_none_or(|(_, rows)| *rows <= limit)
        });
        self.released = 0;
    }
}

pub(crate) fn install(conn: &mut Connection, capture: Arc<Mutex<Capture>>) -> rusqlite::Result<()> {
    conn.preupdate_hook(Some(
        move |_action, db: &str, table: &str, case: &PreUpdateCase| {
            crate::lock(&capture).on_change(db, table, case);
        },
    ))
}

/// A row's key: its rowid, or the primary-key values `value` reads by
/// column index. `None` if a value cannot be read.
fn key<'a>(
    table: &Table,
    rowid: i64,
    value: impl Fn(i32) -> rusqlite::Result<ValueRef<'a>>,
) -> Option<Key> {
    if table.without_rowid {
        table
            .pk
            .iter()
            .map(|&cid| value(cid as i32).ok().map(Raw::from_ref))
            .collect::<Option<Vec<Raw>>>()
            .map(Key::PrimaryKey)
    } else {
        Some(Key::Rowid(rowid))
    }
}
