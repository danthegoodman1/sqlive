//! Records the rows a transaction touches, through the preupdate hook.
//!
//! The hook reports every change SQLite makes to a table, including those
//! from triggers, foreign-key cascades and `REPLACE` conflict resolution. For
//! each row, keyed by rowid or by primary key under the key's collations,
//! the capture keeps the values from the first time the transaction touched
//! it. `write` reads the touched rows back before committing to find what
//! changed.
//!
//! Values stay exactly as SQLite holds them, text bytes included, so change
//! detection and row identity never depend on how text converts to a Rust
//! string.

use crate::compare::{EqKey, eq_key};
use crate::schema::{META_TABLE, Schema, Table};
use rusqlite::hooks::{PreUpdateCase, PreUpdateNewValueAccessor, PreUpdateOldValueAccessor};
use rusqlite::types::{ToSqlOutput, Value, ValueRef};
use rusqlite::{Connection, ToSql};
use std::collections::BTreeSet;
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

    /// The value for a [`crate::Row`]. Text that is not UTF-8 converts
    /// lossily, the one place SqLive's rows differ from what SQLite holds.
    pub fn to_value(&self) -> Value {
        owned(self.as_ref())
    }

    /// Same storage class and same bytes, so `-0.0` differs from `0.0`.
    pub fn same(&self, other: &Raw) -> bool {
        match (self, other) {
            (Raw::Null, Raw::Null) => true,
            (Raw::Integer(a), Raw::Integer(b)) => a == b,
            (Raw::Real(a), Raw::Real(b)) => a.to_bits() == b.to_bits(),
            (Raw::Text(a), Raw::Text(b)) | (Raw::Blob(a), Raw::Blob(b)) => a == b,
            _ => false,
        }
    }
}

impl ToSql for Raw {
    fn to_sql(&self) -> rusqlite::Result<ToSqlOutput<'_>> {
        Ok(ToSqlOutput::Borrowed(self.as_ref()))
    }
}

pub(crate) fn same_raws(a: &[Raw], b: &[Raw]) -> bool {
    a.len() == b.len() && a.iter().zip(b).all(|(x, y)| x.same(y))
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

#[derive(Debug, PartialEq, Eq, PartialOrd, Ord)]
enum KeyIdentity {
    Rowid(i64),
    PrimaryKey(Vec<EqKey>),
}

#[derive(Default)]
pub(crate) struct Capture {
    schema: Option<Arc<Schema>>,
    touched: Vec<Touch>,
    seen: BTreeSet<(String, KeyIdentity)>,
    /// A change the schema could not account for, such as one to a table
    /// created inside the transaction.
    unaccounted: bool,
}

pub(crate) struct Captured {
    pub touched: Vec<Touch>,
    pub unaccounted: bool,
}

impl Capture {
    /// Starts recording a transaction's changes against `schema`.
    pub fn begin(&mut self, schema: Arc<Schema>) {
        self.schema = Some(schema);
        self.touched.clear();
        self.seen.clear();
        self.unaccounted = false;
    }

    /// Stops recording and hands over what was recorded.
    pub fn finish(&mut self) -> Captured {
        self.schema = None;
        self.seen.clear();
        Captured {
            touched: std::mem::take(&mut self.touched),
            unaccounted: std::mem::take(&mut self.unaccounted),
        }
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
        match case {
            PreUpdateCase::Insert(new) => match new_values(&info, new) {
                Some(values) => self.touch(&info, key(&info, new.get_new_row_id(), &values), None),
                None => self.unaccounted = true,
            },
            PreUpdateCase::Delete(old) => match old_values(&info, old) {
                Some(values) => {
                    let key = key(&info, old.get_old_row_id(), &values);
                    self.touch(&info, key, Some(stored(&info, values)));
                }
                None => self.unaccounted = true,
            },
            PreUpdateCase::Update {
                old_value_accessor: old,
                new_value_accessor: new,
            } => match (old_values(&info, old), new_values(&info, new)) {
                (Some(before), Some(after)) => {
                    let old_key = key(&info, old.get_old_row_id(), &before);
                    let new_key = key(&info, new.get_new_row_id(), &after);
                    self.touch(&info, old_key, Some(stored(&info, before)));
                    self.touch(&info, new_key, None);
                }
                _ => self.unaccounted = true,
            },
            PreUpdateCase::Unknown => self.unaccounted = true,
        }
    }

    fn touch(&mut self, table: &Arc<Table>, key: Key, first_old: Option<Vec<Raw>>) {
        let identity = match &key {
            Key::Rowid(rowid) => KeyIdentity::Rowid(*rowid),
            Key::PrimaryKey(values) => KeyIdentity::PrimaryKey(
                values
                    .iter()
                    .zip(&table.pk_collations)
                    .map(|(value, collation)| eq_key(value.as_ref(), collation))
                    .collect(),
            ),
        };
        if self
            .seen
            .insert((table.name.to_ascii_lowercase(), identity))
        {
            self.touched.push(Touch {
                table: table.clone(),
                key,
                first_old,
            });
        }
    }
}

pub(crate) fn install(conn: &mut Connection, capture: Arc<Mutex<Capture>>) -> rusqlite::Result<()> {
    conn.preupdate_hook(Some(
        move |_action, db: &str, table: &str, case: &PreUpdateCase| {
            crate::lock(&capture).on_change(db, table, case);
        },
    ))
}

/// Every column's value, with `Null` for `VIRTUAL` generated columns, which
/// the hook cannot read. `None` if the hook's row does not match the schema.
fn old_values(table: &Table, old: &PreUpdateOldValueAccessor) -> Option<Vec<Raw>> {
    if old.get_column_count() as usize != table.columns.len() {
        return None;
    }
    (0..table.columns.len())
        .map(|i| {
            if table.columns[i].stored {
                old.get_old_column_value(i as i32).ok().map(Raw::from_ref)
            } else {
                Some(Raw::Null)
            }
        })
        .collect()
}

fn new_values(table: &Table, new: &PreUpdateNewValueAccessor) -> Option<Vec<Raw>> {
    if new.get_column_count() as usize != table.columns.len() {
        return None;
    }
    (0..table.columns.len())
        .map(|i| {
            if table.columns[i].stored {
                new.get_new_column_value(i as i32).ok().map(Raw::from_ref)
            } else {
                Some(Raw::Null)
            }
        })
        .collect()
}

fn key(table: &Table, rowid: i64, values: &[Raw]) -> Key {
    if table.without_rowid {
        Key::PrimaryKey(table.pk.iter().map(|&cid| values[cid].clone()).collect())
    } else {
        Key::Rowid(rowid)
    }
}

fn stored(table: &Table, mut values: Vec<Raw>) -> Vec<Raw> {
    table
        .stored_cids
        .iter()
        .map(|&cid| std::mem::replace(&mut values[cid], Raw::Null))
        .collect()
}
