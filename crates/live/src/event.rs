//! Commit events: what one successful `write` changed.

use rusqlite::types::Value;
use std::fmt;
use std::sync::Arc;

/// A commit's position in the database's history. Each successful `write`
/// adds one; the first is 1.
pub type Version = u64;

/// The net row changes of one committed transaction.
#[derive(Clone, Debug, PartialEq)]
pub struct CommitEvent {
    pub version: Version,
    /// The transaction changed the schema, or ran `VACUUM`, which can
    /// renumber rowids. `changes` is then empty: reload what you hold.
    pub schema_changed: bool,
    /// One entry per changed row, in the order the transaction first
    /// touched each row.
    pub changes: Vec<RowChange>,
}

/// One row's net change within a transaction.
#[derive(Clone, Debug, PartialEq)]
pub struct RowChange {
    pub table: String,
    pub op: Op,
    /// The row's rowid; `None` for a `WITHOUT ROWID` table, whose primary
    /// key identifies the row.
    pub rowid: Option<i64>,
    /// The row before the transaction; `None` for an insert.
    pub old: Option<Row>,
    /// The row after the transaction; `None` for a delete.
    pub new: Option<Row>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum Op {
    Insert,
    Update,
    Delete,
}

/// A row's stored columns, in table order. `VIRTUAL` generated columns are
/// left out, since SQLite computes them on read.
#[derive(Clone, PartialEq)]
pub struct Row {
    columns: Arc<[String]>,
    values: Vec<Value>,
}

impl Row {
    pub(crate) fn new(columns: Arc<[String]>, values: Vec<Value>) -> Row {
        debug_assert_eq!(columns.len(), values.len());
        Row { columns, values }
    }

    pub fn columns(&self) -> &[String] {
        &self.columns
    }

    pub fn values(&self) -> &[Value] {
        &self.values
    }

    /// The value of a column, matched without regard to ASCII case, as SQL
    /// identifiers are.
    pub fn get(&self, column: &str) -> Option<&Value> {
        self.index(column).map(|i| &self.values[i])
    }

    pub(crate) fn index(&self, column: &str) -> Option<usize> {
        self.columns
            .iter()
            .position(|c| c.eq_ignore_ascii_case(column))
    }
}

/// A value displayed as a SQL literal.
pub(crate) struct Sql<'a>(pub &'a Value);

impl fmt::Display for Sql<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        literal(self.0, f)
    }
}

/// Writes a value as a SQL literal.
fn literal(value: &Value, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    match value {
        Value::Null => f.write_str("NULL"),
        Value::Integer(i) => write!(f, "{i}"),
        Value::Real(r) => write!(f, "{r:?}"),
        Value::Text(t) => write!(f, "'{}'", t.replace('\'', "''")),
        Value::Blob(b) => {
            f.write_str("x'")?;
            for byte in b {
                write!(f, "{byte:02x}")?;
            }
            f.write_str("'")
        }
    }
}

impl fmt::Debug for Row {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("{")?;
        for (i, (column, value)) in self.columns.iter().zip(&self.values).enumerate() {
            if i > 0 {
                f.write_str(", ")?;
            }
            write!(f, "{column}: ")?;
            literal(value, f)?;
        }
        f.write_str("}")
    }
}
