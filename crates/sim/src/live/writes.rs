//! Random write transactions: inserts with every conflict policy, updates
//! that move keys, deletes, savepoints, failing statements, and occasional
//! schema changes.

use super::schema::{ColumnSpec, SchemaSpec, TableKind, TableSpec};
use crate::prng::Prng;
use rusqlite::types::Value;

/// One statement and its parameters.
#[derive(Clone, Debug)]
pub struct Statement {
    pub sql: String,
    pub params: Vec<Value>,
}

impl Statement {
    fn plain(sql: impl Into<String>) -> Statement {
        Statement {
            sql: sql.into(),
            params: Vec::new(),
        }
    }
}

/// What a write's closure does when a statement fails.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum OnError {
    /// Carry on: SQLite has rolled back just the failed statement.
    Continue,
    /// Return the error, rolling the transaction back.
    Abort,
}

#[derive(Clone, Debug)]
pub struct Transaction {
    pub statements: Vec<Statement>,
    pub on_error: OnError,
    /// The closure returns an error after its statements.
    pub fail_at_end: bool,
}

/// A small domain, so keys collide, collations matter, and values cross
/// storage classes.
pub fn value(prng: &mut Prng, column: &ColumnSpec, strict: bool) -> Value {
    if !column.not_null && prng.chance(0.1) {
        return Value::Null;
    }
    if column.references.is_some() {
        return Value::Integer(prng.range(1..=8) as i64);
    }
    let class = if strict {
        match column.declared {
            "INTEGER" => 0,
            "REAL" => *prng.pick(&[0, 1]),
            "TEXT" => 2,
            "BLOB" => 3,
            _ => prng.below(4),
        }
    } else {
        prng.below(4)
    };
    match class {
        0 => Value::Integer(prng.range(0..=12) as i64 - 2),
        1 => Value::Real(*prng.pick(&[0.0, -0.0, 1.0, 1.5, 2.0, -2.5, 1e20, 3.0])),
        2 => Value::Text(
            (*prng.pick(&[
                "a", "A", "b", "B", "a ", "ab", "Ab", "AB", "10", "1.5", "", "x", " 3",
            ]))
            .to_owned(),
        ),
        _ => Value::Blob(prng.pick(&[&b""[..], b"\x00", b"\x01\x02", b"a"]).to_vec()),
    }
}

fn key_value(prng: &mut Prng) -> Value {
    Value::Integer(prng.range(1..=10) as i64)
}

/// A predicate on one column, with its parameters.
fn predicate(prng: &mut Prng, table: &TableSpec) -> (String, Vec<Value>) {
    let columns: Vec<usize> = (0..table.columns.len()).collect();
    let column = &table.columns[*prng.pick(&columns)];
    match prng.below(6) {
        0 => ("1".into(), Vec::new()),
        1 if table.has_rowid() => ("rowid = ?".into(), vec![key_value(prng)]),
        2 => (
            format!("{} < ?", column.name),
            vec![value(prng, column, table.strict)],
        ),
        3 => (format!("{} IS NULL", column.name), Vec::new()),
        _ => (
            format!("{} = ?", column.name),
            vec![value(prng, column, table.strict)],
        ),
    }
}

pub fn statement(prng: &mut Prng, schema: &SchemaSpec) -> Statement {
    let table = &schema.tables[prng.index(schema.tables.len())];
    let writable = table.writable();
    match prng.below(10) {
        0..=3 => {
            let mut columns = Vec::new();
            let mut params = Vec::new();
            for &i in &writable {
                let column = &table.columns[i];
                let required = column.not_null || table.key.contains(&i);
                if required || prng.chance(0.7) {
                    columns.push(column.name.clone());
                    params.push(if table.kind == TableKind::IntegerKey && i == 0 {
                        if prng.chance(0.2) {
                            Value::Null
                        } else {
                            key_value(prng)
                        }
                    } else {
                        value(prng, column, table.strict)
                    });
                }
            }
            if columns.is_empty() {
                return Statement::plain(format!("INSERT INTO {} DEFAULT VALUES", table.name));
            }
            let verb = *prng.pick(&["INSERT", "INSERT OR REPLACE", "INSERT OR IGNORE", "REPLACE"]);
            let marks = vec!["?"; columns.len()].join(", ");
            let mut sql = format!(
                "{verb} INTO {} ({}) VALUES ({marks})",
                table.name,
                columns.join(", ")
            );
            if verb == "INSERT" && prng.chance(0.3) {
                let target = &columns[prng.index(columns.len())];
                sql.push_str(&format!(
                    " ON CONFLICT DO UPDATE SET {target} = excluded.{target}"
                ));
            }
            Statement { sql, params }
        }
        4..=6 => {
            let column = &table.columns[*prng.pick(&writable)];
            let (filter, mut params) = predicate(prng, table);
            let set =
                if table.kind == TableKind::IntegerKey && column.name == "id" && prng.chance(0.5) {
                    "id = id + 3".to_owned()
                } else {
                    params.insert(0, value(prng, column, table.strict));
                    format!("{} = ?", column.name)
                };
            let verb = *prng.pick(&["UPDATE", "UPDATE OR REPLACE", "UPDATE OR IGNORE"]);
            Statement {
                sql: format!("{verb} {} SET {set} WHERE {filter}", table.name),
                params,
            }
        }
        7 | 8 => {
            let (filter, params) = predicate(prng, table);
            Statement {
                sql: format!("DELETE FROM {} WHERE {filter}", table.name),
                params,
            }
        }
        _ => Statement::plain(format!("DELETE FROM {}", table.name)),
    }
}

pub fn transaction(prng: &mut Prng, schema: &SchemaSpec) -> Transaction {
    let mut statements = Vec::new();
    let mut savepoints = 0;
    for _ in 0..prng.range(1..=8) {
        match prng.below(12) {
            0 => {
                statements.push(Statement::plain(format!("SAVEPOINT s{savepoints}")));
                savepoints += 1;
            }
            1 if savepoints > 0 => {
                let target = prng.below(savepoints);
                let verb = *prng.pick(&["ROLLBACK TO", "RELEASE"]);
                statements.push(Statement::plain(format!("{verb} s{target}")));
                // Both cancel the savepoints opened after the target.
                savepoints = if verb == "RELEASE" {
                    target
                } else {
                    target + 1
                };
            }
            _ => statements.push(statement(prng, schema)),
        }
    }
    Transaction {
        statements,
        on_error: if prng.chance(0.7) {
            OnError::Continue
        } else {
            OnError::Abort
        },
        fail_at_end: prng.chance(0.05),
    }
}

/// A schema change: a new column, a new index, or `ANALYZE`.
pub fn schema_change(prng: &mut Prng, schema: &mut SchemaSpec, serial: u64) -> Statement {
    let t = prng.index(schema.tables.len());
    match prng.below(3) {
        0 => {
            let table = &mut schema.tables[t];
            let name = format!("a{serial}");
            let declared = if table.strict { "ANY" } else { "TEXT" };
            table.columns.push(ColumnSpec {
                name: name.clone(),
                declared,
                collation: None,
                generated: super::schema::Generated::No,
                not_null: false,
                references: None,
            });
            Statement::plain(format!(
                "ALTER TABLE {} ADD COLUMN {name} {declared}",
                table.name
            ))
        }
        1 => {
            let table = &schema.tables[t];
            let column = &table.columns[*prng.pick(&table.writable())].name;
            Statement::plain(format!(
                "CREATE INDEX IF NOT EXISTS x{serial} ON {} ({column})",
                table.name
            ))
        }
        _ => Statement::plain("ANALYZE"),
    }
}
