//! Random queries: filters, joins, subqueries, aggregates, views and CTEs,
//! with `ORDER BY`, `LIMIT` and `OFFSET`.

use super::schema::{SchemaSpec, TableKind, TableSpec};
use super::writes::value;
use crate::prng::Prng;
use rusqlite::types::Value;

#[derive(Clone, Debug)]
pub struct QuerySpec {
    pub sql: String,
    pub params: Vec<Value>,
    /// The `ORDER BY` fixes every row's position, so results compare in
    /// order; otherwise they compare as multisets.
    pub ordered: bool,
}

/// The terms that order a table's rows completely. Key columns compare
/// under the key's own collations, under which the key is unique.
fn tiebreak(table: &TableSpec, alias: &str) -> Vec<String> {
    match table.kind {
        TableKind::WithoutRowid => table
            .key
            .iter()
            .zip(&table.key_collations)
            .map(|(&k, collation)| {
                let column = &table.columns[k];
                let collation = collation.or(column.collation).unwrap_or("BINARY");
                format!("{alias}.{} COLLATE {collation}", column.name)
            })
            .collect(),
        _ => vec![format!("{alias}.rowid")],
    }
}

/// A predicate on one column of `table`, which the query calls `alias`.
fn predicate(prng: &mut Prng, table: &TableSpec, alias: &str, params: &mut Vec<Value>) -> String {
    let column = &table.columns[prng.index(table.columns.len())];
    let name = format!("{alias}.{}", column.name);
    let mut param = |prng: &mut Prng| {
        params.push(value(prng, column, table.strict));
        "?".to_owned()
    };
    match prng.below(16) {
        0..=3 => format!("{name} = {}", param(prng)),
        4 => format!("{name} IN ({}, {})", param(prng), param(prng)),
        5 => format!("{name} < {}", param(prng)),
        6 => format!("{name} >= {}", param(prng)),
        7 => format!("{name} BETWEEN {} AND {}", param(prng), param(prng)),
        8 => format!("{name} IS NULL"),
        9 => format!("{name} IS {}", param(prng)),
        10 => format!("{name} = {} COLLATE NOCASE", param(prng)),
        11 => format!("{name} LIKE {}", param(prng)),
        12 => format!("({name} = {} OR {name} IS NULL)", param(prng)),
        13 => format!("lower({name}) = {}", param(prng)),
        14 => format!(
            "{name} = {}",
            *prng.pick(&["1", "'a'", "2.0", "NULL", "-1"])
        ),
        _ => format!("{name} IS NOT NULL"),
    }
}

fn filter(
    prng: &mut Prng,
    table: &TableSpec,
    alias: &str,
    params: &mut Vec<Value>,
) -> Option<String> {
    let count = prng.below(4);
    (count > 0).then(|| {
        (0..count)
            .map(|_| predicate(prng, table, alias, params))
            .collect::<Vec<_>>()
            .join(" AND ")
    })
}

fn columns(prng: &mut Prng, table: &TableSpec, alias: &str) -> String {
    match prng.below(5) {
        0 => format!("{alias}.*"),
        1 => "count(*)".to_owned(),
        2 if table.kind != TableKind::WithoutRowid => format!("{alias}.rowid AS row_id, {alias}.*"),
        _ => {
            let picked: Vec<String> = table
                .columns
                .iter()
                .filter(|_| prng.chance(0.5))
                .map(|c| format!("{alias}.{}", c.name))
                .collect();
            if picked.is_empty() {
                format!("{alias}.{}", table.columns[0].name)
            } else {
                picked.join(", ")
            }
        }
    }
}

/// `ORDER BY` and `LIMIT` for a query over `table`, and whether the order
/// is total.
fn order_and_limit(
    prng: &mut Prng,
    table: &TableSpec,
    alias: &str,
    params: &mut Vec<Value>,
) -> (String, bool) {
    let mut clause = String::new();
    let mut ordered = false;
    if prng.chance(0.6) {
        let column = &table.columns[prng.index(table.columns.len())];
        let mut terms = vec![format!(
            "{alias}.{}{}{}",
            column.name,
            *prng.pick(&["", " ASC", " DESC"]),
            *prng.pick(&["", "", " NULLS FIRST", " NULLS LAST"])
        )];
        if prng.chance(0.9) {
            terms.extend(tiebreak(table, alias));
            ordered = true;
        }
        clause.push_str(&format!(" ORDER BY {}", terms.join(", ")));
    }
    // Without a total order, a limit picks rows the planner chooses.
    if ordered && prng.chance(0.5) {
        let limit = prng.range(0..=6) as i64;
        if prng.chance(0.5) {
            params.push(Value::Integer(limit));
            clause.push_str(" LIMIT ?");
        } else {
            clause.push_str(&format!(" LIMIT {limit}"));
        }
        if prng.chance(0.3) {
            clause.push_str(&format!(" OFFSET {}", prng.range(0..=3)));
        }
    }
    (clause, ordered)
}

pub fn query(prng: &mut Prng, schema: &SchemaSpec) -> QuerySpec {
    let t = prng.index(schema.tables.len());
    let table = &schema.tables[t];
    let mut params = Vec::new();
    match prng.below(10) {
        // One table, the shape constraint extraction handles.
        0..=4 => {
            let columns = columns(prng, table, "a");
            let mut sql = format!("SELECT {columns} FROM {} AS a", table.name);
            if let Some(filter) = filter(prng, table, "a", &mut params) {
                sql.push_str(&format!(" WHERE {filter}"));
            }
            let aggregate = columns == "count(*)";
            let (clause, ordered) = if aggregate {
                (String::new(), false)
            } else {
                order_and_limit(prng, table, "a", &mut params)
            };
            sql.push_str(&clause);
            QuerySpec {
                sql,
                params,
                ordered: ordered || aggregate,
            }
        }
        // A join, inner or left, possibly of a table with itself.
        5 | 6 => {
            let u = prng.index(schema.tables.len());
            let other = &schema.tables[u];
            let left = &table.columns[prng.index(table.columns.len())].name;
            let right = &other.columns[prng.index(other.columns.len())].name;
            let join = *prng.pick(&["JOIN", "LEFT JOIN"]);
            let mut sql = format!(
                "SELECT a.*, b.{right} AS b_{right} FROM {} AS a {join} {} AS b ON a.{left} = b.{right}",
                table.name, other.name
            );
            let mut filters = Vec::new();
            if let Some(f) = filter(prng, table, "a", &mut params) {
                filters.push(f);
            }
            if prng.chance(0.4) {
                filters.push(predicate(prng, other, "b", &mut params));
            }
            if !filters.is_empty() {
                sql.push_str(&format!(" WHERE {}", filters.join(" AND ")));
            }
            QuerySpec {
                sql,
                params,
                ordered: false,
            }
        }
        // A subquery in the filter.
        7 => {
            let u = prng.index(schema.tables.len());
            let other = &schema.tables[u];
            let left = &table.columns[prng.index(table.columns.len())].name;
            let right = &other.columns[prng.index(other.columns.len())].name;
            let mut inner = format!("SELECT b.{right} FROM {} AS b", other.name);
            if let Some(f) = filter(prng, other, "b", &mut params) {
                inner.push_str(&format!(" WHERE {f}"));
            }
            let mut sql = format!(
                "SELECT a.* FROM {} AS a WHERE a.{left} IN ({inner})",
                table.name
            );
            if let Some(f) = filter(prng, table, "a", &mut params) {
                sql.push_str(&format!(" AND {f}"));
            }
            QuerySpec {
                sql,
                params,
                ordered: false,
            }
        }
        // An aggregate per group.
        8 => {
            let group = &table.columns[prng.index(table.columns.len())].name;
            let mut sql = format!("SELECT a.{group}, count(*) AS n FROM {} AS a", table.name);
            if let Some(f) = filter(prng, table, "a", &mut params) {
                sql.push_str(&format!(" WHERE {f}"));
            }
            sql.push_str(&format!(" GROUP BY a.{group}"));
            QuerySpec {
                sql,
                params,
                ordered: false,
            }
        }
        // A view, or a CTE over the table.
        _ => {
            let sql = if !schema.views.is_empty() && prng.chance(0.5) {
                format!("SELECT * FROM v{}", prng.index(schema.views.len()))
            } else {
                let mut inner = format!("SELECT * FROM {} AS a", table.name);
                if let Some(f) = filter(prng, table, "a", &mut params) {
                    inner.push_str(&format!(" WHERE {f}"));
                }
                format!("WITH q AS ({inner}) SELECT * FROM q")
            };
            QuerySpec {
                sql,
                params,
                ordered: false,
            }
        }
    }
}
