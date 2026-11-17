//! Random queries: filters, joins, subqueries, aggregates, views and CTEs,
//! with `ORDER BY`, `LIMIT` and `OFFSET`.
//!
//! Shapes added after the first draw from their own streams, so every query
//! an existing seed drew from `prng` stays the same until one of them
//! replaces it.

use super::schema::{FullText, SchemaSpec, TableKind, TableSpec};
use super::writes::{value, word};
use crate::prng::Prng;
use rusqlite::types::Value;

#[derive(Clone, Debug)]
pub struct QuerySpec {
    pub sql: String,
    pub params: Vec<Value>,
    /// The `ORDER BY` fixes every row's position, so results compare in
    /// order; otherwise they compare as multisets.
    pub ordered: bool,
    /// Leading result columns whose values SQL leaves open, as it leaves a
    /// group's value to be any member's. Results compare without them.
    pub open: usize,
    pub shape: Shape,
}

/// What a query exercises, for the run's summary.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Shape {
    /// Columns qualified by a table alias, and joins with `ON`.
    Qualified,
    /// Column names without a qualifier.
    BareNames,
    /// An `ORDER BY` name the result gives to another column, or a column
    /// in parentheses.
    OrderByName,
    /// A `USING` or `NATURAL` join, in the query or in a view it reads.
    NamedJoin,
    /// A `NaN` parameter, which SQLite binds as `NULL`.
    Nan,
    /// The database's own state: a pragma function or `dbstat`.
    EngineState,
    /// A full-text table: its index, its vocabulary, rows it reads through
    /// a view, or a join with it by column name.
    FullText,
}

impl Shape {
    pub const ALL: [Shape; 7] = [
        Shape::Qualified,
        Shape::BareNames,
        Shape::OrderByName,
        Shape::NamedJoin,
        Shape::Nan,
        Shape::EngineState,
        Shape::FullText,
    ];
}

/// A stream for each shape that can replace a query, so tuning one shape
/// leaves the others' draws as they were.
pub struct Shapes {
    bare_names: Prng,
    order_by_names: Prng,
    named_joins: Prng,
    nan: Prng,
    engine_state: Prng,
    full_text: Prng,
}

impl Shapes {
    pub fn from_seed(seed: u64) -> Shapes {
        Shapes {
            bare_names: Prng::stream(seed, "live query bare names"),
            order_by_names: Prng::stream(seed, "live query order by names"),
            named_joins: Prng::stream(seed, "live query named joins"),
            nan: Prng::stream(seed, "live query nan"),
            engine_state: Prng::stream(seed, "live query engine state"),
            full_text: Prng::stream(seed, "live query full text"),
        }
    }

    /// `query`, or a query of one of these shapes in its place. Every
    /// stream draws its chance for every query.
    fn replace(&mut self, query: QuerySpec, schema: &SchemaSpec) -> QuerySpec {
        let chosen = [
            self.bare_names.chance(0.06),
            self.order_by_names.chance(0.06),
            self.named_joins.chance(0.1),
            self.nan.chance(0.03),
            self.engine_state.chance(0.02),
            self.full_text.chance(0.15),
        ];
        match (chosen, &schema.full_text) {
            ([true, ..], _) => bare_names(&mut self.bare_names, schema),
            ([_, true, ..], _) => order_by_name(&mut self.order_by_names, schema),
            ([_, _, true, ..], _) => named_join(&mut self.named_joins, schema),
            ([_, _, _, true, ..], _) => nan(&mut self.nan, schema),
            ([_, _, _, _, true, _], _) => engine_state(&mut self.engine_state),
            ([.., true], Some(tables)) => full_text(&mut self.full_text, schema, tables),
            _ => query,
        }
    }
}

/// Whether a view's first column is the value of a group.
fn groups(view: &str) -> bool {
    view.contains(" GROUP BY ")
}

/// A column as a query names it: under the table's alias, or bare when the
/// table has none.
fn named(alias: &str, column: &str) -> String {
    if alias.is_empty() {
        column.to_owned()
    } else {
        format!("{alias}.{column}")
    }
}

/// A table in a `FROM` clause, under `alias` if it has one.
fn source(table: &TableSpec, alias: &str) -> String {
    if alias.is_empty() {
        table.name.clone()
    } else {
        format!("{} AS {alias}", table.name)
    }
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
                format!("{} COLLATE {collation}", named(alias, &column.name))
            })
            .collect(),
        _ => vec![named(alias, "rowid")],
    }
}

/// A predicate on one column of `table`, which the query calls `alias`.
fn predicate(
    prng: &mut Prng,
    extra: Option<&mut Prng>,
    table: &TableSpec,
    alias: &str,
    params: &mut Vec<Value>,
) -> String {
    let mut drawn = Vec::new();
    let first = first_predicate(prng, table, alias, &mut drawn);
    if let Some(extra) = extra
        && extra.chance(0.25)
    {
        return more_predicate(extra, table, alias, params);
    }
    params.extend(drawn);
    first
}

/// Comparisons with the column on the right, explicit collations, and
/// `NULL` in a list.
fn more_predicate(
    prng: &mut Prng,
    table: &TableSpec,
    alias: &str,
    params: &mut Vec<Value>,
) -> String {
    let column = &table.columns[prng.index(table.columns.len())];
    let name = named(alias, &column.name);
    let mut param = |prng: &mut Prng| {
        params.push(value(prng, column, table.strict));
        "?".to_owned()
    };
    match prng.below(6) {
        0 => format!("{} = {name}", param(prng)),
        1 => format!("{} {} {name}", param(prng), *prng.pick(&["<", ">="])),
        2 => format!("{name} COLLATE RTRIM = {}", param(prng)),
        3 => format!("{name} = {} COLLATE BINARY", param(prng)),
        4 => format!("{name} IN ({}, NULL)", param(prng)),
        _ => format!("{name} <= {}", param(prng)),
    }
}

fn first_predicate(
    prng: &mut Prng,
    table: &TableSpec,
    alias: &str,
    params: &mut Vec<Value>,
) -> String {
    let column = &table.columns[prng.index(table.columns.len())];
    let name = named(alias, &column.name);
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
    mut extra: Option<&mut Prng>,
    table: &TableSpec,
    alias: &str,
    params: &mut Vec<Value>,
) -> Option<String> {
    let count = prng.below(4);
    (count > 0).then(|| {
        (0..count)
            .map(|_| predicate(prng, extra.as_deref_mut(), table, alias, params))
            .collect::<Vec<_>>()
            .join(" AND ")
    })
}

fn columns(prng: &mut Prng, table: &TableSpec, alias: &str) -> String {
    let star = named(alias, "*");
    match prng.below(5) {
        0 => star,
        1 => "count(*)".to_owned(),
        2 if table.kind != TableKind::WithoutRowid => {
            format!("{} AS row_id, {star}", named(alias, "rowid"))
        }
        _ => {
            let picked: Vec<String> = table
                .columns
                .iter()
                .filter(|_| prng.chance(0.5))
                .map(|c| named(alias, &c.name))
                .collect();
            if picked.is_empty() {
                named(alias, &table.columns[0].name)
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
    extra: &mut Prng,
    table: &TableSpec,
    alias: &str,
    params: &mut Vec<Value>,
) -> (String, bool) {
    let mut clause = String::new();
    let mut ordered = false;
    if prng.chance(0.6) {
        let column = &table.columns[prng.index(table.columns.len())];
        let (direction, nulls) = (
            *prng.pick(&["", " ASC", " DESC"]),
            *prng.pick(&["", "", " NULLS FIRST", " NULLS LAST"]),
        );
        let collate = *extra.pick(&[
            "",
            "",
            " COLLATE NOCASE",
            " COLLATE BINARY",
            " COLLATE RTRIM",
        ]);
        let mut terms = vec![format!(
            "{}{collate}{direction}{nulls}",
            named(alias, &column.name)
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
        let count = if prng.chance(0.5) {
            params.push(Value::Integer(limit));
            "?".to_owned()
        } else {
            limit.to_string()
        };
        if prng.chance(0.3) {
            let offset = prng.range(0..=3);
            if extra.chance(0.4) {
                clause.push_str(&format!(" LIMIT {offset}, {count}"));
            } else {
                clause.push_str(&format!(" LIMIT {count} OFFSET {offset}"));
            }
        } else {
            clause.push_str(&format!(" LIMIT {count}"));
        }
    }
    (clause, ordered)
}

/// A query. `extra` draws the listings, and `shapes` the shapes added after
/// them.
pub fn query(
    prng: &mut Prng,
    extra: &mut Prng,
    shapes: &mut Shapes,
    schema: &SchemaSpec,
) -> QuerySpec {
    let first = first_query(prng, extra, schema);
    let query = if extra.chance(1.0 / 6.0) {
        listing(extra, schema, "a")
    } else {
        first
    };
    shapes.replace(query, schema)
}

/// A listing ordered by one column and cut by a `LIMIT`, as a feed shows
/// its newest rows.
fn listing(prng: &mut Prng, schema: &SchemaSpec, alias: &str) -> QuerySpec {
    let table = &schema.tables[prng.index(schema.tables.len())];
    let mut params = Vec::new();
    let mut sql = format!("SELECT {} FROM {}", named(alias, "*"), source(table, alias));
    if let Some(filter) = filter(prng, None, table, alias, &mut params) {
        sql.push_str(&format!(" WHERE {filter}"));
    }
    let column = &table.columns[prng.index(table.columns.len())];
    let mut terms = vec![format!(
        "{}{}{}",
        named(alias, &column.name),
        *prng.pick(&["", " COLLATE NOCASE", " COLLATE RTRIM"]),
        *prng.pick(&[" ASC", " DESC", " DESC NULLS FIRST", " NULLS LAST"])
    )];
    terms.extend(tiebreak(table, alias));
    sql.push_str(&format!(" ORDER BY {}", terms.join(", ")));
    params.push(Value::Integer(prng.range(1..=8) as i64));
    if prng.chance(0.3) {
        sql.push_str(&format!(" LIMIT ? OFFSET {}", prng.range(1..=3)));
    } else {
        sql.push_str(" LIMIT ?");
    }
    QuerySpec {
        sql,
        params,
        ordered: true,
        open: 0,
        shape: Shape::Qualified,
    }
}

/// One table, the shape constraint extraction handles.
fn one_table(prng: &mut Prng, extra: &mut Prng, table: &TableSpec, alias: &str) -> QuerySpec {
    let mut params = Vec::new();
    let columns = columns(prng, table, alias);
    let mut sql = format!("SELECT {columns} FROM {}", source(table, alias));
    if let Some(filter) = filter(prng, Some(&mut *extra), table, alias, &mut params) {
        sql.push_str(&format!(" WHERE {filter}"));
    }
    let aggregate = columns == "count(*)";
    let (clause, ordered) = if aggregate {
        (String::new(), false)
    } else {
        order_and_limit(prng, extra, table, alias, &mut params)
    };
    sql.push_str(&clause);
    QuerySpec {
        sql,
        params,
        ordered: ordered || aggregate,
        open: 0,
        shape: Shape::Qualified,
    }
}

/// A one-table query or a listing that names its columns without a
/// qualifier.
fn bare_names(prng: &mut Prng, schema: &SchemaSpec) -> QuerySpec {
    let query = if prng.chance(0.5) {
        listing(prng, schema, "")
    } else {
        let table = &schema.tables[prng.index(schema.tables.len())];
        let mut extra = prng.fork();
        one_table(prng, &mut extra, table, "")
    };
    QuerySpec {
        shape: Shape::BareNames,
        ..query
    }
}

/// A `LIMIT` query whose first `ORDER BY` term is a bare name, plain or in
/// parentheses, quotes or a `COLLATE`. In two of three the result gives
/// that name to another column, which SQLite then sorts by.
fn order_by_name(prng: &mut Prng, schema: &SchemaSpec) -> QuerySpec {
    let table = &schema.tables[prng.index(schema.tables.len())];
    let count = table.columns.len();
    let sorted = prng.index(count);
    let shown = (sorted + 1 + prng.index(count - 1)) % count;
    let (sorted, shown) = (&table.columns[sorted].name, &table.columns[shown].name);
    let mut params = Vec::new();
    let columns = if prng.chance(2.0 / 3.0) {
        format!("a.{shown} AS {sorted}, a.{sorted} AS other")
    } else {
        "a.*".to_owned()
    };
    let mut sql = format!("SELECT {columns} FROM {} AS a", table.name);
    if let Some(filter) = filter(prng, None, table, "a", &mut params) {
        sql.push_str(&format!(" WHERE {filter}"));
    }
    let term = match prng.below(6) {
        0 => sorted.clone(),
        1 => format!("({sorted})"),
        2 => format!("(({sorted}))"),
        3 => format!("({sorted}) COLLATE {}", *prng.pick(&["NOCASE", "BINARY"])),
        4 => format!("\"{sorted}\""),
        _ => format!("({sorted} COLLATE RTRIM)"),
    };
    let mut terms = vec![format!(
        "{term}{}",
        *prng.pick(&["", " DESC", " DESC NULLS FIRST", " NULLS LAST"])
    )];
    terms.extend(tiebreak(table, "a"));
    sql.push_str(&format!(" ORDER BY {}", terms.join(", ")));
    params.push(Value::Integer(prng.range(1..=6) as i64));
    sql.push_str(" LIMIT ?");
    QuerySpec {
        sql,
        params,
        ordered: true,
        open: 0,
        shape: Shape::OrderByName,
    }
}

/// A join by column name, `USING` or `NATURAL`, inner or outer, in the
/// query or in a view it reads. The tables share `c0` and `c1`.
fn named_join(prng: &mut Prng, schema: &SchemaSpec) -> QuerySpec {
    let shape = Shape::NamedJoin;
    if !schema.join_views.is_empty() && prng.chance(0.25) {
        let view = prng.index(schema.join_views.len());
        let columns = *prng.pick(&["*", "count(*) AS n"]);
        return QuerySpec {
            sql: format!("SELECT {columns} FROM j{view}"),
            params: Vec::new(),
            ordered: columns != "*",
            open: usize::from(columns == "*" && groups(&schema.join_views[view])),
            shape,
        };
    }
    let table = &schema.tables[prng.index(schema.tables.len())];
    let other = &schema.tables[prng.index(schema.tables.len())];
    let kind = *prng.pick(&["JOIN", "JOIN", "LEFT JOIN", "RIGHT JOIN", "FULL JOIN"]);
    let join = match prng.below(3) {
        0 => format!("{} AS a NATURAL {kind} {} AS b", table.name, other.name),
        1 => format!("{} AS a {kind} {} AS b USING (c0)", table.name, other.name),
        _ => format!(
            "{} AS a {kind} {} AS b USING (c1, c0)",
            table.name, other.name
        ),
    };
    let left = &table.columns[prng.index(table.columns.len())].name;
    let right = &other.columns[prng.index(other.columns.len())].name;
    let columns = match prng.below(4) {
        0 => "count(*) AS n".to_owned(),
        1 => "*".to_owned(),
        2 => format!("c0, a.{left} AS x, b.{right} AS y"),
        _ => format!("a.{left} AS x, b.{right} AS y"),
    };
    let mut params = Vec::new();
    let mut sql = format!("SELECT {columns} FROM {join}");
    let mut filters = Vec::new();
    if let Some(f) = filter(prng, None, table, "a", &mut params) {
        filters.push(f);
    }
    if prng.chance(0.4) {
        filters.push(predicate(prng, None, other, "b", &mut params));
    }
    if !filters.is_empty() {
        sql.push_str(&format!(" WHERE {}", filters.join(" AND ")));
    }
    QuerySpec {
        ordered: columns == "count(*) AS n",
        sql,
        params,
        open: 0,
        shape,
    }
}

/// A filter that compares a column with a `NaN` parameter.
fn nan(prng: &mut Prng, schema: &SchemaSpec) -> QuerySpec {
    let table = &schema.tables[prng.index(schema.tables.len())];
    let column = &table.columns[prng.index(table.columns.len())];
    let name = format!("a.{}", column.name);
    let mut params = vec![Value::Real(f64::NAN)];
    let test = match prng.below(6) {
        0..=2 => format!("{name} IS ?"),
        3 => format!("? IS {name}"),
        4 => format!("{name} = ?"),
        _ => {
            params.push(value(prng, column, table.strict));
            format!("{name} IN (?, ?)")
        }
    };
    let mut sql = format!("SELECT a.* FROM {} AS a WHERE {test}", table.name);
    if let Some(filter) = filter(prng, None, table, "a", &mut params) {
        sql.push_str(&format!(" AND {filter}"));
    }
    QuerySpec {
        sql,
        params,
        ordered: false,
        open: 0,
        shape: Shape::Nan,
    }
}

/// One row of the database's own state, which changes without row events.
fn engine_state(prng: &mut Prng) -> QuerySpec {
    let sql = *prng.pick(&[
        "SELECT count(*) AS pages, sum(ncell) AS cells, sum(payload) AS payload FROM dbstat",
        "SELECT sum(ncell) AS cells FROM dbstat WHERE pagetype = 'leaf'",
        "SELECT page_count FROM pragma_page_count",
        "SELECT freelist_count FROM pragma_freelist_count",
    ]);
    QuerySpec {
        sql: sql.to_owned(),
        params: Vec::new(),
        ordered: true,
        open: 0,
        shape: Shape::EngineState,
    }
}

fn first_query(prng: &mut Prng, extra: &mut Prng, schema: &SchemaSpec) -> QuerySpec {
    let t = prng.index(schema.tables.len());
    let table = &schema.tables[t];
    let mut params = Vec::new();
    let shape = Shape::Qualified;
    match prng.below(10) {
        0..=4 => one_table(prng, extra, table, "a"),
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
            if let Some(f) = filter(prng, Some(&mut *extra), table, "a", &mut params) {
                filters.push(f);
            }
            if prng.chance(0.4) {
                filters.push(predicate(prng, Some(&mut *extra), other, "b", &mut params));
            }
            if !filters.is_empty() {
                sql.push_str(&format!(" WHERE {}", filters.join(" AND ")));
            }
            QuerySpec {
                sql,
                params,
                ordered: false,
                open: 0,
                shape,
            }
        }
        // A subquery in the filter.
        7 => {
            let u = prng.index(schema.tables.len());
            let other = &schema.tables[u];
            let left = &table.columns[prng.index(table.columns.len())].name;
            let right = &other.columns[prng.index(other.columns.len())].name;
            let mut inner = format!("SELECT b.{right} FROM {} AS b", other.name);
            if let Some(f) = filter(prng, Some(&mut *extra), other, "b", &mut params) {
                inner.push_str(&format!(" WHERE {f}"));
            }
            let mut sql = format!(
                "SELECT a.* FROM {} AS a WHERE a.{left} IN ({inner})",
                table.name
            );
            if let Some(f) = filter(prng, Some(&mut *extra), table, "a", &mut params) {
                sql.push_str(&format!(" AND {f}"));
            }
            QuerySpec {
                sql,
                params,
                ordered: false,
                open: 0,
                shape,
            }
        }
        // An aggregate per group.
        8 => {
            let group = &table.columns[prng.index(table.columns.len())].name;
            let mut sql = format!("SELECT a.{group}, count(*) AS n FROM {} AS a", table.name);
            if let Some(f) = filter(prng, Some(&mut *extra), table, "a", &mut params) {
                sql.push_str(&format!(" WHERE {f}"));
            }
            sql.push_str(&format!(" GROUP BY a.{group}"));
            QuerySpec {
                sql,
                params,
                ordered: false,
                open: 1,
                shape,
            }
        }
        // A view, or a CTE over the table.
        _ => {
            let mut open = 0;
            let sql = if !schema.views.is_empty() && prng.chance(0.5) {
                let view = prng.index(schema.views.len());
                open = usize::from(groups(&schema.views[view]));
                format!("SELECT * FROM v{view}")
            } else {
                let mut inner = format!("SELECT * FROM {} AS a", table.name);
                if let Some(f) = filter(prng, Some(&mut *extra), table, "a", &mut params) {
                    inner.push_str(&format!(" WHERE {f}"));
                }
                format!("WITH q AS ({inner}) SELECT * FROM q")
            };
            QuerySpec {
                sql,
                params,
                ordered: false,
                open,
                shape,
            }
        }
    }
}

/// A query of the full-text tables.
fn full_text(prng: &mut Prng, schema: &SchemaSpec, tables: &FullText) -> QuerySpec {
    let mut params = Vec::new();
    let (sql, ordered) = match (prng.below(6), &tables.mirrored) {
        // The vocabulary keeps no rows of its own.
        (0 | 1, _) => (
            "SELECT term, doc, cnt FROM words ORDER BY term".to_owned(),
            true,
        ),
        (2, _) => {
            params.push(word(prng));
            (
                "SELECT rowid, c0 FROM docs WHERE docs MATCH ? ORDER BY rowid".to_owned(),
                true,
            )
        }
        // The join names no column of `docs` but the one it joins by,
        // which the authorizer never hears of.
        (3, _) => {
            let table = &schema.tables[prng.index(schema.tables.len())];
            let columns = *prng.pick(&["a.c1 AS x", "count(*) AS n"]);
            (
                format!(
                    "SELECT {columns} FROM {} AS a JOIN docs AS d USING (c0)",
                    table.name
                ),
                columns != "a.c1 AS x",
            )
        }
        // The rows come from the view, whatever the index holds.
        (_, Some(_)) => (
            "SELECT rowid, c0 FROM mirror ORDER BY rowid".to_owned(),
            true,
        ),
        (_, None) => ("SELECT count(*) AS n FROM docs".to_owned(), true),
    };
    QuerySpec {
        sql,
        params,
        ordered,
        open: 0,
        shape: Shape::FullText,
    }
}
