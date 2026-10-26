//! Row constraints from a query's SQL.
//!
//! For each table a query reads, the analysis finds the conditions every row
//! the result depends on must meet: the top-level `AND`ed `WHERE` predicates
//! that compare one of the table's columns with a constant or parameter, and,
//! for a `LIMIT` query ordered by a column, a bound at the window's last row.
//! A change whose old and new rows both fail a table's conditions cannot
//! change the result.
//!
//! Everything it cannot model keeps the table unconstrained: subqueries,
//! views, CTEs, compound selects, the nullable side of an outer join, `OR`,
//! expressions over columns, collations other than the built-in ones, and
//! text constants that may parse as numbers in ways SQLite's numeric affinity
//! would treat differently. Dropping a conjunct only widens what matches, so
//! every simplification errs toward re-running the query.

use crate::compare::{Affinity, Collation, compare};
use crate::event::{Row, Sql};
use crate::params::ParamValues;
use crate::schema::{Schema, Table};
use rusqlite::types::Value;
use rusqlite::{Connection, Statement};
use sqlite3_parser::ast::{
    As, Cmd, Expr, FromClause, JoinOperator, JoinType, Literal, NullsOrder, OneSelect, Operator,
    ResultColumn, Select, SelectTable, SortOrder, Stmt, UnaryOperator,
};
use sqlite3_parser::lexer::Scanner;
use sqlite3_parser::lexer::sql::{Parser, TokenType, Tokenizer};
use sqlite3_parser::{Bump, FallibleIterator};
use std::cmp::Ordering;
use std::collections::BTreeMap;
use std::fmt;

/// A condition on one column's value.
#[derive(Clone, Debug, PartialEq)]
pub enum Condition {
    /// Equal to one of these values, none of them `NULL`.
    OneOf(Vec<Value>),
    /// `IS value`: equal, with `NULL` matching `NULL`.
    Is(Value),
    /// Above `low` and below `high`; each bound is inclusive when its flag
    /// is set. Neither bound is `NULL`.
    Range {
        low: Option<(Value, bool)>,
        high: Option<(Value, bool)>,
    },
    IsNull,
    NotNull,
    /// No row satisfies the query's filter, as with `x = NULL` or `LIMIT 0`.
    Never,
    /// At or before `boundary` in `ORDER BY` order: inside a full `LIMIT`
    /// window, ties included.
    Window {
        boundary: Value,
        descending: bool,
        nulls_first: bool,
    },
}

/// A condition on a named column, compared under `collation`.
#[derive(Clone, Debug, PartialEq)]
pub struct Constraint {
    /// The column's name, in lowercase.
    pub column: String,
    pub collation: Collation,
    pub condition: Condition,
}

/// Conditions that all hold for every row of one table occurrence the
/// result depends on.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct Conjunction {
    pub constraints: Vec<Constraint>,
}

/// The rows of a table a result depends on.
#[derive(Clone, Debug, PartialEq)]
pub enum Filter {
    /// Any row.
    All,
    /// Rows meeting at least one conjunction: one per occurrence of the
    /// table in the query.
    AnyOf(Vec<Conjunction>),
}

/// Orders a value within `ORDER BY` order.
pub(crate) fn order_cmp(
    a: &Value,
    b: &Value,
    collation: &Collation,
    descending: bool,
    nulls_first: bool,
) -> Ordering {
    match (a, b) {
        (Value::Null, Value::Null) => Ordering::Equal,
        (Value::Null, _) => {
            if nulls_first {
                Ordering::Less
            } else {
                Ordering::Greater
            }
        }
        (_, Value::Null) => {
            if nulls_first {
                Ordering::Greater
            } else {
                Ordering::Less
            }
        }
        _ => {
            let order = compare(a, b, collation);
            if descending { order.reverse() } else { order }
        }
    }
}

impl Constraint {
    /// Whether `value` meets the condition, as SQLite's `WHERE` would judge
    /// the stored value against the converted constant. Text holding U+FFFD
    /// may have come from bytes that were not UTF-8, which the conversion
    /// changed, so it always may.
    pub fn accepts(&self, value: &Value) -> bool {
        if matches!(value, Value::Text(t) if t.contains('\u{FFFD}')) {
            return !matches!(self.condition, Condition::Never);
        }
        let collation = &self.collation;
        match &self.condition {
            Condition::OneOf(values) => {
                !matches!(value, Value::Null)
                    && values.iter().any(|v| compare(value, v, collation).is_eq())
            }
            Condition::Is(target) => match (value, target) {
                (Value::Null, Value::Null) => true,
                (Value::Null, _) | (_, Value::Null) => false,
                _ => compare(value, target, collation).is_eq(),
            },
            Condition::Range { low, high } => {
                if matches!(value, Value::Null) {
                    return false;
                }
                let above = low.as_ref().is_none_or(|(bound, inclusive)| {
                    let order = compare(value, bound, collation);
                    order.is_gt() || (*inclusive && order.is_eq())
                });
                let below = high.as_ref().is_none_or(|(bound, inclusive)| {
                    let order = compare(value, bound, collation);
                    order.is_lt() || (*inclusive && order.is_eq())
                });
                above && below
            }
            Condition::IsNull => matches!(value, Value::Null),
            Condition::NotNull => !matches!(value, Value::Null),
            Condition::Never => false,
            Condition::Window {
                boundary,
                descending,
                nulls_first,
            } => order_cmp(value, boundary, collation, *descending, *nulls_first).is_le(),
        }
    }

    /// Whether a row meets the condition. A row without the column, such as
    /// one leaving out a `VIRTUAL` generated column, might.
    pub fn matches(&self, row: &Row) -> bool {
        if matches!(self.condition, Condition::Never) {
            return false;
        }
        row.get(&self.column)
            .is_none_or(|value| self.accepts(value))
    }
}

impl Conjunction {
    pub fn matches(&self, row: &Row) -> bool {
        self.constraints.iter().all(|c| c.matches(row))
    }
}

impl Filter {
    /// Rows meeting any of `conjunctions`; any row when one of them is
    /// empty.
    pub(crate) fn any_of(conjunctions: Vec<Conjunction>) -> Filter {
        if conjunctions.is_empty() || conjunctions.iter().any(|c| c.constraints.is_empty()) {
            Filter::All
        } else {
            Filter::AnyOf(conjunctions)
        }
    }

    pub fn matches(&self, row: &Row) -> bool {
        match self {
            Filter::All => true,
            Filter::AnyOf(conjunctions) => conjunctions.iter().any(|c| c.matches(row)),
        }
    }
}

impl fmt::Display for Filter {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Filter::All => f.write_str("TRUE"),
            Filter::AnyOf(conjunctions) if conjunctions.len() == 1 => {
                write!(f, "{}", conjunctions[0])
            }
            Filter::AnyOf(conjunctions) => {
                let parts: Vec<String> = conjunctions.iter().map(|c| format!("({c})")).collect();
                f.write_str(&parts.join(" OR "))
            }
        }
    }
}

impl fmt::Display for Constraint {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let column = &self.column;
        let collate = match self.collation {
            Collation::Binary => String::new(),
            ref other => format!(" COLLATE {}", other.name()),
        };
        match &self.condition {
            Condition::OneOf(values) if values.len() == 1 => {
                write!(f, "{column}{collate} = {}", Sql(&values[0]))
            }
            Condition::OneOf(values) => {
                let list: Vec<String> = values.iter().map(|v| Sql(v).to_string()).collect();
                write!(f, "{column}{collate} IN ({})", list.join(", "))
            }
            Condition::Is(value) => write!(f, "{column}{collate} IS {}", Sql(value)),
            Condition::Range { low, high } => {
                let mut parts = Vec::new();
                if let Some((v, inclusive)) = low {
                    parts.push(format!(
                        "{column}{collate} {} {}",
                        if *inclusive { ">=" } else { ">" },
                        Sql(v)
                    ));
                }
                if let Some((v, inclusive)) = high {
                    parts.push(format!(
                        "{column}{collate} {} {}",
                        if *inclusive { "<=" } else { "<" },
                        Sql(v)
                    ));
                }
                f.write_str(&parts.join(" AND "))
            }
            Condition::IsNull => write!(f, "{column} IS NULL"),
            Condition::NotNull => write!(f, "{column} IS NOT NULL"),
            Condition::Never => f.write_str("FALSE"),
            // A `NULL` boundary admits only `NULL`s when they sort first,
            // and every row when they sort last.
            Condition::Window {
                boundary: Value::Null,
                nulls_first,
                ..
            } => {
                if *nulls_first {
                    write!(f, "{column} IS NULL")
                } else {
                    f.write_str("TRUE")
                }
            }
            Condition::Window {
                boundary,
                descending,
                nulls_first,
            } => {
                let bound = format!(
                    "{column}{collate} {} {}",
                    if *descending { ">=" } else { "<=" },
                    Sql(boundary)
                );
                if *nulls_first {
                    write!(f, "({column} IS NULL OR {bound})")
                } else {
                    f.write_str(&bound)
                }
            }
        }
    }
}

impl fmt::Display for Conjunction {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let parts: Vec<String> = self.constraints.iter().map(ToString::to_string).collect();
        f.write_str(&parts.join(" AND "))
    }
}

/// A `LIMIT` window whose last row bounds the rows the result depends on.
#[derive(Clone, Debug)]
pub(crate) struct WindowPlan {
    pub table: String,
    pub column: String,
    pub collation: Collation,
    pub descending: bool,
    pub nulls_first: bool,
    pub limit: u64,
}

/// What the analysis found, before the query runs.
#[derive(Debug, Default)]
pub(crate) struct Analysis {
    /// Per lowercase table name, a conjunction for each occurrence; tables
    /// missing here are unconstrained.
    pub tables: BTreeMap<String, Vec<Conjunction>>,
    pub window: Option<WindowPlan>,
}

/// A query text with every parameter written as an explicit `?N`.
#[derive(Clone, Debug)]
pub(crate) struct Numbered {
    pub sql: String,
    /// The index of each named parameter, by its name with its prefix.
    named: BTreeMap<String, usize>,
    count: usize,
    /// The outermost `LIMIT` reads `LIMIT offset, count`.
    limit_comma: bool,
}

impl Numbered {
    /// Whether SQLite numbered the statement's parameters the same way.
    pub fn agrees(&self, statement: &Statement<'_>) -> bool {
        statement.parameter_count() == self.count
            && self
                .named
                .iter()
                .all(|(name, &index)| statement.parameter_index(name).ok().flatten() == Some(index))
    }

    /// Each parameter's value by index, as bound: `NULL` where unbound.
    pub fn values(&self, bound: &ParamValues) -> Vec<Option<Value>> {
        let mut values = vec![Some(Value::Null); self.count];
        for (slot, value) in values.iter_mut().zip(&bound.positional) {
            *slot = Some(value.clone());
        }
        for (name, value) in &bound.named {
            if let Some(&index) = self.named.get(name) {
                values[index - 1] = Some(value.clone());
            }
        }
        values
    }
}

/// Rewrites every parameter as an explicit `?N`, numbering them as SQLite
/// does. `None` if the SQL does not tokenize.
pub(crate) fn number_parameters(sql: &str) -> Option<Numbered> {
    let input = sql.as_bytes();
    let mut scanner = Scanner::new(Tokenizer::new());
    let mut out = String::with_capacity(sql.len() + 8);
    let mut copied = 0;
    let mut count = 0usize;
    let mut named = BTreeMap::new();
    loop {
        match scanner.scan(input) {
            Ok((_, None, _)) => break,
            Ok((start, Some((token, TokenType::TK_VARIABLE)), end)) => {
                let text = std::str::from_utf8(token).ok()?;
                let index = if text.is_empty() {
                    count += 1;
                    count
                } else if let Ok(n) = text.parse::<usize>() {
                    count = count.max(n);
                    n
                } else if let Some(&index) = named.get(text) {
                    index
                } else {
                    count += 1;
                    named.insert(text.to_owned(), count);
                    count
                };
                out.push_str(&sql[copied..start]);
                out.push_str(&format!("?{index}"));
                copied = end;
            }
            Ok(_) => {}
            Err(_) => return None,
        }
    }
    out.push_str(&sql[copied..]);
    Some(Numbered {
        sql: out,
        named,
        count,
        limit_comma: limit_uses_comma(sql)?,
    })
}

/// Whether the statement's outermost `LIMIT` takes the `LIMIT offset,
/// count` form, which the parser reads as `LIMIT count OFFSET offset`.
/// `None` if the SQL does not tokenize.
fn limit_uses_comma(sql: &str) -> Option<bool> {
    let input = sql.as_bytes();
    let mut scanner = Scanner::new(Tokenizer::new());
    let mut depth = 0usize;
    let mut comma = false;
    loop {
        match scanner.scan(input) {
            Ok((_, None, _)) => return Some(comma),
            Ok((_, Some((_, token)), _)) => match token {
                TokenType::TK_LP => depth += 1,
                TokenType::TK_RP => depth = depth.saturating_sub(1),
                TokenType::TK_LIMIT if depth == 0 => comma = false,
                TokenType::TK_COMMA if depth == 0 => comma = true,
                _ => {}
            },
            Err(_) => return None,
        }
    }
}

/// SQLite's whitespace, which numeric conversion skips around a number.
fn is_space(b: u8) -> bool {
    matches!(b, b' ' | b'\t' | b'\n' | 0x0b | 0x0c | b'\r')
}

/// Text under numeric affinity: `Some(number)` if the text is a decimal
/// number SQLite converts, `Some(text)` if it plainly is not a number, and
/// `None` when it might parse in a form this function does not model.
pub(crate) fn numeric_affinity(text: &str) -> Option<Value> {
    let bytes = text.as_bytes();
    let start = bytes
        .iter()
        .position(|&b| !is_space(b))
        .unwrap_or(bytes.len());
    let end = bytes
        .iter()
        .rposition(|&b| !is_space(b))
        .map_or(start, |i| i + 1);
    let trimmed = &text[start..end];
    let numberish = trimmed
        .bytes()
        .next()
        .is_some_and(|b| b.is_ascii_digit() || matches!(b, b'+' | b'-' | b'.'));
    if !numberish {
        return Some(Value::Text(text.to_owned()));
    }
    let digits = trimmed.bytes().filter(u8::is_ascii_digit).count();
    let (sign, body) = match trimmed.as_bytes()[0] {
        b'+' | b'-' => (&trimmed[..1], &trimmed[1..]),
        _ => ("", trimmed),
    };
    let integer = !body.is_empty() && body.bytes().all(|b| b.is_ascii_digit());
    if integer && digits <= 18 {
        return format!("{sign}{body}")
            .parse::<i64>()
            .ok()
            .map(Value::Integer);
    }
    // A decimal with at most 15 significant digits converts the same way
    // in Rust and in SQLite.
    let (mantissa, exponent) = match body.find(['e', 'E']) {
        Some(i) => (&body[..i], Some(&body[i + 1..])),
        None => (body, None),
    };
    let mut parts = mantissa.splitn(2, '.');
    let whole = parts.next().unwrap_or("");
    let fraction = parts.next();
    let well_formed = whole.bytes().all(|b| b.is_ascii_digit())
        && fraction.is_none_or(|f| f.bytes().all(|b| b.is_ascii_digit()))
        && !(whole.is_empty() && fraction.is_none_or(str::is_empty))
        && exponent.is_none_or(|e| {
            let e = e.strip_prefix(['+', '-']).unwrap_or(e);
            !e.is_empty() && e.len() <= 3 && e.bytes().all(|b| b.is_ascii_digit())
        });
    if !well_formed || digits > 15 {
        return None;
    }
    let value: f64 = format!("{sign}{body}").parse().ok()?;
    value.is_finite().then_some(Value::Real(value))
}

/// A constant converted for comparison with a column of `affinity`, or
/// `None` when the conversion is not modeled.
fn apply_affinity(conn: &Connection, value: Value, affinity: Affinity) -> Option<Value> {
    match (affinity, value) {
        (Affinity::Text, Value::Integer(i)) => Some(Value::Text(i.to_string())),
        (Affinity::Text, Value::Real(r)) => conn
            .prepare_cached("SELECT CAST(?1 AS TEXT)")
            .and_then(|mut cast| cast.query_row([r], |row| row.get::<_, String>(0)))
            .ok()
            .map(Value::Text),
        (a, Value::Text(t)) if a.is_numeric() => numeric_affinity(&t),
        (_, value) => Some(value),
    }
}

/// An identifier as SQL spells it, without quotes.
fn unquote(name: &str) -> Option<String> {
    let bytes = name.as_bytes();
    let (open, close) = match bytes.first()? {
        b'"' => (b'"', b'"'),
        b'`' => (b'`', b'`'),
        b'[' => (b'[', b']'),
        _ => return Some(name.to_owned()),
    };
    if bytes.len() < 2 || bytes[bytes.len() - 1] != close {
        return None;
    }
    let inner = &name[1..name.len() - 1];
    Some(if open == b'[' {
        inner.to_owned()
    } else {
        inner.replace(
            &format!("{0}{0}", open as char),
            &(open as char).to_string(),
        )
    })
}

struct Occurrence<'s> {
    table: &'s Table,
    /// What the query calls it: its alias, or its name.
    called: String,
    nullable: bool,
}

/// One side of a comparison: a column of an occurrence, or a constant.
enum Operand {
    Column {
        occurrence: usize,
        column: usize,
        collation: Option<Collation>,
    },
    Constant {
        value: Value,
        collation: Option<Collation>,
    },
}

struct Context<'s, 'c> {
    occurrences: Vec<Occurrence<'s>>,
    params: &'c [Option<Value>],
    conn: &'c Connection,
    /// The outermost `LIMIT` reads `LIMIT offset, count`.
    limit_comma: bool,
}

impl Context<'_, '_> {
    fn column(&self, qualifier: Option<&str>, name: &str) -> Option<(usize, usize)> {
        let name = unquote(name)?;
        let mut found = None;
        for (o, occurrence) in self.occurrences.iter().enumerate() {
            if let Some(qualifier) = qualifier
                && !occurrence.called.eq_ignore_ascii_case(qualifier)
            {
                continue;
            }
            if let Some(c) = occurrence
                .table
                .columns
                .iter()
                .position(|c| c.name.eq_ignore_ascii_case(&name))
            {
                if found.is_some() {
                    return None;
                }
                found = Some((o, c));
            }
        }
        found
    }

    fn column_operand(&self, qualifier: Option<&str>, name: &str) -> Option<Operand> {
        let (occurrence, column) = self.column(qualifier, name)?;
        Some(Operand::Column {
            occurrence,
            column,
            collation: None,
        })
    }

    fn operand(&self, expr: &Expr<'_>) -> Option<Operand> {
        match expr {
            Expr::Collate(inner, name) => {
                let collation = Collation::from_name(&unquote(name)?);
                collation.is_builtin().then_some(())?;
                match self.operand(inner)? {
                    Operand::Column {
                        occurrence,
                        column,
                        collation: None,
                    } => Some(Operand::Column {
                        occurrence,
                        column,
                        collation: Some(collation),
                    }),
                    Operand::Constant {
                        value,
                        collation: None,
                    } => Some(Operand::Constant {
                        value,
                        collation: Some(collation),
                    }),
                    _ => None,
                }
            }
            Expr::Id(id) => self.column_operand(None, id.0),
            // A double-quoted name that is not a column is a string, which
            // the lookup leaves alone.
            Expr::Name(name) => self.column_operand(None, name.0),
            Expr::Qualified(table, column) => {
                self.column_operand(Some(&unquote(table.0)?), column.0)
            }
            Expr::DoublyQualified(db, table, column) => {
                unquote(db.0)?.eq_ignore_ascii_case("main").then_some(())?;
                self.column_operand(Some(&unquote(table.0)?), column.0)
            }
            Expr::Parenthesized(inner) if inner.len() == 1 => self.operand(&inner[0]),
            _ => constant(expr, self.params).map(|value| Operand::Constant {
                value,
                collation: None,
            }),
        }
    }

    /// A column compared with a constant, with the collation SQLite would use
    /// and the constant converted by the column's affinity.
    fn comparison(
        &self,
        left: &Expr<'_>,
        right: &Expr<'_>,
    ) -> Option<(usize, Constraint, Value, bool)> {
        let (left, right) = (self.operand(left)?, self.operand(right)?);
        let (column_side, constant_side, flipped) = match (&left, &right) {
            (Operand::Column { .. }, Operand::Constant { .. }) => (left, right, false),
            (Operand::Constant { .. }, Operand::Column { .. }) => (right, left, true),
            _ => return None,
        };
        let (
            Operand::Column {
                occurrence,
                column,
                collation: column_collation,
            },
            Operand::Constant {
                value,
                collation: constant_collation,
            },
        ) = (column_side, constant_side)
        else {
            return None;
        };
        let table = self.occurrences[occurrence].table;
        let info = &table.columns[column];
        // The left operand's explicit collation wins, then the right's,
        // then the column's.
        let explicit = if flipped {
            constant_collation.or(column_collation)
        } else {
            column_collation.or(constant_collation)
        };
        let collation = explicit.unwrap_or_else(|| info.collation.clone());
        if !collation.is_builtin() {
            return None;
        }
        let value = apply_affinity(self.conn, value, info.affinity)?;
        Some((
            occurrence,
            Constraint {
                column: info.name.to_ascii_lowercase(),
                collation,
                condition: Condition::Never,
            },
            value,
            flipped,
        ))
    }

    fn constraint(&self, expr: &Expr<'_>) -> Option<(usize, Constraint)> {
        match expr {
            Expr::Parenthesized(inner) if inner.len() == 1 => self.constraint(&inner[0]),
            Expr::Binary(left, op, right) => {
                let (occurrence, mut constraint, value, flipped) = self.comparison(left, right)?;
                let op = if flipped { flip(*op)? } else { *op };
                let null = matches!(value, Value::Null);
                constraint.condition = match op {
                    Operator::Equals if null => Condition::Never,
                    Operator::Equals => Condition::OneOf(vec![value]),
                    Operator::Is if null => Condition::IsNull,
                    Operator::Is => Condition::Is(value),
                    Operator::Less
                    | Operator::LessEquals
                    | Operator::Greater
                    | Operator::GreaterEquals
                        if null =>
                    {
                        Condition::Never
                    }
                    Operator::Less => Condition::Range {
                        low: None,
                        high: Some((value, false)),
                    },
                    Operator::LessEquals => Condition::Range {
                        low: None,
                        high: Some((value, true)),
                    },
                    Operator::Greater => Condition::Range {
                        low: Some((value, false)),
                        high: None,
                    },
                    Operator::GreaterEquals => Condition::Range {
                        low: Some((value, true)),
                        high: None,
                    },
                    _ => return None,
                };
                Some((occurrence, constraint))
            }
            Expr::InList {
                lhs,
                not: false,
                rhs,
            } => {
                let list = rhs.map_or(&[][..], |list| list);
                let mut values = Vec::new();
                let mut target = None;
                for item in list {
                    if matches!(item, Expr::Collate(..)) {
                        return None;
                    }
                    let (occurrence, constraint, value, flipped) = self.comparison(lhs, item)?;
                    if flipped {
                        return None;
                    }
                    target = Some((occurrence, constraint));
                    if !matches!(value, Value::Null) {
                        values.push(value);
                    }
                }
                match target {
                    Some((occurrence, mut constraint)) => {
                        constraint.condition = if values.is_empty() {
                            Condition::Never
                        } else {
                            Condition::OneOf(values)
                        };
                        Some((occurrence, constraint))
                    }
                    None => {
                        // `x IN ()` matches nothing.
                        let Operand::Column {
                            occurrence, column, ..
                        } = self.operand(lhs)?
                        else {
                            return None;
                        };
                        let info = &self.occurrences[occurrence].table.columns[column];
                        Some((
                            occurrence,
                            Constraint {
                                column: info.name.to_ascii_lowercase(),
                                collation: Collation::Binary,
                                condition: Condition::Never,
                            },
                        ))
                    }
                }
            }
            Expr::Between {
                lhs,
                not: false,
                start,
                end,
            } => {
                let (occurrence, mut constraint, low, flipped_low) = self.comparison(lhs, start)?;
                let (_, high_constraint, high, flipped_high) = self.comparison(lhs, end)?;
                if flipped_low || flipped_high || high_constraint.collation != constraint.collation
                {
                    return None;
                }
                constraint.condition = if matches!(low, Value::Null) || matches!(high, Value::Null)
                {
                    Condition::Never
                } else {
                    Condition::Range {
                        low: Some((low, true)),
                        high: Some((high, true)),
                    }
                };
                Some((occurrence, constraint))
            }
            Expr::IsNull(inner) | Expr::NotNull(inner) => {
                let Operand::Column {
                    occurrence, column, ..
                } = self.operand(inner)?
                else {
                    return None;
                };
                let info = &self.occurrences[occurrence].table.columns[column];
                Some((
                    occurrence,
                    Constraint {
                        column: info.name.to_ascii_lowercase(),
                        collation: Collation::Binary,
                        condition: if matches!(expr, Expr::IsNull(_)) {
                            Condition::IsNull
                        } else {
                            Condition::NotNull
                        },
                    },
                ))
            }
            _ => None,
        }
    }
}

/// The operator with its operands swapped.
fn flip(op: Operator) -> Option<Operator> {
    Some(match op {
        Operator::Less => Operator::Greater,
        Operator::LessEquals => Operator::GreaterEquals,
        Operator::Greater => Operator::Less,
        Operator::GreaterEquals => Operator::LessEquals,
        Operator::Equals | Operator::Is => op,
        _ => return None,
    })
}

/// A literal or bound parameter's value.
fn constant(expr: &Expr<'_>, params: &[Option<Value>]) -> Option<Value> {
    match expr {
        Expr::Literal(Literal::Numeric(text)) => number(text),
        Expr::Literal(Literal::String(text)) => {
            let inner = text.strip_prefix('\'')?.strip_suffix('\'')?;
            Some(Value::Text(inner.replace("''", "'")))
        }
        Expr::Literal(Literal::Blob(hex)) => {
            let bytes = hex.as_bytes();
            (bytes.len() % 2 == 0).then_some(())?;
            bytes
                .chunks(2)
                .map(|pair| u8::from_str_radix(std::str::from_utf8(pair).ok()?, 16).ok())
                .collect::<Option<Vec<u8>>>()
                .map(Value::Blob)
        }
        Expr::Literal(Literal::Null) => Some(Value::Null),
        Expr::Unary(UnaryOperator::Positive, inner) => match constant(inner, params)? {
            value @ (Value::Integer(_) | Value::Real(_)) => Some(value),
            _ => None,
        },
        Expr::Unary(UnaryOperator::Negative, inner) => match constant(inner, params)? {
            Value::Integer(i) => i.checked_neg().map(Value::Integer),
            Value::Real(r) => Some(Value::Real(-r)),
            _ => None,
        },
        Expr::Variable(index) => {
            let index: usize = index.parse().ok()?;
            params.get(index.checked_sub(1)?)?.clone()
        }
        _ => None,
    }
}

/// A numeric literal: decimal or hex integers that fit 64 bits, or decimals
/// SQLite and Rust read alike.
fn number(text: &str) -> Option<Value> {
    if let Some(hex) = text.strip_prefix("0x").or_else(|| text.strip_prefix("0X")) {
        return u64::from_str_radix(hex, 16)
            .ok()
            .map(|n| Value::Integer(n as i64));
    }
    if text.bytes().all(|b| b.is_ascii_digit()) {
        return text.parse::<i64>().ok().map(Value::Integer);
    }
    match numeric_affinity(text)? {
        number @ (Value::Integer(_) | Value::Real(_)) => Some(number),
        _ => None,
    }
}

/// Top-level `AND` terms.
fn conjuncts<'a, 'b>(expr: &'a Expr<'b>, out: &mut Vec<&'a Expr<'b>>) {
    match expr {
        Expr::Binary(left, Operator::And, right) => {
            conjuncts(left, out);
            conjuncts(right, out);
        }
        Expr::Parenthesized(inner) if inner.len() == 1 => conjuncts(&inner[0], out),
        _ => out.push(expr),
    }
}

/// Whether an expression holds a subquery, whose reads the analysis does
/// not model.
fn has_subquery(expr: &Expr<'_>) -> bool {
    match expr {
        Expr::Exists(_) | Expr::Subquery(_) | Expr::InSelect { .. } | Expr::InTable { .. } => true,
        Expr::Between {
            lhs, start, end, ..
        } => has_subquery(lhs) || has_subquery(start) || has_subquery(end),
        Expr::Binary(left, _, right) => has_subquery(left) || has_subquery(right),
        Expr::Case {
            base,
            when_then_pairs,
            else_expr,
        } => {
            base.is_some_and(has_subquery)
                || when_then_pairs
                    .iter()
                    .any(|(w, t)| has_subquery(w) || has_subquery(t))
                || else_expr.is_some_and(has_subquery)
        }
        Expr::Cast { expr, .. }
        | Expr::Collate(expr, _)
        | Expr::IsNull(expr)
        | Expr::NotNull(expr)
        | Expr::Unary(_, expr) => has_subquery(expr),
        // `FILTER`, `OVER` and an aggregate's `ORDER BY` can hold
        // subqueries too; the analysis leaves such calls alone.
        Expr::FunctionCall {
            args,
            order_by,
            filter_over,
            ..
        } => {
            order_by.is_some()
                || filter_over.is_some()
                || args.is_some_and(|args| args.iter().any(has_subquery))
        }
        Expr::FunctionCallStar { filter_over, .. } => filter_over.is_some(),
        Expr::InList { lhs, rhs, .. } => {
            has_subquery(lhs) || rhs.is_some_and(|list| list.iter().any(has_subquery))
        }
        Expr::Like {
            lhs, rhs, escape, ..
        } => has_subquery(lhs) || has_subquery(rhs) || escape.is_some_and(has_subquery),
        Expr::Parenthesized(list) => list.iter().any(has_subquery),
        Expr::Raise(_, inner) => inner.is_some_and(has_subquery),
        _ => false,
    }
}

/// Whether an expression names a column and nothing more, so it cannot
/// hide an aggregate.
fn is_plain_column(expr: &Expr<'_>) -> bool {
    match expr {
        Expr::Id(_) | Expr::Name(_) | Expr::Qualified(..) | Expr::DoublyQualified(..) => true,
        Expr::Collate(inner, _) => is_plain_column(inner),
        Expr::Parenthesized(list) => list.len() == 1 && is_plain_column(&list[0]),
        _ => false,
    }
}

/// A plain table of the main database that a `FROM` clause names, or
/// `None` for anything else.
fn occurrence<'s>(
    table: &SelectTable<'_>,
    schema: &'s Schema,
    nullable: bool,
) -> Option<Occurrence<'s>> {
    let SelectTable::Table(name, alias, _) = table else {
        return None;
    };
    if let Some(db) = &name.db_name
        && !unquote(db.0)?.eq_ignore_ascii_case("main")
    {
        return None;
    }
    let table_name = unquote(name.name.0)?;
    let table = schema.table(&table_name)?;
    let called = match alias {
        Some(As::As(alias) | As::Elided(alias)) => unquote(alias.0)?,
        None => table_name,
    };
    Some(Occurrence {
        table,
        called,
        nullable,
    })
}

/// The occurrences a `FROM` clause names, or `None` if it names anything
/// but plain tables of the main database.
fn occurrences<'s>(from: &FromClause<'_>, schema: &'s Schema) -> Option<Vec<Occurrence<'s>>> {
    let mut out = vec![occurrence(from.select?, schema, false)?];
    for join in from.joins.iter().flatten() {
        let kind = match join.operator {
            JoinOperator::TypedJoin(Some(kind)) => kind,
            _ => JoinType::INNER,
        };
        if let Some(sql3_constraint) = &join.constraint
            && let sqlite3_parser::ast::JoinConstraint::On(on) = sql3_constraint
            && has_subquery(on)
        {
            return None;
        }
        out.push(occurrence(
            &join.table,
            schema,
            kind.contains(JoinType::LEFT),
        )?);
        if kind.contains(JoinType::RIGHT) {
            // A right join makes every table before it nullable.
            let last = out.len() - 1;
            for occurrence in &mut out[..last] {
                occurrence.nullable = true;
            }
        }
    }
    Some(out)
}

fn result_alias_named(columns: &[ResultColumn<'_>], name: &str) -> bool {
    columns.iter().any(|column| match column {
        ResultColumn::Expr(_, Some(As::As(alias) | As::Elided(alias))) => {
            unquote(alias.0).is_some_and(|a| a.eq_ignore_ascii_case(name))
        }
        _ => false,
    })
}

/// Analyzes a statement for the constraints of the tables it reads.
/// `params` holds each parameter's value by index.
pub(crate) fn analyze(
    numbered: &Numbered,
    params: &[Option<Value>],
    schema: &Schema,
    conn: &Connection,
) -> Analysis {
    let limit_comma = numbered.limit_comma;
    let bump = Bump::new();
    let mut parser = Parser::new(&bump, numbered.sql.as_bytes());
    let Ok(Some(Cmd::Stmt(Stmt::Select(select)))) = parser.next() else {
        return Analysis::default();
    };
    if !matches!(parser.next(), Ok(None)) {
        return Analysis::default();
    }
    analyze_select(select, params, schema, conn, limit_comma).unwrap_or_default()
}

fn analyze_select(
    select: &Select<'_>,
    params: &[Option<Value>],
    schema: &Schema,
    conn: &Connection,
    limit_comma: bool,
) -> Option<Analysis> {
    if select.with.is_some() || select.body.compounds.is_some() {
        return None;
    }
    let OneSelect::Select {
        distinctness,
        columns,
        from,
        where_clause,
        group_by,
        having,
        window_clause,
    } = &select.body.select
    else {
        return None;
    };
    let from = from.as_ref()?;
    let mut nested = columns
        .iter()
        .any(|c| matches!(c, ResultColumn::Expr(e, _) if has_subquery(e)));
    nested |= where_clause.is_some_and(has_subquery);
    nested |= group_by.is_some_and(|g| g.iter().any(has_subquery));
    nested |= having.is_some_and(has_subquery);
    nested |= select
        .order_by
        .is_some_and(|o| o.iter().any(|s| has_subquery(&s.expr)));
    nested |= select
        .limit
        .is_some_and(|l| has_subquery(&l.expr) || l.offset.as_ref().is_some_and(has_subquery));
    if nested {
        return None;
    }
    let context = Context {
        occurrences: occurrences(from, schema)?,
        params,
        conn,
        limit_comma,
    };
    let mut conjunctions: Vec<Conjunction> = (0..context.occurrences.len())
        .map(|_| Conjunction::default())
        .collect();
    if let Some(filter) = where_clause {
        let mut terms = Vec::new();
        conjuncts(filter, &mut terms);
        for term in terms {
            if let Some((occurrence, constraint)) = context.constraint(term)
                && !context.occurrences[occurrence].nullable
            {
                conjunctions[occurrence].constraints.push(constraint);
            }
        }
    }
    let window = (context.occurrences.len() == 1
        && distinctness.is_none()
        && group_by.is_none()
        && having.is_none()
        && window_clause.is_none()
        // Plain columns everywhere, so no aggregate turns the rows into a
        // summary of the whole table.
        && columns.iter().all(|c| match c {
            ResultColumn::Expr(e, _) => is_plain_column(e),
            ResultColumn::Star | ResultColumn::TableStar(_) => true,
        })
        && select
            .order_by
            .is_some_and(|terms| terms.iter().all(|term| is_plain_column(&term.expr))))
    .then(|| window_plan(select, columns, &context))
    .flatten();
    let mut tables: BTreeMap<String, Vec<Conjunction>> = BTreeMap::new();
    for (occurrence, conjunction) in context.occurrences.iter().zip(conjunctions) {
        tables
            .entry(occurrence.table.name.to_ascii_lowercase())
            .or_default()
            .push(conjunction);
    }
    Some(Analysis { tables, window })
}

fn window_plan(
    select: &Select<'_>,
    columns: &[ResultColumn<'_>],
    context: &Context<'_, '_>,
) -> Option<WindowPlan> {
    let first = select.order_by?.first()?;
    let clause = select.limit?;
    let (count, offset) = if context.limit_comma {
        (clause.offset.as_ref()?, Some(&clause.expr))
    } else {
        (&clause.expr, clause.offset.as_ref())
    };
    let limit = match constant(count, context.params)? {
        Value::Integer(n) if n >= 0 => n as u64,
        _ => return None,
    };
    if let Some(offset) = offset {
        match constant(offset, context.params)? {
            Value::Integer(n) if n >= 0 => {}
            _ => return None,
        }
    }
    let (inner, collation) = match &first.expr {
        Expr::Collate(inner, name) => (&**inner, Some(Collation::from_name(&unquote(name)?))),
        expr => (expr, None),
    };
    // SQLite resolves a bare name in ORDER BY to a result alias first.
    if let Expr::Id(id) = inner
        && result_alias_named(columns, &unquote(id.0)?)
    {
        return None;
    }
    let Operand::Column {
        occurrence,
        column,
        collation: None,
    } = context.operand(inner)?
    else {
        return None;
    };
    let table = context.occurrences[occurrence].table;
    let info = &table.columns[column];
    let collation = collation.unwrap_or_else(|| info.collation.clone());
    if !collation.is_builtin() {
        return None;
    }
    let descending = first.order == Some(SortOrder::Desc);
    let nulls_first = match first.nulls {
        Some(NullsOrder::First) => true,
        Some(NullsOrder::Last) => false,
        None => !descending,
    };
    Some(WindowPlan {
        table: table.name.to_ascii_lowercase(),
        column: info.name.to_ascii_lowercase(),
        collation,
        descending,
        nulls_first,
        limit,
    })
}
