//! The tables and columns a query reads. SQLite's authorizer reports them
//! while it prepares the statement, through views, subqueries, CTEs and
//! joins alike, except for the columns a `USING` or `NATURAL` join matches:
//! SQLite resolves those itself. A statement that joins that way, in its
//! own text or in a view it names, takes its tables from the b-trees its
//! bytecode opens, and every column of each counts.

use crate::read_set::Recorded;
use crate::schema::Schema;
use rusqlite::hooks::{AuthAction, AuthContext, Authorization};
use rusqlite::{Connection, OptionalExtension};
use sqlite3_parser::lexer::Scanner;
use sqlite3_parser::lexer::sql::{TokenType, Tokenizer};
use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, Mutex};

/// Each table a query reads, by lowercase name.
pub(crate) type Reads = BTreeMap<String, Recorded>;

/// Where a reader connection's authorizer records reads; `None` outside
/// [`collect`].
pub(crate) type Slot = Arc<Mutex<Option<Reads>>>;

/// Installs the authorizer that records reads. While it records, it also
/// refuses statements that would end the snapshot's transaction, reach
/// another database, or change the connection.
pub(crate) fn install(conn: &mut Connection) -> rusqlite::Result<Slot> {
    let slot: Slot = Arc::new(Mutex::new(None));
    let recorder = slot.clone();
    conn.authorizer(Some(move |context: AuthContext<'_>| {
        let mut slot = crate::lock(&recorder);
        let Some(reads) = slot.as_mut() else {
            return Authorization::Allow;
        };
        match context.action {
            AuthAction::Read {
                table_name,
                column_name,
            } => {
                // SQLite names no database for a table read without its
                // columns, as `count(*)` reads it; recording it may only add
                // re-runs.
                if matches!(context.database_name, Some("main") | None) {
                    reads
                        .entry(table_name.to_ascii_lowercase())
                        .or_insert_with(|| Recorded::new(table_name))
                        .record(column_name);
                }
                Authorization::Allow
            }
            AuthAction::Transaction { .. }
            | AuthAction::Savepoint { .. }
            | AuthAction::Attach { .. }
            | AuthAction::Detach { .. }
            | AuthAction::Pragma { .. } => Authorization::Deny,
            _ => Authorization::Allow,
        }
    }))?;
    Ok(slot)
}

/// Runs `prepare` while recording the reads SQLite authorizes.
pub(crate) fn collect<T>(slot: &Slot, prepare: impl FnOnce() -> T) -> (T, Reads) {
    *crate::lock(slot) = Some(Reads::new());
    let prepared = prepare();
    let reads = crate::lock(slot).take().unwrap_or_default();
    (prepared, reads)
}

/// SQLite's schema table, as the authorizer reports it.
const SCHEMA_TABLE: &str = "sqlite_master";

/// Table-valued functions whose rows depend on their arguments alone.
const PURE_FUNCTIONS: [&str; 4] = ["json_each", "json_tree", "jsonb_each", "jsonb_tree"];

/// Completes the reads the authorizer recorded while SQLite prepared `sql`,
/// and returns whether every commit counts, whatever rows it changes.
/// `explains` is set for an `EXPLAIN`, which reads no rows.
///
/// - A view drops out, since the authorizer reports its base tables too.
/// - A virtual table adds its shadow tables, which hold its rows and carry
///   its changes, and an FTS table's external content table, which it reads
///   rows from as the query runs. Any change to these counts.
/// - Every commit counts for a virtual table that keeps its rows in neither,
///   as `fts5vocab` reads another table's index; for an FTS table whose
///   content is a view; and for a table-valued function that reads the
///   database's own state, as the pragma functions and `dbstat` do.
/// - A statement that joins by column name adds each table its bytecode
///   opens, and every column of each table counts. Every commit counts when
///   it opens a table the bytecode leaves unnamed, such as a virtual table.
pub(crate) fn resolve(
    conn: &Connection,
    sql: &str,
    schema: &Schema,
    explains: bool,
    reads: &mut Reads,
) -> rusqlite::Result<bool> {
    let mut kind = conn.prepare_cached(
        "SELECT type FROM pragma_table_list WHERE schema = 'main' AND name = ?1 COLLATE NOCASE",
    )?;
    let mut module = conn
        .prepare_cached("SELECT count(*) FROM pragma_module_list WHERE name = ?1 COLLATE NOCASE")?;
    let mut shadows = conn.prepare_cached(
        "SELECT name FROM pragma_table_list WHERE schema = 'main' AND type = 'shadow'",
    )?;
    let mut declared =
        conn.prepare_cached("SELECT sql FROM sqlite_schema WHERE type = 'table' AND name = ?1")?;
    let mut every_commit = false;
    let names: Vec<String> = reads.values().map(|t| t.name().to_owned()).collect();
    for name in names {
        let key = name.to_ascii_lowercase();
        let found = kind
            .query_row([&name], |r| r.get::<_, String>(0))
            .optional()?;
        match found.as_deref() {
            Some("view") => {
                reads.remove(&key);
            }
            Some("virtual") => {
                // SQLite gives a shadow table to the virtual table named
                // before its last underscore.
                let mut tables: Vec<String> = shadows
                    .query_map([], |r| r.get(0))?
                    .collect::<rusqlite::Result<Vec<String>>>()?
                    .into_iter()
                    .filter(|table| {
                        table
                            .rsplit_once('_')
                            .is_some_and(|(owner, _)| owner.eq_ignore_ascii_case(&name))
                    })
                    .collect();
                let definition: String = declared.query_row([&name], |r| r.get(0))?;
                if let Some(content) = content_table(&definition) {
                    let content_kind = kind
                        .query_row([&content], |r| r.get::<_, String>(0))
                        .optional()?;
                    every_commit |= !matches!(content_kind.as_deref(), Some("table" | "shadow"));
                    tables.push(content);
                }
                every_commit |= tables.is_empty();
                for table in tables {
                    reads
                        .entry(table.to_ascii_lowercase())
                        .or_insert_with(|| Recorded::new(&table))
                        .record_every_change();
                }
            }
            Some(_) => {}
            // A table-valued function, which is a module, or a name the
            // statement defines itself, as for a common table expression.
            None => {
                let is_function = module.query_row([&name], |r| r.get::<_, i64>(0))? > 0;
                every_commit |= is_function && !PURE_FUNCTIONS.contains(&key.as_str());
            }
        }
    }
    if !explains && joins_by_name(sql, &schema.views_joined_by_name) {
        every_commit |= !add_opened(conn, sql, reads)?;
        for read in reads.values_mut() {
            read.record_every_column();
        }
    }
    Ok(every_commit)
}

/// The lowercase names of the views that join tables by column name, in
/// their own definitions or in those of the views they name. SQLite can
/// merge a view into the statement that reads it before the authorizer
/// hears of the view, so these come from the schema.
pub(crate) fn views_joined_by_name(conn: &Connection) -> rusqlite::Result<BTreeSet<String>> {
    let views: Vec<(String, String)> = conn
        .prepare("SELECT name, sql FROM sqlite_schema WHERE type = 'view'")?
        .query_map([], |r| {
            Ok((r.get::<_, String>(0)?.to_ascii_lowercase(), r.get(1)?))
        })?
        .collect::<rusqlite::Result<_>>()?;
    let mut joined = BTreeSet::new();
    loop {
        let found: Vec<&String> = views
            .iter()
            .filter(|(name, definition)| {
                !joined.contains(name) && joins_by_name(definition, &joined)
            })
            .map(|(name, _)| name)
            .collect();
        if found.is_empty() {
            return Ok(joined);
        }
        joined.extend(found.into_iter().cloned());
    }
}

/// Whether SQL joins tables by column name: with `USING` or `NATURAL`, or
/// through one of `views`, which do. Any token that spells a view's name
/// counts as naming it, and SQL that does not tokenize might join so.
fn joins_by_name(sql: &str, views: &BTreeSet<String>) -> bool {
    let mut scanner = Scanner::new(Tokenizer::new());
    let input = sql.as_bytes();
    loop {
        match scanner.scan(input) {
            Ok((_, None, _)) => return false,
            Ok((_, Some((_, TokenType::TK_USING)), _)) => return true,
            Ok((_, Some((token, kind)), _)) => {
                if kind == TokenType::TK_JOIN_KW && token.eq_ignore_ascii_case(b"natural") {
                    return true;
                }
                if views.is_empty() {
                    continue;
                }
                let Ok(text) = std::str::from_utf8(token) else {
                    return true;
                };
                let mut name = unquote(text);
                name.make_ascii_lowercase();
                if views.contains(&name) {
                    return true;
                }
            }
            Err(_) => return true,
        }
    }
}

/// Adds each table of the main database whose b-tree, or whose index's, the
/// bytecode of `sql` opens. Returns false if the statement opens anything
/// else, such as a virtual table, whose name the bytecode leaves out.
fn add_opened(conn: &Connection, sql: &str, reads: &mut Reads) -> rusqlite::Result<bool> {
    let mut explain = conn.prepare(&format!("EXPLAIN {sql}"))?;
    // The table each b-tree belongs to, by root page.
    let owners: BTreeMap<i64, String> = conn
        .prepare_cached("SELECT rootpage, tbl_name FROM sqlite_schema WHERE rootpage > 0")?
        .query_map([], |r| Ok((r.get(0)?, r.get(1)?)))?
        .collect::<rusqlite::Result<_>>()?;
    let mut named = true;
    // The statement's parameters stay unbound.
    let mut rows = explain.raw_query();
    while let Some(row) = rows.next()? {
        match row.get_ref(1)?.as_str()? {
            "OpenRead" | "OpenWrite" | "ReopenIdx" => {
                let (root, database): (i64, i64) = (row.get(3)?, row.get(4)?);
                let table = match (database, root) {
                    (0, 1) => Some(SCHEMA_TABLE),
                    (0, root) => owners.get(&root).map(String::as_str),
                    _ => None,
                };
                match table {
                    Some(table) => {
                        reads
                            .entry(table.to_ascii_lowercase())
                            .or_insert_with(|| Recorded::new(table));
                    }
                    None => named = false,
                }
            }
            "VOpen" => named = false,
            _ => {}
        }
    }
    Ok(named)
}

/// The external content table a `CREATE VIRTUAL TABLE` statement names with
/// a `content = name` option, as FTS4 and FTS5 tables do.
fn content_table(declared: &str) -> Option<String> {
    let mut scanner = Scanner::new(Tokenizer::new());
    let input = declared.as_bytes();
    let mut tokens = Vec::new();
    while let Ok((_, Some((token, kind)), _)) = scanner.scan(input) {
        tokens.push((token, kind));
    }
    tokens.windows(3).find_map(|window| match window {
        [
            (option, TokenType::TK_ID),
            (_, TokenType::TK_EQ),
            (value, kind),
        ] if option.eq_ignore_ascii_case(b"content")
            && matches!(kind, TokenType::TK_ID | TokenType::TK_STRING) =>
        {
            let value = std::str::from_utf8(value).ok()?;
            let name = unquote(value);
            (!name.is_empty()).then_some(name)
        }
        _ => None,
    })
}

/// A name or string without its quotes.
fn unquote(text: &str) -> String {
    let bytes = text.as_bytes();
    match (bytes.first(), bytes.last()) {
        (Some(&open @ (b'\'' | b'"' | b'`')), Some(&close))
            if open == close && bytes.len() >= 2 =>
        {
            let quote = open as char;
            text[1..text.len() - 1].replace(&format!("{quote}{quote}"), &quote.to_string())
        }
        (Some(b'['), Some(b']')) => text[1..text.len() - 1].to_owned(),
        _ => text.to_owned(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn joins_by_name_finds_using_and_natural_in_any_case() {
        let none = BTreeSet::new();
        for sql in [
            "SELECT 1 FROM a JOIN b USING (k)",
            "SELECT 1 FROM a natural join b",
            "SELECT 1 FROM a NATURAL LEFT OUTER JOIN b",
            "WITH q AS (SELECT 1 FROM a JOIN b using(k)) SELECT * FROM q",
            "CREATE VIEW v AS SELECT a.x FROM a LEFT JOIN b USING (k)",
            "SELECT 'unterminated",
        ] {
            assert!(joins_by_name(sql, &none), "{sql}");
        }
        for sql in [
            "SELECT 1 FROM a JOIN b ON a.k = b.k",
            "SELECT 'using', \"natural\" FROM a LEFT JOIN b ON 1",
            "SELECT 1 FROM a CROSS JOIN b -- natural using",
        ] {
            assert!(!joins_by_name(sql, &none), "{sql}");
        }
    }

    #[test]
    fn joins_by_name_finds_a_view_under_any_spelling_of_its_name() {
        let views = BTreeSet::from(["pairs".to_owned(), "odd name".to_owned()]);
        for sql in [
            "SELECT count(*) FROM pairs",
            "SELECT count(*) FROM main.PAIRS AS p",
            "SELECT count(*) FROM \"Pairs\"",
            "SELECT count(*) FROM 'pairs'",
            "SELECT count(*) FROM [odd name]",
            "SELECT count(*) FROM `Odd Name`",
        ] {
            assert!(joins_by_name(sql, &views), "{sql}");
        }
        assert!(!joins_by_name("SELECT count(*) FROM pair", &views));
    }

    #[test]
    fn views_joined_by_name_follow_the_views_they_name() {
        let conn = Connection::open_in_memory().unwrap();
        conn.execute_batch(
            "CREATE TABLE a (k, x);
             CREATE TABLE b (k, y);
             CREATE VIEW plain AS SELECT x FROM a;
             CREATE VIEW Joined AS SELECT 1 AS one FROM a JOIN b USING (k);
             CREATE VIEW outer_view AS SELECT one FROM \"joined\";
             CREATE VIEW outermost AS SELECT count(*) AS n FROM outer_view, plain;",
        )
        .unwrap();
        let joined = views_joined_by_name(&conn).unwrap();
        let names: Vec<&str> = joined.iter().map(String::as_str).collect();
        assert_eq!(names, ["joined", "outer_view", "outermost"]);
    }
}
