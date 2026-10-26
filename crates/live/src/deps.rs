//! The tables and columns a query reads, as SQLite's authorizer reports them
//! while it prepares the statement: through views, subqueries, CTEs and
//! joins alike.

use crate::read_set::TableRead;
use rusqlite::Connection;
use rusqlite::hooks::{AuthAction, AuthContext, Authorization};
use sqlite3_parser::lexer::Scanner;
use sqlite3_parser::lexer::sql::{TokenType, Tokenizer};
use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

/// Each table a query reads, by lowercase name.
pub(crate) type Reads = BTreeMap<String, TableRead>;

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
                        .or_insert_with(|| TableRead::new(table_name))
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

/// Drops the views a query reads, whose base tables the authorizer reports
/// too, and adds each virtual table's shadow tables, which hold its rows and
/// carry its changes, along with an FTS table's external content table,
/// which it reads rows from as the query runs. Any change to these counts.
pub(crate) fn resolve(conn: &Connection, reads: &mut Reads) -> rusqlite::Result<()> {
    let mut kind = conn
        .prepare_cached("SELECT type FROM pragma_table_list WHERE schema = 'main' AND name = ?1")?;
    let mut shadows = conn.prepare_cached(
        "SELECT name FROM pragma_table_list WHERE schema = 'main' AND type = 'shadow'",
    )?;
    let names: Vec<String> = reads.values().map(|t| t.name().to_owned()).collect();
    for name in names {
        let found = match kind.query_row([&name], |r| r.get::<_, String>(0)) {
            Ok(found) => found,
            Err(rusqlite::Error::QueryReturnedNoRows) => continue,
            Err(e) => return Err(e),
        };
        if found == "view" {
            reads.remove(&name.to_ascii_lowercase());
        }
        if found != "virtual" {
            continue;
        }
        let prefix = format!("{}_", name.to_ascii_lowercase());
        let mut tables: Vec<String> = shadows
            .query_map([], |r| r.get(0))?
            .collect::<rusqlite::Result<Vec<String>>>()?
            .into_iter()
            .filter(|table| table.to_ascii_lowercase().starts_with(&prefix))
            .collect();
        let declared: String = conn.query_row(
            "SELECT sql FROM sqlite_schema WHERE type = 'table' AND name = ?1",
            [&name],
            |r| r.get(0),
        )?;
        tables.extend(content_table(&declared));
        for table in tables {
            reads
                .entry(table.to_ascii_lowercase())
                .or_insert_with(|| TableRead::new(&table))
                .record_every_column();
        }
    }
    Ok(())
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
