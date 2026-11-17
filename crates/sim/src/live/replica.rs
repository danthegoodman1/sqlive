//! A replica built only from commit events, in an in-memory SQLite database,
//! and the table dumps that compare it with the real one.
//!
//! Replica tables have no types or constraints, so values stay exactly as the
//! events carry them, but key columns keep their collations, so a `WITHOUT
//! ROWID` key matches rows the way the real table does. Each applied change
//! must touch exactly the rows it claims to. A virtual table's shadow tables
//! replicate as the tables they are.
//!
//! When an event names a table whole, the replica reads the table again at
//! whatever version the database has reached, keeps following the stream,
//! and skips the table's rows in events at or below that version.

use rusqlite::types::Value;
use rusqlite::{Connection, params_from_iter};
use sqlive::{Changes, CommitEvent, Error, LiveDb, Op, RowChange, Version};
use std::collections::BTreeMap;

#[derive(Clone, Debug, PartialEq)]
pub struct Shape {
    pub name: String,
    /// Stored columns, in table order.
    pub columns: Vec<String>,
    pub collations: Vec<String>,
    pub without_rowid: bool,
    /// Primary-key columns of a `WITHOUT ROWID` table, in key order, with
    /// the collation the key compares each with.
    pub key: Vec<(String, String)>,
}

/// Each table's rows: `[rowid, columns...]` by rowid, or `[columns...]` by key.
pub type Dump = BTreeMap<String, Vec<Vec<Value>>>;

fn quote(ident: &str) -> String {
    format!("\"{}\"", ident.replace('"', "\"\""))
}

/// The user tables of the main database, shadow tables included, as SQLite
/// reports them.
pub fn shapes(conn: &Connection) -> rusqlite::Result<Vec<Shape>> {
    let mut list = conn.prepare(
        "SELECT name, wr FROM pragma_table_list
         WHERE schema = 'main' AND type IN ('table', 'shadow')
           AND substr(name, 1, 7) <> 'sqlite_' AND name <> '_sqlive_meta'
         ORDER BY name",
    )?;
    let tables: Vec<(String, bool)> = list
        .query_map([], |r| Ok((r.get(0)?, r.get(1)?)))?
        .collect::<rusqlite::Result<_>>()?;
    let mut shapes = Vec::new();
    for (name, without_rowid) in tables {
        let mut info =
            conn.prepare("SELECT name, pk, hidden FROM pragma_table_xinfo(?1) ORDER BY cid")?;
        let rows: Vec<(String, i64, i64)> = info
            .query_map([&name], |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)))?
            .collect::<rusqlite::Result<_>>()?;
        let mut columns = Vec::new();
        let mut collations = Vec::new();
        let mut key = Vec::new();
        for (column, pk, hidden) in rows {
            if pk > 0 {
                key.push((pk, column.clone()));
            }
            if hidden == 2 {
                continue;
            }
            let collation = conn
                .column_metadata(Some("main"), name.as_str(), column.as_str())
                .ok()
                .and_then(|(_, c, ..)| c.map(|c| c.to_string_lossy().into_owned()))
                .unwrap_or_else(|| "BINARY".to_owned());
            columns.push(column);
            collations.push(collation);
        }
        key.sort();
        let key = if without_rowid {
            let index: String = conn.query_row(
                "SELECT name FROM pragma_index_list(?1) WHERE origin = 'pk'",
                [&name],
                |r| r.get(0),
            )?;
            let mut info =
                conn.prepare("SELECT name, coll FROM pragma_index_xinfo(?1) WHERE key = 1")?;
            let collations: Vec<(String, String)> = info
                .query_map([&index], |r| Ok((r.get(0)?, r.get(1)?)))?
                .collect::<rusqlite::Result<_>>()?;
            key.into_iter()
                .map(|(_, column)| {
                    let collation = collations
                        .iter()
                        .find(|(c, _)| c.eq_ignore_ascii_case(&column))
                        .map_or_else(|| "BINARY".to_owned(), |(_, coll)| coll.clone());
                    (column, collation)
                })
                .collect()
        } else {
            Vec::new()
        };
        shapes.push(Shape {
            name,
            columns,
            collations,
            without_rowid,
            key,
        });
    }
    Ok(shapes)
}

fn select_all(shape: &Shape, rowid: &str) -> String {
    let list: Vec<String> = shape.columns.iter().map(|c| quote(c)).collect();
    if shape.without_rowid {
        let order: Vec<String> = shape
            .key
            .iter()
            .map(|(c, collation)| format!("{} COLLATE {collation}", quote(c)))
            .collect();
        format!(
            "SELECT {} FROM {} ORDER BY {}",
            list.join(", "),
            quote(&shape.name),
            order.join(", ")
        )
    } else {
        format!(
            "SELECT {rowid}, {} FROM {} ORDER BY {rowid}",
            list.join(", "),
            quote(&shape.name)
        )
    }
}

pub fn dump(conn: &Connection, shapes: &[Shape], rowid: &str) -> rusqlite::Result<Dump> {
    let mut dump = Dump::new();
    for shape in shapes {
        let mut statement = conn.prepare(&select_all(shape, rowid))?;
        let width = statement.column_count();
        let rows = statement
            .query_map([], |r| (0..width).map(|i| r.get::<_, Value>(i)).collect())?
            .collect::<rusqlite::Result<Vec<Vec<Value>>>>()?;
        dump.insert(shape.name.clone(), rows);
    }
    Ok(dump)
}

/// Whether two rows hold identical values: same storage classes and same
/// bytes, so `-0.0` differs from `0.0`.
fn identical(a: &[Value], b: &[Value]) -> bool {
    a.len() == b.len()
        && a.iter().zip(b).all(|pair| match pair {
            (Value::Real(x), Value::Real(y)) => x.to_bits() == y.to_bits(),
            (x, y) => x == y,
        })
}

/// The first difference between two dumps, if any.
pub fn difference(expected: &Dump, actual: &Dump) -> Option<String> {
    for (table, rows) in expected {
        let Some(other) = actual.get(table) else {
            return Some(format!("table {table} is missing"));
        };
        for (i, (want, got)) in rows.iter().zip(other).enumerate() {
            if !identical(want, got) {
                return Some(format!("{table} row {i}: expected {want:?}, found {got:?}"));
            }
        }
        if rows.len() != other.len() {
            return Some(format!(
                "{table} has {} rows, expected {}",
                other.len(),
                rows.len()
            ));
        }
    }
    actual
        .keys()
        .find(|t| !expected.contains_key(*t))
        .map(|t| format!("table {t} should not exist"))
}

/// Starts a read transaction on `conn` and returns the version it reads,
/// ending the transaction if the read fails.
pub fn begin_read(conn: &Connection) -> rusqlite::Result<Version> {
    conn.execute_batch("BEGIN")?;
    let version = conn.query_row("SELECT version FROM _sqlive_meta WHERE id = 0", [], |r| {
        r.get::<_, i64>(0)
    });
    if version.is_err() && !conn.is_autocommit() {
        let _ = conn.execute_batch("ROLLBACK");
    }
    version.map(|v| v as Version)
}

/// Runs `read` in the transaction [`begin_read`] started, then ends it.
pub fn finish_read<R>(
    conn: &Connection,
    read: impl FnOnce() -> rusqlite::Result<R>,
) -> rusqlite::Result<R> {
    let result = read().and_then(|value| conn.execute_batch("COMMIT").map(|()| value));
    if !conn.is_autocommit() {
        let _ = conn.execute_batch("ROLLBACK");
    }
    result
}

/// Runs `read` in a read transaction, with the version it sees.
pub fn read_at<R>(
    conn: &Connection,
    read: impl FnOnce(Version) -> rusqlite::Result<R>,
) -> rusqlite::Result<R> {
    let version = begin_read(conn)?;
    finish_read(conn, || read(version))
}

/// The database's version, tables and rows, read in one transaction.
pub fn read(conn: &Connection) -> rusqlite::Result<(Version, Vec<Shape>, Dump)> {
    read_at(conn, |version| {
        let shapes = shapes(conn)?;
        let dump = dump(conn, &shapes, "rowid")?;
        Ok((version, shapes, dump))
    })
}

/// The database's version and the rows of the tables named, read in one
/// transaction.
pub fn read_tables(
    conn: &Connection,
    names: &[String],
) -> rusqlite::Result<(Version, Vec<Shape>, Dump)> {
    read_at(conn, |version| {
        let mut shapes = shapes(conn)?;
        shapes.retain(|shape| names.iter().any(|n| n.eq_ignore_ascii_case(&shape.name)));
        let dump = dump(conn, &shapes, "rowid")?;
        Ok((version, shapes, dump))
    })
}

pub struct Replica {
    pub name: &'static str,
    conn: Connection,
    shapes: Vec<Shape>,
    pub version: Version,
    stream: Option<Changes>,
    /// The replica must reload from the database before it can continue.
    pub stale: bool,
    /// Tables the last event named whole, which the replica reloads before
    /// it applies another event.
    pub reload: Vec<String>,
    /// The version each reloaded table's rows are of, by lowercase name,
    /// while the replica is behind it: the table already reflects the
    /// events at or below that version.
    reloaded: BTreeMap<String, Version>,
    /// The most rows of one table an event may list.
    limit: usize,
}

#[derive(Debug, Default)]
pub struct Drained {
    pub events: u64,
    pub changes: u64,
    pub lagged: bool,
    pub schema_changed: bool,
    /// Tables the events named whole.
    pub whole_tables: u64,
}

impl Replica {
    /// A replica of a database whose events list at most `limit` rows of
    /// one table.
    pub fn new(name: &'static str, limit: usize) -> Replica {
        Replica {
            name,
            conn: Connection::open_in_memory().expect("an in-memory database"),
            shapes: Vec::new(),
            version: 0,
            stream: None,
            stale: true,
            reload: Vec::new(),
            reloaded: BTreeMap::new(),
            limit,
        }
    }

    /// Reloads every table from `oracle` and follows `db` from the
    /// version it read.
    pub fn rebuild(&mut self, oracle: &Connection, db: &LiveDb) -> Result<(), String> {
        let (version, shapes, dump) = read(oracle)
            .map_err(|e| format!("{} replica could not read the database: {e}", self.name))?;
        self.load(&shapes, &dump)
            .map_err(|e| format!("{} replica could not load: {e}", self.name))?;
        self.shapes = shapes;
        self.version = version;
        self.stream = Some(db.changes_since(version).map_err(|e| {
            format!(
                "{} replica could not follow version {version}: {e}",
                self.name
            )
        })?);
        self.stale = false;
        self.reload.clear();
        self.reloaded.clear();
        Ok(())
    }

    /// Replaces the rows of each table the last event named whole with
    /// `dump`'s, read at `version` from tables of `shapes`. Returns false,
    /// changing nothing, if a table has changed shape since, as it has when
    /// a schema change lies ahead in the stream.
    pub fn reload_from(
        &mut self,
        version: Version,
        shapes: &[Shape],
        dump: &Dump,
    ) -> Result<bool, String> {
        let known = |name: &str, shapes: &[Shape]| {
            shapes
                .iter()
                .find(|shape| shape.name.eq_ignore_ascii_case(name))
                .cloned()
        };
        let mut tables = Vec::new();
        for name in &self.reload {
            match (known(name, &self.shapes), known(name, shapes)) {
                (Some(held), Some(read)) if held == read => tables.push(held),
                _ => return Ok(false),
            }
        }
        for shape in tables {
            let reloaded = self
                .conn
                .execute_batch(&format!("DELETE FROM {}", quote(&shape.name)))
                .and_then(|()| self.insert_rows(&shape, &dump[&shape.name]));
            reloaded.map_err(|e| format!("{} replica reloading {}: {e}", self.name, shape.name))?;
            if version > self.version {
                self.reloaded
                    .insert(shape.name.to_ascii_lowercase(), version);
            }
        }
        self.reload.clear();
        Ok(true)
    }

    /// Whether `table` was reloaded at `version` or later, so its rows
    /// already reflect an event of that version.
    fn reflects(&self, table: &str, version: Version) -> bool {
        self.reloaded
            .get(&table.to_ascii_lowercase())
            .is_some_and(|reloaded| *reloaded >= version)
    }

    fn insert_rows(&self, shape: &Shape, rows: &[Vec<Value>]) -> rusqlite::Result<()> {
        let width = shape.columns.len() + usize::from(!shape.without_rowid);
        let marks = vec!["?"; width].join(", ");
        let mut insert = self.conn.prepare(&format!(
            "INSERT INTO {} VALUES ({marks})",
            quote(&shape.name)
        ))?;
        for row in rows {
            insert.execute(params_from_iter(row))?;
        }
        Ok(())
    }

    fn load(&self, shapes: &[Shape], dump: &Dump) -> rusqlite::Result<()> {
        let names: Vec<String> = self
            .conn
            .prepare("SELECT name FROM sqlite_schema WHERE type = 'table'")?
            .query_map([], |r| r.get(0))?
            .collect::<rusqlite::Result<_>>()?;
        for name in names {
            self.conn
                .execute_batch(&format!("DROP TABLE {}", quote(&name)))?;
        }
        for shape in shapes {
            let mut columns: Vec<String> = shape
                .columns
                .iter()
                .zip(&shape.collations)
                .map(|(c, collation)| format!("{} COLLATE {collation}", quote(c)))
                .collect();
            if !shape.without_rowid {
                columns.insert(0, "_rowid INTEGER".to_owned());
            }
            self.conn.execute_batch(&format!(
                "CREATE TABLE {} ({})",
                quote(&shape.name),
                columns.join(", ")
            ))?;
            self.insert_rows(shape, &dump[&shape.name])?;
        }
        Ok(())
    }

    /// Follows a reopened database from this replica's version. A replica
    /// behind the reopened log lags on its next drain and rebuilds; one
    /// ahead of the database fails, since it saw a commit the restart lost.
    pub fn resume(&mut self, db: &LiveDb) -> Result<(), String> {
        if self.stale {
            self.stream = None;
            return Ok(());
        }
        self.stream = Some(db.changes_since(self.version).map_err(|e| {
            format!(
                "{} replica at version {} cannot follow the reopened database: {e}",
                self.name, self.version
            )
        })?);
        Ok(())
    }

    pub fn dump(&self) -> rusqlite::Result<Dump> {
        dump(&self.conn, &self.shapes, "_rowid")
    }

    /// Applies up to `budget` events that have committed, stopping at one
    /// that names a table whole.
    pub fn drain(&mut self, budget: usize) -> Result<Drained, String> {
        let mut drained = Drained::default();
        while drained.events < budget as u64 && !self.stale && self.reload.is_empty() {
            let Some(stream) = self.stream.as_mut() else {
                break;
            };
            match stream.try_next() {
                None => break,
                Some(Err(Error::Lagged { .. })) => {
                    drained.lagged = true;
                    self.stale = true;
                }
                Some(Err(e)) => return Err(format!("{} replica stream failed: {e}", self.name)),
                Some(Ok(event)) => {
                    drained.events += 1;
                    drained.changes += event.changes.len() as u64;
                    drained.schema_changed |= event.schema_changed;
                    drained.whole_tables += event.whole_tables.len() as u64;
                    self.apply(&event)?;
                }
            }
        }
        Ok(drained)
    }

    fn apply(&mut self, event: &CommitEvent) -> Result<(), String> {
        if event.version != self.version + 1 {
            return Err(format!(
                "{} replica at version {} received event {}",
                self.name, self.version, event.version
            ));
        }
        self.version = event.version;
        if event.schema_changed {
            self.stale = true;
            return Ok(());
        }
        let mut listed: BTreeMap<String, usize> = BTreeMap::new();
        for change in &event.changes {
            if event
                .whole_tables
                .iter()
                .any(|table| table.eq_ignore_ascii_case(&change.table))
            {
                return Err(format!(
                    "event {} names {} whole and lists its row {change:?}",
                    event.version, change.table
                ));
            }
            let rows = listed.entry(change.table.to_ascii_lowercase()).or_default();
            *rows += 1;
            if *rows > self.limit {
                return Err(format!(
                    "event {} lists more than {} rows of {}",
                    event.version, self.limit, change.table
                ));
            }
            // A table reloaded at a later version already holds the change.
            if self.reflects(&change.table, event.version) {
                continue;
            }
            self.apply_change(change).map_err(|e| {
                format!(
                    "{} replica applying {change:?} from event {}: {e}",
                    self.name, event.version
                )
            })?;
        }
        self.reload = event
            .whole_tables
            .iter()
            .filter(|table| !self.reflects(table, event.version))
            .cloned()
            .collect();
        self.reloaded
            .retain(|_, reloaded| *reloaded > event.version);
        Ok(())
    }

    fn apply_change(&self, change: &RowChange) -> Result<(), String> {
        let shape = self
            .shapes
            .iter()
            .find(|s| s.name.eq_ignore_ascii_case(&change.table))
            .ok_or("no such table")?;
        for row in [&change.old, &change.new].into_iter().flatten() {
            if row.columns() != shape.columns.as_slice() {
                return Err(format!(
                    "row columns {:?} differ from {:?}",
                    row.columns(),
                    shape.columns
                ));
            }
        }
        let table = quote(&shape.name);
        let (filter, key): (String, Vec<Value>) = if shape.without_rowid {
            let row = change
                .old
                .as_ref()
                .or(change.new.as_ref())
                .ok_or("a change without rows")?;
            let filter: Vec<String> = shape
                .key
                .iter()
                .map(|(c, collation)| format!("{} = ? COLLATE {collation}", quote(c)))
                .collect();
            let key = shape
                .key
                .iter()
                .map(|(c, _)| row.get(c).cloned().ok_or("a key column is missing"))
                .collect::<Result<_, _>>()?;
            (filter.join(" AND "), key)
        } else {
            let rowid = change.rowid.ok_or("a rowid table change without a rowid")?;
            ("_rowid = ?".to_owned(), vec![Value::Integer(rowid)])
        };
        let matching: i64 = self
            .conn
            .query_row(
                &format!("SELECT count(*) FROM {table} WHERE {filter}"),
                params_from_iter(&key),
                |r| r.get(0),
            )
            .map_err(|e| e.to_string())?;
        let expected = i64::from(change.op != Op::Insert);
        if matching != expected {
            return Err(format!(
                "{matching} rows match its key, expected {expected}"
            ));
        }
        let sql_error = |e: rusqlite::Error| e.to_string();
        match change.op {
            Op::Insert => {
                let new = change.new.as_ref().ok_or("an insert without a new row")?;
                let mut values = new.values().to_vec();
                if !shape.without_rowid {
                    values.insert(0, Value::Integer(change.rowid.unwrap_or_default()));
                }
                let marks = vec!["?"; values.len()].join(", ");
                self.conn
                    .execute(
                        &format!("INSERT INTO {table} VALUES ({marks})"),
                        params_from_iter(&values),
                    )
                    .map_err(sql_error)?;
            }
            Op::Delete => {
                if change.old.is_none() || change.new.is_some() {
                    return Err("a delete must carry only an old row".into());
                }
                self.conn
                    .execute(
                        &format!("DELETE FROM {table} WHERE {filter}"),
                        params_from_iter(&key),
                    )
                    .map_err(sql_error)?;
            }
            Op::Update => {
                let new = change.new.as_ref().ok_or("an update without a new row")?;
                if change.old.is_none() {
                    return Err("an update without an old row".into());
                }
                let set: Vec<String> = shape
                    .columns
                    .iter()
                    .map(|c| format!("{} = ?", quote(c)))
                    .collect();
                let mut values = new.values().to_vec();
                values.extend(key);
                self.conn
                    .execute(
                        &format!("UPDATE {table} SET {} WHERE {filter}", set.join(", ")),
                        params_from_iter(&values),
                    )
                    .map_err(sql_error)?;
            }
        }
        Ok(())
    }
}
