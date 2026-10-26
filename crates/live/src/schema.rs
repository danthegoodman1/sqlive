//! What SqLive knows about each table in the main database: columns,
//! affinities, collations, and how to address a row.

use crate::compare::Collation;
use rusqlite::Connection;
use std::collections::BTreeMap;
use std::sync::Arc;

/// The table that holds the database's version.
pub(crate) const META_TABLE: &str = "_sqlive_meta";

#[derive(Debug)]
pub(crate) struct Column {
    pub name: String,
    /// False for a `VIRTUAL` generated column.
    pub stored: bool,
}

#[derive(Debug)]
pub(crate) struct Table {
    pub name: String,
    /// Every column, in `cid` order.
    pub columns: Vec<Column>,
    /// Names of the stored columns: the layout of a [`crate::Row`].
    pub stored: Arc<[String]>,
    /// The `cid` of each stored column.
    pub stored_cids: Vec<usize>,
    /// The `cid`s of the primary key, in key order.
    pub pk: Vec<usize>,
    /// The collation each primary-key column compares with in the key,
    /// which `PRIMARY KEY (c COLLATE ...)` can set apart from the column's.
    pub pk_collations: Vec<Collation>,
    pub without_rowid: bool,
    /// A name for the rowid that no column shadows.
    pub rowid_name: Option<&'static str>,
    /// Reads a row's stored columns by rowid or primary key.
    pub select_by_key: String,
}

#[derive(Debug, Default)]
pub(crate) struct Schema {
    /// `PRAGMA schema_version` when loaded.
    pub version: i64,
    tables: BTreeMap<String, Arc<Table>>,
}

/// Quotes an identifier for SQL.
pub(crate) fn quote(ident: &str) -> String {
    format!("\"{}\"", ident.replace('"', "\"\""))
}

impl Schema {
    pub fn table(&self, name: &str) -> Option<&Arc<Table>> {
        self.tables.get(&name.to_ascii_lowercase())
    }

    pub fn load(conn: &Connection) -> rusqlite::Result<Schema> {
        let version: i64 = conn.query_row("PRAGMA schema_version", [], |r| r.get(0))?;
        let mut list = conn.prepare(
            "SELECT name, wr FROM pragma_table_list
             WHERE schema = 'main' AND type IN ('table', 'shadow')
               AND substr(name, 1, 7) <> 'sqlite_'
             ORDER BY name",
        )?;
        let names: Vec<(String, bool)> = list
            .query_map([], |r| Ok((r.get(0)?, r.get(1)?)))?
            .collect::<rusqlite::Result<_>>()?;
        let mut tables = BTreeMap::new();
        for (name, without_rowid) in names {
            let table = load_table(conn, &name, without_rowid)?;
            tables.insert(name.to_ascii_lowercase(), Arc::new(table));
        }
        Ok(Schema { version, tables })
    }
}

fn load_table(conn: &Connection, name: &str, without_rowid: bool) -> rusqlite::Result<Table> {
    let mut info = conn
        .prepare("SELECT cid, name, type, pk, hidden FROM pragma_table_xinfo(?1) ORDER BY cid")?;
    let rows: Vec<(usize, String, String, i64, i64)> = info
        .query_map([name], |r| {
            Ok((
                r.get::<_, i64>(0)? as usize,
                r.get(1)?,
                r.get(2)?,
                r.get(3)?,
                r.get(4)?,
            ))
        })?
        .collect::<rusqlite::Result<_>>()?;
    let mut columns = Vec::new();
    let mut pk = Vec::new();
    for (cid, column, _declared, pk_position, hidden) in rows {
        if pk_position > 0 {
            pk.push((pk_position, cid));
        }
        columns.push(Column {
            name: column,
            stored: hidden != 2,
        });
    }
    pk.sort_unstable();
    let pk: Vec<usize> = pk.into_iter().map(|(_, cid)| cid).collect();
    let pk_collations = if without_rowid {
        key_collations(conn, name, &pk)?
    } else {
        Vec::new()
    };
    let stored_cids: Vec<usize> = (0..columns.len()).filter(|&i| columns[i].stored).collect();
    let stored: Arc<[String]> = stored_cids
        .iter()
        .map(|&i| columns[i].name.clone())
        .collect();
    let rowid_name = if without_rowid {
        None
    } else {
        ["rowid", "_rowid_", "oid"]
            .into_iter()
            .find(|alias| !columns.iter().any(|c| c.name.eq_ignore_ascii_case(alias)))
    };
    let list = stored
        .iter()
        .map(|c| quote(c))
        .collect::<Vec<_>>()
        .join(", ");
    let condition = if without_rowid {
        pk.iter()
            .zip(&pk_collations)
            .enumerate()
            .map(|(i, (&cid, collation))| {
                format!(
                    "{} = ?{} COLLATE {}",
                    quote(&columns[cid].name),
                    i + 1,
                    quote(collation.name())
                )
            })
            .collect::<Vec<_>>()
            .join(" AND ")
    } else {
        format!("{} = ?1", rowid_name.unwrap_or("rowid"))
    };
    let select_by_key = format!("SELECT {list} FROM main.{} WHERE {condition}", quote(name));
    Ok(Table {
        name: name.to_owned(),
        columns,
        stored,
        stored_cids,
        pk,
        pk_collations,
        without_rowid,
        rowid_name,
        select_by_key,
    })
}

/// The collations of a `WITHOUT ROWID` table's primary-key index, in key
/// order.
fn key_collations(
    conn: &Connection,
    table: &str,
    pk: &[usize],
) -> rusqlite::Result<Vec<Collation>> {
    let index: String = conn.query_row(
        "SELECT name FROM pragma_index_list(?1) WHERE origin = 'pk'",
        [table],
        |r| r.get(0),
    )?;
    let mut info =
        conn.prepare("SELECT cid, coll FROM pragma_index_xinfo(?1) WHERE key = 1 ORDER BY seqno")?;
    let keyed: Vec<(i64, String)> = info
        .query_map([&index], |r| Ok((r.get(0)?, r.get(1)?)))?
        .collect::<rusqlite::Result<_>>()?;
    Ok(pk
        .iter()
        .map(|&cid| {
            keyed
                .iter()
                .find(|(c, _)| *c as usize == cid)
                .map_or(Collation::Binary, |(_, name)| Collation::from_name(name))
        })
        .collect())
}
