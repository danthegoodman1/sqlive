//! Result hashes and `rerun`: equal results hash the same, any difference in
//! a column name, a value, its storage class, the row order or the result's
//! shape changes the hash, and a re-run whose rows hash the same returns
//! none. Queries name their columns, so each pair below differs only where
//! it means to.

mod common;

use common::open;
use sqlive::rusqlite::Row;
use sqlive::{LiveDb, ResultHash};

fn hash(db: &LiveDb, sql: &str) -> ResultHash {
    db.query(sql, (), |_| Ok(())).expect(sql).hash
}

#[test]
fn the_same_result_hashes_the_same() {
    let db = open(
        "same",
        "CREATE TABLE t (a, b); INSERT INTO t VALUES (1, 'x'), (2, NULL);",
    );
    let sql = "SELECT a, b FROM t ORDER BY a";
    assert_eq!(hash(&db, sql), hash(&db, sql));
    // Another query with the same columns and rows hashes the same.
    assert_eq!(
        hash(&db, sql),
        hash(
            &db,
            "SELECT column1 AS a, column2 AS b FROM (VALUES (1, 'x'), (2, NULL))"
        )
    );
}

#[test]
fn column_names_change_the_hash() {
    let db = open("names", "");
    let pairs = [
        ("SELECT 1 AS a", "SELECT 1 AS b"),
        // Where one name ends and the next begins.
        ("SELECT 1 AS ab, 2 AS c", "SELECT 1 AS a, 2 AS bc"),
        // With no rows, the columns alone tell results apart.
        ("SELECT 1 AS a WHERE 0", "SELECT 1 AS a, 2 AS b WHERE 0"),
    ];
    for (a, b) in pairs {
        assert_ne!(hash(&db, a), hash(&db, b), "{a} and {b}");
    }
    assert_eq!(
        hash(&db, "SELECT 1 AS a WHERE 0"),
        hash(&db, "SELECT 2 AS a WHERE 0")
    );
}

#[test]
fn every_difference_changes_the_hash() {
    let db = open("differ", "");
    let pairs = [
        // Storage classes.
        ("SELECT 1 AS v", "SELECT '1' AS v"),
        ("SELECT 1 AS v", "SELECT 1.0 AS v"),
        ("SELECT 0 AS v", "SELECT 0.0 AS v"),
        ("SELECT 'a' AS v", "SELECT x'61' AS v"),
        ("SELECT NULL AS v", "SELECT '' AS v"),
        ("SELECT '' AS v", "SELECT x'' AS v"),
        // Reals by bits.
        ("SELECT 0.0 AS v", "SELECT -0.0 AS v"),
        // Where one value ends and the next begins.
        ("SELECT 'ab' AS v, 'c' AS w", "SELECT 'a' AS v, 'bc' AS w"),
        (
            "SELECT x'0102' AS v, x'03' AS w",
            "SELECT x'01' AS v, x'0203' AS w",
        ),
        // Rows.
        ("VALUES (1)", "VALUES (1), (1)"),
        ("VALUES (1), (2)", "VALUES (2), (1)"),
    ];
    for (a, b) in pairs {
        assert_ne!(hash(&db, a), hash(&db, b), "{a} and {b}");
    }
}

#[test]
fn large_results_hash_every_byte() {
    let db = open("large", "");
    // A value large enough to bypass the buffer, with one byte changed in
    // the middle.
    let wide = "SELECT printf('%.*c', 100000, 'a') AS v";
    assert_eq!(
        hash(&db, wide),
        hash(
            &db,
            "SELECT replace(printf('%.*c', 100000, 'z'), 'z', 'a') AS v"
        )
    );
    assert_ne!(
        hash(&db, wide),
        hash(
            &db,
            "SELECT printf('%.*c', 50000, 'a') || 'b' || printf('%.*c', 49999, 'a') AS v"
        )
    );
    // Many small values that fill the buffer several times over, with one
    // changed near the end.
    let rows = |last: &str| {
        format!(
            "WITH RECURSIVE n(i) AS (SELECT 1 UNION ALL SELECT i + 1 FROM n WHERE i < 2000)
             SELECT i, CASE WHEN i = 1999 THEN '{last}' ELSE printf('row %d', i) END AS v FROM n"
        )
    };
    assert_eq!(hash(&db, &rows("row 1999")), hash(&db, &rows("row 1999")));
    assert_ne!(hash(&db, &rows("row 1999")), hash(&db, &rows("row 1998")));
}

/// A hash outlives the process that computed it, as an ETag a client keeps
/// does, so the encoding stays fixed. SQLite leaves the names of unnamed
/// expressions unspecified, so the query names its columns.
#[test]
fn a_hash_stays_the_same_across_releases() {
    let db = open("stable", "");
    let found = hash(
        &db,
        "SELECT 1 AS a, 2.5 AS b, 'text' AS c, x'00ff' AS d, NULL AS e",
    );
    assert_eq!(
        found.to_string(),
        "9ce58f40b6842c562abb8b4735cc1c6a556803f5e1455bf3cfcae11dca5c5aaf"
    );
    assert_eq!(ResultHash::from_bytes(*found.as_bytes()), found);
}

#[test]
fn a_hash_reads_back_from_its_hex() {
    let db = open("hex", "");
    let found = hash(&db, "SELECT 1, 'a'");
    let hex = found.to_string();
    assert_eq!(ResultHash::from_hex(&hex), Some(found));
    assert_eq!(ResultHash::from_hex(&hex.to_uppercase()), Some(found));
    assert_eq!(ResultHash::from_hex(&hex[1..]), None, "too short");
    assert_eq!(ResultHash::from_hex(&format!("{hex}0")), None, "too long");
    assert_eq!(
        ResultHash::from_hex(&format!("g{}", &hex[1..])),
        None,
        "not hex"
    );
}

const TODOS: &str = "SELECT id, title FROM todos WHERE list_id = ?1 ORDER BY id DESC LIMIT 2";

fn title(row: &Row<'_>) -> sqlive::rusqlite::Result<String> {
    row.get(1)
}

#[test]
fn a_rerun_returns_rows_only_when_they_changed() {
    let db = open(
        "rerun",
        "CREATE TABLE todos (id INTEGER PRIMARY KEY, list_id INTEGER, title TEXT);
         INSERT INTO todos (list_id, title) VALUES (5, 'a'), (5, 'b'), (5, 'c'), (6, 'd');",
    );
    let first = db.query(TODOS, [5], title).unwrap();
    assert_eq!(first.rows, ["c", "b"]);
    assert!(!first.unchanged, "a plain query never reports unchanged");

    // A write elsewhere leaves the rows as they were.
    db.write(|tx| tx.execute("UPDATE todos SET title = 'e' WHERE list_id = 6", []))
        .unwrap();
    let same = db.rerun(TODOS, [5], first.hash, title).unwrap();
    assert!(same.unchanged);
    assert!(same.rows.is_empty(), "an unchanged result carries no rows");
    assert_eq!(same.hash, first.hash);
    assert!(same.version > first.version);
    // The read set is this snapshot's, window bound included.
    let fresh = db.query(TODOS, [5], title).unwrap();
    assert_eq!(same.read_set.to_string(), fresh.read_set.to_string());
    assert_eq!(same.read_set.version(), same.version);

    // A write to a returned row changes the hash and returns the rows.
    db.write(|tx| tx.execute("UPDATE todos SET title = 'B' WHERE title = 'b'", []))
        .unwrap();
    let changed = db.rerun(TODOS, [5], same.hash, title).unwrap();
    assert!(!changed.unchanged);
    assert_eq!(changed.rows, ["c", "B"]);
    assert_ne!(changed.hash, same.hash);

    // Rows that change and change back hash as they did before.
    db.write(|tx| tx.execute("UPDATE todos SET title = 'b' WHERE title = 'B'", []))
        .unwrap();
    let back = db.rerun(TODOS, [5], first.hash, title).unwrap();
    assert!(back.unchanged);
}

#[test]
fn a_snapshot_reruns_at_its_own_version() {
    let db = open(
        "snapshot",
        "CREATE TABLE todos (id INTEGER PRIMARY KEY, list_id INTEGER, title TEXT);
         INSERT INTO todos (list_id, title) VALUES (5, 'a');",
    );
    let snapshot = db.snapshot().unwrap();
    let first = snapshot.query(TODOS, [5], title).unwrap();
    db.write(|tx| tx.execute("INSERT INTO todos (list_id, title) VALUES (5, 'b')", []))
        .unwrap();
    let again = snapshot.rerun(TODOS, [5], first.hash, title).unwrap();
    assert!(again.unchanged, "the snapshot predates the insert");
    drop(snapshot);
    let later = db.rerun(TODOS, [5], first.hash, title).unwrap();
    assert_eq!(later.rows, ["b", "a"]);
}
