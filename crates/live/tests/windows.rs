//! Constraint read sets on chosen queries: `LIMIT` window bounds, the
//! shapes that fall back to whole tables, parameter forms, and affinity.

mod common;

use common::open;
use sqlive::rusqlite::{ToSql, named_params};
use sqlive::{CommitEvent, LiveDb, QueryParams, ReadSet};

/// The event a write publishes.
fn commit(db: &LiveDb, sql: &str) -> CommitEvent {
    let ((), version) = db.write(|tx| tx.execute_batch(sql)).expect("write");
    let event = db
        .changes_since(version - 1)
        .expect("stream")
        .try_next()
        .expect("an event")
        .expect("no lag");
    event.as_ref().clone()
}

fn read_set(db: &LiveDb, sql: &str, params: impl QueryParams) -> ReadSet {
    db.query(sql, params, |_| Ok(())).unwrap().read_set
}

/// Room 1 holds chats created at 1 through 15; room 2 holds one.
fn chats() -> LiveDb {
    let mut ddl = String::from(
        "CREATE TABLE chats (id INTEGER PRIMARY KEY, room INTEGER, created_at INTEGER, body TEXT);",
    );
    for t in 1..=15 {
        ddl.push_str(&format!(
            "INSERT INTO chats (room, created_at, body) VALUES (1, {t}, 'chat {t}');"
        ));
    }
    ddl.push_str("INSERT INTO chats (room, created_at, body) VALUES (2, 100, 'elsewhere');");
    open("chats", &ddl)
}

/// Whether a write makes the read set's query re-run. Rows the write
/// inserts are deleted again, so each check starts from the same rows.
fn reruns(db: &LiveDb, read_set: &ReadSet, sql: &str) -> bool {
    let event = commit(db, sql);
    let hit = read_set.intersects(&event);
    for change in &event.changes {
        if let Some(rowid) = change.rowid.filter(|_| change.op == sqlive::Op::Insert) {
            commit(db, &format!("DELETE FROM chats WHERE id = {rowid}"));
        }
    }
    hit
}

fn insert(room: i64, created_at: &str) -> String {
    format!("INSERT INTO chats (room, created_at, body) VALUES ({room}, {created_at}, 'new')")
}

#[test]
fn a_full_window_bounds_the_rows_at_its_last_sort_key() {
    let db = chats();
    let sql = "SELECT * FROM chats WHERE room = ?1 ORDER BY created_at DESC LIMIT 10";
    let result = db.query(sql, [1], |r| r.get::<_, i64>(2)).unwrap();
    assert_eq!(result.rows, (6..=15).rev().collect::<Vec<_>>());
    let read_set = result.read_set;
    assert_eq!(
        read_set.to_string(),
        "chats[body, created_at, id, room] where room = 1 AND created_at >= 6"
    );
    assert!(!reruns(&db, &read_set, &insert(1, "3")), "below the window");
    assert!(
        reruns(&db, &read_set, &insert(1, "6")),
        "a tie at the boundary"
    );
    assert!(reruns(&db, &read_set, &insert(1, "20")), "above the window");
    assert!(!reruns(&db, &read_set, &insert(2, "20")), "another room");
    assert!(
        !reruns(
            &db,
            &read_set,
            "UPDATE chats SET body = 'x' WHERE created_at = 2"
        ),
        "a row below the window"
    );
    assert!(
        reruns(
            &db,
            &read_set,
            "UPDATE chats SET body = 'x' WHERE created_at = 10"
        ),
        "a row inside the window"
    );
    assert!(
        reruns(
            &db,
            &read_set,
            "UPDATE chats SET created_at = 12 WHERE created_at = 2"
        ),
        "a row moving into the window"
    );
    assert!(
        reruns(&db, &read_set, "DELETE FROM chats WHERE created_at = 9"),
        "a row deleted from the window"
    );
}

#[test]
fn a_window_that_is_not_full_bounds_nothing() {
    let db = chats();
    let read_set = read_set(
        &db,
        "SELECT * FROM chats WHERE room = 1 ORDER BY created_at DESC LIMIT 50",
        (),
    );
    assert_eq!(
        read_set.to_string(),
        "chats[body, created_at, id, room] where room = 1"
    );
    assert!(reruns(&db, &read_set, &insert(1, "0")));
}

#[test]
fn rows_before_an_offset_window_count() {
    let db = chats();
    for sql in [
        "SELECT * FROM chats WHERE room = 1 ORDER BY created_at DESC LIMIT 5 OFFSET 5",
        "SELECT * FROM chats WHERE room = 1 ORDER BY created_at DESC LIMIT 5, 5",
    ] {
        let result = db.query(sql, (), |r| r.get::<_, i64>(2)).unwrap();
        assert_eq!(result.rows, [10, 9, 8, 7, 6], "{sql}");
        let read_set = result.read_set;
        assert!(
            read_set.to_string().ends_with("created_at >= 6"),
            "{read_set}"
        );
        assert!(
            reruns(&db, &read_set, &insert(1, "14")),
            "shifts the window"
        );
        assert!(!reruns(&db, &read_set, &insert(1, "3")), "after the window");
    }
}

#[test]
fn a_zero_limit_depends_on_no_rows() {
    let db = chats();
    let read_set = read_set(&db, "SELECT * FROM chats ORDER BY created_at LIMIT 0", ());
    assert!(!reruns(&db, &read_set, &insert(1, "1")));
    assert!(read_set.intersects(&commit(&db, "CREATE TABLE more (x)")));
}

#[test]
fn nulls_sort_first_ascending_and_last_descending_unless_told() {
    let db = open(
        "nulls",
        "CREATE TABLE t (id INTEGER PRIMARY KEY, x);
         INSERT INTO t (x) VALUES (NULL), (NULL), (1), (4), (5);",
    );
    let check = |sql: &str, rows: &[Option<i64>], hits: &[(&str, bool)]| {
        let result = db.query(sql, (), |r| r.get::<_, Option<i64>>(1)).unwrap();
        assert_eq!(result.rows, rows, "{sql}");
        for (value, expected) in hits {
            let event = commit(&db, &format!("INSERT INTO t (x) VALUES ({value})"));
            assert_eq!(
                result.read_set.intersects(&event),
                *expected,
                "{sql}: inserting {value}; read set {}",
                result.read_set
            );
            commit(&db, "DELETE FROM t WHERE id = (SELECT max(id) FROM t)");
        }
    };
    check(
        "SELECT * FROM t ORDER BY x LIMIT 3",
        &[None, None, Some(1)],
        &[("NULL", true), ("1", true), ("2", false)],
    );
    check(
        "SELECT * FROM t ORDER BY x DESC LIMIT 2",
        &[Some(5), Some(4)],
        &[("NULL", false), ("4", true), ("10", true), ("3", false)],
    );
    check(
        "SELECT * FROM t ORDER BY x DESC NULLS FIRST LIMIT 3",
        &[None, None, Some(5)],
        &[("NULL", true), ("5", true), ("3", false)],
    );
    check(
        "SELECT * FROM t ORDER BY x NULLS LAST LIMIT 2",
        &[Some(1), Some(4)],
        &[("NULL", false), ("2", true), ("6", false)],
    );
    check(
        "SELECT * FROM t ORDER BY x LIMIT 2",
        &[None, None],
        &[("NULL", true), ("1", false)],
    );
    // The filter prints as SQL that selects the same rows.
    let shown = |sql: &str| db.query(sql, (), |_| Ok(())).unwrap().read_set.to_string();
    assert_eq!(
        shown("SELECT * FROM t ORDER BY x LIMIT 3"),
        "t[id, x] where (x IS NULL OR x <= 1)"
    );
    assert_eq!(
        shown("SELECT * FROM t ORDER BY x DESC LIMIT 2"),
        "t[id, x] where x >= 4"
    );
    assert_eq!(
        shown("SELECT * FROM t ORDER BY x LIMIT 2"),
        "t[id, x] where x IS NULL"
    );
    assert_eq!(shown("SELECT * FROM t ORDER BY x DESC LIMIT 5"), "t[id, x]");
}

#[test]
fn a_window_orders_text_under_the_sort_collation() {
    let db = open(
        "collated",
        "CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT COLLATE NOCASE);
         INSERT INTO t (name) VALUES ('a'), ('B'), ('c');",
    );
    let read_set = read_set(&db, "SELECT * FROM t ORDER BY name LIMIT 2", ());
    assert!(read_set.intersects(&commit(&db, "INSERT INTO t (name) VALUES ('b')")));
    assert!(!read_set.intersects(&commit(&db, "INSERT INTO t (name) VALUES ('C')")));
    let binary = db
        .query(
            "SELECT * FROM t ORDER BY name COLLATE BINARY LIMIT 2",
            (),
            |_| Ok(()),
        )
        .unwrap()
        .read_set;
    // Under BINARY, uppercase sorts first: the window holds 'B' and 'C'.
    assert!(!binary.intersects(&commit(&db, "INSERT INTO t (name) VALUES ('d')")));
    assert!(binary.intersects(&commit(&db, "INSERT INTO t (name) VALUES ('A')")));
}

#[test]
fn a_window_needs_its_sort_key_among_the_results() {
    let db = chats();
    let read_set = read_set(
        &db,
        "SELECT body FROM chats WHERE room = 1 ORDER BY created_at DESC LIMIT 10",
        (),
    );
    assert_eq!(
        read_set.to_string(),
        "chats[body, created_at, room] where room = 1"
    );
    assert!(reruns(&db, &read_set, &insert(1, "3")));
}

#[test]
fn shapes_the_analysis_cannot_model_fall_back_to_whole_tables() {
    let db = open(
        "fallbacks",
        "CREATE TABLE a (id INTEGER PRIMARY KEY, x, y);
         CREATE TABLE b (id INTEGER PRIMARY KEY, a_id, w);
         CREATE VIEW av AS SELECT * FROM a WHERE x = 1;
         INSERT INTO a (x, y) VALUES (1, 1), (2, 2);
         INSERT INTO b (a_id, w) VALUES (1, 1);",
    );
    for sql in [
        "SELECT * FROM a WHERE x = 1 OR x = 2",
        "SELECT * FROM a WHERE lower(y) = 'q'",
        "SELECT * FROM a WHERE id IN (SELECT a_id FROM b WHERE w = 1) AND x = 1",
        "SELECT * FROM av",
        "WITH q AS (SELECT * FROM a WHERE x = 1) SELECT * FROM q",
        "SELECT x FROM a WHERE x = 1 UNION SELECT w FROM b",
        "SELECT * FROM a WHERE x + 0 = 1",
        // An aggregate summarizes every row, wherever it hides.
        "SELECT x, count(*) BETWEEN 1 AND 2 FROM a ORDER BY x LIMIT 1",
        // A subquery can hide in a `FILTER` or `OVER` clause.
        "SELECT x, count(*) FILTER (WHERE y > (SELECT avg(y) FROM a)) FROM a WHERE x = 1",
        "SELECT x, row_number() OVER (ORDER BY (SELECT max(y) FROM a)) FROM a WHERE x = 1",
    ] {
        let read_set = read_set(&db, sql, ());
        let event = commit(&db, "INSERT INTO a (x, y) VALUES (9, 9)");
        assert!(read_set.intersects(&event), "{sql}: {read_set}");
    }
    let joined = read_set(
        &db,
        "SELECT * FROM a LEFT JOIN b ON a.id = b.a_id WHERE a.x = 1 AND b.w = 1",
        (),
    );
    assert_eq!(
        joined.to_string(),
        "a[id, x, y] where x = 1; b[a_id, id, w]",
        "the nullable side of a left join stays whole"
    );
    assert!(joined.intersects(&commit(&db, "INSERT INTO b (a_id, w) VALUES (1, 2)")));
    let grouped = read_set(
        &db,
        "SELECT x, count(*) FROM a GROUP BY x ORDER BY x LIMIT 1",
        (),
    );
    assert!(
        grouped.intersects(&commit(&db, "INSERT INTO a (x, y) VALUES (50, 0)")),
        "a grouped window has no bound: {grouped}"
    );
}

#[test]
fn an_equality_on_one_key_skips_writes_to_other_keys() {
    let db = chats();
    let read_set = read_set(&db, "SELECT * FROM chats WHERE room = ?1", [1]);
    assert!(!reruns(&db, &read_set, &insert(2, "1")));
    assert!(!reruns(
        &db,
        &read_set,
        "UPDATE chats SET body = 'x' WHERE room = 2"
    ));
    assert!(reruns(&db, &read_set, &insert(1, "1")));
    assert!(
        reruns(
            &db,
            &read_set,
            "UPDATE chats SET room = 2 WHERE created_at = 4"
        ),
        "a row leaving the key"
    );
}

#[test]
fn every_parameter_form_binds_its_value_into_the_constraint() {
    let db = chats();
    let positional: [(&str, &[&dyn ToSql]); 3] = [
        ("SELECT * FROM chats WHERE room = ?", &[&1]),
        (
            "SELECT * FROM chats WHERE created_at > ? AND room = ?2",
            &[&0, &1],
        ),
        ("SELECT * FROM chats WHERE room = ?3", &[&0, &0, &1]),
    ];
    for (sql, params) in positional {
        let read_set = read_set(&db, sql, params);
        assert!(
            read_set.to_string().contains("room = 1"),
            "{sql}: {read_set}"
        );
    }
    for sql in [
        "SELECT * FROM chats WHERE room = :room",
        "SELECT * FROM chats WHERE room = @room",
        "SELECT * FROM chats WHERE room = $room",
    ] {
        let prefix = &sql[sql.len() - 5..sql.len() - 4];
        let name = format!("{prefix}room");
        let named: &[(&str, &dyn ToSql)] = &[(name.as_str(), &1)];
        let read_set = read_set(&db, sql, named);
        assert!(
            read_set.to_string().contains("room = 1"),
            "{sql}: {read_set}"
        );
    }
    let mixed = read_set(
        &db,
        "SELECT * FROM chats WHERE room = :room AND created_at >= :since",
        named_params! { ":since": 3, ":room": 1 },
    );
    assert!(
        mixed.to_string().contains("room = 1 AND created_at >= 3"),
        "{mixed}"
    );
}

#[test]
fn a_constant_converts_by_the_column_affinity() {
    let db = chats();
    let read_set = read_set(&db, "SELECT * FROM chats WHERE created_at = ?1", ["5"]);
    assert!(
        read_set.to_string().contains("created_at = 5"),
        "{read_set}"
    );
    assert!(reruns(&db, &read_set, &insert(1, "5")));
    assert!(!reruns(&db, &read_set, &insert(1, "6")));
    let text = read_set_text(&db);
    assert!(text.intersects(&commit(
        &db,
        "INSERT INTO chats (room, body) VALUES (1, '5')"
    )));
}

/// `body` has TEXT affinity, so an integer constant compares as text.
fn read_set_text(db: &LiveDb) -> ReadSet {
    let read_set = read_set(db, "SELECT * FROM chats WHERE body = ?1", [5]);
    assert!(read_set.to_string().contains("body = '5'"), "{read_set}");
    read_set
}

/// A reader caches the statements it runs. After a column before the sort
/// key is dropped, a statement cached earlier must still find the sort key,
/// now one column to the left.
#[test]
fn a_window_finds_its_sort_key_after_a_column_before_it_is_dropped() {
    let db = open(
        "drop-column",
        "CREATE TABLE t (id INTEGER PRIMARY KEY, a, c, d);
         INSERT INTO t (a, c, d) VALUES (1, 10, 100), (2, 20, 200), (3, 30, 300);",
    );
    let sql = "SELECT * FROM t ORDER BY c DESC LIMIT 2";
    for _ in 0..2 {
        assert_eq!(
            read_set(&db, sql, ()).to_string(),
            "t[a, c, d, id] where c >= 20"
        );
    }
    db.write(|tx| tx.execute_batch("ALTER TABLE t DROP COLUMN a"))
        .unwrap();
    for _ in 0..2 {
        assert_eq!(
            read_set(&db, sql, ()).to_string(),
            "t[c, d, id] where c >= 20"
        );
    }
}
