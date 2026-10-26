//! Queries against SQLite on disk: rows, versions, snapshots, and the read
//! sets that decide when a result is stale.

mod common;

use common::open;
use sqlive::rusqlite::{named_params, params};
use sqlive::{CommitEvent, LiveDb};

fn ints(row: &sqlive::rusqlite::Row<'_>) -> sqlive::rusqlite::Result<i64> {
    row.get(0)
}

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

#[test]
fn a_query_returns_rows_its_version_and_what_it_read() {
    let db = open(
        "query",
        "CREATE TABLE todos (id INTEGER PRIMARY KEY, list_id INTEGER, title TEXT, done INTEGER);
         INSERT INTO todos (list_id, title, done) VALUES (5, 'a', 0), (5, 'b', 1), (6, 'c', 0);",
    );
    let result = db
        .query(
            "SELECT title FROM todos WHERE list_id = ?1 ORDER BY id DESC",
            [5],
            |r| r.get::<_, String>(0),
        )
        .unwrap();
    assert_eq!(result.rows, ["b", "a"]);
    assert_eq!(result.version, db.version());
    assert_eq!(result.read_set.version(), result.version);
    assert_eq!(
        result.read_set.to_string(),
        "todos[id, list_id, title] where list_id = 5"
    );
}

#[test]
fn parameters_bind_by_position_and_by_name() {
    let db = open(
        "params",
        "CREATE TABLE t (a, b); INSERT INTO t VALUES (1, 'x'), (2, 'y'), (3, 'x');",
    );
    let by_tuple = db
        .query("SELECT a FROM t WHERE a > ?1 AND b = ?2", (1, "x"), ints)
        .unwrap();
    assert_eq!(by_tuple.rows, [3]);
    let by_vec = db
        .query("SELECT a FROM t WHERE a IN (?1, ?2)", vec![1, 2], ints)
        .unwrap();
    assert_eq!(by_vec.rows, [1, 2]);
    let by_name = db
        .query(
            "SELECT a FROM t WHERE b = :b",
            named_params! { ":b": "y" },
            ints,
        )
        .unwrap();
    assert_eq!(by_name.rows, [2]);
    let by_macro = db
        .query("SELECT a FROM t WHERE a = ?1", params![3], ints)
        .unwrap();
    assert_eq!(by_macro.rows, [3]);
    let none = db.query("SELECT count(*) FROM t", (), ints).unwrap();
    assert_eq!(none.rows, [3]);
    assert!(
        db.query("SELECT ?1", [1, 2], ints).is_err(),
        "too many parameters"
    );
    assert!(
        db.query("SELECT a FROM t WHERE a = ?1 OR b = ?2", [1], ints)
            .is_err(),
        "too few parameters"
    );
}

#[test]
fn views_ctes_subqueries_and_joins_report_their_base_tables() {
    let db = open(
        "deps",
        "CREATE TABLE a (id INTEGER PRIMARY KEY, x, y, z);
         CREATE TABLE b (id INTEGER PRIMARY KEY, a_id, w);
         CREATE TABLE c (id INTEGER PRIMARY KEY, v);
         CREATE VIEW av AS SELECT x FROM a WHERE y > 0;",
    );
    let read = |sql: &str| db.query(sql, (), |_| Ok(())).unwrap().read_set.to_string();
    assert_eq!(read("SELECT * FROM av"), "a[x, y]");
    assert_eq!(read("WITH q AS (SELECT z FROM a) SELECT * FROM q"), "a[z]");
    assert_eq!(
        read("SELECT x FROM a WHERE id IN (SELECT a_id FROM b WHERE w = 1)"),
        "a[id, x]; b[a_id, w]"
    );
    assert_eq!(
        read("SELECT a.x, c.v FROM a JOIN c ON c.id = a.z"),
        "a[x, z]; c[id, v]"
    );
    assert_eq!(read("SELECT count(*) FROM c"), "c[]");
}

#[test]
fn intersects_finds_the_events_that_can_change_a_result() {
    let db = open(
        "intersects",
        "CREATE TABLE t (id INTEGER PRIMARY KEY, a, b, c);
         CREATE TABLE other (x);
         INSERT INTO t VALUES (1, 1, 1, 1);",
    );
    let read_set = db
        .query("SELECT a FROM t WHERE b = 1", (), |_| Ok(()))
        .unwrap()
        .read_set;
    assert_eq!(read_set.to_string(), "t[a, b] where b = 1");
    let count = db
        .query("SELECT count(*) FROM t", (), |_| Ok(()))
        .unwrap()
        .read_set;

    let unread = commit(&db, "UPDATE t SET c = 2");
    assert!(
        !read_set.intersects(&unread),
        "an update to an unread column"
    );
    assert!(!count.intersects(&unread), "count(*) reads no column");
    let selected = commit(&db, "UPDATE t SET a = 5");
    assert!(read_set.intersects(&selected), "a matching row changed");
    commit(&db, "UPDATE t SET a = 0.0");
    let negative_zero = commit(&db, "UPDATE t SET a = -0.0");
    assert_eq!(negative_zero.changes.len(), 1);
    assert!(
        read_set.intersects(&negative_zero),
        "0.0 to -0.0 is a change"
    );
    let leaving = commit(&db, "UPDATE t SET b = 2");
    assert!(read_set.intersects(&leaving), "a row left the filter");
    let outside = commit(&db, "UPDATE t SET a = 6");
    assert!(!read_set.intersects(&outside), "a row outside the filter");
    let elsewhere = commit(&db, "INSERT INTO other VALUES (1)");
    assert!(!read_set.intersects(&elsewhere), "another table");
    let missing = commit(&db, "INSERT INTO t VALUES (2, 0, 0, 0)");
    assert!(
        !read_set.intersects(&missing),
        "an insert the filter rejects"
    );
    assert!(count.intersects(&missing));
    let entering = commit(&db, "INSERT INTO t VALUES (3, 0, 1, 0)");
    assert!(
        read_set.intersects(&entering),
        "an insert the filter accepts"
    );
    let deleted = commit(&db, "DELETE FROM t WHERE id = 3");
    assert!(read_set.intersects(&deleted), "a matching row deleted");
    assert!(count.intersects(&deleted));
    let schema = commit(&db, "CREATE TABLE more (y)");
    assert!(read_set.intersects(&schema), "a schema change");

    let current = db
        .query("SELECT a FROM t WHERE b = 1", (), |_| Ok(()))
        .unwrap()
        .read_set;
    assert!(
        !current.intersects(&deleted),
        "an event at or below the read set's version"
    );
    let next = commit(&db, "INSERT INTO t VALUES (4, 0, 1, 0)");
    assert_eq!(next.version, current.version() + 1);
    assert!(
        current.intersects(&next),
        "the event right after the version"
    );
}

#[test]
fn an_update_between_texts_that_are_not_utf8_intersects() {
    let db = open(
        "lossy",
        "CREATE TABLE t (id INTEGER PRIMARY KEY, body);
         INSERT INTO t VALUES (1, CAST(x'ff' AS TEXT));",
    );
    let read_set = db
        .query("SELECT body FROM t", (), |_| Ok(()))
        .unwrap()
        .read_set;
    let event = commit(&db, "UPDATE t SET body = CAST(x'fe' AS TEXT)");
    assert_eq!(event.changes.len(), 1, "the bytes changed");
    assert!(
        read_set.intersects(&event),
        "both texts read as U+FFFD, but they differ"
    );
}

#[test]
fn a_query_of_the_version_table_sees_every_commit() {
    let db = open("meta", "CREATE TABLE t (a);");
    let read_set = db
        .query("SELECT version FROM _sqlive_meta", (), ints)
        .unwrap()
        .read_set;
    assert!(read_set.intersects(&commit(&db, "INSERT INTO t VALUES (1)")));
}

#[test]
fn a_query_of_an_internal_table_sees_every_commit() {
    let db = open(
        "sequence",
        "CREATE TABLE t (id INTEGER PRIMARY KEY AUTOINCREMENT, x);",
    );
    let read_set = db
        .query("SELECT seq FROM sqlite_sequence WHERE name = 't'", (), ints)
        .unwrap()
        .read_set;
    let event = commit(&db, "INSERT INTO t (x) VALUES (1)");
    assert!(
        event.changes.iter().all(|c| c.table == "t"),
        "the hook reports no sqlite_sequence change: {event:?}"
    );
    assert!(read_set.intersects(&event));
}

#[test]
fn a_held_snapshot_ignores_later_writes() {
    let db = open("snapshot", "CREATE TABLE t (a); INSERT INTO t VALUES (1);");
    let snapshot = db.snapshot().unwrap();
    let held = snapshot.version();
    db.write(|tx| tx.execute_batch("INSERT INTO t VALUES (2)"))
        .unwrap();
    let old = snapshot.query("SELECT count(*) FROM t", (), ints).unwrap();
    assert_eq!((old.rows[0], old.version), (1, held));
    let fresh = db.query("SELECT count(*) FROM t", (), ints).unwrap();
    assert_eq!((fresh.rows[0], fresh.version), (2, held + 1));
    let again = snapshot.query("SELECT a FROM t", (), ints).unwrap();
    assert_eq!(again.rows, [1]);
    drop(snapshot);
    let late = db.changes_since(held).unwrap().try_next().unwrap().unwrap();
    assert!(
        old.read_set.intersects(&late),
        "the commit after the snapshot reaches a stream from its version"
    );
}

#[test]
fn a_query_may_not_end_its_snapshot_reach_another_database_or_write() {
    let db = open("guard", "CREATE TABLE t (a);");
    let snapshot = db.snapshot().unwrap();
    for sql in [
        "COMMIT",
        "BEGIN",
        "SAVEPOINT s",
        "ATTACH ':memory:' AS other",
        "PRAGMA query_only = 0",
        "PRAGMA user_version",
        "INSERT INTO t VALUES (1)",
    ] {
        assert!(snapshot.query(sql, (), |_| Ok(())).is_err(), "{sql}");
    }
    let columns = snapshot
        .query("SELECT name FROM pragma_table_info('t')", (), |r| {
            r.get::<_, String>(0)
        })
        .unwrap();
    assert_eq!(columns.rows, ["a"], "pragma functions are reads");
}

#[test]
fn a_schema_change_refreshes_the_reads_of_a_repeated_query() {
    let db = open("cache", "CREATE TABLE t (a);");
    let sql = "SELECT * FROM t";
    let before = db.query(sql, (), |_| Ok(())).unwrap();
    assert_eq!(before.read_set.to_string(), "t[a]");
    db.write(|tx| tx.execute_batch("ALTER TABLE t ADD COLUMN b"))
        .unwrap();
    let after = db.query(sql, (), |_| Ok(())).unwrap();
    assert_eq!(after.read_set.to_string(), "t[a, b]");
}

#[test]
fn a_full_text_query_depends_on_its_shadow_tables() {
    let db = open("fts", "CREATE VIRTUAL TABLE docs USING fts5 (body);");
    let read_set = db
        .query("SELECT rowid FROM docs WHERE docs MATCH ?1", ["milk"], ints)
        .unwrap()
        .read_set;
    let event = commit(&db, "INSERT INTO docs VALUES ('buy milk')");
    assert!(!event.changes.is_empty());
    assert!(
        event.changes.iter().all(|c| c.table.starts_with("docs_")),
        "writes reach only the shadow tables: {event:?}"
    );
    assert!(read_set.intersects(&event), "{read_set}");
}

#[test]
fn a_result_never_runs_ahead_of_the_log() {
    let db = open("race", "CREATE TABLE t (a);");
    let mut follower = db.changes().unwrap();
    #[allow(clippy::disallowed_methods)]
    std::thread::scope(|scope| {
        scope.spawn(|| {
            for i in 0..300 {
                db.write(|tx| tx.execute("INSERT INTO t VALUES (?1)", [i]))
                    .unwrap();
            }
        });
        for _ in 0..4 {
            scope.spawn(|| {
                for _ in 0..300 {
                    let result = db.query("SELECT count(*) FROM t", (), ints).unwrap();
                    db.changes_since(result.version)
                        .expect("a result's version has been published");
                }
            });
        }
    });
    let last = db.query("SELECT count(*) FROM t", (), ints).unwrap();
    assert_eq!(last.rows, [300]);
    assert_eq!(last.version, db.version());
    // Readers that saw a commit before its publish waited for it, so the
    // log never lost track and a stream from before the writes saw them all.
    for _ in 0..300 {
        follower
            .try_next()
            .expect("an event")
            .expect("a reader never forces a lag");
    }
}

#[test]
fn an_external_content_query_depends_on_its_content_table() {
    let db = open(
        "content",
        "CREATE TABLE items (id INTEGER PRIMARY KEY, body TEXT);
         CREATE VIRTUAL TABLE f USING fts5 (body, content = 'items', content_rowid = 'id');
         INSERT INTO items VALUES (1, 'hello');
         INSERT INTO f (rowid, body) VALUES (1, 'hello');",
    );
    let read_set = db
        .query("SELECT body FROM f WHERE f MATCH 'hello'", (), |r| {
            r.get::<_, String>(0)
        })
        .unwrap()
        .read_set;
    assert!(read_set.tables().any(|t| t.name() == "items"), "{read_set}");
    let event = commit(&db, "UPDATE items SET body = 'hello there'");
    assert!(read_set.intersects(&event), "{read_set}");
}
