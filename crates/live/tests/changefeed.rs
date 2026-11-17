//! Commit events against SQLite on disk: what each kind of change publishes.

mod common;

use common::{open, temp_db};
use sqlive::{CommitEvent, Error, LiveDb, Op, Options, ReadSetIndex, Value};

/// The next event, which must already have committed.
fn next(stream: &mut sqlive::Changes) -> CommitEvent {
    stream
        .try_next()
        .expect("an event")
        .expect("no lag")
        .as_ref()
        .clone()
}

fn summary(event: &CommitEvent) -> Vec<(String, Op, Option<i64>)> {
    event
        .changes
        .iter()
        .map(|c| (c.table.clone(), c.op, c.rowid))
        .collect()
}

#[test]
fn inserts_updates_and_deletes_publish_net_changes() {
    let db = open(
        "basic",
        "CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT);",
    );
    let mut stream = db.changes().unwrap();
    let ((), v1) = db
        .write(|tx| {
            tx.execute("INSERT INTO t VALUES (1, 'a'), (2, 'b')", [])?;
            Ok(())
        })
        .unwrap();
    let event = next(&mut stream);
    assert_eq!(event.version, v1);
    assert_eq!(
        summary(&event),
        [
            ("t".into(), Op::Insert, Some(1)),
            ("t".into(), Op::Insert, Some(2))
        ]
    );
    assert_eq!(
        event.changes[0].new.as_ref().unwrap().get("NAME"),
        Some(&Value::Text("a".into()))
    );

    db.write(|tx| {
        tx.execute("UPDATE t SET name = 'z' WHERE id = 1", [])?;
        tx.execute("DELETE FROM t WHERE id = 2", [])
    })
    .unwrap();
    let event = next(&mut stream);
    assert_eq!(event.version, v1 + 1);
    assert_eq!(
        summary(&event),
        [
            ("t".into(), Op::Update, Some(1)),
            ("t".into(), Op::Delete, Some(2))
        ]
    );
    let update = &event.changes[0];
    assert_eq!(
        update.old.as_ref().unwrap().get("name"),
        Some(&Value::Text("a".into()))
    );
    assert_eq!(
        update.new.as_ref().unwrap().get("name"),
        Some(&Value::Text("z".into()))
    );
}

#[test]
fn changes_within_one_transaction_net_out() {
    let db = open("net", "CREATE TABLE t (id INTEGER PRIMARY KEY, n INTEGER);");
    db.write(|tx| tx.execute_batch("INSERT INTO t VALUES (1, 1)"))
        .unwrap();
    let mut stream = db.changes().unwrap();
    db.write(|tx| {
        tx.execute_batch(
            "INSERT INTO t VALUES (2, 2);
             DELETE FROM t WHERE id = 2;
             UPDATE t SET n = 5 WHERE id = 1;
             UPDATE t SET n = 6 WHERE id = 1;
             DELETE FROM t WHERE id = 1;",
        )
    })
    .unwrap();
    let event = next(&mut stream);
    assert_eq!(summary(&event), [("t".into(), Op::Delete, Some(1))]);
    assert_eq!(
        event.changes[0].old.as_ref().unwrap().get("n"),
        Some(&Value::Integer(1)),
        "a delete carries the row as it was before the transaction"
    );

    db.write(|tx| {
        tx.execute_batch("INSERT INTO t VALUES (3, 3); UPDATE t SET n = 3 WHERE id = 3;")
    })
    .unwrap();
    let event = next(&mut stream);
    assert_eq!(summary(&event), [("t".into(), Op::Insert, Some(3))]);
}

#[test]
fn a_rolled_back_savepoint_leaves_no_change() {
    let db = open(
        "savepoint",
        "CREATE TABLE t (id INTEGER PRIMARY KEY, n INTEGER);",
    );
    let mut stream = db.changes().unwrap();
    db.write(|tx| {
        tx.execute_batch(
            "INSERT INTO t VALUES (1, 1);
             SAVEPOINT s;
             INSERT INTO t VALUES (2, 2);
             UPDATE t SET n = 9 WHERE id = 1;
             ROLLBACK TO s;
             RELEASE s;",
        )
    })
    .unwrap();
    let event = next(&mut stream);
    assert_eq!(summary(&event), [("t".into(), Op::Insert, Some(1))]);
    assert_eq!(
        event.changes[0].new.as_ref().unwrap().get("n"),
        Some(&Value::Integer(1))
    );
}

#[test]
fn replace_conflicts_cascades_and_triggers_appear() {
    let db = open(
        "indirect",
        "CREATE TABLE parent (id INTEGER PRIMARY KEY, email TEXT UNIQUE);
         CREATE TABLE child (id INTEGER PRIMARY KEY, parent INTEGER REFERENCES parent (id) ON DELETE CASCADE);
         CREATE TABLE audit (id INTEGER PRIMARY KEY, what TEXT);
         CREATE TRIGGER parent_audit AFTER INSERT ON parent BEGIN
             INSERT INTO audit (what) VALUES ('insert ' || new.id);
         END;",
    );
    db.write(|tx| {
        tx.execute_batch("INSERT INTO parent VALUES (1, 'a'); INSERT INTO child VALUES (10, 1);")
    })
    .unwrap();
    let mut stream = db.changes().unwrap();
    db.write(|tx| tx.execute_batch("INSERT OR REPLACE INTO parent VALUES (2, 'a')"))
        .unwrap();
    let event = next(&mut stream);
    let mut changes = summary(&event);
    changes.sort();
    assert_eq!(
        changes,
        [
            ("audit".into(), Op::Insert, Some(2)),
            ("child".into(), Op::Delete, Some(10)),
            ("parent".into(), Op::Insert, Some(2)),
            ("parent".into(), Op::Delete, Some(1)),
        ]
    );
}

#[test]
fn without_rowid_rows_are_keyed_by_primary_key_under_its_collation() {
    let db = open(
        "without-rowid",
        "CREATE TABLE w (k TEXT COLLATE NOCASE PRIMARY KEY, n INTEGER) WITHOUT ROWID;",
    );
    db.write(|tx| tx.execute_batch("INSERT INTO w VALUES ('Ab', 1)"))
        .unwrap();
    let mut stream = db.changes().unwrap();
    db.write(|tx| {
        tx.execute_batch("UPDATE w SET k = 'AB'; INSERT OR REPLACE INTO w VALUES ('ab', 2);")
    })
    .unwrap();
    let event = next(&mut stream);
    assert_eq!(summary(&event), [("w".into(), Op::Update, None)]);
    let change = &event.changes[0];
    assert_eq!(
        change.old.as_ref().unwrap().values(),
        [Value::Text("Ab".into()), Value::Integer(1)]
    );
    assert_eq!(
        change.new.as_ref().unwrap().values(),
        [Value::Text("ab".into()), Value::Integer(2)]
    );
}

#[test]
fn rows_leave_out_virtual_generated_columns() {
    let db = open(
        "generated",
        "CREATE TABLE g (
             id INTEGER PRIMARY KEY,
             b TEXT,
             v TEXT GENERATED ALWAYS AS (b || '!') VIRTUAL,
             s TEXT GENERATED ALWAYS AS (b || '?') STORED
         );",
    );
    let mut stream = db.changes().unwrap();
    db.write(|tx| tx.execute_batch("INSERT INTO g (id, b) VALUES (1, 'x')"))
        .unwrap();
    let event = next(&mut stream);
    let row = event.changes[0].new.clone().unwrap();
    assert_eq!(row.columns(), ["id", "b", "s"]);
    assert_eq!(row.get("s"), Some(&Value::Text("x?".into())));
    assert_eq!(row.get("v"), None);
}

#[test]
fn a_failed_closure_rolls_back_and_publishes_nothing() {
    let db = open("rollback", "CREATE TABLE t (id INTEGER PRIMARY KEY);");
    let before = db.version();
    let mut stream = db.changes().unwrap();
    let result = db.write(|tx| {
        tx.execute_batch("INSERT INTO t VALUES (1)")?;
        tx.execute_batch("INSERT INTO t VALUES (1)")
    });
    assert!(result.is_err());
    assert_eq!(db.version(), before);
    assert!(stream.try_next().is_none());
    let ((), version) = db
        .write(|tx| tx.execute_batch("INSERT INTO t VALUES (1)"))
        .unwrap();
    assert_eq!(version, before + 1, "versions have no gaps");
    assert_eq!(next(&mut stream).changes.len(), 1);
}

#[test]
fn a_closure_cannot_end_the_transaction_or_touch_the_version() {
    let db = open("guard", "CREATE TABLE t (id INTEGER PRIMARY KEY);");
    for statement in [
        "COMMIT",
        "ROLLBACK",
        "BEGIN",
        "UPDATE _sqlive_meta SET version = 0",
        "PRAGMA synchronous = OFF",
        "ATTACH ':memory:' AS other",
    ] {
        let result = db.write(|tx| tx.execute_batch(statement));
        assert!(result.is_err(), "{statement} should be refused");
    }
    let ((), version) = db
        .write(|tx| tx.execute_batch("INSERT INTO t VALUES (1)"))
        .unwrap();
    assert_eq!(
        version, 2,
        "the schema write was 1; refused writes publish nothing"
    );
}

#[test]
fn schema_changes_and_vacuum_set_schema_changed() {
    let db = open("schema", "CREATE TABLE t (id INTEGER PRIMARY KEY);");
    let mut stream = db.changes().unwrap();
    db.write(|tx| {
        tx.execute_batch("ALTER TABLE t ADD COLUMN n INTEGER; INSERT INTO t VALUES (1, 1);")
    })
    .unwrap();
    let event = next(&mut stream);
    assert!(event.schema_changed);
    assert!(event.changes.is_empty());
    db.write(|tx| tx.execute_batch("INSERT INTO t VALUES (2, 2)"))
        .unwrap();
    let event = next(&mut stream);
    assert!(!event.schema_changed);
    assert_eq!(
        event.changes[0].new.as_ref().unwrap().columns(),
        ["id", "n"]
    );
    db.vacuum().unwrap();
    assert!(next(&mut stream).schema_changed);
}

#[test]
fn a_stream_behind_the_log_lags_and_a_reopen_forgets_the_log() {
    let path = temp_db("lag");
    let options = Options {
        log_capacity: 2,
        ..Options::default()
    };
    let db = LiveDb::open_with(&path, options.clone()).unwrap();
    db.write(|tx| tx.execute_batch("CREATE TABLE t (id INTEGER PRIMARY KEY)"))
        .unwrap();
    let start = db.version();
    for i in 0..3 {
        db.write(|tx| tx.execute("INSERT INTO t VALUES (?1)", [i]).map(drop))
            .unwrap();
    }
    let mut lagging = db.changes_since(start).unwrap();
    assert!(matches!(
        lagging.try_next(),
        Some(Err(Error::Lagged { .. }))
    ));
    let mut recent = db.changes_since(start + 1).unwrap();
    assert_eq!(next(&mut recent).version, start + 2);
    assert!(matches!(
        db.changes_since(db.version() + 1),
        Err(Error::FutureVersion { .. })
    ));

    let latest = db.version();
    drop(db);
    let db = LiveDb::open_with(&path, options).unwrap();
    assert_eq!(db.version(), latest, "the version survives a reopen");
    assert!(matches!(
        db.changes_since(latest - 1).unwrap().try_next(),
        Some(Err(Error::Lagged { .. }))
    ));
    let mut current = db.changes_since(latest).unwrap();
    db.write(|tx| tx.execute_batch("INSERT INTO t VALUES (100)"))
        .unwrap();
    assert_eq!(next(&mut current).version, latest + 1);
}

#[test]
fn closing_the_database_checkpoints_and_deletes_the_wal() {
    let path = temp_db("close");
    let db = LiveDb::open(&path).unwrap();
    db.write(|tx| tx.execute_batch("CREATE TABLE t (x); INSERT INTO t VALUES (1)"))
        .unwrap();
    db.query("SELECT x FROM t", (), |row| row.get::<_, i64>(0))
        .unwrap();
    let exists = |suffix: &str| std::path::Path::new(&format!("{path}{suffix}")).exists();
    assert!(exists("-wal"), "the open database writes through the WAL");
    drop(db);
    assert!(!exists("-wal") && !exists("-shm"));
    let conn = sqlive::rusqlite::Connection::open(&path).unwrap();
    let x: i64 = conn.query_row("SELECT x FROM t", [], |r| r.get(0)).unwrap();
    assert_eq!(x, 1, "the commit reached the database file");
}

#[test]
fn a_primary_key_collation_differs_from_its_column_collation() {
    let db = open(
        "key-collation",
        "CREATE TABLE b (a TEXT COLLATE NOCASE NOT NULL, v, PRIMARY KEY (a COLLATE BINARY)) WITHOUT ROWID;
         CREATE TABLE n (a TEXT NOT NULL, v, PRIMARY KEY (a COLLATE NOCASE)) WITHOUT ROWID;
         INSERT INTO b VALUES ('A', 1), ('a', 2);
         INSERT INTO n VALUES ('A', 1);",
    );
    let mut stream = db.changes().unwrap();
    db.write(|tx| tx.execute_batch("UPDATE b SET v = v + 10; UPDATE n SET a = 'a' WHERE a = 'A';"))
        .unwrap();
    let event = next(&mut stream);
    assert_eq!(
        summary(&event),
        [
            ("b".into(), Op::Update, None),
            ("b".into(), Op::Update, None),
            ("n".into(), Op::Update, None),
        ],
        "two binary keys are two rows; a key that changes case under NOCASE is one row"
    );
    assert_eq!(
        event.changes[2].new.as_ref().unwrap().get("a"),
        Some(&Value::Text("a".into()))
    );
}

#[test]
fn text_that_is_not_utf8_still_shows_its_changes() {
    let db = open(
        "utf8",
        "CREATE TABLE t (id INTEGER PRIMARY KEY, v);
         INSERT INTO t VALUES (1, CAST(x'fe' AS TEXT));",
    );
    let mut stream = db.changes().unwrap();
    db.write(|tx| tx.execute_batch("UPDATE t SET v = CAST(x'ff' AS TEXT)"))
        .unwrap();
    assert_eq!(
        summary(&next(&mut stream)),
        [("t".into(), Op::Update, Some(1))]
    );
}

#[test]
fn a_closure_cannot_attach_triggers_to_or_alter_the_version_table() {
    let db = open("meta-guard", "CREATE TABLE t (id INTEGER PRIMARY KEY);");
    for statement in [
        "CREATE TRIGGER x AFTER UPDATE ON _sqlive_meta BEGIN INSERT INTO t VALUES (1); END",
        "ALTER TABLE _sqlive_meta RENAME TO z",
        "ALTER TABLE _sqlive_meta ADD COLUMN y",
        "CREATE INDEX x ON _sqlive_meta (version)",
    ] {
        assert!(
            db.write(|tx| tx.execute_batch(statement)).is_err(),
            "{statement} should be refused"
        );
    }
}

#[test]
fn a_schema_changed_outside_a_write_flags_the_next_event() {
    let path = temp_db("outside");
    let db = LiveDb::open(&path).unwrap();
    db.write(|tx| tx.execute_batch("CREATE TABLE t (id INTEGER PRIMARY KEY)"))
        .unwrap();
    let mut stream = db.changes().unwrap();
    let other = sqlive::rusqlite::Connection::open(&path).unwrap();
    other.execute_batch("ALTER TABLE t ADD COLUMN n").unwrap();
    db.write(|tx| tx.execute_batch("INSERT INTO t VALUES (1, 1)"))
        .unwrap();
    assert!(next(&mut stream).schema_changed);
    db.write(|tx| tx.execute_batch("INSERT INTO t VALUES (2, 2)"))
        .unwrap();
    let event = next(&mut stream);
    assert!(!event.schema_changed, "the schema reloads once");
    assert_eq!(
        event.changes[0].new.as_ref().unwrap().columns(),
        ["id", "n"]
    );
}

#[test]
fn writes_to_virtual_table_shadow_tables_are_row_changes() {
    let db = open("fts", "CREATE VIRTUAL TABLE docs USING fts5 (body);");
    let mut stream = db.changes().unwrap();
    db.write(|tx| tx.execute_batch("INSERT INTO docs (body) VALUES ('hello world')"))
        .unwrap();
    let event = next(&mut stream);
    assert!(!event.schema_changed);
    assert!(
        event.changes.iter().any(|c| c.table.starts_with("docs_")),
        "{event:?}"
    );
}

#[test]
fn a_schema_moved_while_closed_is_published_at_open() {
    let path = temp_db("moved-closed");
    let db = LiveDb::open(&path).unwrap();
    db.write(|tx| tx.execute_batch("CREATE TABLE t (n)"))
        .unwrap();
    let before = db.version();
    drop(db);
    // A VACUUM whose event never committed leaves the schema cookie moved.
    sqlive::rusqlite::Connection::open(&path)
        .unwrap()
        .execute_batch("VACUUM")
        .unwrap();
    let db = LiveDb::open(&path).unwrap();
    assert_eq!(db.version(), before + 1, "opening publishes the move");
    let event = next(&mut db.changes_since(before).unwrap());
    assert!(event.schema_changed);
}

#[test]
fn statements_after_an_automatic_rollback_do_not_commit() {
    let db = open(
        "auto-rollback",
        "CREATE TABLE t (id INTEGER PRIMARY KEY); INSERT INTO t VALUES (1);",
    );
    for after in [
        "INSERT INTO t VALUES (3)",
        "SAVEPOINT s; INSERT INTO t VALUES (3);",
    ] {
        let before = db.version();
        let result = db.write(|tx| {
            tx.execute_batch("INSERT INTO t VALUES (2)")?;
            // OR ROLLBACK ends the whole transaction on a conflict.
            assert!(
                tx.execute_batch("INSERT OR ROLLBACK INTO t VALUES (1)")
                    .is_err()
            );
            let _ = tx.execute_batch(after);
            Ok(())
        });
        assert!(
            matches!(result, Err(Error::RolledBack)),
            "{after}: {result:?}"
        );
        assert_eq!(db.version(), before);
        let rows: i64 = db
            .write(|tx| tx.query_row("SELECT count(*) FROM t", [], |r| r.get(0)))
            .unwrap()
            .0;
        assert_eq!(rows, 1, "{after}: neither 2 nor 3 committed");
    }
}

/// A database that lists at most three rows of a table in one event.
fn capped(name: &str) -> LiveDb {
    let options = Options {
        table_changes: 3,
        ..Options::default()
    };
    let db = LiveDb::open_with(temp_db(name), options).unwrap();
    db.write(|tx| {
        tx.execute_batch(
            "CREATE TABLE big (id INTEGER PRIMARY KEY, a);
             CREATE TABLE small (id INTEGER PRIMARY KEY, a);
             CREATE TABLE keyed (k TEXT PRIMARY KEY, a) WITHOUT ROWID;
             CREATE TABLE other (x);",
        )
    })
    .unwrap();
    db
}

#[test]
fn a_table_past_the_change_cap_is_named_whole_and_lists_no_rows() {
    let db = capped("cap");
    let read_set = |sql: &str| db.query(sql, (), |_| Ok(())).unwrap().read_set;
    let on_big = read_set("SELECT a FROM big WHERE id = 100");
    let on_keyed = read_set("SELECT a FROM keyed WHERE k > 'x' ORDER BY k LIMIT 2");
    let on_small = read_set("SELECT a FROM small WHERE id = 100");
    let on_no_rows = read_set("SELECT a FROM big WHERE id = NULL");
    let on_no_rows_of_one_key = read_set("SELECT a FROM big WHERE id = 100 AND a = NULL");
    let on_other = read_set("SELECT x FROM other");
    let mut stream = db.changes().unwrap();
    db.write(|tx| {
        tx.execute_batch(
            "INSERT INTO big VALUES (1, 'a'), (2, 'b'), (3, 'c');
             INSERT INTO small VALUES (1, 'a'), (2, 'b'), (3, 'c');
             INSERT INTO keyed VALUES ('a', 1), ('b', 2), ('c', 3), ('d', 4);
             INSERT INTO big VALUES (4, 'd');
             UPDATE big SET a = 'z' WHERE id = 1;",
        )
    })
    .unwrap();
    let event = next(&mut stream);
    assert_eq!(event.whole_tables, ["big", "keyed"]);
    assert_eq!(
        summary(&event),
        [
            ("small".into(), Op::Insert, Some(1)),
            ("small".into(), Op::Insert, Some(2)),
            ("small".into(), Op::Insert, Some(3)),
        ],
        "a table at the cap lists its rows, and one past it lists none"
    );
    assert!(
        on_big.intersects(&event),
        "a filter cannot rule out unlisted rows"
    );
    assert!(on_keyed.intersects(&event));
    assert!(
        !on_small.intersects(&event),
        "its listed rows fail the filter"
    );
    assert!(
        !on_no_rows.intersects(&event),
        "its result depends on no row"
    );
    assert!(!on_no_rows_of_one_key.intersects(&event));
    assert!(!on_other.intersects(&event));

    let mut index = ReadSetIndex::new();
    let read_sets = [
        on_big,
        on_keyed,
        on_small,
        on_no_rows,
        on_no_rows_of_one_key,
        on_other,
    ];
    for (key, read_set) in read_sets.into_iter().enumerate() {
        index.insert(key, read_set);
    }
    assert_eq!(index.affected(&event).collect::<Vec<_>>(), [0, 1]);

    // Each transaction counts its rows afresh.
    db.write(|tx| tx.execute_batch("INSERT INTO big VALUES (5, 'e')"))
        .unwrap();
    let event = next(&mut stream);
    assert!(event.whole_tables.is_empty());
    assert_eq!(summary(&event), [("big".into(), Op::Insert, Some(5))]);
}

#[test]
fn a_capped_table_stays_whole_through_rollbacks_and_names_nothing_under_a_schema_change() {
    let db = capped("cap-edges");
    let mut stream = db.changes().unwrap();
    // The savepoint undoes the rows, but the table's count stands.
    db.write(|tx| {
        tx.execute_batch(
            "SAVEPOINT s;
             INSERT INTO big VALUES (1, 'a'), (2, 'b'), (3, 'c'), (4, 'd');
             ROLLBACK TO s;
             INSERT INTO other VALUES (1);",
        )
    })
    .unwrap();
    let event = next(&mut stream);
    assert_eq!(event.whole_tables, ["big"]);
    assert_eq!(summary(&event), [("other".into(), Op::Insert, Some(1))]);
    // A schema change already tells every consumer to reload.
    db.write(|tx| {
        tx.execute_batch(
            "INSERT INTO big VALUES (1, 'a'), (2, 'b'), (3, 'c'), (4, 'd');
             CREATE TABLE more (y);",
        )
    })
    .unwrap();
    let event = next(&mut stream);
    assert!(event.schema_changed);
    assert!(event.whole_tables.is_empty() && event.changes.is_empty());
}

#[test]
fn the_log_drops_its_oldest_events_past_its_byte_budget() {
    let options = Options {
        log_bytes: 4_000,
        ..Options::default()
    };
    let db = LiveDb::open_with(temp_db("budget"), options).unwrap();
    db.write(|tx| tx.execute_batch("CREATE TABLE t (id INTEGER PRIMARY KEY, body)"))
        .unwrap();
    let start = db.version();
    let mut head = db.changes().unwrap();
    for _ in 0..3 {
        db.write(|tx| tx.execute("INSERT INTO t (body) VALUES (?1)", ["x".repeat(1_500)]))
            .unwrap();
    }
    assert!(
        matches!(
            db.changes_since(start).unwrap().try_next(),
            Some(Err(Error::Lagged { .. }))
        ),
        "three events of 1,500 bytes pass a 4,000-byte budget"
    );
    let mut recent = db.changes_since(start + 1).unwrap();
    assert_eq!(next(&mut recent).version, start + 2);
    assert_eq!(next(&mut recent).version, start + 3);
    assert!(
        matches!(head.try_next(), Some(Err(Error::Lagged { .. }))),
        "a stream that fell behind the budget lags"
    );
}
