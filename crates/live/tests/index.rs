//! `ReadSetIndex` against brute force: for random read sets and events,
//! `affected` returns exactly the keys whose read set intersects the event.

mod common;

use sqlive::rusqlite::params_from_iter;
use sqlive::rusqlite::types::Value;
use sqlive::{CommitEvent, LiveDb, ReadSet, ReadSetIndex};
use std::collections::BTreeMap;

/// A small deterministic PRNG, so a failure replays.
struct Rng(u64);

impl Rng {
    fn next(&mut self) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0
    }

    fn below(&mut self, n: usize) -> usize {
        (self.next() % n as u64) as usize
    }

    fn pick<'a, T>(&mut self, items: &'a [T]) -> &'a T {
        &items[self.below(items.len())]
    }
}

fn value(rng: &mut Rng) -> Value {
    match rng.below(8) {
        0 => Value::Null,
        1..=3 => Value::Integer(rng.below(6) as i64),
        4 => Value::Real(*rng.pick(&[0.0, -0.0, 1.0, 2.5])),
        5 | 6 => Value::Text(rng.pick(&["a", "A", "b", "a ", "B", "é"]).to_string()),
        _ => Value::Blob(rng.pick(&[&b"a"[..], b"b"]).to_vec()),
    }
}

/// A query over `t` or `u` that the analysis narrows in most of its forms.
fn query(rng: &mut Rng) -> (String, Vec<Value>) {
    let table = *rng.pick(&["t", "u"]);
    let column = *rng.pick(&["a", "b", "c"]);
    let mut params = Vec::new();
    let mut param = |rng: &mut Rng| {
        params.push(value(rng));
        format!("?{}", params.len())
    };
    let filter = match rng.below(12) {
        0..=2 => format!("{column} = {}", param(rng)),
        3 => format!("{column} IN ({}, {})", param(rng), param(rng)),
        4 => format!("{column} > {}", param(rng)),
        5 => format!("{column} <= {}", param(rng)),
        6 => format!("{column} BETWEEN {} AND {}", param(rng), param(rng)),
        7 => format!("{column} IS NULL"),
        8 => format!("{column} = {} COLLATE NOCASE", param(rng)),
        9 => format!("{column} = {} OR {column} IS NULL", param(rng)),
        10 => format!("{column} IS NOT NULL AND a = {}", param(rng)),
        _ => "1".to_owned(),
    };
    let tail = match rng.below(4) {
        0 => format!(
            " ORDER BY {column}{} , rowid LIMIT {}",
            rng.pick(&["", " DESC", " DESC NULLS FIRST", " NULLS LAST"]),
            1 + rng.below(4)
        ),
        _ => String::new(),
    };
    (
        format!("SELECT * FROM {table} WHERE {filter}{tail}"),
        params,
    )
}

fn write(rng: &mut Rng) -> (String, Vec<Value>) {
    let table = *rng.pick(&["t", "u"]);
    match rng.below(5) {
        0 | 1 => (
            format!("INSERT INTO {table} (a, b, c) VALUES (?1, ?2, ?3)"),
            vec![value(rng), value(rng), value(rng)],
        ),
        2 => (
            format!(
                "UPDATE {table} SET {} = ?1 WHERE rowid = (SELECT rowid FROM {table} ORDER BY rowid LIMIT 1 OFFSET ?2)",
                rng.pick(&["a", "b", "c"])
            ),
            vec![value(rng), Value::Integer(rng.below(30) as i64)],
        ),
        3 => (
            format!(
                "DELETE FROM {table} WHERE rowid = (SELECT rowid FROM {table} ORDER BY rowid LIMIT 1 OFFSET ?1)"
            ),
            vec![Value::Integer(rng.below(30) as i64)],
        ),
        _ => (
            format!("UPDATE {table} SET c = ?1 WHERE a = ?2"),
            vec![value(rng), value(rng)],
        ),
    }
}

fn commit(db: &LiveDb, sql: &str, params: &[Value]) -> CommitEvent {
    let ((), version) = db
        .write(|tx| tx.execute(sql, params_from_iter(params)).map(|_| ()))
        .unwrap();
    let event = db
        .changes_since(version - 1)
        .unwrap()
        .try_next()
        .unwrap()
        .unwrap();
    event.as_ref().clone()
}

#[test]
fn affected_matches_testing_every_read_set() {
    let db = common::open(
        "index",
        "CREATE TABLE t (a, b TEXT COLLATE NOCASE, c);
         CREATE TABLE u (a INTEGER, b TEXT, c REAL);",
    );
    let mut rng = Rng(0x1dec_0de5_eed5_0001);
    for _ in 0..40 {
        let (sql, params) = write(&mut rng);
        commit(&db, &sql, &params);
    }
    let mut index = ReadSetIndex::new();
    let mut brute: BTreeMap<u32, ReadSet> = BTreeMap::new();
    let (mut events, mut hits) = (0, 0);
    for step in 0..2000 {
        match rng.below(10) {
            // Subscribe, or re-run a subscription and replace its read set.
            0..=3 => {
                let key = rng.below(60) as u32;
                let (sql, params) = query(&mut rng);
                let read_set = db.query(&sql, params, |_| Ok(())).unwrap().read_set;
                index.insert(key, read_set.clone());
                brute.insert(key, read_set);
            }
            4 => {
                let key = rng.below(60) as u32;
                assert_eq!(index.remove(&key).is_some(), brute.remove(&key).is_some());
            }
            _ => {
                let (sql, params) = write(&mut rng);
                let event = commit(&db, &sql, &params);
                let expected: Vec<u32> = brute
                    .iter()
                    .filter(|(_, read_set)| read_set.intersects(&event))
                    .map(|(key, _)| *key)
                    .collect();
                let found: Vec<u32> = index.affected(&event).collect();
                assert_eq!(found, expected, "step {step}: {event:?}");
                events += 1;
                hits += found.len();
            }
        }
        assert_eq!(index.len(), brute.len());
    }
    let schema = commit(&db, "CREATE TABLE v (x)", &[]);
    assert_eq!(index.affected(&schema).count(), index.len());
    assert!(events > 500 && hits > 0, "{events} events, {hits} hits");
}

#[test]
fn affected_covers_missing_columns_lossy_text_and_every_commit() {
    let db = common::open(
        "index-edges",
        "CREATE TABLE g (a INTEGER, b TEXT, v GENERATED ALWAYS AS (a + 1) VIRTUAL);
         INSERT INTO g (a, b) VALUES (1, CAST(x'ff' AS TEXT));",
    );
    let mut index = ReadSetIndex::new();
    let queries = [
        // Rows leave out the virtual column, so any change may match.
        "SELECT * FROM g WHERE v = 5",
        // Text that is not UTF-8 reads lossily, so it may match anything.
        "SELECT * FROM g WHERE b = 'x'",
        "SELECT version FROM _sqlive_meta",
        "SELECT * FROM g WHERE a = 7",
    ];
    for (key, sql) in queries.iter().enumerate() {
        index.insert(key, db.query(sql, (), |_| Ok(())).unwrap().read_set);
    }
    let found = |index: &ReadSetIndex<usize>, event: &CommitEvent| {
        index.affected(event).collect::<Vec<_>>()
    };
    assert_eq!(
        found(
            &index,
            &commit(
                &db,
                "INSERT INTO g (a, b) VALUES (?1, 'y')",
                &[Value::Integer(4)]
            )
        ),
        [0, 2]
    );
    assert_eq!(
        found(
            &index,
            &commit(&db, "UPDATE g SET b = CAST(x'fe' AS TEXT) WHERE a = 1", &[])
        ),
        [0, 1, 2]
    );
    assert_eq!(
        found(
            &index,
            &commit(&db, "INSERT INTO g (a, b) VALUES (7, 'z')", &[])
        ),
        [0, 2, 3]
    );
    index.remove(&0);
    index.remove(&2);
    assert_eq!(
        found(
            &index,
            &commit(&db, "INSERT INTO g (a, b) VALUES (8, 'z')", &[])
        ),
        Vec::<usize>::new()
    );
    assert_eq!(index.len(), 2);
}
