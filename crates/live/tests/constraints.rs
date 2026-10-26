//! Read-set constraints judged against SQLite: for random stored values,
//! columns of every affinity and collation, and every supported operator,
//! a read set must intersect each insert or delete of a row the query's
//! `WHERE` accepts.

mod common;

use sqlive::rusqlite::types::Value;
use sqlive::rusqlite::{ToSql, params_from_iter};
use sqlive::{CommitEvent, LiveDb};

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
    match rng.below(12) {
        0 => Value::Null,
        1 => {
            Value::Integer(*rng.pick(&[0, 1, -1, 2, 10, 9_007_199_254_740_993, i64::MAX, i64::MIN]))
        }
        2 => Value::Integer(rng.below(5) as i64),
        3 => Value::Real(*rng.pick(&[
            0.0,
            -0.0,
            1.0,
            1.5,
            2.0,
            -2.5,
            9_007_199_254_740_992.0,
            1e300,
        ])),
        4 | 5 => Value::Text(
            rng.pick(&[
                "a", "A", "b", "B", "a ", "A  ", "", "ab", "Ab", " a", "é", "É",
            ])
            .to_string(),
        ),
        6 => Value::Text(
            rng.pick(&[
                "1", "1.0", " 1 ", "01", "1e0", "+1", "-0", "2.5", "1x", "0x1",
            ])
            .to_string(),
        ),
        7 => Value::Text(
            rng.pick(&[
                "9007199254740993",
                "123456789012345678901",
                "1.0000000000000001",
            ])
            .to_string(),
        ),
        8 => Value::Blob(rng.pick(&[&b""[..], b"a", b"A", b"\x00", b"1"]).to_vec()),
        _ => Value::Integer(rng.below(3) as i64),
    }
}

/// A `WHERE` clause on column `c` and its parameters.
fn predicate(rng: &mut Rng) -> (String, Vec<Value>) {
    let mut params = Vec::new();
    let mut param = |rng: &mut Rng| {
        params.push(value(rng));
        format!("?{}", params.len())
    };
    let collate = *rng.pick(&[
        "",
        "",
        "",
        " COLLATE NOCASE",
        " COLLATE RTRIM",
        " COLLATE BINARY",
    ]);
    let sql = match rng.below(14) {
        0 | 1 => format!("c{collate} = {}", param(rng)),
        2 => format!("{} = c{collate}", param(rng)),
        3 => format!("c{collate} IN ({}, {})", param(rng), param(rng)),
        4 => format!("c{collate} < {}", param(rng)),
        5 => format!("c{collate} <= {}", param(rng)),
        6 => format!("c{collate} > {}", param(rng)),
        7 => format!("{} >= c{collate}", param(rng)),
        8 => format!("c{collate} BETWEEN {} AND {}", param(rng), param(rng)),
        9 => format!("c{collate} IS {}", param(rng)),
        10 => "c IS NULL".to_owned(),
        11 => "c IS NOT NULL".to_owned(),
        12 => format!(
            "c{collate} >= {} AND c{collate} < {}",
            param(rng),
            param(rng)
        ),
        _ => format!(
            "c = {}",
            *rng.pick(&["1", "'1'", "1.0", "'a'", "x'61'", "NULL", "-1", "0x10"])
        ),
    };
    (sql, params)
}

const DECLARED: [&str; 6] = ["INTEGER", "TEXT", "REAL", "NUMERIC", "BLOB", ""];
const COLLATIONS: [&str; 4] = ["", " COLLATE NOCASE", " COLLATE RTRIM", " COLLATE BINARY"];

fn tables() -> Vec<String> {
    let mut ddl = Vec::new();
    for (d, declared) in DECLARED.iter().enumerate() {
        for (c, collation) in COLLATIONS.iter().enumerate() {
            ddl.push(format!("CREATE TABLE t{d}_{c} (c {declared}{collation});"));
        }
    }
    ddl
}

fn commit(db: &LiveDb, sql: &str, params: &[Value]) -> CommitEvent {
    let ((), version) = db
        .write(|tx| tx.execute(sql, params_from_iter(params)).map(|_| ()))
        .unwrap();
    db.changes_since(version - 1)
        .unwrap()
        .try_next()
        .unwrap()
        .unwrap()
        .as_ref()
        .clone()
}

#[test]
fn read_sets_intersect_every_insert_and_delete_their_where_accepts() {
    let ddl = tables().join("\n");
    let db = common::open("constraints", &ddl);
    let mut rng = Rng(0x5eed_1234_abcd_0001);
    let (mut matched, mut skipped, mut cases) = (0, 0, 0);
    for case in 0..3000 {
        let table = format!(
            "t{}_{}",
            rng.below(DECLARED.len()),
            rng.below(COLLATIONS.len())
        );
        let stored = value(&mut rng);
        let (filter, params) = predicate(&mut rng);
        let sql = format!("SELECT * FROM {table} WHERE {filter}");
        let read_set = db.query(&sql, params.clone(), |_| Ok(())).unwrap().read_set;

        let insert = commit(
            &db,
            &format!("INSERT INTO {table} (c) VALUES (?1)"),
            std::slice::from_ref(&stored),
        );
        let rowid = insert.changes[0].rowid.unwrap();
        let mut check = params.clone();
        check.push(Value::Integer(rowid));
        let accepts: i64 = db
            .query(
                &format!(
                    "SELECT count(*) FROM {table} WHERE ({filter}) AND rowid = ?{}",
                    check.len()
                ),
                check
                    .iter()
                    .map(|v| v as &dyn ToSql)
                    .collect::<Vec<_>>()
                    .as_slice(),
                |r| r.get(0),
            )
            .unwrap()
            .rows[0];
        let delete = commit(
            &db,
            &format!("DELETE FROM {table} WHERE rowid = ?1"),
            &[Value::Integer(rowid)],
        );
        let context =
            format!("case {case}: {sql} {params:?} on stored {stored:?}; read set {read_set}");
        cases += 1;
        if accepts == 1 {
            matched += 1;
            assert!(read_set.intersects(&insert), "insert missed: {context}");
            assert!(read_set.intersects(&delete), "delete missed: {context}");
        } else if !read_set.intersects(&insert) && !read_set.intersects(&delete) {
            skipped += 1;
        }
    }
    let rejected = cases - matched;
    // Most rejected rows skip; the rest hit cases the analysis leaves
    // unconstrained, such as text that may convert to a number.
    assert!(
        skipped * 10 >= rejected * 7,
        "{skipped} of {rejected} rejected rows skipped"
    );
}
