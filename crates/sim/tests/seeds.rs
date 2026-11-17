//! A fixed range of seeds, each run twice to check that it replays exactly,
//! plus seeds that once failed.

use sqlive_sim::{Harness, Simulator};
use std::sync::Mutex;

/// Runs share SQLite's process-wide PRNG, which each run reseeds, so the
/// tests in this file run one at a time.
static SQLITE_PRNG: Mutex<()> = Mutex::new(());

/// Runs a seed twice and returns its summary.
fn replays(harness: Harness, seed: u64) -> String {
    let run = || {
        Simulator::from_seed(seed)
            .harness(harness)
            .run()
            .unwrap_or_else(|failure| panic!("{failure}"))
    };
    let first = run();
    let second = run();
    assert_eq!(
        (first.digest, &first.text),
        (second.digest, &second.text),
        "seed {seed} produced two different runs"
    );
    first.text
}

#[test]
fn storage_seeds_0_to_63_pass_and_replay() {
    let _serial = SQLITE_PRNG.lock().unwrap_or_else(|e| e.into_inner());
    for seed in 0..64 {
        replays(Harness::Storage, seed);
    }
}

#[test]
fn live_seeds_0_to_63_pass_and_replay() {
    let _serial = SQLITE_PRNG.lock().unwrap_or_else(|e| e.into_inner());
    for seed in 0..64 {
        replays(Harness::Live, seed);
    }
}

/// After an injected WAL read error, SQLite ended a snapshot's read
/// transaction while the statement that read its version succeeded, and
/// later queries read rows past the snapshot's version. Each seed must still
/// end a snapshot, or it no longer tests the fix.
#[test]
fn seeds_5476_and_883_a_snapshot_sqlite_ended_reads_nothing_newer() {
    let _serial = SQLITE_PRNG.lock().unwrap_or_else(|e| e.into_inner());
    for seed in [5476, 883] {
        let summary = replays(Harness::Live, seed);
        let ended: u64 = summary
            .split(" snapshots ended")
            .next()
            .and_then(|before| before.rsplit('(').next())
            .and_then(|count| count.parse().ok())
            .unwrap_or(0);
        assert!(
            ended > 0,
            "seed {seed} no longer ends a snapshot: {summary}"
        );
    }
}

/// The results a run's summary counts for one query shape, by the shape's
/// place in `Shape::ALL`.
fn results_of_shape(summary: &str, shape: usize) -> u64 {
    summary
        .split("by shape [")
        .nth(1)
        .and_then(|shapes| shapes.split(']').next())
        .and_then(|shapes| shapes.split(", ").nth(shape))
        .and_then(|count| count.parse().ok())
        .unwrap_or(0)
}

/// A query that reads a view that joins by column name depends on the join
/// columns, whether or not the authorizer hears of the view: SQLite merges a
/// view into a query that names none of its columns before it reports
/// anything. With the views' joins unseen, `SELECT count(*) FROM j0` in seed
/// 847 and `SELECT * FROM j0` in seed 494 hold stale rows after a join
/// column changes. Each seed must still query joins by column name.
#[test]
fn seeds_847_and_494_a_view_that_joins_by_column_name_widens_its_queries() {
    let _serial = SQLITE_PRNG.lock().unwrap_or_else(|e| e.into_inner());
    for seed in [847, 494] {
        let summary = replays(Harness::Live, seed);
        assert!(
            results_of_shape(&summary, 3) > 0,
            "seed {seed} no longer queries a join by column name: {summary}"
        );
    }
}

/// A join by column name reaches a full-text table through a column the
/// authorizer never hears of, and the bytecode leaves a virtual table
/// unnamed, so every commit counts. With the table left out, a count over
/// `t2 JOIN docs USING (c0)` in seed 586, and one over `t0` in seed 1254,
/// miss the documents a commit adds or removes. Each seed must still query
/// the full-text tables.
#[test]
fn seeds_586_and_1254_a_join_by_column_name_with_a_full_text_table_sees_every_commit() {
    let _serial = SQLITE_PRNG.lock().unwrap_or_else(|e| e.into_inner());
    for seed in [586, 1254] {
        let summary = replays(Harness::Live, seed);
        assert!(
            results_of_shape(&summary, 6) > 0,
            "seed {seed} no longer queries the full-text tables: {summary}"
        );
    }
}
