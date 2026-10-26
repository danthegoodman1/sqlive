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
fn seeds_5476_and_5551_a_snapshot_sqlite_ended_reads_nothing_newer() {
    let _serial = SQLITE_PRNG.lock().unwrap_or_else(|e| e.into_inner());
    for seed in [5476, 5551] {
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
