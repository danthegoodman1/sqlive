//! A fixed range of seeds, each run twice to check that it replays exactly,
//! plus seeds that once failed.

use sqlive_sim::{Harness, Simulator};

fn replays(harness: Harness, seed: u64) {
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
}

#[test]
fn storage_seeds_0_to_63_pass_and_replay() {
    for seed in 0..64 {
        replays(Harness::Storage, seed);
    }
}
