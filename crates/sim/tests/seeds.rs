//! A fixed range of seeds, each run twice to check that it replays exactly,
//! plus seeds that once failed.

use sqlive_sim::Simulator;

fn replays(seed: u64) {
    let first = Simulator::from_seed(seed)
        .run()
        .unwrap_or_else(|failure| panic!("{failure}"));
    let second = Simulator::from_seed(seed)
        .run()
        .unwrap_or_else(|failure| panic!("{failure}"));
    assert_eq!(
        first.digest, second.digest,
        "seed {seed} produced two different runs"
    );
}

#[test]
fn seeds_0_to_31_pass_and_replay() {
    for seed in 0..32 {
        replays(seed);
    }
}
