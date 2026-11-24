# SqLive

`README.md` is the API contract: a change to the public API or its guarantees updates it in the same commit. `PLAN.md` is the build plan: work in its order and update its ledgers with evidence.

## Core rules

- `intersects` may over-report and never under-reports. Each read-set refinement ships with a simulator property that compares live results against fresh queries.
- Changes come from the preupdate hook on the writer connection. The VFS in `crates/sim` serves only the simulator.
- All writes run through `LiveDb::write` on one writer connection. Queries run on read-only connections.
- `crates/live` starts no threads, reads no clocks, and iterates only ordered collections; `clippy.toml` in `crates/live` and `crates/sim` enforces this.
- `crates/node` is the Node.js binding, published to npm as `@danthegoodman/sqlive`. Its reads run on the calling thread; its writes commit on one writer thread per database. A change to its API updates `crates/node/README.md` and `index.d.ts`.

## Testing

- Every feature needs simulator coverage, a property that fails on a wrong answer, and a planted bug in `scripts/mutants` that a test catches.
- The simulator checks against SQLite itself (fresh queries, table dumps), never against the library's own state.
- A seed replays its run exactly. Give each new source of randomness its own `Prng::stream`, so it leaves existing draws unchanged.
- A bug the simulator finds becomes a regression test in `crates/sim/tests/seeds.rs` that runs its seed. The commit message records the seed and the commit that failed.

## Commands

```console
scripts/check [SEEDS]                                   # fmt, clippy, tests, sweep
cargo test --workspace
cargo run --release -p sqlive-sim [-- SEED] [--seeds N]  # SIM_TRACE=1 prints a run
scripts/mutants [--only NAME] [--layers unit,sim,chat,seeds,node]
cargo run -p chat-example                               # http://127.0.0.1:3000

cd crates/node
npm ci && npm run build                                 # sqlive.<target>.node
npm test                                                # SQLIVE_SEED=N SQLIVE_SEEDS=1 replays a property run
npm run bench [-- writes|queries|index]
node scripts/release.mjs version X.Y.Z                  # a merged version bump publishes to npm
```
