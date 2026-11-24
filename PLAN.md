# Development Plan

## Overarching Goal

Build SqLive (crate `sqlive`), the Rust library `README.md` describes. Queries return rows with a versioned read set. Each committed write publishes a versioned commit event with net row changes. `ReadSet::intersects` and `ReadSetIndex` tell an app which queries to re-run. A subscription that re-runs on every intersecting event always holds the result a fresh query would return. A re-run reports, by a hash of the rows, whether they changed, so an app keeps no rows between runs. A deterministic simulator proves this under random schemas, queries, faults and crashes before each phase closes. A live chat web page demonstrates the library end to end.

Non-goals until a phase names them: writers in other processes, ATTACHed databases, virtual tables as change sources, read sets from instrumented b-tree cursors (a patched SQLite), an async API, and client transport, which belongs to the app.

## Implementation Principles

- `README.md` is the API contract. A change to the public API or its guarantees updates the README in the same commit.
- `intersects` may over-report; it never under-reports. Each read-set refinement ships with a simulator property that compares live results against fresh queries.
- Changes come from the preupdate hook on the writer connection. The project's VFS serves only the simulator.
- All writes run through `write` on one writer connection. Queries run on read-only connections.
- The library starts no threads, reads no clocks and iterates only ordered collections. Events list changes in first-touch order. `clippy.toml` in `crates/live` and `crates/sim` enforces this, so a seed replays exactly.
- Every feature ships with simulator coverage, a property that fails on a wrong answer, and a planted bug the simulator catches.
- Build the smallest implementation that meets the phase gate.
- Execution order: Phase 0 through Phase 15.

## Testing Strategy

- **Unit tests** for pure logic: net-change coalescing, the value comparator, constraint extraction, window bounds and the index.
- **Simulator (VOPR style, after `../turtlekv-rs` and `../s3-accelerator`):** one seed draws the options, schema, workload, faults and crash points. Each concern draws from its own `Prng::stream`. `cargo test` runs a fixed seed range, and each phase closes with a 10,000-seed sweep that has no failures. A seed or git commit hash replays a run; `SIM_TRACE` prints it.
- **Oracles:** SQLite itself. The simulator compares live results against fresh queries, and event-built replicas against table dumps. It never checks against the library's own state.
- **Determinism:** `cargo test` runs each seed twice and compares trace hashes.
- **Planted bugs:** `scripts/mutants` plants known bugs one at a time in a scratch copy and reports which test layer catches each. Every phase adds its own bugs and must catch all of them.
- **Regression seeds:** a seed the simulator fails on becomes a test in `crates/sim/tests/seeds.rs`. The commit message records the seed and the failing commit.
- **Example app:** HTTP integration tests plus a manual browser check.
- **Code review:** each phase ends with `/code-review`. Findings are fixed or recorded in the phase ledger before the phase closes.

## Phase 0: Scaffold

Goal:
A workspace with the library crate, a simulator with seed tooling, a local check gate, and CI.

Scope:
- 0A Git repository and Cargo workspace: `crates/live` (`sqlive`), `crates/sim` (`sqlive-sim`), `examples/chat` (`chat-example`). rusqlite with `bundled`, `hooks` and `preupdate_hook`.
- 0B `clippy.toml` in `crates/live` and `crates/sim` banning `HashMap`, `HashSet`, `std::time` and `std::thread`. The sweep runner opts out with `#[expect]`.
- 0C Simulator PRNG (xoshiro256** seeded through SplitMix64, with `Prng::stream(seed, purpose)`), plus the driver CLI: `[SEED | COMMIT] [--seeds N]`, a parallel sweep, `SIM_TRACE`, a replay line on failure, and a trace digest.
- 0D `scripts/check` (fmt, clippy `-D warnings`, tests, seed sweep) and `scripts/mutants`.
- 0E `CLAUDE.md` with the core rules and commands.
- 0F GitHub Actions CI: fmt, clippy, tests, a run seeded by the commit hash, and a 1,000-seed sweep.

Completion gate:
`scripts/check` passes, `scripts/mutants` catches the Phase 0 planted bugs, and CI passes on `main`.

Testing plan:
- PRNG unit tests: fixed outputs for fixed seeds; drawing from one stream leaves another unchanged.
- A planted failing property makes the sweep exit non-zero and print a replay line.

Status ledger:

| Status | Type | Item | Evidence / Gap |
| --- | --- | --- | --- |
| Complete | Work | 0A: Git repository and workspace | `Cargo.toml`, `crates/live/Cargo.toml`, `crates/sim/Cargo.toml`, `examples/chat/Cargo.toml`; rusqlite 0.41 bundles SQLite 3.54.1. |
| Complete | Work | 0B: Clippy bans | `crates/live/clippy.toml`, `crates/sim/clippy.toml`; a planted `HashMap` in `crates/live/src/lib.rs` fails `cargo clippy -p sqlive -- -D warnings` with "use of a disallowed type". |
| Complete | Work | 0C: PRNG and driver CLI | `crates/sim/src/prng.rs`, `crates/sim/src/main.rs`, `crates/sim/src/trace.rs`; `parse_seed` accepts decimal seeds and commit hashes (`seeds_parse_from_decimal_and_commit_hashes`). |
| Complete | Work | 0D: `scripts/check` and `scripts/mutants` | `scripts/check 200` passes; `scripts/mutants --seeds 100`: 2 of 2 caught. |
| Complete | Work | 0E: `CLAUDE.md` | `CLAUDE.md`. |
| Complete | Work | 0F: CI | `.github/workflows/ci.yml`; run `40265783138` on `acb6509`: `test` and `simulate` succeeded. |
| Complete | Test | PRNG fixed-output and stream-independence tests | `a_seed_always_draws_the_same_numbers`, `streams_with_different_purposes_differ`, `drawing_from_one_stream_leaves_another_unchanged`, `below_stays_in_bounds`. |
| Complete | Test | Planted failing property caught by the sweep | Mutant "a planted failure on every fifth seed": caught by `crates/sim/tests/seeds.rs`; in the mutated copy, `--seeds 10` prints "2 of 10 seeds from 0 failed" with replay lines for seeds 3 and 8 and exits 1. |
| Complete | Gate | `scripts/check` passes | `scripts/check 200`: fmt, clippy, tests, "0 of 200 seeds from 0 failed". |

## Phase 1: Simulated VFS

Goal:
SQLite runs deterministically on an in-memory VFS that fails and crashes like a real disk, in WAL mode with several connections.

Scope:
- 1A VFS on the FFI (`rusqlite::ffi`): files, locks shared across connections, `xShm*` regions for the WAL index, `xRandomness` from a seed stream, `xCurrentTime` from a simulated clock, and `xSleep` advancing that clock. Each run reseeds SQLite's own PRNG, which picks WAL salts, from its seed.
- 1B Faults: read, write and sync errors; spurious `SQLITE_BUSY` on lock requests. A crash keeps each unsynced sector with a seeded probability, producing lost and torn writes within the device characteristics the VFS reports.
- 1C Storage harness: plain-SQLite workloads (random puts and deletes, WAL, `synchronous=FULL`) on one writer and up to three readers, with checkpoints, crashes and clean reopens, checked against a row model, as in `../turtlekv-rs/turtlekv/src/sim/vopr.rs`. A failed `COMMIT` is read back to learn whether it took effect; one this process cannot see stays a candidate for recovery.

Out of scope:
- SqLive itself (Phase 2).

Completion gate:
All properties hold over a 10,000-seed sweep; `scripts/mutants` catches every Phase 1 planted bug; `/code-review` findings are resolved.

Testing plan:
- VFS unit tests: WAL readers keep their snapshot; shm locks exclude a second writer; a crash keeps synced commits and drops unsynced ones.
- Properties: same seed, same trace digest and summary. Every commit acknowledged before a crash survives it. Recovery yields the last acknowledged commit or a failed commit after it, exactly. Reader snapshots match the model at their version. `PRAGMA integrity_check` passes after every restart.
- Planted bugs: `xSync` that persists nothing; exclusive shm locks that ignore shared holders; a WAL index per connection; crash decisions that differ between runs of a seed.

Status ledger:

| Status | Type | Item | Evidence / Gap |
| --- | --- | --- | --- |
| Complete | Decision | Build the VFS on the FFI directly, because `sqlite-plugin` leaves `xShm*` unset | `../gRPSQLite/sqlite_vfs/src/sqlite-plugin/src/vfs.rs` registers `xShmMap: None` (checked 2026-10-25). |
| Complete | Work | 1A: VFS with shm, randomness and clock | `crates/sim/src/vfs.rs`; `wal_readers_keep_their_snapshot_while_the_writer_commits`, `shm_locks_exclude_a_second_exclusive_holder`; `Simulator::run` reseeds SQLite's PRNG with `SQLITE_TESTCTRL_PRNG_SEED`. |
| Complete | Work | 1B: Fault and crash model | `FaultConfig::from_prng`, `SimVfs::begin_crash`/`finish_crash`; `a_crash_keeps_synced_commits`, `a_crash_drops_unsynced_writes`. Over seeds 0 to 400, 124 seeds see failed reads and 13 recover a commit whose `COMMIT` failed. |
| Complete | Work | 1C: Storage harness with row model | `crates/sim/src/storage.rs`; `storage_seeds_0_to_63_pass_and_replay` in `crates/sim/tests/seeds.rs`. |
| Complete | Test | Determinism, durability, recovery, snapshot and integrity properties | `Harness::restart` (durability, recovery, integrity) and `Harness::read_with` (snapshots); the seed test compares digests and summaries of two runs. |
| Complete | Test | Phase 1 planted bugs | `scripts/mutants --seeds 300`: 6 of 6 caught (4 Phase 1, 2 Phase 0), all by `crates/sim/tests/seeds.rs`. |
| Complete | Gate | 10,000-seed sweep | `cargo run --release -p sqlive-sim -- 0 --seeds 10000`: 0 of 10,000 failed, after the review fixes. |
| Complete | Gate | Code review | `/code-review high` found 10 issues, all resolved: the harness drops its VFS last, so a failing seed reports instead of aborting (checked with a planted mid-run failure: "50 of 50 seeds from 0 failed" with replay lines); the final close runs without faults; each run reseeds SQLite's PRNG; `configure_sqlite` runs before registration and reports a late call; the trace records each read's verdict; snapshots share rows through `Rc`; the VFS keys locks and WAL indexes without allocating per call; unused VFS methods are gone and I/O counts appear in the summary; this ledger. |

## Phase 2: Changefeed

Goal:
Every successful `write` publishes one versioned event with net row changes. A replica built only from events matches the database.

Scope:
- 2A `LiveDb::open` and `open_with(Options { vfs, readers, log_capacity, busy_timeout, checkpoint_pages })`: WAL, `synchronous=FULL`, `foreign_keys=ON`, and a `_sqlive_meta` version row.
- 2B `write`: `BEGIN IMMEDIATE`, the closure, version bump, read-back, `COMMIT`, publish. An authorizer on the writer denies `BEGIN`, `COMMIT`, `ROLLBACK`, `ATTACH`, durability pragmas, and any write, trigger, index or `ALTER` on `_sqlive_meta` inside the closure, and allows savepoints. An `Err` or a failed statement rolls back and publishes nothing.
- 2C Capture: the preupdate hook records touched rows (table, key, first old value) in first-touch order and skips `_sqlive_meta`. Values stay exactly as SQLite holds them, and `WITHOUT ROWID` keys compare under the primary key's own collations.
- 2D Net changes: read back each touched key's current row and emit Insert, Update, Delete or nothing, comparing values bit for bit.
- 2E Schema events: a `PRAGMA schema_version` change during the closure or since the last write sets `schema_changed`; `db.vacuum()` publishes `schema_changed`. Virtual-table shadow tables count as tables.
- 2F Retained log: `changes()` and `changes_since(v)`, as a blocking iterator plus `try_next`, with bounded capacity, `Lagged`, and `FutureVersion`.
- 2G Simulator workloads. Schemas: rowid tables with and without `INTEGER PRIMARY KEY`, composite keys, `WITHOUT ROWID`, `STRICT`, generated columns, declared affinities and collations, key collations that differ from their columns', unique constraints, foreign-key cascades, triggers, indexes. Operations: every insert conflict policy, upserts, updates that move keys, deletes, savepoints with `ROLLBACK TO`, failing statements, closures returning `Err`, DDL, and `vacuum`.

Out of scope:
- Faults and crashes on live workloads (Phase 3).
- Rows reproduce text that is not UTF-8 lossily; change detection stays exact.

Completion gate:
All properties hold over a 10,000-seed sweep; `scripts/mutants` catches every Phase 2 planted bug; `/code-review` findings are resolved.

Testing plan:
- Unit tests for net-change coalescing: insert then delete yields nothing; update then delete yields Delete with the first old value.
- Properties: a replica that applies events (and reloads from a dump on `schema_changed` or `Lagged`) equals a full dump at every check. Versions have no gaps and match `_sqlive_meta`. Rolled-back and rejected transactions publish nothing. Each applied change must match exactly the rows its key names.
- Planted bugs: unrecorded trigger and cascade deletes; keys compared without their collation; read-back without the key's collation; shadow tables left out; schema changes unflagged, during or outside a write; triggers allowed on the version table; every touch recorded; reals compared by value; `changes_since` resuming late; no `Lagged` after the log drops events.

Status ledger:

| Status | Type | Item | Evidence / Gap |
| --- | --- | --- | --- |
| Complete | Decision | Net changes by reading touched rows back before `COMMIT` | `README.md` Guarantees, "Net"; `net_changes` in `crates/live/src/db.rs`. |
| Complete | Work | 2A: Open and options | `crates/live/src/db.rs` (`LiveDb::open_with`); `a_stream_behind_the_log_lags_and_a_reopen_forgets_the_log` reopens and reads the version back. |
| Complete | Work | 2B: `write` lifecycle and transaction guard | `LiveDb::write`, `install_guard`; `a_failed_closure_rolls_back_and_publishes_nothing`, `a_closure_cannot_end_the_transaction_or_touch_the_version`, `a_closure_cannot_attach_triggers_to_or_alter_the_version_table`. |
| Complete | Work | 2C: Preupdate capture | `crates/live/src/capture.rs`; `replace_conflicts_cascades_and_triggers_appear`, `without_rowid_rows_are_keyed_by_primary_key_under_its_collation`, `a_primary_key_collation_differs_from_its_column_collation`, `text_that_is_not_utf8_still_shows_its_changes`, `rows_leave_out_virtual_generated_columns`. |
| Complete | Work | 2D: Net changes | `inserts_updates_and_deletes_publish_net_changes`, `changes_within_one_transaction_net_out`, `a_rolled_back_savepoint_leaves_no_change`. |
| Complete | Work | 2E: Schema events and `vacuum` | `schema_changes_and_vacuum_set_schema_changed`, `a_schema_changed_outside_a_write_flags_the_next_event`, `writes_to_virtual_table_shadow_tables_are_row_changes`. |
| Complete | Work | 2F: Retained log, `changes_since`, `Lagged` | `crates/live/src/log.rs` with 5 unit tests; `a_stream_behind_the_log_lags_and_a_reopen_forgets_the_log`. |
| Complete | Work | 2G: Simulator schemas and write workloads | `crates/sim/src/live/{schema,writes,replica,mod}.rs`; seed 4: 172 writes, 811 statements (95 failed), 682 row changes. |
| Complete | Test | Replica, version and rollback properties | `Live::finish_write`, `Live::check`, `Replica::apply_change`; `live_seeds_0_to_63_pass_and_replay`. |
| Complete | Test | Phase 2 planted bugs | `scripts/mutants --seeds 300`: 17 of 17 caught (11 Phase 2). With `--layers sim,seeds`, the simulator alone catches every Phase 2 bug it can reach; shadow tables, schema changes outside a write and version-table triggers need setups only the unit tests build. |
| Complete | Gate | 10,000-seed sweep | `cargo run --release -p sqlive-sim -- 0 --seeds 10000` (storage and live): 0 of 10,000 failed, after the review fixes. |
| Complete | Gate | Code review | `/code-review high` found 10 issues, all resolved: keys use the primary key's collations (`pragma_index_xinfo`), and the simulator generates differing ones; capture keeps raw values; the guard refuses triggers, indexes and `ALTER` on `_sqlive_meta`, and recording runs through the version bump; a schema moved outside a write flags the next event; shadow tables count; the seed tests serialize on SQLite's PRNG; `--layers` rejects unknown names; the `schema_change` doc matches; the README documents `rowid`, `Options`, `try_next` and `FutureVersion`. The commit-error paths it flagged as untested are Phase 3's scope. |

## Phase 3: Crashes and Faults

Goal:
The changefeed stays correct through I/O errors, commits with unknown outcomes, automatic rollbacks, and restarts.

Scope:
- 3A Commit errors: `write` returns the error and rolls back if the transaction is still open. It reads the version back: a commit that took effect is published and `write` succeeds; one that did not publishes nothing; an unreadable version leaves the writer unsure, so the next write resets the log and sets `schema_changed`.
- 3B Automatic rollbacks: a commit hook refuses every commit while the closure runs, so statements SQLite runs one by one after rolling the transaction back on an I/O error fail instead of committing. A rollback hook records the rollback, even when a later `SAVEPOINT` opens a new transaction, and `write` returns `Error::RolledBack`.
- 3C Restart: `open` reads the version from `_sqlive_meta` and starts an empty log. `changes_since(v)` yields `Lagged` for `v` below the current version and resumes for `v` equal to it. `_sqlive_meta` also stores the schema cookie as of the last commit; if it differs at open, `open` publishes a `schema_changed` event.
- 3D Vacuum: if `VACUUM` or its event fails, every open stream lags and the next write sets `schema_changed`, since rowids may have moved.
- 3E Simulator: Phase 1 faults and crashes enabled for live workloads. Replicas keep their version across restarts, resume or lag and rebuild. The oracle reads with faults suspended. Peeks and restarts draw from their own PRNG streams.

Out of scope:
- Queries (Phase 4).

Completion gate:
All properties hold over a 10,000-seed sweep with faults and crashes enabled; `scripts/mutants` catches every Phase 3 planted bug; `/code-review` findings are resolved.

Testing plan:
- Scenario: a sync error on a commit's WAL write, followed by a crash, recovers that transaction without an event; a follower at the last published version lags.
- Properties: every published event's transaction survives a crash, and `PRAGMA integrity_check` passes after every restart. After a restart, each replica either resumes or receives `Lagged`, and it equals a dump once caught up. Nothing a write does is visible to another connection before it commits. `write` never reports success for a transaction SQLite rolled back.
- Planted bugs: `synchronous=NORMAL`; publishing when `COMMIT` failed and the version did not move; a reopened log resuming streams from before the restart; no commit hook; no rollback check; a rollback hidden by a later `SAVEPOINT`; a failed vacuum leaving streams following; a schema moved while closed going unpublished.

Status ledger:

| Status | Type | Item | Evidence / Gap |
| --- | --- | --- | --- |
| Complete | Risk | A commit that fails after its WAL frames reach disk can reappear after a crash without an event | `crates/sim/tests/scenarios.rs`, `a_commit_that_failed_to_sync_can_come_back_after_a_crash_and_streams_lag`; over live seeds 0–299, 2 restarts recovered an unpublished commit and every replica lagged and rebuilt. |
| Complete | Work | 3A: Commit errors and uncertain outcomes | `LiveDb::attempt` in `crates/live/src/db.rs` reads the version back after a failed `COMMIT`; `Live::finish_write` in `crates/sim/src/live/mod.rs` checks versions, and marks the outcome unknown only for a write that reached `COMMIT`. Seeds 0–299: 6,872 failed writes, 1,080 sync errors. |
| Complete | Work | 3B: Automatic rollbacks | Commit and rollback hooks in `LiveDb::open_with`; `statements_after_an_automatic_rollback_do_not_commit` (a plain statement, and a `SAVEPOINT` after the rollback); the simulator checks `is_autocommit` after every statement. Seeds 0–299: 100 rolled-back writes across 43 seeds. |
| Complete | Work | 3C: Restart recovery, `Lagged`, and the schema cookie | `a_stream_behind_the_log_lags_and_a_reopen_forgets_the_log`, `a_schema_moved_while_closed_is_published_at_open`; `Live::restart`. Seeds 0–299: 789 restarts, 563 of them crashes. |
| Complete | Work | 3D: Failed vacuum | `LiveDb::vacuum` and `doubt`; `a_failed_vacuum_lags_every_stream_and_flags_the_next_write` in `crates/sim/tests/scenarios.rs` reproduces the failure live seed 1159 found at 9a1ec13. |
| Complete | Work | 3E: Live workloads under faults and crashes | `LiveOptions::from_seed` draws fault rates, crash and reopen rates, and peek rates; `SimVfs::quietly` suspends faults for checks. |
| Complete | Test | Durability, integrity, resume-or-lag, isolation, and rollback properties | `Live::restart` (published versions survive, `integrity_check`), `Replica::resume`, `peek_at`, the rollback check in `Live::write`; `live_seeds_0_to_63_pass_and_replay`. |
| Complete | Test | Phase 3 planted bugs | `scripts/mutants --layers unit,sim,seeds`: 25 of 25 caught, 9 of them Phase 3. The failed-vacuum mutant needs `a_failed_vacuum_lags_every_stream_and_flags_the_next_write`, since the stored schema cookie flags the next write on its own. |
| Complete | Gate | 10,000-seed sweep with faults | `cargo run --release -p sqlive-sim -- 0 --seeds 10000` (storage and live): 0 of 10,000 failed, after the review fixes. |
| Complete | Gate | Code review | `/code-review high` found 10 issues, all resolved: a rollback hook catches a `SAVEPOINT` that reopens a rolled-back transaction; the writer stays unsure until a publish succeeds; `_sqlive_meta` stores the schema cookie and `open` reconciles it; the simulator marks outcomes unknown only for writes that reached `COMMIT`, draws peeks and restarts from their own streams, runs `integrity_check` after restarts, shares one database read between the harness and replicas, takes its isolation baseline from the eager replica, and documents `Replica::resume`; the README documents `RolledBack`, commit errors and failed vacuums. |

## Phase 4: Live Queries

Goal:
Queries return rows, a version and a table- and column-level read set. A subscription that re-runs on `intersects` always holds the fresh result.

Scope:
- 4A Reader pool of read-only connections. `query(sql, params, map)` and `snapshot()` read the version inside the same read transaction as the query. A snapshot that sees a commit before its publish waits for it, so a result's version is always one the log has reached; one that finds a commit the log lacks catches the log up, and every stream lags. A snapshot whose transaction SQLite ended after an I/O error returns `Error::SnapshotEnded`.
- 4B Authorizer dependencies: each (table, column) a statement reads, through views, subqueries and CTEs, including the column-less read `count(*)` makes. Views drop out, and a virtual table's read covers its shadow tables and any external content table. Each reader caches reads by SQL text until the schema cookie changes. While recording, the authorizer refuses `PRAGMA`, `ATTACH`, `DETACH` and transaction statements.
- 4C `ReadSet { version, tables }` with `Display`. `intersects` returns false for events at or below `version` and true for `schema_changed` or a read of `_sqlive_meta` or an internal SQLite table such as `sqlite_sequence`. It returns true for an insert or delete on a read table, and for an update that changes a read column, comparing values bit for bit and treating text that holds U+FFFD as changed.
- 4D Simulator queries and subscriptions. Queries: single-table filters, joins, views, subqueries, aggregates, CTEs, `ORDER BY` with a total tiebreak, and `LIMIT`/`OFFSET`. Eager subscriptions process events after every step. Lagging subscriptions process them late and shuffled. Snapshots are held across writes beside a witness transaction, and late subscriptions start from a held snapshot's result.

Out of scope:
- Constraints and window bounds (Phase 5).

Completion gate:
All properties hold over a 10,000-seed sweep with faults and crashes enabled; `scripts/mutants` catches every Phase 4 planted bug; `/code-review` findings are resolved.

Testing plan:
- Unit tests for authorizer dependencies on views, CTEs, joins and `count(*)`, the guard, the read cache, full-text tables, lossy text, and a threaded test that every result's version is published.
- Property: a subscription that has applied every event through version `V` holds the result of a fresh query at `V`. A held snapshot returns its version's rows. The summary reports re-runs and the share that left the result unchanged.
- Planted bugs: deletes ignored; a column dropped from the read set; the version read outside the query's transaction; view base tables missing; `intersects` skipping events at `version + 1`; virtual tables without shadow tables; a stale read cache; reals compared by value; readers that skip waiting for a commit in flight; internal-table reads that miss their changes; a snapshot that keeps reading after SQLite ends its transaction.

Status ledger:

| Status | Type | Item | Evidence / Gap |
| --- | --- | --- | --- |
| Complete | Work | 4A: Reader pool, `query`, `snapshot`, commits in flight | `crates/live/src/read.rs`, `Log::begin_commit` and `Log::catch_up`; `a_held_snapshot_ignores_later_writes`, `a_result_never_runs_ahead_of_the_log` (four reader threads beside a writer; no result runs ahead and no stream lags) in `crates/live/tests/queries.rs`; `seeds_5476_and_5551_a_snapshot_sqlite_ended_reads_nothing_newer` in `crates/sim/tests/seeds.rs`. |
| Complete | Work | 4B: Authorizer dependencies | `crates/live/src/deps.rs`; `views_ctes_subqueries_and_joins_report_their_base_tables`, `a_query_may_not_end_its_snapshot_reach_another_database_or_write`, `a_schema_change_refreshes_the_reads_of_a_repeated_query`, `a_full_text_query_depends_on_its_shadow_tables`, `an_external_content_query_depends_on_its_content_table`. |
| Complete | Work | 4C: Table-level `ReadSet` and `intersects` | `crates/live/src/read_set.rs`; `intersects_finds_the_events_that_can_change_a_result`, `an_update_between_texts_that_are_not_utf8_intersects`, `a_query_of_the_version_table_sees_every_commit`, `a_query_of_an_internal_table_sees_every_commit`. |
| Complete | Work | 4D: Simulator queries and subscriptions | `crates/sim/src/live/{queries,subscriptions}.rs`; views in `crates/sim/src/live/schema.rs`; `Live::follow`, `Live::hold`, `Live::check_hold`, `Live::settle`. Subscription options draw from their own PRNG stream, and a held snapshot spans the step's own write, so earlier draws stay unchanged. |
| Complete | Test | Live-result property and precision summary | Live seeds 0–299: 196,039 subscription checks and 2,669 snapshot checks; 101,456 of 229,605 events intersected; 100,888 re-runs, 76,166 (75%) of them unchanged; 3,709 subscription lags. |
| Complete | Test | Phase 4 planted bugs | `scripts/mutants --layers unit,sim,seeds`: 36 of 36 caught, 11 of them Phase 4. With `--layers sim,seeds`, the simulator alone catches the 5 the plan lists. A snapshot that keeps reading after SQLite ends its transaction is caught by the regression seeds. |
| Complete | Gate | 10,000-seed sweep | `cargo run --release -p sqlive-sim -- 0 --seeds 10000` (storage and live): 0 of 10,000 failed. |
| Complete | Gate | Code review | `/code-review high` found 10 issues, all resolved: reads of internal SQLite tables intersect every commit; missing parameters fail as in rusqlite; `params!` and `named_params!` arrays bind, and the README uses them; subscription options draw from their own stream; readers wait for a commit in flight instead of a gate held across `COMMIT`; the `version()` doc is current; external content tables join FTS read sets; parameters convert once; subscription lags show in the summary; one helper runs the simulator's read transactions. The sweep then found snapshots SQLite had silently ended; snapshots now check their transaction, and the writer refuses every commit but its own. |

## Phase 5: Constraint Read Sets

Goal:
Read sets carry per-table constraints with bound values and `LIMIT` window bounds, so writes to other keys skip.

Scope:
- 5A Extraction: top-level `AND`ed `WHERE` predicates that compare one table's column with a constant or parameter: `=`, `IN (list)`, `<`, `<=`, `>`, `>=`, `BETWEEN`, `IS`, `IS NULL`. Parameters bind in every form (`?`, `?NNN`, `:name`, `@name`, `$name`).
- 5B Comparator: SQLite's storage-class order, column affinity applied to operands, and the `BINARY`, `NOCASE` and `RTRIM` collations, both declared and explicit. Columns with app-registered collations fall back to table level.
- 5C Window bounds: when the first `ORDER BY` term is a plain column of a constrained table and the query returned `LIMIT` rows, bound by the last row in `ORDER BY` order (direction, collation, `NULLS FIRST`/`LAST`), inclusive. Applies with `OFFSET` too. Drop the bound when fewer rows return.
- 5D Fallback: any table the authorizer reports without extracted constraints gets table level. So do tables under `OR`, functions of columns, subqueries, and the nullable side of a `LEFT JOIN`. Skip window bounds with `GROUP BY` and `DISTINCT`.
- 5E Simulator: the query generator covers supported and fallback shapes, mixed-type values, collations, `NULL`s, tied sort keys, `OFFSET` and `LIMIT offset, count`. Shapes added in this phase draw from their own PRNG stream.

Out of scope:
- Join keys collected from results for inner tables.

Completion gate:
All properties hold over a 10,000-seed sweep with faults and crashes enabled; `scripts/mutants` catches every Phase 5 planted bug; `/code-review` findings are resolved.

Testing plan:
- Differential tests: the comparator agrees with SQLite (`SELECT ?1 < ?2 COLLATE ...`) on every pair of edge values under each collation, and read sets agree with SQLite's `WHERE` on random stored values for every operator, affinity and collation.
- Unit tests for extraction and window bounds, including ties, `OFFSET`, `NULLS FIRST`/`LAST` and `NULL` boundaries.
- Properties: the Phase 4 live-result property. An equality subscription on key A never re-runs for an event that touches only key B.
- Planted bugs: affinity ignored; `NOCASE` and `RTRIM` ignored; inexact integer-real comparison; exclusive bounds at ties; only new rows checked; the bound kept when fewer than `LIMIT` rows return; `NULL` placed first under `DESC`; the `LIMIT offset, count` form misread; the nullable side of a left join constrained; `OR` split like `AND`; flipped comparisons; aggregates in expressions given a window; subqueries in `FILTER`/`OVER` missed.

Status ledger:

| Status | Type | Item | Evidence / Gap |
| --- | --- | --- | --- |
| Complete | Decision | Parser crate | `sqlite3-parser` 0.17 (`Cargo.toml`). It reads `LIMIT a, b` as `LIMIT a OFFSET b`, so `limit_uses_comma` in `crates/live/src/constraints.rs` detects the form from the tokens. |
| Complete | Work | 5A: Constraint extraction and parameter binding | `crates/live/src/constraints.rs` (`analyze`, `number_parameters`); `every_parameter_form_binds_its_value_into_the_constraint` in `crates/live/tests/windows.rs`; `read_sets_intersect_every_insert_and_delete_their_where_accepts` (3,000 random cases) in `crates/live/tests/constraints.rs`. |
| Complete | Work | 5B: SQLite comparator | `compare` in `crates/live/src/compare.rs`; `compare_orders_every_pair_as_sqlite_does`, `null_sorts_before_every_other_value`; `a_constant_converts_by_the_column_affinity`. A column whose collation SqLive cannot name gets no constraints. |
| Complete | Work | 5C: Window bounds | `window_plan` in `crates/live/src/constraints.rs` and `constrain` in `crates/live/src/read.rs`; `a_full_window_bounds_the_rows_at_its_last_sort_key`, `a_window_that_is_not_full_bounds_nothing`, `rows_before_an_offset_window_count`, `a_zero_limit_depends_on_no_rows`, `nulls_sort_first_ascending_and_last_descending_unless_told`, `a_window_orders_text_under_the_sort_collation`, `a_window_needs_its_sort_key_among_the_results`. |
| Complete | Work | 5D: Fallbacks | `shapes_the_analysis_cannot_model_fall_back_to_whole_tables` (`OR`, functions, subqueries, views, CTEs, compounds, expressions, left joins, grouped windows, aggregates in expressions, subqueries in `FILTER` and `OVER`). Tables the analysis does not name keep `Filter::All`, since `constrain` narrows only tables it names. |
| Complete | Work | 5E: Simulator query shapes and values | `crates/sim/src/live/queries.rs` (`more_predicate`, `listing`, the comma form); live seeds 0–299: 66,980 queries, 17,627 with constraints, 1,162 with window bounds. |
| Complete | Test | Live-result and precision properties | Live seeds 0–299: 195,698 subscription checks; re-runs fell from 100,888 at table level to 60,728, and the share that left rows unchanged from 75% to 59%. `an_equality_on_one_key_skips_writes_to_other_keys`. |
| Complete | Test | Phase 5 planted bugs | `scripts/mutants --layers unit,sim,seeds`: 50 of 50 caught, 14 of them Phase 5; the simulator alone catches the `LIMIT offset, count` misreading. A table the authorizer reports but the analysis leaves out cannot lose its reads, since `constrain` narrows only tables it names, so that planted bug has no code to mutate. |
| Complete | Gate | 10,000-seed sweep | `cargo run --release -p sqlive-sim -- 0 --seeds 10000` (storage and live): 0 of 10,000 failed, after the review fixes. |
| Complete | Gate | Code review | `/code-review high` found 9 issues, all resolved: windows need plain columns in the results and `ORDER BY`; calls with `FILTER`, `OVER` or an aggregate `ORDER BY` fall back; an unknown collation keeps constraints off a column; new query shapes draw from their own stream; the `LIMIT` form is computed once per query text and the affinity cast is a cached statement; a stale doc is gone; windows display `NULLS FIRST` and `NULL` boundaries as SQL that selects the same rows; only the row that fills a window is copied; one helper resolves column operands. The regression seeds for ended snapshots changed to 5476 and 5551, and the test now asserts each still ends a snapshot. |

## Phase 6: Read-Set Index

Goal:
`ReadSetIndex<K>` returns the subscriptions an event affects without testing each one.

Scope:
- 6A `insert` (which replaces), `remove`, `get`, `len`, `affected(&event)`.
- 6B Structure: each conjunction registers under one anchor constraint. Equalities go in maps keyed by SQLite's equality key under their collation; lower and upper bounds, window bounds included, go in maps ordered by SQLite's comparison; everything else goes in a per-table bucket. A row without an indexed column, or with lossy text, reaches every condition on it. Every candidate is checked with `intersects`, and `schema_changed` returns every key. Each key records its registrations, so removal touches only its own.
- 6C Benchmark: 100,000 subscriptions over 10,000 rooms, recording events per second in the README.

Out of scope:
- Persistence of subscriptions.

Completion gate:
The differential property holds over a 10,000-seed sweep; the benchmark is recorded; `scripts/mutants` catches every Phase 6 planted bug; `/code-review` findings are resolved.

Testing plan:
- Property: for every event, `affected` equals the set of keys whose read set `intersects` it, in a unit test with random read sets and in the simulator over every subscription.
- Planted bugs: equality keys without `NOCASE` folding; exclusive interval endpoints; `insert` that keeps the read set it replaces; a row without an indexed column finding nothing; a `NULLS FIRST` window missing `NULL` rows.

Status ledger:

| Status | Type | Item | Evidence / Gap |
| --- | --- | --- | --- |
| Complete | Work | 6A: Index API | `crates/live/src/index.rs`; `affected_matches_testing_every_read_set` (2,000 steps of inserts, replacements, removals and writes) in `crates/live/tests/index.rs`. |
| Complete | Work | 6B: Equality, bound and table-level structures | `ColumnIndex` and `places` in `crates/live/src/index.rs`; `affected_covers_missing_columns_lossy_text_and_every_commit`. |
| Complete | Work | 6C: Benchmark | `crates/bench/src/main.rs` (`cargo run --release -p sqlive-bench`); README "Performance": 100,682 events/s with the index, 35 without. |
| Complete | Test | Index matches brute force | `Live::check_index` and `Live::sync_index` in `crates/sim/src/live/mod.rs` check each event before subscriptions process it, against the harness's own copies of the read sets. Live seeds 0–299: 37,346 events checked, 94,421 read sets found. |
| Complete | Test | Phase 6 planted bugs | `scripts/mutants --layers unit,sim,seeds`: 55 of 55 caught, 5 of them Phase 6; with `--layers sim,seeds`, the simulator alone catches all 5. |
| Complete | Gate | 10,000-seed sweep | `cargo run --release -p sqlive-sim -- 0 --seeds 10000` (storage and live): 0 of 10,000 failed. |
| Complete | Gate | Code review | `/code-review high` found 10 issues, all resolved: the simulator checks each event before subscriptions re-run and compares with its own copies of the read sets, so it catches all five planted bugs; stream errors other than lag fail the run; the README tells custom indexes to check `ReadSet::every_commit()`; a read set that registers nowhere leaves no empty table entry; the anchor doc matches the code, which prefers an equality on a value over `IS NULL`; the module doc notes that two-sided ranges register under their lower bound; the collation lives in the column key alone; the benchmark moved to `crates/bench`, so `crates/live` reads no clocks. |

## Phase 7: Live Chat Example

Goal:
A web page where people join a room by id and post chats. SQLite persists the chats, and the page shows the room's last 10 chats, updated live through read-set intersection.

Scope:
- 7A `examples/chat` (axum, tokio) on `127.0.0.1:3000` with `chat.db` (`CHAT_ADDR` and `CHAT_DB` override them). Schema `chats(id INTEGER PRIMARY KEY, room TEXT NOT NULL, author TEXT NOT NULL, body TEXT NOT NULL, created_at INTEGER NOT NULL)` with a unique index on `(room, created_at)`, so each room's order is total; a post takes the clock's time, or one past the room's newest chat if that is later. Routes: `GET /`, `POST /rooms/{room}/chats`, `GET /rooms/{room}/live` (server-sent events).
- 7B Live query: `SELECT * FROM chats WHERE room = ?1 ORDER BY created_at DESC LIMIT 10`. One follower thread consumes `changes()` and checks a `ReadSetIndex` of the open rooms, re-runs each hit, and pushes the version, rows and read set to the room's viewers; every room's counters (re-runs, skips) update as a separate event. A new room queries and joins the index under the hub's lock, so the follower checks no commit between them. The follower re-runs queries outside that lock, and a result replaces a room's only when it is newer. A failed re-run retries on the next commit; a lag re-runs every room.
- 7C Page: room id and name inputs, a message box, and a list that renders exactly the query's rows. A callout explains that the list shows only the last 10 chats because it is the live query's result: each new chat pushes the oldest off the list. A panel shows the read set (`chats[...] where room = 'lobby' AND created_at >= ...`) and the counters, so a chat in another room visibly adds a skip and changes nothing else.
- 7D README section "Example: live chat" matches the running app.

Out of scope:
- Authentication, history paging, multiple server processes.

Completion gate:
Integration tests pass; a browser check with two rooms open shows live updates in the posting room and a skip in the other; `/code-review` findings are resolved.

Testing plan:
- Integration tests in `examples/chat/tests/live.rs` with viewers in rooms A and B. A chat posted to A pushes a new result to A and adds a skip to B with no push. After 11 chats, A holds the 10 newest, and a chat older than the window skips. A post sorts after a chat stamped ahead of the clock in its room, and only its room. A room closes with its last viewer. A follower stalled past the log's capacity re-runs every room. Reopening `chat.db` keeps the chats. The routes serve the page, accept a post, stream it, reject an empty name, and limit room ids by characters.
- Planted bugs in the follower, the re-run, post timestamps, viewer counting and the lag path, caught by the chat tests.
- Browser: headless Firefox renders two rooms side by side while a chat posts to one.

Status ledger:

| Status | Type | Item | Evidence / Gap |
| --- | --- | --- | --- |
| Complete | Work | 7A: Server, schema, routes | `examples/chat/src/main.rs`, `router` in `examples/chat/src/lib.rs`; `the_page_and_routes_serve_over_http`, `a_post_sorts_after_every_earlier_chat_in_its_room`. |
| Complete | Work | 7B: Live query loop with `ReadSetIndex` and counters | `follow`, `affected`, `rerun` and `Chat::watch` in `examples/chat/src/lib.rs`; `a_chat_reaches_its_room_and_skips_the_others`, `a_room_shows_its_ten_newest_chats`, `a_room_closes_when_its_last_viewer_leaves`, `a_follower_behind_the_log_queries_every_room_again`. |
| Complete | Work | 7C: Page with callout and read-set panel | `examples/chat/static/index.html`; `examples/chat/screenshot.png`. |
| Complete | Doc | 7D: README example section matches the app | `README.md` "Example: live chat", checked against the running server. |
| Complete | Test | Room isolation, 10-row window, chat order, closing rooms, lag, persistence across restart | `examples/chat/tests/live.rs`: 7 tests pass, and 30 of 30 release runs pass under 16 busy CPU threads; `scripts/check 10000` passes. |
| Complete | Test | Browser check | `examples/chat/screenshot.png`: headless Firefox with `lobby` and `kitchen` open while a chat posts to `lobby`. The lobby list gains the chat and drops its oldest (version 16, 1 re-run); kitchen keeps version 15 and counts 1 skip. |
| Complete | Gate | Code review | `/code-review high`, 10 findings, all resolved: room and name limits count characters and the page reports a failed join; a failed re-run logs and retries; the timestamp `max` reads only the room; re-runs run outside the hub lock; a lag counts its re-runs; the unique index and room-scoped timestamps fix the order; Phase 7 mutants; `affected` collects a `BTreeSet`; the derivable commit counter is gone; the page tracks its first render and writes the author to the hash. |
| Complete | Test | Phase 7 planted bugs | `scripts/mutants` (all layers): 61 of 61 caught, 6 of them Phase 7, each by `examples/chat/tests/live.rs`. |

## Phase 8: Result Hashes and Unchanged Re-runs

Goal:
Every result carries a hash of its rows, and `rerun` compares a new result with a previous hash, returning no rows when nothing changed. An app keeps a read set and a 32-byte hash per live query instead of its rows, and pushes only real changes.

Scope:
- 8A `ResultHash`: BLAKE3 in derive-key mode over a header (the column count, then each column name with its length, read after the statement steps) and each row (each value's storage-class tag, its bytes, and a length for text and blobs; reals by bits). Rows are hashed as they stream out of SQLite, in 16 KiB blocks gathered in a buffer each reader reuses; a large value goes to BLAKE3 directly. `QueryResult` gains `hash` and `unchanged`. A hash prints as 64 hex digits, and `from_hex` reads them back.
- 8B `LiveDb::rerun` and `Snapshot::rerun`: run the query, compare its hash with the previous one, and return no rows when they match, with the new version, read set and hash.
- 8C A reader flushes its prepared statements when the schema changes, since a statement prepared under the old schema reports its old columns until its first step.
- 8D Chat example: a room keeps only the version and hash of the rows its viewers hold. Each viewer has a mailbox holding its newest unsent result, which a newer result replaces. A viewer who joins ahead of the follower pushes the rows it read to the room's viewers. A changed re-run applies even when a viewer joined during its query; an unchanged one against a hash a join replaced runs again. Counters update outside the hub's lock, and the room's version reaches pages through them, so tabs agree after an unchanged re-run. The page counts unchanged re-runs.
- 8E README: the hash, `rerun`, and the samples that keep only read sets and hashes.
- 8F Chat reconnects and shutdown: each result event's id is its hash; a browser's `Last-Event-ID` resumes the room and skips rows it holds; `Chat::close`, called on Ctrl-C or SIGTERM, ends every live stream before a graceful shutdown; the first event sets a one-second `retry`. The README covers restarts and blue/green deploys.

Out of scope:
- Hashing declared types.
- Skipping the row mapping for unchanged results; `rerun` maps rows as it reads them.

Completion gate:
All properties hold over a 10,000-seed sweep; `scripts/mutants` catches every Phase 8 planted bug; the chat tests pass; `/code-review` findings are resolved.

Testing plan:
- Integration tests in `crates/live/tests/hashes.rs`: equal results hash the same; column names, name boundaries, the columns of an empty result, storage classes, reals' sign, value boundaries, row count and order each change the hash; one changed byte in a 100 KB value or in the 1,999th of 2,000 rows changes it; a fixed result keeps a reference hash computed by hand from the encoding; `rerun` returns rows only when they changed; a snapshot re-runs at its own version.
- `crates/live/tests/windows.rs`: a statement cached before a column is dropped still finds its window's sort key.
- Simulator: subscriptions re-run with `rerun` against the held hash. An unchanged re-run must carry no rows and keeps the held rows, which the oracle checks once the subscription catches up. A changed re-run of a completely ordered query must differ from the held result in its rows or column names.
- Chat tests: a viewer who holds the current rows resumes without them, over HTTP as well; result events carry their hash as their id; closing the chat ends every live stream; an unchanged re-run pushes nothing and moves the room's version; a slow viewer skips to the newest result; a viewer who joins ahead of the follower brings the room's other viewers along; the lag path's re-runs count as unchanged.
- Planted bugs in the encoding, the final flush, large values, hex parsing, `rerun`'s comparison, the statement flush, and the chat's push paths and mailbox.

Status ledger:

| Status | Type | Item | Evidence / Gap |
| --- | --- | --- | --- |
| Complete | Work | 8A: `ResultHash` and the row encoding | `crates/live/src/hash.rs`; `the_same_result_hashes_the_same`, `column_names_change_the_hash`, `every_difference_changes_the_hash`, `large_results_hash_every_byte`, `a_hash_stays_the_same_across_releases` (a reference hash computed by hand from the encoding), `a_hash_reads_back_from_its_hex` in `crates/live/tests/hashes.rs`. |
| Complete | Work | 8B: `rerun` | `Snapshot::run` in `crates/live/src/read.rs`; `a_rerun_returns_rows_only_when_they_changed`, `a_snapshot_reruns_at_its_own_version`. |
| Complete | Work | 8C: Statements flushed on schema change | `Snapshot` setup in `crates/live/src/read.rs`; `a_window_finds_its_sort_key_after_a_column_before_it_is_dropped` fails without the flush (`InvalidColumnIndex(3)`; before hashing, the window bound came from the wrong column). |
| Complete | Work | 8D: Chat rooms keep only hashes and versions | `examples/chat/src/lib.rs`; `a_rerun_that_finds_the_same_rows_pushes_nothing`, `a_slow_viewer_skips_to_the_newest_result`, `a_viewer_who_joins_ahead_of_the_follower_brings_the_room_along`, `a_follower_behind_the_log_queries_every_room_again`; 10 tests pass, and 30 of 30 release runs pass at a load average of 21 on 24 cores. |
| Complete | Work | 8F: Chat reconnects and shutdown | `Chat::resume`, `Chat::close`, `live` and `result_event` in `examples/chat/src/lib.rs`, and `stopped` in `examples/chat/src/main.rs`; `a_viewer_who_holds_the_current_rows_resumes_without_them`, `a_reconnecting_browser_gets_rows_only_when_they_changed`, `closing_the_chat_ends_every_live_stream`, `the_page_and_routes_serve_over_http`; 13 tests pass, and 30 of 30 release runs pass. The release binary, sent SIGTERM with a live stream open, ended the stream cleanly and exited within 5 ms. |
| Complete | Doc | 8E: README | "React to changes", "Many live queries", the **Hashes** guarantee, "How it works" and the chat section. |
| Complete | Test | Simulator property | `Subscription::rerun` in `crates/sim/src/live/subscriptions.rs`. Before the row marker and the statement flush, it failed seeds 1, 7, 20, 30, 38, 45, 47, 57 and 61, each in `live_seeds_0_to_63_pass_and_replay`. Over seeds 0–299, 35,488 of 60,728 re-runs (58%) came back unchanged. Comparing rows alone finds 35,795: the difference is unordered queries that returned the same rows in another order, and empty `SELECT *` results whose columns grew with `ADD COLUMN`. |
| Complete | Gate | 10,000-seed sweep | `scripts/check 10000`: fmt, clippy and every test pass; 0 of 10,000 seeds failed. |
| Complete | Test | Phase 8 planted bugs | `scripts/mutants` holds 81 bugs, 20 of them Phase 8. On the final tree, the 4 reconnect and shutdown bugs are caught by the chat tests across every layer, and all 13 chat bugs are caught by the chat tests. The 13 library bugs of Phase 8 were caught on 6c3b3e5, and the 55 bugs of Phases 0–6 in the full run on 1997262; the code they target has not changed since. On 1997262, `--layers sim,seeds` alone caught the 4 that break `rerun`'s comparison or the final flush; the encoding collisions, large values and the statement flush need values and schema changes only the unit tests build. |
| Complete | Test | Browser check | `examples/chat/screenshot.png`: headless Firefox with `lobby` and `kitchen` open while a chat posts to `lobby`. Lobby shows version 16 with 1 re-run, 0 unchanged and 0 skips; kitchen keeps version 15 with 0 re-runs, 0 unchanged and 1 skip. |
| Complete | Gate | Code review | `/code-review high`, 9 findings, all resolved: per-viewer queues that dropped slow viewers became latest-wins mailboxes; skip counts moved outside the hub lock; a changed re-run applies when a viewer joined during its query, and only an unchanged one against a replaced hash runs again; the display string is built only when pushing; the room's version travels in `RoomStats` (monotonic, since counters update outside the lock), so tabs agree after an unchanged re-run; each reader reuses one hash buffer; `ResultHash::from_hex` reads back the hex `Display` writes, and the README documents it with `as_bytes` and `from_bytes`; doc phrasing; the constant mutant reads `let unchanged = false`. |

## Phase 9: Performance and Scale

Goal:
Measure what SqLive costs and how the chat server scales, publish the numbers in the README, and fix what the measurements expose.

Scope:
- 9A `sqlive-bench` subcommands: `writes` (single-row and 1,000-row commits through `LiveDb::write` against plain rusqlite with the same settings), `queries` (chat-query latency on one thread and eight, `rerun` unchanged, full scans), `index`, and `memory` (a counting allocator, run in its own process, for read sets alone and in a `ReadSetIndex`, and retained events). Databases live on disk in `target/bench`.
- 9B `chat-load`: starts the release server, opens live streams across rooms, posts at a fixed rate, and reports per-chat delivery latency, the post ceiling, and the server's RSS and CPU.
- 9C Fixes: each viewer's counters arrive at most every `STATS_EVERY` (500 ms); the chat server turns off SQLite's memory statistics before opening the database.
- 9D A browser reconnect across a real restart.
- 9E The README's Performance section.

Out of scope:
- Shrinking read sets, whose B-tree nodes are mostly empty capacity.
- Batching posts into shared commits.

Completion gate:
Every benchmark runs from one command and its numbers are in the README; the chat load test delivers every chat; the throttle has a test and a planted bug.

Testing plan:
- Run `sqlive-bench` with memory statistics on and off, and `chat-load` spread across rooms, fanned out within rooms, and past the fsync ceiling.
- `a_viewers_counters_arrive_at_most_twice_a_second`, on a multi-threaded runtime so the stream runs while the posts block.
- Headless Firefox through a logging proxy, with the server sent SIGTERM and restarted on the same database.

Status ledger:

| Status | Type | Item | Evidence / Gap |
| --- | --- | --- | --- |
| Complete | Work | 9A: Benchmark suite | `crates/bench/src/{main,writes,queries,index,memory}.rs`; `cargo run --release -p sqlive-bench`. |
| Complete | Work | 9B: Chat load test | `examples/chat/src/bin/chat-load.rs`. 1,000 rooms × 10 viewers at 100 posts/s: 30,000 of 30,000 delivered, p50 4.7 ms, p99 31.9 ms, 238 MB, 38% of a core. 10 rooms × 1,000 viewers at 50 posts/s: 1,500,000 of 1,500,000, p50 6.2 ms, p99 18.9 ms. Ceiling: 223 posts/s, the drive's fsync rate. |
| Complete | Work | 9C: Counter throttle and memory statistics | Before the throttle, 10,000 viewers at 100 posts/s took 8.5 cores; after, 0.38. With memory statistics off, eight threads ran 421,577 chat queries/s against 52,732. `a_viewers_counters_arrive_at_most_twice_a_second` catches the planted bug "every counter change reaches every viewer"; on the single-threaded test runtime it missed it, so the test runs on two worker threads. |
| Complete | Test | 9D: Browser reconnect | Headless Firefox through a proxy logging each connection: SIGTERM ended the first stream 3 ms later; the browser reconnected 1.0 s later with `Last-Event-ID` equal to the last result's id; the new server sent only the result for a chat posted after the restart, and the page showed it. |
| Complete | Doc | 9E: Published numbers | README "Performance". |
| Complete | Gate | Full gate | `scripts/check 10000`: fmt, clippy and every test pass, 0 of 10,000 seeds failed. On the final tree, all 14 chat planted bugs are caught by the chat tests; `scripts/mutants` now holds 82. |

## Phase 10: Performance Recovery

Goal:
Cut what SqLive adds to queries and writes, keeping every result, read set, hash and event the same.

Scope:
- 10A Snapshot statements: `BEGIN`, the version and schema cookie, read in one statement, and `ROLLBACK` stay prepared.
- 10B Constraint plans: `constraints::plan` parses a query text once per schema into conditions whose constants are terms over its parameters; `Plan::bind` fills them in on each run with the same affinity and `NULL` rules.
- 10C Kept statements: each reader keeps each query text's statement in a slot and releases its bound values when the run ends. rusqlite's statement cache clears bindings when it takes a statement back, which makes SQLite prepare it again whenever planning looked at any parameter.
- 10D Threads: idle readers sit in per-thread slots instead of one locked stack, and the log's `catch_up` and `latest` read an atomic copy of its latest version.
- 10E Read sets: a query text's tables and columns are shared by every read set it returns; tables sit in a sorted slice found without allocating.
- 10F Writes: the hook reads old values only on a row's first touch and new values only for a `WITHOUT ROWID` key; an update that keeps its rowid touches one row; touched rows are keyed by the table's index in the schema; read-back statements are prepared once per commit; a run of consecutive rowids reads back in one range scan; read-back rows are compared in place and old values move into the event.
- 10G The README's Performance section.

Out of scope:
- Planner settings: the query planner stability guarantee would avoid the re-prepare too, but it also stops plans that look at bound values, such as a `LIKE ?` prefix search on an index.
- Batching posts into shared commits.

Completion gate:
Every library and simulator test passes unchanged; the 10,000-seed sweep has no failures; `scripts/mutants` catches every planted bug, including those for the new code; `/code-review` findings are resolved; the README's numbers come from the final tree.

Testing plan:
- `a_named_parameter_left_unbound_is_null_on_every_run`, `a_query_runs_inside_another_querys_row_mapping` and `a_read_set_finds_its_tables_in_any_case` cover kept statements and the case-insensitive table lookup.
- `a_result_never_runs_ahead_of_the_log` checkpoints after every commit, so readers see commits before their events publish on every run.
- Planted bugs for each new path: kept bindings, the log's fast path, parameter binding in plans, table lookup by case, rowid-moving updates, the range read-back's end, runs across tables, and per-table read-back statements.

Status ledger:

| Status | Type | Item | Evidence / Gap |
| --- | --- | --- | --- |
| Complete | Work | 10A: Snapshot statements | `Reader::begin` and `LiveDb::release` in `crates/live/src/read.rs` run `BEGIN`, one `SELECT` of the version and schema cookie, and `ROLLBACK` through `prepare_cached`. The chat query in a profiling loop went from 11.7 to 10.8 µs. |
| Complete | Work | 10B: Constraint plans | `Plan`, `Pending`, `Test`, `Term`, `PendingWindow` and `plan` in `crates/live/src/constraints.rs`; `Cached::planned` and `Cached::boundary_column` in `read.rs`. 10.8 to 9.0 µs. |
| Complete | Work | 10C: Kept statements | `Reader::statement`, `Reader::keep` and `bind` in `read.rs`; the connection lives behind `NonNull` so statements can borrow it, and `Drop` finalizes them first. Through rusqlite's cache SQLite re-prepared the chat query on 1,000 of 1,000 runs, and a held statement on 2. 8.8 to 6.2 µs. |
| Complete | Work | 10D: Threads | `Readers` in `read.rs`; `Log::reached` in `crates/live/src/log.rs`. Eight threads with memory statistics off: 421,577 to 962,398 queries/s; with them on, 52,732 to 703,957. |
| Complete | Work | 10E: Read sets | `ColumnsRead`, `Recorded`, `TableRead::new` and `ReadSet::table` in `crates/live/src/read_set.rs`; `lowercase` in `schema.rs` for `ReadSetIndex::affected`. A chat-query read set: 2,164 to 718 bytes, 2,822 to 1,369 in a `ReadSetIndex`; `intersects` on every read set, 35 to 115 events/s; the index, 100,682 to 115,532. |
| Complete | Work | 10F: Writes | `Capture::written`, `removed` and `touch` in `crates/live/src/capture.rs`; `net_changes`, `run_end`, `read_back` and `change` in `db.rs`; `Table::index` and `Table::select_range` in `schema.rs`; the writer's `PRAGMA schema_version` through `prepare_cached`. A 1,000-row update on tmpfs in a profiling loop: 1,057 to 515 µs; a 1,000-row insert, 1,489 to 1,040 µs. |
| Complete | Doc | 10G: Published numbers | README "Performance", from the final tree, with a tmpfs table for writes, a rusqlite row that holds its statement, and where the query benchmark's pages come from: `pages` in `crates/bench/src/queries.rs` counts 23.4 per query from SQLite's page cache and 1.7 from the OS, and `cold` empties the OS page cache with `posix_fadvise` for a run that reads from the drive, 12.6–13.0 µs through SqLive and 13.4–15.5 through rusqlite. |
| Complete | Test | Tests for the new paths | `a_named_parameter_left_unbound_is_null_on_every_run`, `a_query_runs_inside_another_querys_row_mapping` and `a_read_set_finds_its_tables_in_any_case` in `crates/live/tests/queries.rs`. `a_result_never_runs_ahead_of_the_log` caught "readers do not wait for a commit under way" in 6 of 20 runs before it checkpointed every commit, and 20 of 20 after. |
| Complete | Test | Phase 10 planted bugs | `scripts/mutants` holds 90, 8 of them Phase 10; its full run caught 89. The miss, "a schema change keeps statements prepared under the old one", no longer planted a bug: a re-planned query prepares a fresh statement into its slot. The bug now also reuses the kept statement, and `a_window_finds_its_sort_key_after_a_column_before_it_is_dropped` catches it. The 13 bugs in `db.rs`, rerun after its last change: 13 of 13 caught. |
| Complete | Gate | Code review | `/code-review high`, 8 findings. Fixed: kept statements release their bound values; one slot per text replaces a remove and reinsert, so a run allocates no key; `Log::latest` reads the atomic; a plan keeps only its parameter numbering, not the rewritten SQL; `ReadSetIndex::affected` looks tables up without allocating; this ledger. Answered: the slots keep seeds deterministic, since one thread visits them in the same cyclic order whatever its number (now in the `Readers` doc); and binding re-prepares only a parameter planning looked at, while `sqlite3_clear_bindings` re-prepares when planning looked at any, which the 1,000 against 2 re-prepares above show. |
| Complete | Gate | Full gate | `scripts/check 10000` on the final tree: fmt, clippy and every test pass, 0 of 10,000 seeds failed. |

## Phase 11: Clean Shutdown

Goal:
Closing a `LiveDb` leaves the database in one file: the writer closes last, checkpoints the WAL into the database file, and deletes it.

Scope:
- 11A Close order: `LiveDb`'s `Drop` closes the idle readers before the writer. SQLite checkpoints and deletes the WAL only when the last connection to close can take an exclusive lock, which a read-only file descriptor cannot.
- 11B VFS fidelity: a handle the simulated VFS opens read-only takes only shared locks and never writes, as on disk.
- 11C Simulator property: every clean close, at a reopen and at the end of a run, leaves no WAL.
- 11D README: "Open a database" says what closing does.

Out of scope:
- A database another connection still holds open keeps its WAL, as SQLite does.

Completion gate:
The library test and the simulator property fail without the fix and pass with it; the 10,000-seed sweep has no failures; `scripts/mutants` catches both new planted bugs; `/code-review` findings are resolved.

Testing plan:
- `closing_the_database_checkpoints_and_deletes_the_wal` on disk.
- `the_last_connection_to_close_removes_the_wal_unless_it_is_read_only` for the VFS.
- The live harness's `close`, checked at each clean reopen and at the end of every run.
- Planted bugs: readers closing after the writer, and read-only handles that lock and write.

Status ledger:

| Status | Type | Item | Evidence / Gap |
| --- | --- | --- | --- |
| Complete | Work | 11A: Close order | `Readers::close` in `crates/live/src/read.rs`, called from `LiveDb`'s `Drop` in `db.rs`. On disk, a closed `LiveDb` left `-wal` and `-shm` files beside the database; now it leaves the database file alone, and the query benchmark's directory holds only `bench.db` after its run. |
| Complete | Work | 11B: VFS fidelity | `SimFile::read_only` in `crates/sim/src/vfs.rs`: `x_lock` refuses levels above shared with `SQLITE_IOERR_LOCK`, and `x_write` and `x_truncate` refuse writes. Before this, the simulator passed 50 of 50 seeds against the unfixed library, since a read-only connection closing last could checkpoint. |
| Complete | Test | 11C: Simulator property | `Live::close` in `crates/sim/src/live/mod.rs`. Against the unfixed library, every live seed of 0 to 19 failed with "a clean close left live.db-wal behind"; with the fix, 0 of 200 failed. |
| Complete | Doc | 11D: README | "Open a database". |
| Complete | Test | Tests | `closing_the_database_checkpoints_and_deletes_the_wal` in `crates/live/tests/changefeed.rs` fails without the fix; `the_last_connection_to_close_removes_the_wal_unless_it_is_read_only` in `vfs.rs`. |
| Complete | Test | Phase 11 planted bugs | `scripts/mutants` holds 92, 2 of them Phase 11. With `--only`, after the unmutated code passed every layer: "a closed database closes its readers after its writer", caught by the library tests; "read-only handles lock and write like read-write ones", caught by the simulator's tests. |
| Complete | Gate | Code review | `/code-review high`, 7 findings, no correctness bugs. Fixed: the cold benchmark's `evict` asserts the WAL is gone instead of assuming it; the `Drop` comment says "no other connection", as the README does; `Readers::close` empties each slot and keeps the pool's size; the live harness's `drop_connections` fails when a handle to the database outlives its close, and both restart paths share it. Answered: closing runs SQLite's own checkpoint on close, as dropping a plain rusqlite connection does; and the bug came from a disk measurement, not a seed, while every live seed catches it, those in `live_seeds_0_to_63_pass_and_replay` included. |
| Complete | Gate | Full gate | `scripts/check 10000`: fmt, clippy and every test pass, 0 of 10,000 seeds failed. |

## Phase 12: Read-Set Soundness

Goal:
Every query gets a read set that covers the tables, columns and rows its result depends on, or a wider one. The simulator generates the query shapes that hid the gaps this phase closes.

Scope:
- 12A `USING` and `NATURAL` joins. SQLite resolves their join columns itself, so the authorizer reports a join column only where the SQL also names it, and reports a table only through a column the SQL names, or when the statement uses none of its columns. A statement that joins this way takes its tables from the b-trees its bytecode opens (the `OpenRead` root pages of `EXPLAIN`, looked up in `sqlite_schema`), counts every column of each, and keeps its row filters. When it opens a table the bytecode leaves unnamed, as for a virtual table, its read set intersects every commit. A statement joins this way when its text holds `USING` or `NATURAL`, or names a view that does, directly or through other views. The schema lists those views, since SQLite can merge a view into the statement before the authorizer hears of it.
- 12B Window sort keys. `window_plan` looks through parentheses and `COLLATE` before it checks whether an `ORDER BY` name is a result alias, since SQLite does.
- 12C `NaN` parameters. SQLite binds `NaN` as `NULL`, and the constraint plan reads the parameter the same way.
- 12D Text encodings. `compare` orders text by its UTF-8 bytes, while a UTF-16 database orders `BINARY` text by UTF-16 code units. `open` returns `Error::Unsupported` unless `PRAGMA encoding` is UTF-8.
- 12E Tables that change without row events. A read intersects every commit when it is of a table-valued function other than the JSON ones, such as a pragma function or `dbstat`; of a virtual table with neither shadow tables nor a content table, such as `fts5vocab`; or of a full-text table whose content is a view. A shadow table belongs to the virtual table named before its last underscore, as SQLite assigns it.
- 12F The **Conservative** guarantee covers what SQL determines. The README names what SQL leaves open, where a fresh run may differ without an intersecting event: row order without a complete `ORDER BY`, the rows a `LIMIT` keeps without one, a bare column in an aggregate, a group's value where a collation makes different values equal, and functions that read the clock or draw random numbers.
- 12G Simulator queries, each new shape from its own PRNG stream: unqualified column names; `ORDER BY` names in parentheses, quotes and `COLLATE`, with and without a result alias that gives the name to another column; `USING` and `NATURAL` joins, inner and outer, direct and through views; `NaN` parameters; `dbstat` and pragma functions.
- 12H Simulator full-text tables, in three schemas of ten: an FTS5 table, its `fts5vocab` vocabulary, an FTS5 table whose name extends the vocabulary's, and an FTS5 table whose content is a view. Transactions write them, queries read each and join one by column name, and replicas apply the changes to their shadow tables.

Out of scope:
- Column-level reads for `USING` and `NATURAL` joins, which would count only the join columns.
- Constraint read sets for UTF-16 databases.
- Naming the virtual tables a `USING` or `NATURAL` statement opens, which would spare it every commit.
- Counting the key columns of the indexes a plan scans. It would pin a result SQL leaves open against updates to those columns, until SQLite plans the query another way.
- Telling a view's name from another use of the same word. Any token that spells the name of a view joined by column name counts as naming it, so a statement that uses the word as a column or alias counts every column of its tables. A test on the parsed statement would have to list every form that holds a table name, and a form it missed would under-report.
- FTS5's totals after a read error. A read error inside an FTS5 insert leaves FTS5 holding the row and size totals it failed to load, and the next commit stores them, so `PRAGMA integrity_check` reports a malformed inverted index. Plain SQLite does the same on the simulated VFS, with a read error at the insert's third read. The simulator suspends fault injection for the statement that writes a full-text table.

Completion gate:
Each reproduction in the ledger intersects; all properties hold over a 10,000-seed sweep that includes the 12G and 12H shapes; `scripts/mutants` catches every Phase 12 planted bug; `/code-review` findings are resolved.

Testing plan:
- Unit tests, one per reproduction in the ledger, in `crates/live/tests/queries.rs` and `crates/live/tests/windows.rs`: the write changes the rows, and its event intersects the read set. A UTF-16 database fails to open.
- Simulator: the Phase 2 replica property over shadow tables, and the Phase 4 live-result property and the Phase 6 index property over the 12G and 12H shapes.
- Planted bugs: tables taken from the authorizer alone; a `USING` join that keeps column-level reads; a view's join left unseen; a view reached through another view; a virtual table left unnamed; parentheses that hide an alias; `NaN` kept as a real; a UTF-16 database that opens; a pragma function with a row-level read set; a shadowless virtual table with a row-level read set; a content view read as a table; shadow tables taken by name prefix; recording stopped before virtual tables write; a common table expression read as a function.

Status ledger:

| Status | Type | Item | Evidence / Gap |
| --- | --- | --- | --- |
| Complete | Work | 12A: `USING` and `NATURAL` joins | `resolve`, `joins_by_name`, `views_joined_by_name` and `add_opened` in `crates/live/src/deps.rs`; `Schema::views_joined_by_name`. On 9dfea38, with `a(k, x)` and `b(k, y)`: `SELECT a.x FROM a JOIN b USING (k)` and `SELECT a.x FROM a NATURAL JOIN b` returned the read set `a[x]`; `SELECT count(*) FROM a JOIN b USING (k)` returned an empty one; `SELECT x FROM v`, over `CREATE VIEW v AS SELECT a.x AS x FROM a JOIN b USING (k)`, returned `a[x]`; and an `UPDATE` of `k`, in `a` or in `b`, changed each result and intersected none. Each now returns `a[*]; b[*]`, as do `SELECT count(*) FROM v` and a count over a view that names a joining view. `a_join_by_column_name_reads_every_column_of_the_tables_it_opens` runs 11 such queries against both updates; `a_join_by_column_name_keeps_its_row_filters`; `a_join_by_column_name_with_a_virtual_table_sees_every_commit`; `an_explain_of_a_join_by_column_name_runs`. |
| Complete | Decision | 12A: what a `USING` or `NATURAL` statement falls back to | The owner chose, on 2026-11-17: tables from the bytecode with every column counted, so commits to other tables still skip. |
| Complete | Work | 12B: Parenthesized `ORDER BY` aliases | `bare` and `window_plan` in `crates/live/src/constraints.rs`. On 9dfea38, with `t(a, b)` holding (1, 10), (2, 20) and (3, 30), `SELECT b AS a, a AS x FROM t ORDER BY (a) LIMIT 2` sorted by `b` and returned the read set `t[a, b] where (a IS NULL OR a <= 2)`, and `INSERT INTO t VALUES (100, 5)` entered the window and skipped. `an_order_by_name_that_is_a_result_alias_bounds_no_window` runs seven spellings of the name; `an_order_by_column_in_parentheses_bounds_its_window` keeps the bound for a column no alias shadows, under its `COLLATE`. |
| Complete | Work | 12C: `NaN` parameters | `Parameters::values` in `crates/live/src/constraints.rs`. On 9dfea38, with `t(id INTEGER PRIMARY KEY, a REAL)`, `SELECT id FROM t WHERE a IS ?1` bound to `f64::NAN` returned the read set `t[a, id] where a IS NaN`, and `INSERT INTO t VALUES (2, NULL)` added a row and skipped. `a_nan_parameter_reads_as_the_null_sqlite_binds`: the read set is `t[a, id] where a IS NULL`. |
| Complete | Work | 12D: UTF-16 databases | `LiveDb::open_with` checks `PRAGMA encoding` before it changes the database. On 9dfea38, in a UTF-16le database with `t(name TEXT)`, `SELECT name FROM t WHERE name < ?1` bound to `'a'` returned `t[name] where name < 'a'`, and inserting `'Ā'` (U+0100), which UTF-16LE orders before `'a'`, added a row and skipped. `a_utf16_database_fails_to_open_and_stays_as_it_was`. |
| Complete | Decision | 12D: refuse UTF-16 databases, or open them and constrain text by equality alone | The owner chose, on 2026-11-17: refuse at `open`, since change detection also compares text by its UTF-8 bytes. |
| Complete | Work | 12E: Pragma functions, `dbstat` and virtual tables SqLive cannot follow | `resolve` in `crates/live/src/deps.rs`; `ReadSet::new` takes the flag. On 9dfea38, `SELECT user_version FROM pragma_user_version` skipped a commit that ran `PRAGMA user_version = 7`; `SELECT page_count FROM pragma_page_count` and `SELECT sum(pgsize) FROM dbstat` skipped a commit that inserted 2,000 rows; `SELECT term FROM vocab`, over `CREATE VIRTUAL TABLE vocab USING fts5vocab (docs, row)`, skipped an insert into `docs`; and a full-text table over `content = 'a_view'` skipped an update of the view's table. `reads_of_the_databases_own_state_see_every_commit`, beside a full-text table named `vocab_idx`; `a_full_text_query_whose_content_is_a_view_sees_every_commit`. `a_json_list_and_the_schema_table_keep_row_level_read_sets` and `a_common_table_expression_keeps_a_row_level_read_set` keep row-level read sets for `json_each`, `sqlite_schema` and a common table expression SQLite reports by name. |
| Complete | Doc | 12F: The **Conservative** guarantee names what SQL leaves open | README "Guarantees". With `t(id INTEGER PRIMARY KEY, a, b, c)` and an index on `(b, c, a)`, `SELECT a FROM t WHERE b > 0 LIMIT 2` returns other rows after `UPDATE t SET c = 0 WHERE id = 3`, and its read set `t[a, b] where b > 0` skips the update. The simulator limits only completely ordered queries (`order_and_limit` in `crates/sim/src/live/queries.rs`), and compares grouped results without the group's value (`QuerySpec::open`): in seed 117131, `SELECT a.c0, count(*) FROM t3 AS a GROUP BY a.c0`, over a `NOCASE` column, showed `'a'` or `'A'` by the order of an index on a column the query never reads. |
| Complete | Work | 12G: Simulator query shapes | `Shapes`, `bare_names`, `order_by_name`, `named_join`, `nan` and `engine_state` in `crates/sim/src/live/queries.rs`; `join_views` in `crates/sim/src/live/schema.rs`. With every new shape switched off, live seeds 0–199 kept the digests of 9dfea38. |
| Complete | Work | 12H: Simulator full-text tables | `FullText` in `crates/sim/src/live/schema.rs`, `full_text_statement` in `writes.rs`, `full_text` in `queries.rs`; `replica::shapes` lists shadow tables. The version bump's `RETURNING` makes FTS5 write its held-back index entries before the read-back (`bump_version` in `crates/live/src/db.rs`): with a plain `UPDATE`, they reach the shadow tables during `COMMIT`, after recording ends. |
| Complete | Test | Unit tests, one per reproduction | The tests named above, in `crates/live/tests/queries.rs` (26 tests) and `crates/live/tests/windows.rs` (14), and three in `crates/live/src/deps.rs`. |
| Complete | Test | Live-result and index properties over the 12G and 12H shapes | Live seeds 0–299: 72,070 results, of them 3,006 with bare names, 3,158 with `ORDER BY` names, 10,583 with joins by column name, 421 with a `NaN` parameter, 3,087 of the database's own state and 5,312 of full-text tables; 8,223 read sets intersect every commit; 195,314 subscription checks and 37,219 index checks. |
| Complete | Test | Regression seeds | `crates/sim/tests/seeds.rs`. Seed 92 failed against a draft that looked for joining views among the views the authorizer reported: `SELECT count(*) FROM j0`, over a view joining `t0` with itself by `USING (c0)`, held a stale count under the read set `t0[c2, id]`. Under the final queries, seeds 847 and 494 fail with a view's join unseen, and seeds 586 and 1254 with the full-text table of a join by column name left out; 8 of 10,000 seeds catch the second. Seed 883 stands beside 5476 for the ended snapshot: both end a snapshot and fail with the fix removed, which seed 5551 no longer does. |
| Complete | Test | Phase 12 planted bugs | `scripts/mutants` holds 106, 14 of them Phase 12. With `--only`, after the unmutated code passed every layer, the library tests caught each one, and the Phase 4 bug "virtual tables do not reach their shadow tables", rewritten for the new code. With `--layers sim,seeds`, the simulator's tests caught the 11 it can reach. The other three need a view that names a view, a UTF-16 database, and a common table expression, whose bug only over-reports. |
| Complete | Gate | 10,000-seed sweep | `scripts/check 10000`: fmt, clippy and every test pass, 0 of 10,000 seeds failed. |
| Complete | Gate | Code review | `/code-review high`, 10 findings, 1 an under-report. Fixed: a virtual table took the shadow tables of any table whose name extended its own, which hid that `fts5vocab` has none; a common table expression SQLite reports by name read as a function of every commit; a failure to prepare the `EXPLAIN` was cached as every commit, and now fails the query; the simulator gained full-text tables (12H); seed 1035, which passes with the fix removed, gave way to 883; a second check for joining views, and the planted bug that removed only it, are gone; `add_opened` scans the schema once; `window_plan` and `is_plain_column` share `bare`, and one lookup gives a table's kind. Recorded under "Out of scope": any token that spells a joining view's name counts as naming it. |

## Phase 13: Batch Events and Read-Set Size

Goal:
`ReadSetIndex::affected` takes time in proportion to an event's changes and the keys they reach, and a read set holds only the memory its constraints need.

Scope:
- 13A `affected` checks each candidate with `ReadSet::affected_by` against the change that found it, after the version check `intersects` makes. A key registered under a whole table is checked once, against the table's changes, until one affects it.
- 13B `Filter::any_of`, which builds each read set's filter, trims the conjunctions and their constraints to their length.
- 13C `sqlive-bench index` measures events of 1, 100, 1,000 and 5,000 changes, and the README's index and memory tables carry the new numbers.

Out of scope:
- Recomputing an entry's registrations on removal in place of storing them.
- Readers beyond the pool. Each opens a connection, loads the schema and prepares its statement for one query: 16 threads on 4 readers with a 50-table schema ran 331,000 chat queries a second, against 492,000 on 16 readers.
- Keys registered under a whole table that an event leaves unaffected: each checks every change to the table, so the cost is their product. That covers updates to columns such a key leaves unread, and rows that fail a filter with no equality or bound to register under.
- One definition for the event-level rules of `intersects` and `affected`. The index finds keys through its registrations where `intersects` tests one read set, so each states the rules in its own terms; the simulator and `affected_matches_testing_every_read_set` compare the two on every event.

Completion gate:
The index properties pass; `sqlive-bench index` shows a 5,000-change event costing at most twice as much per change as a 1-change event; `scripts/mutants` catches every Phase 13 planted bug; `/code-review` findings are resolved.

Testing plan:
- `affected_matches_testing_every_read_set` gains transactions of several statements across both tables, and checks events that some read sets already reflect. The simulator compares `affected` with `intersects` on every event, before the subscriptions process it and again once their newer read sets are indexed.
- Planted bugs: a candidate accepted without its check; a candidate checked against an event's first change alone; a whole-table key checked against the first change alone; the version check dropped; constraints left with spare room.
- `sqlive-bench index` and `sqlive-bench memory`.

Status ledger:

| Status | Type | Item | Evidence / Gap |
| --- | --- | --- | --- |
| Complete | Work | 13A: `affected` checks a candidate against the change that found it | `ReadSetIndex::affected` and `changed` in `crates/live/src/index.rs`. On 9dfea38, with 100,000 chat-query read sets over 10,000 rooms on tmpfs, one event inserting a chat into each of N rooms took 5.8 µs for N = 1, 845 µs for 100, 58.7 ms for 1,000 and 1.43 s for 5,000, each looked up repeatedly. Now, looking each event up once: 8.6 µs, 849 µs, 8.2 ms and 41.0 ms. |
| Complete | Work | 13B: Constraints trimmed to their length | `Filter::any_of` in `crates/live/src/constraints.rs`. A `Constraint` takes 128 bytes, and a `Vec` that grows by `push` starts at four. A chat-query read set went from 718 to 390 bytes, and from 1,369 to 1,041 in a `ReadSetIndex`; `query` still takes 6.1 µs (`sqlive-bench queries`). An `IN` list takes room for the values it was given. `a_read_set_keeps_no_spare_room_for_its_constraints` in `crates/live/tests/windows.rs`. |
| Complete | Work | 13C: Benchmark rows and README | `crates/bench/src/index.rs` looks up 2,000, 20, 2 and 2 events of 1, 100, 1,000 and 5,000 changes, each event once, over rooms the lookups before it left alone. README "Many live queries", "Finding affected subscriptions" and "Memory". |
| Complete | Test | Index matches brute force on batch events | `affected_matches_testing_every_read_set` in `crates/live/tests/index.rs`: a third of its transactions run two to seven statements across both tables, and each step also looks up an earlier event that some read sets already reflect. It asserts over 100 events of several changes. |
| Complete | Test | Phase 13 planted bugs | `scripts/mutants` holds 111, 5 of them Phase 13. With `--only`, after the unmutated code passed every layer, the library tests caught each one. With `--layers sim,seeds`, the simulator's tests caught the four that return wrong keys; the fifth wastes memory. |
| Complete | Gate | Cost per change stays flat | `sqlive-bench index` on the final tree: 8.6 µs per change in a 1-change event, 8.5 in 100, 8.2 in 1,000 and 8.2 in 5,000. `sqlive-bench memory`: 37.2 MB for 100,000 read sets held alone, 99.3 MB in a `ReadSetIndex`. |
| Complete | Gate | 10,000-seed sweep | `scripts/check 10000`: fmt, clippy and every test pass, 0 of 10,000 seeds failed. |
| Complete | Gate | Code review | `/code-review high`, 8 findings, no correctness bugs. Fixed: `affected` tests a key's version before it checks the key against changes, so a read set that already reflects an event costs one comparison; a change's candidates are deduplicated; the simulator checks the index again once newer read sets join it (`Live::check_reflected`), which catches the dropped version check; an `IN` list's values take exact room; readers alone load the views that join by column name; the benchmark's comments match what it measures; the README and "Out of scope" state where an event costs keys times changes. Answered under "Out of scope": the index and `intersects` each state the event-level rules. |

## Phase 14: Bounded Event Memory

Goal:
An event lists a bounded number of rows of each table, and the log retains a bounded number of bytes, whatever a transaction writes.

Scope:
- 14A Log budget. `Options::log_bytes` bounds the size of the retained events beside `log_capacity`. `publish` sizes each event before it takes the log's lock. The oldest events drop first, and streams behind them lag; the log always keeps its newest event, so a stream at the head receives it. `net_changes` trims an event's changes to their length.
- 14B Change cap. `Options::table_changes` caps the rows of one table a transaction records. Past the cap, capture releases what it recorded for the table and stops recording it, and the event names the table in `CommitEvent::whole_tables` and lists none of its rows. Releases cost in proportion to the rows recorded: capture drops a whole table's rows once such rows make up half of what it holds. `intersects` returns true for a read set that reads rows of a named table (`ReadSet::reads_rows_of`), and `ReadSetIndex::affected` returns each such key registered under it.
- 14C Simulator. Options draw a small cap and a small budget from their own stream. A replica applies the rows an event lists, as the README tells an app to: it reads each table the event names whole again at the version the database has reached, and skips that table's rows in events at or below it. No event lists more rows of a table than the cap, and a replica that holds every commit receives the next one. Subscriptions keep the live-result property.
- 14D README: the options, the event's field, the **Complete** and **Bounded** guarantees, and a bulk-transaction table under "Memory".

Out of scope:
- Spilling events to disk.
- Streaming a transaction's changes before it commits.
- A byte bound on one event. The cap counts rows, so an event holds up to the cap's worth of rows, of whatever size, for each table its transaction writes; an app with large rows sets a lower cap.
- A simulator property for the log's size. The log exposes no size, so the unit tests in `crates/live/src/log.rs` hold the budget.

Completion gate:
All properties hold over a 10,000-seed sweep with the cap and budget drawn; `sqlive-bench memory` shows the log within its budget after bulk commits; `scripts/mutants` catches every Phase 14 planted bug; `/code-review` findings are resolved.

Testing plan:
- Unit tests in `crates/live/src/log.rs`: events drop by size, oldest first; the newest event stays; a stream behind the dropped events lags.
- `crates/live/tests/changefeed.rs`: a table past the cap is named and lists no rows, while another table in the same transaction lists its rows; a read set on the named table intersects, and one on another table skips.
- Simulator: the Phase 2 replica property and the Phase 4 live-result property with the cap and budget drawn.
- Planted bugs: the budget ignored; the newest event dropped; a table past the cap left unnamed; a named table that keeps its earlier rows; a row recorded past the cap; a count carried across transactions; `intersects` blind to `whole_tables`; the index blind to `whole_tables`; the index reaching only keys that read a table whole.
- `sqlive-bench memory`: one 200,000-row insert, then three updates of all 200,000 rows.

Status ledger:

| Status | Type | Item | Evidence / Gap |
| --- | --- | --- | --- |
| Complete | Work | 14A: Log budget | `Log::publish` and `LogState::forget` in `crates/live/src/log.rs`; `CommitEvent::bytes` in `crates/live/src/event.rs`; `Options::log_bytes`. On 9dfea38 with default options and chat rows of a 60-byte body, one commit inserting 200,000 rows left 72 MB of Rust heap in the log, and each commit updating all 200,000 added 110 MB: 401 MB after three, held until 4,096 later commits. With every row listed and the default budget, the same commits leave the newest event, 97 MB. Trimmed changes take a retained one-chat event from 777 to 433 bytes. |
| Complete | Work | 14B: Change cap and `whole_tables` | `Capture::touch` and `Capture::compact` in `crates/live/src/capture.rs`; `CommitEvent::whole_tables`; `ReadSet::reads_rows_of`; `ReadSetIndex::changed` and `TableIndex::keys`. Capture kept each touched row's old values until the commit, and the event kept each changed row's old and new values: about 360 bytes for an inserted chat row and 550 for an updated one. |
| Complete | Decision | 14B: name the table, or set `schema_changed` | The owner chose, on 2026-11-17: `whole_tables`, so a bulk write to one table re-runs only the queries that read it. |
| Complete | Decision | Defaults for `log_bytes` and `table_changes` | The owner chose, on 2026-11-17: 64 MiB and 10,000 rows; an app that replicates from events raises the cap. |
| Complete | Work | 14C: Simulator cap, budget and replica reload | `LiveOptions::from_seed` draws both from the stream "live event bounds"; `Replica::apply`, `Replica::reload_from` and `Live::reload` in `crates/sim/src/live`. A replica fails if an event names a table whole and lists one of its rows, or lists more rows of a table than the cap. `Live::step` fails if the eager replica held every commit and lagged on the next. |
| Complete | Doc | 14D: README | "Open a database", "Write", "React to changes", "Many live queries", "Guarantees", "How it works" and "Memory". |
| Complete | Test | Log and capture unit tests | `events_drop_by_size_oldest_first` and `the_newest_event_stays_whatever_its_size` in `crates/live/src/log.rs`; `a_table_past_the_change_cap_is_named_whole_and_lists_no_rows`, `a_capped_table_stays_whole_through_rollbacks_and_names_nothing_under_a_schema_change` and `the_log_drops_its_oldest_events_past_its_byte_budget` in `crates/live/tests/changefeed.rs`. |
| Complete | Test | Replica and live-result properties under the cap and budget | Live seeds 0–299, of which half draw a cap of 3 or 20 rows and half a budget of 1,500 or 30,000 bytes: events named tables whole 2,936 times, in 75 seeds, and replicas reloaded them in 2,527 passes; replica lags rose from 1,823 to 2,369; 53,731 replica checks, 195,253 subscription checks and 37,216 index checks passed, and 45,150 more of an earlier event. |
| Complete | Test | Phase 14 planted bugs | `scripts/mutants` holds 120, 9 of them Phase 14. With `--only`, after the unmutated code passed every layer, the library tests caught each one. With `--layers sim,seeds`, the simulator's tests caught the seven that give a wrong event, a wrong answer or a lag; the other two keep too many events or name a table in later transactions. The whole suite, run without `--only` on the final tree after the unmutated code passed every layer: 120 of 120 caught, 90 by the library tests, 15 by the simulator's tests, 14 by the chat example's tests and 1 by the seed sweep. |
| Complete | Gate | The log stays within its budget | `sqlive-bench memory`, one 200,000-row insert then three updates of every row. Default options: 3.0 MB at the peak, and four events that name `chats`, under 1 KB, left in the log. With `table_changes: usize::MAX`: 216 MB at the peak and 97 MB left, the newest event. With `log_bytes: usize::MAX` as well: 374 MB and 353 MB. `sqlive-bench writes` on tmpfs stays within 2% of the README's figures: 58,343 one-row commits a second, and 983,285, 1,626,471 and 1,597,993 rows a second in commits of 1,000. |
| Complete | Gate | 10,000-seed sweep | `scripts/check 10000`: fmt, clippy and every test pass, 0 of 10,000 seeds failed. |
| Complete | Gate | Code review | `/code-review high`, 10 findings, none a wrong answer in the cap, `whole_tables`, the index or the log budget. Fixed: the README told a replica to resume from the version at which it read a named table again, which loses other tables' changes, and now tells it to keep following and skip that table's earlier rows, as the simulator's replicas do; the simulator checks the cap on every event and that a current replica never lags; capture keeps one count for each table in place of two maps, and releases rows in passes that cost in proportion to what it holds; the benchmark's peak no longer counts a reallocated block twice; the docs of `CommitEvent::changes`, `capture.rs` and the crate say that an event can name a table in place of its rows. Answered under "Out of scope": the cap bounds rows, not bytes. Answered in Phase 12: seed 117131, outside the gate's range, held `'a'` for a group whose fresh value was `'A'`; SQL leaves a group's value open, so the harness now compares grouped results without it, the README's **Conservative** guarantee names the case, and the seed is a regression test. |

## Phase 15: Node.js Binding

Goal:
Node.js apps use SqLive through an npm package, `@danthegoodman/sqlive`, built on napi-rs. Reads cost close to what they cost in Rust; writes never block the event loop on fsync; live queries take one call. CI builds and tests the addon on every platform it ships for, and a version bump on `main` publishes it.

Scope:
- 15A Crate `crates/node` (`sqlive-node`, a cdylib) and the package's JavaScript layer (`index.js`, `index.d.ts`). The binding starts one writer thread per database, which `crates/live` may not.
- 15B Reads. `query`, `rerun` and `snapshot.query` run on the calling thread; `queryAsync` and `rerunAsync` run on libuv's thread pool. Rows cross packed: numbers in an 8-byte slot of a reused buffer, text in strings of at most 4,096 UTF-16 units that JavaScript slices, blobs as `Buffer`s; a constructor compiled once per column list builds the row objects. A read set crosses as a type-tagged external.
- 15C Writes. `write(fn)` runs `fn` on the JavaScript thread, holding the transaction across its awaits, while the writer thread waits; then the writer thread commits. `writeSync` commits on the calling thread and refuses while an asynchronous write is pending. `vacuum` runs on the writer thread. A write inside a write refuses instead of deadlocking, and a `tx` works only during its own callback.
- 15D Change streams as async iterators, woken by the binding's own commits; events stay in Rust until JavaScript reads `changes`. `ReadSetIndex` with number and string keys.
- 15E `subscribe(sql, params)`: one index and one follower per database; re-runs store their read sets straight into the index; a lagged follower re-runs every subscription.
- 15F Values: integers within 53 bits as numbers and the rest a `RangeError`, or every integer a `bigint` with `safeIntegers`; `-0` binds as a real; errors carry SQLite's code names or `ERR_SQLIVE_*` codes with their fields.
- 15G Packaging and release: six platform packages (`@danthegoodman/sqlive-<target>`), CI jobs that build and test each and upload its addon, and `.github/workflows/release.yml`, which publishes the addons a green push to `main` built whenever the package version is new to npm.
- 15H Benchmarks: `npm run bench` runs `sqlive-bench`'s workloads; the README compares the two.

Out of scope:
- Linux on 32-bit or other architectures, macOS on Intel, and Windows on arm64.
- Group commit: queued writes each commit alone.
- Sharing a database between worker threads; each worker opens its own `LiveDb`.
- Simulator coverage. The binding adds no change capture or read-set logic: its properties check its conversions and its loops against SQLite itself, through `node:sqlite`.

Completion gate:
`npm test` passes on Node 22 and 24 and on every CI target; the properties hold over 20 seeds; `scripts/mutants --layers node` catches every Phase 15 planted bug; the README carries the Node benchmarks beside Rust's.

Testing plan:
- `test/basics.test.mjs`: value round trips with their storage classes, parameter forms, write outcomes and their refusals, snapshots, closing, and a process that exits once idle.
- `test/changes.test.mjs`: event shape, streams, lag, the change cap, read sets, the index, and a follower that falls behind the log.
- `test/live.test.mjs`: seeded random writes in all three write modes, some rolled back; each subscription's rows equal a fresh `node:sqlite` query, a replica built from events equals a table dump, `affected` equals testing every read set, and each delivery's hash differs from the one before.
- Planted bugs: `-0` bound as an integer; bare keys given the colon prefix; non-ASCII text measured in bytes; large integers rounded; text that never moves to its next chunk; event rows swapped; a failed callback committed by the writer, or reported as a success; a kept transaction reaching a later write; a write that wakes no stream; a subscription skipping a key; a lagged follower that stops; re-runs that ignore the previous hash; an open write left waiting at teardown; the cleanup hook's reference freed twice; the event loop let go before every job settles; rows that take their width from their column names; a typed array bound one byte per element; a vacuum allowed inside a write.
- `scripts/release.test.mjs`: version changes, drift, staging and idempotent publishing.

Status ledger:

| Status | Type | Item | Evidence / Gap |
| --- | --- | --- | --- |
| Complete | Work | 15A: Crate and JavaScript layer | `crates/node/src/{db,query,rows,events,read_set,values,errors}.rs`; `crates/node/index.js`, `index.mjs`, `index.d.ts`, `binding.js`. `npm run typecheck` checks `test/types.ts` against the declarations. |
| Complete | Decision | Reads on the calling thread, writes off the event loop | The owner chose, on 2026-11-22. A commit waits for fsync: on the drive, `writeSync` stalls the event loop 4.03 ms at the median and 9.99 ms at worst across 1,000 single-row commits, while under `write` a 1 ms timer never waits more than 2.01 ms. A hot query takes 7.9 µs, less than a trip through the thread pool. |
| Complete | Work | 15B: Reads | `Packed` in `crates/node/src/rows.rs`, `unpack` in `index.js`, `read_set::to_js`. The chat query took 10.5 µs with one Node-API call per value; packing rows took it to 9.1, a type-tagged external in place of a class instance for the read set to 8.5, and reusing each thread's packer to 7.9. Rust takes 6.1 µs and `node:sqlite` 6.0. A full scan reads 3.64 million rows a second, against 4.97 million in Rust and 3.09 million through `node:sqlite`. |
| Complete | Work | 15C: Writes | `NativeDb::queue`, `run_writer` and `write_sync` in `crates/node/src/db.rs`; `WriteJob` in `index.js`. The JavaScript and writer threads each hold a count on the dispatcher, and an environment cleanup hook, registered after it so it runs first, releases both and rolls back a write whose callback can no longer finish. Two failures led there: Node finalized `NativeDb` while tearing down the dispatcher, holding the lock the finalizer's release took, which hung exit on Node 24; and the writer thread released a dispatcher Node had already destroyed, which aborted exit on Node 22 (CI run on PR #2). `the process exits once no write is pending, and not before` and `a worker that ends inside a write rolls it back and lets the lock go` check both. |
| Complete | Work | 15D: Change streams and the index | `ChangeStream` and `Hub` in `index.js`; `CommitEvent` and `NativeIndex` in Rust. A read wakes the streams only when it finds a version newer than any reported. `affected` handles 107,560 events a second over 100,000 read sets, against 124,673 in Rust. |
| Complete | Work | 15E: `subscribe` | `Live` and `Subscription` in `index.js`; `NativeDb::query_into`. A subscription copies its parameters, so a caller's later change cannot reach its re-runs. |
| Complete | Work | 15F: Values and errors | `crates/node/src/values.rs`, `errors.rs`. |
| Complete | Work | 15G: Packaging and release | `crates/node/npm/*`, `scripts/release.mjs`, `scripts/build.mjs`, `scripts/glibc.mjs`; `.github/workflows/ci.yml` and `release.yml`, which `actionlint` passes. libsqlite3-sys runs bindgen, so every build needs libclang, and on musl its build scripts must link the C runtime dynamically to load it; the napi CLI always passes `--target`, which keeps them static, so `scripts/build.mjs` runs `cargo build` for the host. Built locally: linux-x64-gnu in the pinned manylinux_2_34 image, which needs at most GLIBC_2.34 and passes `npm test` on the host, and linux-x64-musl in Alpine, which passes `npm test` there. |
| Complete | Decision | npm name `@danthegoodman/sqlive` | npm refused the unscoped `sqlive` on 2026-11-24 as too similar to `sqlite` and `sqlite3`, and the `sqlive` org name was taken, so the owner chose their personal scope. The platform packages are `@danthegoodman/sqlive-<target>`. A packed `@danthegoodman/sqlive` and `@danthegoodman/sqlive-linux-x64-gnu`, installed together, load and query from CommonJS and ESM. |
| Complete | Decision | Release on a new version, not on every merge | A merged version bump releases; no bot commits to `main`. The workflow ships the addons the green CI run built, names the platform packages as optional dependencies only at publish time, so the lockfile never refers to an unpublished package, and skips each version npm has, so a rerun finishes a partial release. |
| Complete | Work | 15H: Benchmarks | `crates/node/bench/bench.mjs`; README "Node.js against Rust", run back to back with `sqlive-bench`. |
| Complete | Test | Unit and property tests | `npm test`: 39 tests, on Node 24.18.0 and 22.23.5, among them `live results match fresh queries` for seeds 1–3, 200 steps each. `SQLIVE_SEEDS=20 SQLIVE_STEPS=300` passes. `scripts/release.test.mjs`: 5 tests. `cargo test -p sqlive-node`: 3. |
| Complete | Gate | CI on every target | PR #2, run 45064952969 on 3837126: `npm test` passes on linux-x64-gnu and linux-arm64-gnu in manylinux_2_34, linux-x64-musl and linux-arm64-musl in Alpine, darwin-arm64, win32-x64-msvc, and Node 22; the package job passes `release.mjs check`, the release tests, the typecheck and `npm pack`. |
| Complete | Test | Phase 15 planted bugs | `scripts/mutants --layers node`: 19 of 19 caught. Two needed stronger tests. A lagged follower that stops without re-running survived until `a subscription that falls behind the log re-runs and catches up` let the follower take one event before the lag. Letting the event loop go before every job settles survived until the exit test's vacuum ran long enough to finish after the write settled. |
| Complete | Gate | Code review | `/code-review high`, 10 findings, 7 reproduced. Fixed: an empty value between two that each fill a text chunk came back empty, as did the value after it; `vacuum` inside a write's callback deadlocked; a worker terminated inside a write left the writer waiting and the database locked; a huge `busyTimeout` aborted the process; a `Float16Array` parameter read past its buffer; a lone column with an empty name threw; a bare key took a prefix from inside a string literal; the event loop could exit before the last job settled, since the writer counted jobs done before JavaScript settled them; the ledger. Each has a test. Answered in the README: a named parameter the object leaves out binds `NULL`, as SQLite and SqLive's Rust API do. |
