# Development Plan

## Overarching Goal

Build SqLive (crate `sqlive`), the Rust library `README.md` describes. Queries return rows with a versioned read set. Each committed write publishes a versioned commit event with net row changes. `ReadSet::intersects` and `ReadSetIndex` tell an app which queries to re-run. A subscription that re-runs on every intersecting event always holds the result a fresh query would return. A deterministic simulator proves this under random schemas, queries, faults and crashes before each phase closes. A live chat web page demonstrates the library end to end.

Non-goals until a phase names them: writers in other processes, ATTACHed databases, virtual tables as change sources, read sets from instrumented b-tree cursors (a patched SQLite), an async API, and client transport, which belongs to the app.

## Implementation Principles

- `README.md` is the API contract. A change to the public API or its guarantees updates the README in the same commit.
- `intersects` may over-report; it never under-reports. Each read-set refinement ships with a simulator property that compares live results against fresh queries.
- Changes come from the preupdate hook on the writer connection. The project's VFS serves only the simulator.
- All writes run through `write` on one writer connection. Queries run on read-only connections.
- The library starts no threads, reads no clocks and iterates only ordered collections. Events list changes in first-touch order. `clippy.toml` in `crates/live` and `crates/sim` enforces this, so a seed replays exactly.
- Every feature ships with simulator coverage, a property that fails on a wrong answer, and a planted bug the simulator catches.
- Build the smallest implementation that meets the phase gate.
- Execution order: Phase 0 through Phase 7.

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
- 6A `insert` (which replaces), `remove`, `affected(&event)`.
- 6B Structure: equality maps keyed by (table, column, comparison key normalized for collation and affinity); interval maps for ranges and window bounds; a table-level bucket. `schema_changed` returns every key.
- 6C Benchmark: 100,000 subscriptions over 10,000 rooms, recording events per second in `BENCHMARKS.md`.

Out of scope:
- Persistence of subscriptions.

Completion gate:
The differential property holds over a 10,000-seed sweep; the benchmark is recorded; `scripts/mutants` catches every Phase 6 planted bug; `/code-review` findings are resolved.

Testing plan:
- Property: for every event, `affected` equals the set of keys whose read set `intersects` it.
- Planted bugs: equality keys without `NOCASE` folding; exclusive interval endpoints; `insert` that drops the replacement read set.

Status ledger:

| Status | Type | Item | Evidence / Gap |
| --- | --- | --- | --- |
| Incomplete | Work | 6A: Index API | Missing: `crates/live/src/index.rs`; unit tests. |
| Incomplete | Work | 6B: Equality, interval and table-level structures | Missing: implementation; unit tests. |
| Incomplete | Work | 6C: Benchmark | Missing: bench target and `BENCHMARKS.md` entry. |
| Incomplete | Test | Index matches brute force | Missing: simulator property and seed tests. |
| Incomplete | Test | Phase 6 planted bugs | Missing: `scripts/mutants` report, 3 of 3 caught. |
| Incomplete | Gate | 10,000-seed sweep | Missing: sweep output. |
| Incomplete | Gate | Code review | Missing: `/code-review` findings and resolutions. |

## Phase 7: Live Chat Example

Goal:
A web page where people join a room by id and post chats. SQLite persists the chats, and the page shows the room's last 10 chats, updated live through read-set intersection.

Scope:
- 7A `examples/chat` (axum, tokio) on `127.0.0.1:3000` with `chat.db`. Schema `chats(id INTEGER PRIMARY KEY, room TEXT NOT NULL, author TEXT NOT NULL, body TEXT NOT NULL, created_at INTEGER NOT NULL)` with an index on `(room, created_at)`. Routes: `GET /`, `POST /rooms/{room}/chats`, `GET /rooms/{room}/live` (server-sent events).
- 7B Live query: `SELECT * FROM chats WHERE room = ?1 ORDER BY created_at DESC LIMIT 10`. One task consumes `changes()` and checks a `ReadSetIndex`. Each hit re-runs its query on `spawn_blocking` and pushes the version, rows, read set and counters (events seen, re-runs, skips).
- 7C Page: room id and name inputs, a message box, and a list that renders exactly the query's rows. A callout explains that the list shows only the last 10 chats because it is the live query's result: each new chat pushes the oldest off the list. A panel shows the read set (`chats ... where room = 'lobby' AND created_at >= ...`) and the counters, so a chat in another room visibly adds a skip and changes nothing else.
- 7D README section "Example: live chat" matches the running app.

Out of scope:
- Authentication, history paging, multiple server processes.

Completion gate:
Integration tests pass; a manual check with two browser tabs in different rooms shows live updates in the posting room and a skip in the other; `/code-review` findings are resolved.

Testing plan:
- Integration tests in `examples/chat/tests/live.rs` with subscribers in rooms A and B. A chat posted to A pushes a new result to A and adds a skip to B with no push. After 11 chats, A holds the 10 newest. Restarting the server on the same `chat.db` keeps the chats.
- Manual: two tabs, screenshots of the callout, the list and the counters.

Status ledger:

| Status | Type | Item | Evidence / Gap |
| --- | --- | --- | --- |
| Incomplete | Work | 7A: Server, schema, routes | Missing: `examples/chat/src/main.rs`. |
| Incomplete | Work | 7B: Live query loop with `ReadSetIndex` and counters | Missing: implementation. |
| Incomplete | Work | 7C: Page with callout and read-set panel | Missing: `examples/chat/static/index.html`. |
| Incomplete | Doc | 7D: README example section matches the app | Missing: run against the README commands. |
| Incomplete | Test | Room isolation, 10-row window, persistence across restart | Missing: `examples/chat/tests/live.rs`. |
| Incomplete | Test | Manual two-tab check | Missing: screenshots. |
| Incomplete | Gate | Code review | Missing: `/code-review` findings and resolutions. |
