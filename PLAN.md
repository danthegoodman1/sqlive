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
A workspace with the library crate, a simulator with seed tooling, and a local check gate.

Scope:
- 0A Git repository and Cargo workspace: `crates/live` (`sqlive`), `crates/sim` (`sqlive-sim`), `examples/chat` (`chat-example`). rusqlite with `bundled`, `hooks` and `preupdate_hook`.
- 0B `clippy.toml` in `crates/live` and `crates/sim` banning `HashMap`, `HashSet`, `std::time` and `std::thread`. The sweep runner opts out with `#[expect]`.
- 0C Simulator PRNG (xoshiro256** seeded through SplitMix64, with `Prng::stream(seed, purpose)`), plus the driver CLI: `[SEED | COMMIT] [--seeds N]`, a parallel sweep, `SIM_TRACE`, a replay line on failure, and a trace hash.
- 0D `scripts/check` (fmt, clippy `-D warnings`, tests, 1,000-seed sweep) and `scripts/mutants`.
- 0E `CLAUDE.md` with the core rules and commands.

Out of scope:
- Hosted CI, which needs a remote.

Completion gate:
`scripts/check` passes, and `scripts/mutants` catches the Phase 0 planted bugs.

Testing plan:
- PRNG unit tests: fixed outputs for fixed seeds; drawing from one stream leaves another unchanged.
- A planted failing property makes the sweep exit non-zero and print a replay line.

Status ledger:

| Status | Type | Item | Evidence / Gap |
| --- | --- | --- | --- |
| Incomplete | Work | 0A: Git repository and workspace | Missing: `Cargo.toml`, the three crate manifests, first commit. |
| Incomplete | Work | 0B: Clippy bans | Missing: `crates/live/clippy.toml`, `crates/sim/clippy.toml`; a clippy run rejecting a planted `HashMap`. |
| Incomplete | Work | 0C: PRNG and driver CLI | Missing: `crates/sim/src/prng.rs`, `crates/sim/src/main.rs`, sweep output. |
| Incomplete | Work | 0D: `scripts/check` and `scripts/mutants` | Missing: both scripts and a passing run. |
| Incomplete | Work | 0E: `CLAUDE.md` | Missing: file. |
| Incomplete | Test | PRNG fixed-output and stream-independence tests | Missing: tests in `crates/sim/src/prng.rs`. |
| Incomplete | Test | Planted failing property caught by the sweep | Missing: `scripts/mutants` report. |
| Incomplete | Gate | `scripts/check` passes | Missing: run output. |

## Phase 1: Simulated VFS

Goal:
SQLite runs deterministically on an in-memory VFS that fails and crashes like a real disk, in WAL mode with several connections.

Scope:
- 1A VFS on `libsqlite3-sys` FFI: files, locks shared across connections, `xShm*` regions for the WAL index, `xRandomness` from a seed stream, `xCurrentTime` from a simulated clock, and `xSleep` advancing that clock.
- 1B Faults: read, write and sync errors; short reads; lock contention. A crash keeps each unsynced sector with a seeded probability, producing lost and torn writes within the device characteristics the VFS reports.
- 1C Harness: plain-SQLite workloads (random DML, WAL, `synchronous=FULL`) on several connections, with crashes and restarts, checked against a row model with an undo log, as in `../turtlekv-rs/turtlekv/src/sim/vopr.rs`.

Out of scope:
- SqLive itself (Phase 2).

Completion gate:
All properties hold over a 10,000-seed sweep; `scripts/mutants` catches every Phase 1 planted bug; `/code-review` findings are resolved.

Testing plan:
- VFS unit tests: shm locks exclude conflicting holders; a crash keeps synced bytes.
- Properties: same seed, same trace hash. Every commit acknowledged before a crash survives it. Recovery yields the state after some commit between the last acknowledged and the last attempted. `PRAGMA integrity_check` passes after every restart.
- Planted bugs: `xSync` that persists nothing; shm locks that admit two writers; `xRandomness` drawn from the OS.

Status ledger:

| Status | Type | Item | Evidence / Gap |
| --- | --- | --- | --- |
| Complete | Decision | Build the VFS on the FFI directly, because `sqlite-plugin` leaves `xShm*` unset | `../gRPSQLite/sqlite_vfs/src/sqlite-plugin/src/vfs.rs` registers `xShmMap: None` (checked 2026-10-25). |
| Incomplete | Work | 1A: VFS with shm, randomness and clock | Missing: `crates/sim/src/vfs.rs`; WAL test with one writer and two readers. |
| Incomplete | Work | 1B: Fault and crash model | Missing: `FaultConfig`, `crash()`; unit tests. |
| Incomplete | Work | 1C: Plain-SQLite harness with row model | Missing: `crates/sim/src/sqlite_workload.rs`; seed tests. |
| Incomplete | Test | Determinism, durability, prefix recovery, integrity properties | Missing: property functions and seed tests. |
| Incomplete | Test | Phase 1 planted bugs | Missing: `scripts/mutants` report, 3 of 3 caught. |
| Incomplete | Gate | 10,000-seed sweep | Missing: sweep output. |
| Incomplete | Gate | Code review | Missing: `/code-review` findings and resolutions. |

## Phase 2: Changefeed

Goal:
Every successful `write` publishes one versioned event with net row changes. A replica built only from events matches the database.

Scope:
- 2A `LiveDb::open` and `open_with(Options { vfs, readers, log_capacity })`: WAL, `synchronous=FULL`, `foreign_keys=ON`, and a `_live_meta` version row.
- 2B `write`: `BEGIN IMMEDIATE`, the closure, version bump, read-back, `COMMIT`, publish. An authorizer on the writer denies `BEGIN`, `COMMIT` and `ROLLBACK` inside the closure and allows savepoints. An `Err` or a failed statement rolls back and publishes nothing.
- 2C Capture: the preupdate hook records touched rows (table, key, first old value) in first-touch order and skips `_live_meta`.
- 2D Net changes: read back each touched key's current row and emit Insert, Update, Delete or nothing.
- 2E Schema events: a `PRAGMA schema_version` change across the closure sets `schema_changed`; `db.vacuum()` publishes `schema_changed`.
- 2F Retained log: `changes()` and `changes_since(v)`, as a blocking iterator plus `try_next`, with bounded capacity and `Lagged`.
- 2G Simulator workloads. Schemas: rowid tables with and without `INTEGER PRIMARY KEY`, composite keys, `WITHOUT ROWID`, `STRICT`, generated columns, declared affinities and collations. Operations: triggers, FK cascades, `REPLACE` and upserts, `INSERT OR IGNORE`, savepoints with `ROLLBACK TO`, failing statements, closures returning `Err`, DDL, and `vacuum`.

Out of scope:
- Faults and crashes on live workloads (Phase 3).

Completion gate:
All properties hold over a 10,000-seed sweep; `scripts/mutants` catches every Phase 2 planted bug; `/code-review` findings are resolved.

Testing plan:
- Unit tests for net-change coalescing: insert then delete yields nothing; update then delete yields Delete with the first old value.
- Properties: a replica that applies events (and reloads from a dump on `schema_changed`) equals a full dump at every check. Versions have no gaps and match `_live_meta`. Rolled-back and rejected transactions publish nothing.
- Planted bugs: capture through `update_hook`, which misses REPLACE deletes; skipped read-back, which leaks `ROLLBACK TO` changes; publishing when `COMMIT` fails; `changes_since` resuming at `v` or `v + 2`; no `Lagged` after the log drops `v + 1`.

Status ledger:

| Status | Type | Item | Evidence / Gap |
| --- | --- | --- | --- |
| Complete | Decision | Net changes by reading touched rows back before `COMMIT` | `README.md` Guarantees, "Net". |
| Incomplete | Work | 2A: Open and options | Missing: `crates/live/src/db.rs`; unit test reading pragmas back. |
| Incomplete | Work | 2B: `write` lifecycle and transaction-statement guard | Missing: implementation; tests for `Err`, a failed statement, and a denied `COMMIT`. |
| Incomplete | Work | 2C: Preupdate capture | Missing: `crates/live/src/capture.rs`; tests for REPLACE, cascade and `WITHOUT ROWID`. |
| Incomplete | Work | 2D: Net changes | Missing: coalescing unit tests. |
| Incomplete | Work | 2E: Schema events and `vacuum` | Missing: tests for DDL and `vacuum`. |
| Incomplete | Work | 2F: Retained log, `changes_since`, `Lagged` | Missing: `crates/live/src/log.rs`; tests at log boundaries. |
| Incomplete | Work | 2G: Simulator schemas and write workloads | Missing: `crates/sim/src/schema.rs`, `crates/sim/src/writes.rs`. |
| Incomplete | Test | Replica, version and rollback properties | Missing: property functions and seed tests. |
| Incomplete | Test | Phase 2 planted bugs | Missing: `scripts/mutants` report, 5 of 5 caught. |
| Incomplete | Gate | 10,000-seed sweep | Missing: sweep output. |
| Incomplete | Gate | Code review | Missing: `/code-review` findings and resolutions. |

## Phase 3: Crashes and Faults

Goal:
The changefeed stays correct through I/O errors, commits with unknown outcomes, and restarts.

Scope:
- 3A Commit errors: `write` returns the error, rolls back if the transaction is still open, publishes nothing, and keeps the in-memory version equal to `_live_meta`.
- 3B Restart: `open` reads the version from `_live_meta` and starts an empty log. `changes_since(v)` yields `Lagged` for `v` below the current version and resumes for `v` equal to it.
- 3C Simulator: Phase 1 faults and crashes enabled for live workloads. Replicas keep their version across restarts, resubscribe, and rebuild from a dump on `Lagged`.

Out of scope:
- Queries (Phase 4).

Completion gate:
All properties hold over a 10,000-seed sweep with faults and crashes enabled; `scripts/mutants` catches every Phase 3 planted bug; `/code-review` findings are resolved.

Testing plan:
- Scenario: a sync error on a commit's WAL write, followed by a crash, recovers that transaction without an event. Replicas then receive `Lagged`.
- Properties: every published event's transaction survives a crash. After a restart, each replica either resumes with no gaps or receives `Lagged`, and it equals a dump once caught up. `PRAGMA integrity_check` passes.
- Planted bugs: `synchronous=NORMAL`; publishing when `COMMIT` returns an error; a version bumped in memory before `COMMIT` and kept after a failure; resuming `changes_since(v)` for `v` below the version after a restart.

Status ledger:

| Status | Type | Item | Evidence / Gap |
| --- | --- | --- | --- |
| Incomplete | Risk | A commit that fails after its WAL frames reach disk can reappear after a crash without an event; `Lagged` after restart must cover it | Missing: the scenario test above. |
| Incomplete | Work | 3A: Commit-error handling | Missing: tests with injected sync and write errors. |
| Incomplete | Work | 3B: Restart recovery and `Lagged` | Missing: restart tests. |
| Incomplete | Work | 3C: Live workloads under faults and crashes | Missing: simulator options drawing fault rates and crash points. |
| Incomplete | Test | Durability, resume-or-lag, and integrity properties | Missing: property functions and seed tests. |
| Incomplete | Test | Phase 3 planted bugs | Missing: `scripts/mutants` report, 4 of 4 caught. |
| Incomplete | Gate | 10,000-seed sweep with faults | Missing: sweep output. |
| Incomplete | Gate | Code review | Missing: `/code-review` findings and resolutions. |

## Phase 4: Live Queries

Goal:
Queries return rows, a version and a table- and column-level read set. A subscription that re-runs on `intersects` always holds the fresh result.

Scope:
- 4A Reader pool of read-only connections. `query(sql, params, map)` and `snapshot()` read the version inside the same read transaction as the query.
- 4B Authorizer dependencies: each (table, column) a statement reads, through views, subqueries and CTEs, including the empty-column report for `count(*)`. Cached by SQL text and schema version.
- 4C `ReadSet { version, tables }` with `Display`. `intersects` returns false for events at or below `version` and true for `schema_changed`. It returns true for an insert or delete on a read table, and for an update that changes a read column.
- 4D Simulator queries and subscriptions. Queries: single-table, joins, views, subqueries, aggregates, `ORDER BY` and `LIMIT`. Eager subscriptions are checked after every commit. Lagged subscriptions receive events delayed and reordered, and are checked once drained. Snapshots are held across writes, and subscriptions resubscribe on `Lagged`.

Out of scope:
- Constraints and window bounds (Phase 5).

Completion gate:
All properties hold over a 10,000-seed sweep with faults and crashes enabled; `scripts/mutants` catches every Phase 4 planted bug; `/code-review` findings are resolved.

Testing plan:
- Unit tests for authorizer dependencies on views, CTEs and `count(*)`.
- Property: a subscription that has applied every event through version `V` holds the result of a fresh query at `V`. The summary reports re-runs and the share of re-runs that left the result unchanged.
- Planted bugs: deletes ignored; columns read only in `WHERE` dropped from the dependencies; the version read outside the query's transaction; view base tables missing from the dependencies; `intersects` skipping events at `version + 1`.

Status ledger:

| Status | Type | Item | Evidence / Gap |
| --- | --- | --- | --- |
| Incomplete | Work | 4A: Reader pool, `query`, `snapshot` | Missing: `crates/live/src/read.rs`; test that a held snapshot ignores later writes. |
| Incomplete | Work | 4B: Authorizer dependencies | Missing: `crates/live/src/deps.rs`; unit tests. |
| Incomplete | Work | 4C: Table-level `ReadSet` and `intersects` | Missing: `crates/live/src/read_set.rs`; unit tests. |
| Incomplete | Work | 4D: Simulator queries and subscriptions | Missing: `crates/sim/src/queries.rs`, `crates/sim/src/subscriptions.rs`. |
| Incomplete | Test | Live-result property and precision summary | Missing: property function, seed tests, summary output. |
| Incomplete | Test | Phase 4 planted bugs | Missing: `scripts/mutants` report, 5 of 5 caught. |
| Incomplete | Gate | 10,000-seed sweep | Missing: sweep output. |
| Incomplete | Gate | Code review | Missing: `/code-review` findings and resolutions. |

## Phase 5: Constraint Read Sets

Goal:
Read sets carry per-table constraints with bound values and `LIMIT` window bounds, so writes to other keys skip.

Scope:
- 5A Extraction: top-level `AND`ed `WHERE` predicates that compare one table's column with a constant or parameter: `=`, `IN (list)`, `<`, `<=`, `>`, `>=`, `BETWEEN`, `IS`, `IS NULL`. Parameters bind in every form (`?`, `?NNN`, `:name`, `@name`, `$name`).
- 5B Comparator: SQLite's storage-class order, column affinity applied to operands, and the `BINARY`, `NOCASE` and `RTRIM` collations, both declared and explicit. Columns with app-registered collations fall back to table level.
- 5C Window bounds: when the first `ORDER BY` term is a plain column of a constrained table and the query returned `LIMIT` rows, bound by the last row in `ORDER BY` order (direction, collation, `NULLS FIRST`/`LAST`), inclusive. Applies with `OFFSET` too. Drop the bound when fewer rows return.
- 5D Fallback: any table the authorizer reports without extracted constraints gets table level. So do tables under `OR`, functions of columns, subqueries, and the nullable side of a `LEFT JOIN`. Skip window bounds with `GROUP BY` and `DISTINCT`.
- 5E Simulator: the query generator covers supported and fallback shapes, mixed-type values, collations, `NULL`s, tied sort keys and `OFFSET`.

Out of scope:
- Join keys collected from results for inner tables.

Completion gate:
All properties hold over a 10,000-seed sweep with faults and crashes enabled; `scripts/mutants` catches every Phase 5 planted bug; `/code-review` findings are resolved.

Testing plan:
- Differential test: the comparator agrees with SQLite (`SELECT ?1 <op> ?2 COLLATE ...`) on random value pairs for every operator, affinity and collation.
- Unit tests for extraction and window bounds, including ties and `NULLS LAST`.
- Properties: the Phase 4 live-result property. An equality subscription on key A never re-runs for an event that touches only key B.
- Planted bugs: affinity ignored; `NOCASE` ignored; exclusive bounds at ties; only new rows checked; the bound kept when fewer than `LIMIT` rows return; `NULL` placed first under `DESC`; a table the authorizer reports dropped when extraction finds nothing.

Status ledger:

| Status | Type | Item | Evidence / Gap |
| --- | --- | --- | --- |
| Incomplete | Decision | Parser crate: `sqlite3-parser` or its turso fork | Needs: comparison of API stability and SQLite grammar coverage. |
| Incomplete | Work | 5A: Constraint extraction and parameter binding | Missing: `crates/live/src/constraints.rs`; unit tests. |
| Incomplete | Work | 5B: SQLite comparator | Missing: `crates/live/src/compare.rs`; differential test. |
| Incomplete | Work | 5C: Window bounds | Missing: unit tests for ties, `OFFSET` and `NULLS FIRST`/`LAST`. |
| Incomplete | Work | 5D: Fallbacks | Missing: unit tests for each fallback shape. |
| Incomplete | Work | 5E: Simulator query shapes and values | Missing: generator changes in `crates/sim/src/queries.rs`. |
| Incomplete | Test | Live-result and precision properties | Missing: seed tests; precision property. |
| Incomplete | Test | Phase 5 planted bugs | Missing: `scripts/mutants` report, 7 of 7 caught. |
| Incomplete | Gate | 10,000-seed sweep | Missing: sweep output. |
| Incomplete | Gate | Code review | Missing: `/code-review` findings and resolutions. |

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
