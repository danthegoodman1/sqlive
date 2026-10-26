# SqLive

Live queries for SQLite. Every query returns its rows plus a **read set**: the tables,
columns, and key ranges the result depends on. Every committed write emits a
**commit event** listing the rows it changed, with old and new values.
`read_set.intersects(&event)` tells you which queries to re-run.

SqLive reports what changed and what each query depends on. Your app tracks
subscriptions and pushes updates to clients.

Status: every phase in [`PLAN.md`](PLAN.md) is built and tested. SqLive is young and
unpublished; expect the API to change.

## Install

```toml
[dependencies]
sqlive = { git = "https://github.com/danthegoodman1/sqlive" }
```

The crate bundles SQLite built with `SQLITE_ENABLE_PREUPDATE_HOOK`.

## Open a database

```rust
use sqlive::LiveDb;

let db = LiveDb::open("app.db")?;

db.write(|tx| {
    tx.execute_batch(
        "CREATE TABLE IF NOT EXISTS todos (
             id      INTEGER PRIMARY KEY,
             list_id INTEGER NOT NULL,
             title   TEXT NOT NULL,
             done    INTEGER NOT NULL DEFAULT 0
         );
         CREATE INDEX IF NOT EXISTS todos_by_list ON todos (list_id, id);",
    )
})?;
```

`LiveDb::open_with(path, Options { .. })` sets the VFS, the number of pooled read
connections (`readers`), how many events the log retains (`log_capacity`), the busy
timeout, and the WAL checkpoint threshold (`checkpoint_pages`). SqLive puts the database
in WAL mode with `synchronous = FULL` and keeps its version in a `_sqlive_meta` table.

## Write

`write` runs your closure inside a SQLite transaction and returns the closure's value
with the commit version.

```rust
let (todo_id, version) = db.write(|tx| {
    tx.execute(
        "INSERT INTO todos (list_id, title) VALUES (?1, ?2)",
        (5, "Buy milk"),
    )?;
    Ok(tx.last_insert_rowid())
})?;
```

- The closure receives a `rusqlite::Transaction`. Run any SQL; changes made by
  triggers and foreign-key cascades appear in the commit event.
- `write` owns `BEGIN` and `COMMIT`. Use `SAVEPOINT` for nested work.
- Return `Err` to roll back. A rolled-back transaction emits no event.
- SQLite can roll a transaction back on its own, after an I/O error or an
  `OR ROLLBACK` conflict, and run later statements each in a transaction of its own.
  SqLive refuses those commits, so `write` returns `Error::RolledBack` and publishes
  nothing, even when the closure returns `Ok`.
- If `COMMIT` reports an error, `write` checks whether the transaction took effect. If
  it did, `write` publishes the event and succeeds; otherwise it returns the error.
- Writes serialize on one writer connection. Keep closures short; they hold the write
  lock.
- Run `VACUUM` through `db.vacuum()`, since SQLite runs it outside a transaction.
- A closure may not touch `_sqlive_meta`, attach databases, or change `journal_mode`,
  `synchronous`, `locking_mode` or `query_only`.
- `db.version()` is the latest version the log has reached.

## Query

```rust
const SQL: &str =
    "SELECT id, title, done FROM todos WHERE list_id = ?1 ORDER BY id DESC LIMIT 50";

struct Todo { id: i64, title: String, done: bool }

fn to_todo(row: &rusqlite::Row) -> rusqlite::Result<Todo> {
    Ok(Todo { id: row.get(0)?, title: row.get(1)?, done: row.get(2)? })
}

let todos = db.query(SQL, [5], to_todo)?;

todos.rows;      // Vec<Todo>
todos.version;   // commit version of the snapshot the query read
todos.read_set;  // todos[done, id, list_id, title] where list_id = 5 AND id >= 412
                 //   (412 = id of the 50th row)
```

Queries run on read-only connections, concurrently with writes (WAL mode).
`params` takes what rusqlite's do: arrays, tuples, vectors, `params![...]`, or
`named_params! { ":list": 5 }`, and must bind every parameter. A query may not run
`PRAGMA`, `ATTACH`, `DETACH` or transaction statements; pragma functions such as
`pragma_table_info('todos')` work.

`db.snapshot()` runs several queries against one version. If an I/O error ends the
snapshot's read transaction, its queries return `Error::SnapshotEnded`; start a new
snapshot.

```rust
let snapshot = db.snapshot()?;
let lists = snapshot.query("SELECT id, name FROM lists", (), to_list)?;
let todos = snapshot.query(SQL, [5], to_todo)?;
assert_eq!(lists.version, todos.version);
```

A read set narrows each table to the rows the result depends on, when the query
lets it:

- Top-level `AND`ed `WHERE` terms that compare one column with a constant or bound
  parameter: `=`, `IN (...)`, `<`, `<=`, `>`, `>=`, `BETWEEN`, `IS`, `IS NULL`,
  `IS NOT NULL`. The constant converts by the column's affinity, and text compares
  under the column's collation, or an explicit `BINARY`, `NOCASE` or `RTRIM`.
- A `LIMIT` window: when the query orders by a column it returns and fills its
  `LIMIT`, rows past the last one in that order cannot change the result.
- Everything else leaves the table whole: `OR`, expressions over columns, subqueries,
  views, CTEs, compound selects, the nullable side of an outer join, and windows over
  `GROUP BY` or `DISTINCT`.

A read set names every table and column the query reads, through views, CTEs,
subqueries and joins. A read of a full-text (FTS5) table covers its shadow tables,
which carry its changes, and its external content table, if it has one. A read of
`_sqlive_meta` or of SQLite's internal tables, such as `sqlite_sequence`, intersects
every commit.

## React to changes

`changes_since(v)` yields every commit event after version `v`, starting with events
already in the retained log. Commits that land between your query and your
subscription are therefore included. Iterating blocks until the next commit and ends
when the database closes; `try_next()` returns at once. A `v` that has not committed
yet is an `Error::FutureVersion`.

```rust
let mut current = db.query(SQL, [5], to_todo)?;
push_to_clients(&current.rows);

for event in db.changes_since(current.version)? {
    // Err(Lagged): the log no longer reaches back to this version.
    // Re-query and resubscribe.
    let event = event?;
    if current.read_set.intersects(&event) {
        current = db.query(SQL, [5], to_todo)?;
        push_to_clients(&current.rows);
    }
}
```

A read set carries its snapshot version, and `intersects` returns false for events at
or below it, since the result already reflects them.

An event prints as:

```text
CommitEvent {
    version: 1043,
    schema_changed: false,
    changes: [
        RowChange { table: "todos", op: Insert, rowid: Some(519), old: None,
                    new: Some({id: 519, list_id: 5, title: 'Buy milk', done: 0}) },
    ],
}
```

## Many live queries

With many live queries, you could call `intersects` on every read set for every
commit, but `ReadSetIndex` does that bookkeeping for you. Store each query's read set
under a key you choose, such as a subscription id, and `affected(&event)` returns the
keys whose queries the commit may change. It looks them up by the read sets'
constraints instead of testing each one: with 100,000 subscriptions it handles about
100,000 events a second, against 35 for calling `intersects` on each
([`BENCHMARKS.md`](BENCHMARKS.md)).

One stream of changes then serves every query:

```rust
let mut subs: ReadSetIndex<SubId> = ReadSetIndex::new();

let result = db.query(SQL, [5], to_todo)?;
subs.insert(sub_id, result.read_set);
push_to_clients(sub_id, &result.rows);

for event in db.changes_since(result.version)? {
    let event = event?;                       // on Err(Lagged), re-run every query
    let hits: Vec<SubId> = subs.affected(&event).collect();
    for sub_id in hits {
        let result = rerun(sub_id)?;          // app code
        subs.insert(sub_id, result.read_set); // replaces the old read set
        push_to_clients(sub_id, &result.rows);
    }
}
```

- `insert` replaces a key's read set; `remove` drops it when the client leaves.
- A query added while the loop runs must join the index before the loop checks a
  commit newer than the query's version. The [chat example](#example-live-chat) runs
  the query and the insert under the same lock its loop holds while checking a commit.

To index read sets yourself, first check `ReadSet::every_commit()`: a read set that
reads `_sqlive_meta` or one of SQLite's internal tables intersects every commit. Then
read each table's `filter()` from `ReadSet::tables()`: either `Filter::All` or
`Filter::AnyOf` conjunctions of `Constraint { column, collation, condition }`.
`Constraint::matches(row)` applies SQLite's comparison rules (storage class,
collation, `NULL` handling) for you.

## Guarantees

- **Complete:** every committed row change made through `write` appears in its
  transaction's event. Versions have no gaps.
- **Net:** each event lists one change per row: the first old value and the final new
  value.
- **Conservative:** `intersects` returns false only when a change cannot affect the
  result. It may return true for a change that leaves the result the same.
- **Durable:** a published event's transaction survives a crash. A commit that
  reported an error can still reappear after a crash, without an event; streams from
  before it lag.
- **Uncertain outcomes:** when SqLive cannot tell whether a transaction took effect,
  every open stream lags and the next event sets `schema_changed`.
- **Schema:** DDL and `db.vacuum()` set `schema_changed` and leave `changes` empty;
  reload what you hold. `intersects` returns true for every read set. If `vacuum`
  fails, it may still have renumbered rowids, so every open stream lags and the next
  write, or the next open, sets `schema_changed`.
- **Rows:** a `RowChange` carries the row's stored columns, before and after, plus its
  rowid (`None` for a `WITHOUT ROWID` table, whose key identifies it). `VIRTUAL`
  generated columns are left out. Text that is not valid UTF-8 appears lossily
  converted, though changes to it are still detected exactly.
- **Restarts:** the retained log lives in memory. After a restart, `changes_since(v)`
  for any `v` below the current version yields `Lagged`.
- **Scope:** SqLive sees writes made through `write` on this `LiveDb`. Route every
  write through it.

## How it works

- The **preupdate hook** on the writer connection records each touched row. Before
  `COMMIT`, `write` reads those rows back to compute net changes, then publishes the
  event once the commit succeeds.
- A snapshot can see a commit a moment before its event is published. It then waits
  for the publish, so `changes_since(result.version)` always succeeds.
- The **authorizer** reports every table and column a query reads, through views,
  subqueries, and CTEs. Each reader caches these reads by SQL text until the schema
  changes. A SQL parser adds constraints from the `WHERE` clause with
  bound parameter values, and a bound from the last row of a `LIMIT` query.
- `intersects` checks whether an event's old or new row satisfies a read set's
  constraints, using SQLite's comparison rules.

## Example: live chat

```console
cargo run -p chat-example
```

Open <http://127.0.0.1:3000>, enter a room id and your name, and post. Chats persist in
`chat.db` (set `CHAT_DB` to move it, and `CHAT_ADDR` to listen elsewhere).

Each room shows the result of one live query:

```sql
SELECT * FROM chats WHERE room = ?1 ORDER BY created_at DESC LIMIT 10
```

so the page lists only the ten newest chats, and each new chat pushes the oldest off.
Below the list, the page shows the query's read set and two counters: re-runs and
skips. Post in one room while another room is open in a second tab: the posting room's
list updates and its re-runs count rises, while the other room's list stays put and
its skips count rises.

![Two rooms: a chat posted to lobby re-runs its query, while kitchen skips it](examples/chat/screenshot.png)

The server keeps one `ReadSetIndex` of the open rooms' read sets. A follower thread
reads `db.changes()`, asks the index which rooms each commit affects, re-runs those
rooms' queries, and pushes the new rows to the room's viewers over server-sent events
(`GET /rooms/{room}/live`). If the follower falls behind the log, it re-runs every
room. `POST /rooms/{room}/chats` writes a chat.

## Development

```console
cargo test --workspace
cargo run --release -p sqlive-sim [-- SEED] [--seeds N]
scripts/mutants
```

The simulator runs SQLite on a deterministic in-memory VFS with injected faults and
crashes, and checks live results against fresh queries. See `PLAN.md` for the testing
strategy.
