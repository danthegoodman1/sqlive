# SqLive

Live queries for SQLite. Every query returns its rows plus a **read set**: the tables,
columns, and key ranges the result depends on. Every committed write emits a
**commit event** listing the rows it changed, with old and new values.
`read_set.intersects(&event)` tells you which queries to re-run.

SqLive reports what changed and what each query depends on. Your app tracks
subscriptions and pushes updates to clients.

Status: design stage. [`PLAN.md`](PLAN.md) tracks the build.

## Install

```toml
[dependencies]
sqlive = "0.1"
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
  `OR ROLLBACK` conflict. `write` then returns `Error::RolledBack` and publishes
  nothing, even when the closure returns `Ok`.
- If `COMMIT` reports an error, `write` checks whether the transaction took effect. If
  it did, `write` publishes the event and succeeds; otherwise it returns the error.
- Writes serialize on one writer connection. Keep closures short; they hold the write
  lock.
- Run `VACUUM` through `db.vacuum()`, since SQLite runs it outside a transaction.
- A closure may not touch `_sqlive_meta`, attach databases, or change `journal_mode`,
  `synchronous`, `locking_mode` or `query_only`.
- `db.version()` is the latest published version.

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
todos.read_set;  // todos[id, list_id, title, done] where list_id = 5 AND id >= 412
                 //   (412 = id of the 50th row)
```

Queries run on read-only connections, concurrently with writes (WAL mode).
`db.snapshot()` runs several queries against one version.

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

## Many subscriptions

`ReadSetIndex` finds the subscriptions an event affects without testing each one.

```rust
let mut subs: ReadSetIndex<SubId> = ReadSetIndex::new();
subs.insert(sub_id, result.read_set);

for event in db.changes()? {
    let event = event?;
    let hits: Vec<SubId> = subs.affected(&event).collect();
    for sub_id in hits {
        let result = rerun(sub_id)?;          // app code
        subs.insert(sub_id, result.read_set); // replaces the old read set
        push_to_clients(sub_id, &result.rows);
    }
}
```

To build your own index, read the constraints with `ReadSet::constraints()`. Your
index then owns SQLite's comparison rules (affinity, collation, NULL handling).

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
- The **authorizer** reports every table and column a query reads, through views,
  subqueries, and CTEs. A SQL parser adds constraints from the `WHERE` clause with
  bound parameter values, and a bound from the last row of a `LIMIT` query.
- `intersects` checks whether an event's old or new row satisfies a read set's
  constraints, using SQLite's comparison rules.

## Example: live chat

```console
cargo run -p chat-example
```

Open <http://127.0.0.1:3000>, enter a room id, and post. The page shows the live query
`SELECT * FROM chats WHERE room = ?1 ORDER BY created_at DESC LIMIT 10`; chats in other
rooms leave it untouched.

## Development

```console
cargo test --workspace
cargo run --release -p sqlive-sim [-- SEED] [--seeds N]
scripts/mutants
```

The simulator runs SQLite on a deterministic in-memory VFS with injected faults and
crashes, and checks live results against fresh queries. See `PLAN.md` for the testing
strategy.
