# @danthegoodman/sqlive

Live queries for SQLite, in Node.js. Every query returns its rows plus a **read set**:
the tables, columns and key ranges the result depends on. Every committed write
publishes a **commit event** listing the rows it changed, with old and new values.
`readSet.intersects(event)` says which queries to re-run, and `db.subscribe()` re-runs
them for you.

This package binds the [SqLive](https://github.com/danthegoodman1/sqlive) Rust
library. Its guarantees are SqLive's; the
[repository README](https://github.com/danthegoodman1/sqlive#guarantees) states them.

```console
npm install @danthegoodman/sqlive
```

Node.js 22 or later. Prebuilt for Linux x64 and arm64 (glibc 2.34 or later, or musl),
macOS arm64, and Windows x64.

## A live query

```js
import { LiveDb } from '@danthegoodman/sqlive'

const db = new LiveDb('app.db')

await db.write((tx) => {
  tx.exec(`CREATE TABLE IF NOT EXISTS todos (
             id      INTEGER PRIMARY KEY,
             list_id INTEGER NOT NULL,
             title   TEXT NOT NULL,
             done    INTEGER NOT NULL DEFAULT 0)`)
})

const live = db.subscribe('SELECT id, title, done FROM todos WHERE list_id = ? ORDER BY id DESC LIMIT 50', [5])
for await (const { rows, version } of live) {
  render(rows) // first the current rows, then each result whose rows changed
}
```

Elsewhere, a write commits and the subscription re-runs:

```js
const { value, version } = await db.write((tx) =>
  tx.run('INSERT INTO todos (list_id, title) VALUES (?, ?)', [5, 'Buy milk']),
)
value.lastInsertRowid // 1
```

A subscription re-runs its query only when a commit intersects its read set, and
yields only results whose rows changed. A consumer slower than the commits gets the
newest result. `break`, or `live.close()`, ends it.

## Reads are synchronous; writes are not

A query reads pages already in memory, so it costs a few microseconds of CPU: about
8 µs for a ten-row indexed query. Handing it to a thread would cost more than that,
so `query` runs on the calling thread, as better-sqlite3 and `node:sqlite` do.
`queryAsync` runs a query on libuv's thread pool instead, for scans and joins long
enough to stall the event loop; several run at once across the reader connections.

A commit waits for fsync, which takes milliseconds. `await db.write(fn)` runs `fn` on
the calling thread, so each statement in it costs microseconds, then hands the commit
to the database's writer thread; the event loop runs while the drive syncs. `fn` may
be `async`: the transaction stays open across its awaits, and other writes queue
behind it. `writeSync` commits on the calling thread, for scripts and migrations.

## API

### `new LiveDb(path, options?)`

Opens or creates a database in WAL mode with `synchronous = FULL`.

| Option | Default | |
| --- | ---: | --- |
| `readers` | 4 | Read-only connections kept open for queries |
| `logCapacity` | 4,096 | Commit events the log retains for `changesSince` |
| `logBytes` | 64 MiB | Bytes of commit events the log retains |
| `tableChanges` | 10,000 | Rows of one table an event lists before it names the table in `wholeTables`; `Infinity` lists every row |
| `busyTimeout` | 5,000 | Milliseconds a connection waits for a lock |
| `checkpointPages` | 1,000 | WAL pages after which a commit checkpoints |
| `vfs` | | The SQLite VFS to open the database with |
| `safeIntegers` | `false` | Read every integer as a `bigint` |

`db.version` is the latest version the log has reached. `db.close()` closes the
database once its queued writes finish; streams and subscriptions end.

### Writes

`await db.write((tx) => …)` resolves with `{ value, version }`: what the callback
returned and the commit's version. A callback that throws, or rejects, rolls the
transaction back, and the promise rejects with its error. Writes commit one at a time,
in the order they were called.

- `tx.run(sql, params?)` runs a statement and returns `{ changes, lastInsertRowid }`.
- `tx.all(sql, params?)` and `tx.get(sql, params?)` read inside the transaction, so
  they see its writes.
- `tx.exec(sql)` runs a script of statements without parameters.
- A `tx` works until its callback finishes. A write called inside another's callback
  rejects with `ERR_SQLIVE_WRITE_PENDING`, since it would wait for the lock the outer
  write holds.

`db.writeSync(fn)` returns `{ value, version }` after the commit's fsync, and throws
`ERR_SQLIVE_WRITE_PENDING` while an asynchronous write is queued. `await db.vacuum()`
runs `VACUUM` on the writer thread. A write may not touch `_sqlive_meta`, attach
databases, or change `journal_mode`, `synchronous`, `locking_mode` or `query_only`.

### Queries

```js
const result = db.query('SELECT id, title FROM todos WHERE list_id = ? ORDER BY id DESC LIMIT 50', [5])
result.rows     // [{ id: 7, title: 'Buy milk' }, …]
result.version  // the version of the snapshot the query read
result.readSet  // todos[id, list_id, title] where list_id = 5 AND id >= 412
result.hash     // 64 hex digits of a BLAKE3 digest of the column names and rows

const again = db.rerun(sql, [5], result.hash)
again.unchanged // true when the rows hash the same; `rows` is then empty
```

`queryAsync` and `rerunAsync` return the same results as promises. `db.snapshot()`
holds a read transaction, so several queries read one version: call
`snapshot.query(...)` and `snapshot.rerun(...)`, then `snapshot.close()`, or declare
it with `using`. A query may not write, or run `PRAGMA`, `ATTACH`, `DETACH` or
transaction statements; pragma functions such as `pragma_table_info('todos')` work.

Parameters are an array for `?` and `?N`, or an object for `:name`, `@name` and
`$name`. A key may carry its prefix; a bare key takes the prefix the SQL uses. A
named parameter the object leaves out binds `NULL`, as in SQLite.

| SQLite | JavaScript |
| --- | --- |
| `NULL` | `null` (binds from `null` or `undefined`) |
| `INTEGER` | `number`, or `bigint` with `safeIntegers`. Without it, an integer past 53 bits throws a `RangeError`. Integral numbers, bigints and booleans bind as integers. |
| `REAL` | `number`. Other numbers bind as reals, `-0` among them. |
| `TEXT` | `string` |
| `BLOB` | `Buffer`; binds from any `ArrayBuffer`, typed array or `DataView` |

Rows are objects keyed by column name; of two columns with one name, the last wins.

### Change streams

```js
for await (const event of db.changesSince(result.version)) {
  if (result.readSet.intersects(event)) { /* re-run */ }
}
```

`db.changesSince(v)` yields every event after version `v`, starting with those the
log retains; `db.changes()` starts from now. A stream waits for the next commit and
ends when the database closes; `stream.tryNext()` returns the next event or
`undefined` at once. A stream that falls behind the retained log rejects once with
`ERR_SQLIVE_LAGGED`: query again and follow from the new result's version.

An event has a `version`; `changes`, each `{ table, op, rowid, old, new }` with `op`
one of `'insert'`, `'update'` and `'delete'`; `schemaChanged`, set by DDL and
`VACUUM`, when `changes` is empty and you reload what you hold; and `wholeTables`,
the tables the transaction changed more than `tableChanges` rows of. Events stay in
Rust until you read `changes`.

### Many live queries

`db.subscribe` keeps one index of read sets and one change stream per database. To
run the loop yourself, store each query's read set under a key:

```js
import { ReadSetIndex } from '@danthegoodman/sqlive'

const index = new ReadSetIndex()
index.insert(subscriptionId, result.readSet)
for await (const event of db.changesSince(result.version)) {
  for (const id of index.affected(event)) { /* re-run id's query, insert its new read set */ }
}
```

`affected` looks keys up by the read sets' constraints instead of testing each one.
Keys are numbers or strings. `index.delete(key)` drops one.

A read set also has `version`, `everyCommit`, `tables` (each `{ name, columns,
everyColumn, filter }`), `readsRowsOf(table)` and `toString()`.

### Errors

Errors carry a `code`: SQLite's name for a SQLite failure, such as
`SQLITE_CONSTRAINT_UNIQUE` or `SQLITE_BUSY`, or one of these:

| Code | |
| --- | --- |
| `ERR_SQLIVE_LAGGED` | A stream fell behind the log; `requested` and `oldest` say where |
| `ERR_SQLIVE_FUTURE_VERSION` | `changesSince` asked for a version past `latest` |
| `ERR_SQLIVE_ROLLED_BACK` | SQLite rolled a write back after an error, so nothing committed |
| `ERR_SQLIVE_SNAPSHOT_ENDED` | SQLite ended a snapshot's read transaction; start a new one |
| `ERR_SQLIVE_UNSUPPORTED` | A query that writes, or a database SqLive cannot follow |
| `ERR_SQLIVE_PARAMETERS` | Too few or too many positional values, or a name the statement lacks |
| `ERR_SQLIVE_TRANSACTION_ENDED` | A `tx` used after its callback finished |
| `ERR_SQLIVE_WRITE_PENDING` | A write inside a write, or `writeSync` while one is queued |
| `ERR_SQLIVE_CLOSED` | The database or snapshot is closed |

## Performance

See [the repository README](https://github.com/danthegoodman1/sqlive#nodejs-against-rust)
for this binding measured against the Rust library.
