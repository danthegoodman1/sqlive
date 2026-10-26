# Benchmarks

Measured on an Intel Core Ultra 9 285 (24 cores) with a Micron 2 TB NVMe drive
(ext4), Linux 7.0, Rust 1.98.0, and the SQLite that rusqlite 0.41 bundles. Databases
live on the drive, since every commit waits for fsync.

```console
cargo run --release -p sqlive-bench [writes|queries|index|memory]
cargo build --release -p chat-example
target/release/chat-load [ROOMS] [VIEWERS] [POSTS_PER_SECOND] [SECONDS]
```

## Writes

Each workload runs through `LiveDb::write` and through plain rusqlite with the same
settings, WAL and `synchronous = FULL`. Rows are chat rows with a 60-byte body.

| Workload | SqLive commits/s | SqLive rows/s | rusqlite rows/s | SqLive / rusqlite |
| --- | ---: | ---: | ---: | ---: |
| Insert 1 row per commit | 231 | 231 | 233 | 0.99 |
| Insert 1,000 rows per commit | 131 | 130,989 | 144,870 | 0.90 |
| Update 1,000 rows per commit | 167 | 167,245 | 224,390 | 0.75 |
| Delete 1,000 rows per commit | 143 | 143,126 | 156,040 | 0.92 |

A single-row commit costs one fsync, about 4.3 ms on this drive, so SqLive and
rusqlite run at the same pace. In large transactions, capturing changes costs 8–25%:
updates cost the most, since SqLive reads each touched row back before `COMMIT` to
compute its net change.

## Queries

The chat query, `SELECT * FROM chats WHERE room = ?1 ORDER BY created_at DESC LIMIT
10`, over 10,000 rooms of 12 chats, mapping each row to a tuple. 50,000 queries per
run.

| Query | SqLive µs | SqLive queries/s | rusqlite µs | rusqlite queries/s |
| --- | ---: | ---: | ---: | ---: |
| `query`, 1 thread | 12.7 | 79,015 | 6.6 | 150,688 |
| `rerun`, unchanged, 1 thread | 12.8 | 78,348 | | |
| `query`, 8 threads | | 52,732 | | 60,992 |
| `query`, 8 threads, memory statistics off | | 421,577 | | 961,606 |

SqLive's extra 6 µs per query buys the snapshot's version and schema checks, the read
set with its constraints and window bound, and the result hash. An unchanged `rerun`
costs the same as `query`; it saves the push, not the query.

SQLite keeps memory statistics by default, and they take one global mutex on every
allocation, in every connection. With them on, eight threads run slower than one, in
SqLive and in plain rusqlite alike. Turning them off before SQLite starts, as the chat
example's `main` does, lets eight threads run 8x the queries:

```rust
// SAFETY: before any connection opens.
unsafe {
    sqlive::rusqlite::ffi::sqlite3_config(sqlive::rusqlite::ffi::SQLITE_CONFIG_MEMSTATUS, 0);
}
```

This turns off `sqlite3_memory_used` and the soft heap limit, which rely on those
statistics.

| Full scan of 120,000 rows | SqLive rows/s | rusqlite rows/s |
| --- | ---: | ---: |
| `SELECT * FROM chats` | 4.7–5.8 million | 7.0–8.9 million |

The ranges span runs. Hashing every value and building the read set cost about a
third of plain rusqlite's scan rate.

## Finding affected subscriptions

`ReadSetIndex::affected` against testing every read set with `intersects`, for 100,000
subscriptions to the chat query over 10,000 rooms, ten per room. Each of 2,000 events
inserts one chat into one room, which affects that room's ten subscriptions.

| Method | Events per second | Affected per event |
| --- | ---: | ---: |
| `ReadSetIndex::affected` | 100,682 | 10 |
| `intersects` on every read set | 35 | 10 |

Building the 100,000 read sets, one query each, took 1.35 s.

## Memory

Rust heap measured with a counting allocator; SQLite's heap with
`sqlite3_memory_used`.

| Held | Count | Rust heap | Per item |
| --- | ---: | ---: | ---: |
| Chat-query read sets, held alone | 100,000 | 206.4 MB | 2,164 bytes |
| Chat-query read sets in a `ReadSetIndex` | 100,000 | 269.2 MB | 2,822 bytes |
| Retained events, each inserting one chat | 4,096 | 3.0 MB | 777 bytes |

SQLite's own heap, page caches included, held 4.2 MB. A result hash adds 32 bytes per
subscription.

Most of a read set's 2.2 KB is empty B-tree capacity: its map of tables allocates a
1,160-byte node with room for eleven tables to hold the chat query's one, and its
set of column names another node of about 280 bytes. Small sorted vectors would hold
the same read set in well under half the space.

## Chat server under load

`chat-load` starts the release `chat-example` server, opens live streams spread across
rooms, posts chats at a fixed rate over 16 keep-alive connections, and times each chat
from its post to its arrival at every viewer of its room. The server and the load
generator share the machine.

| Rooms × viewers each | Posts/s | Delivered | Delivery p50 | p90 | p99 | Server RSS | Server CPU |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 1,000 × 10 | 100 | 30,000 of 30,000 | 4.7 ms | 7.4 ms | 31.9 ms | 238 MB | 38% of a core |
| 10 × 1,000 | 50 | 1,500,000 of 1,500,000 | 6.2 ms | 10.1 ms | 18.9 ms | 227 MB | 114% of a core |
| 1,000 × 1 | 400 asked, 223 reached | 6,000 of 6,000 | 67.1 ms | 78.8 ms | 127.2 ms | 64 MB | 17% of a core |

- Delivery takes about one commit: the post's own fsync is 4.3 ms at the median.
- 10,000 viewers joined in 1.4 s, about 7,200 per second, and each held about 21 KB of
  server memory, most of it connection buffers.
- Posts top out at 223 per second, the drive's fsync rate for single-row commits.
  Asked for more, posts queue, and delivery waits behind them. Batching several posts
  into one commit would raise the ceiling.
- Every commit changes every room's counters, so each viewer gets its counters at
  most twice a second. Sending them on every change took 8.5 cores at 100 posts per
  second with 10,000 viewers, about a million stats events a second.
