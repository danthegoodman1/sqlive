# Benchmarks

## Finding affected subscriptions

`ReadSetIndex::affected` against testing every read set with `intersects`, for
100,000 subscriptions to the chat query over 10,000 rooms, ten per room:

```sql
SELECT * FROM chats WHERE room = ?1 ORDER BY created_at DESC LIMIT 10
```

Each room holds twelve chats, so every read set carries `room = R` and a window bound
on `created_at`. Each of 2,000 events inserts one chat into one room, which affects
that room's ten subscriptions.

```console
cargo run --release -p sqlive-bench
```

| Method | Events per second | Affected per event |
| --- | ---: | ---: |
| `ReadSetIndex::affected` | 103,029 | 10 |
| `intersects` on every read set | 35 | 10 |

Building the 100,000 read sets, one query each, took 1.15 s.

Measured with the Phase 6 index on an Intel Core Ultra 9 285 (24 cores, one used) with
Rust 1.98.0.
