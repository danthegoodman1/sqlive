//! Query latency for the chat query, on one thread and across several, and
//! full-scan throughput, through SqLive and through plain rusqlite.

use crate::{CHATS, QUERY, fill, fresh_dir, per_second};
use sqlive::rusqlite::{self, Connection, Row};
use sqlive::{LiveDb, Options, ResultHash};
use std::hint::black_box;
use std::path::Path;
use std::time::{Duration, Instant};

const ROOMS: i64 = 10_000;
const QUERIES: usize = 50_000;
const THREADS: usize = 8;
const SCANS: usize = 5;
const SCAN: &str = "SELECT * FROM chats";

type Chat = (i64, i64, i64, String);

fn chat(row: &Row<'_>) -> rusqlite::Result<Chat> {
    Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?))
}

fn room(i: usize) -> i64 {
    (i as i64 * 7919) % ROOMS
}

fn plain_query(
    conn: &Connection,
    sql: &str,
    params: impl rusqlite::Params,
) -> rusqlite::Result<Vec<Chat>> {
    let mut statement = conn.prepare_cached(sql)?;
    let rows = statement.query_map(params, chat)?;
    rows.collect()
}

/// Runs `QUERIES` chat queries on this thread.
fn timed(mut query: impl FnMut(usize)) -> Duration {
    let started = Instant::now();
    for i in 0..QUERIES {
        query(i);
    }
    started.elapsed()
}

/// Runs `QUERIES` chat queries split across `threads` threads.
fn across(threads: usize, query: impl Fn(usize) + Sync) -> Duration {
    let started = Instant::now();
    std::thread::scope(|scope| {
        for thread in 0..threads {
            let query = &query;
            scope.spawn(move || {
                for i in (thread..QUERIES).step_by(threads) {
                    query(i);
                }
            });
        }
    });
    started.elapsed()
}

fn micros(elapsed: Duration, count: usize) -> f64 {
    elapsed.as_secs_f64() * 1e6 / count as f64
}

pub fn run() -> sqlive::Result<()> {
    let dir = fresh_dir("queries");
    let path = dir.join("bench.db");
    let options = Options {
        readers: THREADS,
        ..Options::default()
    };
    let db = LiveDb::open_with(path.to_string_lossy(), options)?;
    db.write(|tx| {
        tx.execute_batch(CHATS)?;
        fill(tx, ROOMS)
    })?;
    let hashes = (0..ROOMS)
        .map(|room| {
            db.query(QUERY, [room], |_| Ok(()))
                .map(|result| result.hash)
        })
        .collect::<sqlive::Result<Vec<ResultHash>>>()?;
    let open_plain = |path: &Path| Connection::open(path).expect("a plain connection");
    let plain = open_plain(&path);

    let live_one = timed(|i| {
        black_box(db.query(QUERY, [room(i)], chat).expect("a query"));
    });
    let rerun_one = timed(|i| {
        let room = room(i);
        let result = db
            .rerun(QUERY, [room], hashes[room as usize], chat)
            .expect("a re-run");
        assert!(result.unchanged);
    });
    let plain_one = timed(|i| {
        black_box(plain_query(&plain, QUERY, [room(i)]).expect("a query"));
    });
    let live_many = across(THREADS, |i| {
        black_box(db.query(QUERY, [room(i)], chat).expect("a query"));
    });
    let plain_many = {
        let connections: Vec<std::sync::Mutex<Connection>> = (0..THREADS)
            .map(|_| std::sync::Mutex::new(open_plain(&path)))
            .collect();
        across(THREADS, |i| {
            let conn = connections[i % THREADS].lock().expect("unpoisoned");
            black_box(plain_query(&conn, QUERY, [room(i)]).expect("a query"));
        })
    };

    println!("\n## queries: the chat query over {ROOMS} rooms of 12 chats\n");
    println!("| Query | SqLive µs | SqLive queries/s | rusqlite µs | rusqlite queries/s |");
    println!("| --- | ---: | ---: | ---: | ---: |");
    println!(
        "| `query`, 1 thread | {:.1} | {:.0} | {:.1} | {:.0} |",
        micros(live_one, QUERIES),
        per_second(QUERIES, live_one),
        micros(plain_one, QUERIES),
        per_second(QUERIES, plain_one)
    );
    println!(
        "| `rerun`, unchanged, 1 thread | {:.1} | {:.0} | | |",
        micros(rerun_one, QUERIES),
        per_second(QUERIES, rerun_one)
    );
    println!(
        "| `query`, {THREADS} threads | | {:.0} | | {:.0} |",
        per_second(QUERIES, live_many),
        per_second(QUERIES, plain_many)
    );

    let rows = (ROOMS * 12) as usize;
    let started = Instant::now();
    for _ in 0..SCANS {
        black_box(db.query(SCAN, (), chat)?);
    }
    let live_scan = started.elapsed();
    let started = Instant::now();
    for _ in 0..SCANS {
        black_box(plain_query(&plain, SCAN, ())?);
    }
    let plain_scan = started.elapsed();
    println!("\n| Full scan of {rows} rows | SqLive rows/s | rusqlite rows/s |");
    println!("| --- | ---: | ---: |");
    println!(
        "| `SELECT * FROM chats` | {:.0} | {:.0} |",
        per_second(rows * SCANS, live_scan),
        per_second(rows * SCANS, plain_scan)
    );
    Ok(())
}
