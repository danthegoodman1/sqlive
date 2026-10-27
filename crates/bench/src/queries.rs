//! Query latency for the chat query, on one thread and across several, and
//! full-scan throughput, through SqLive and through plain rusqlite. The
//! database is hot, every page in memory, except in the run that empties
//! the OS page cache first.

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

/// Pages the connection fetched since the last call: from SQLite's page
/// cache, and from the VFS, which reads the OS page cache or the drive.
fn pages(conn: &Connection) -> (i32, i32) {
    let counter = |op| {
        let (mut current, mut highest) = (0, 0);
        // SAFETY: reads and resets a counter of an open connection.
        unsafe {
            rusqlite::ffi::sqlite3_db_status(conn.handle(), op, &mut current, &mut highest, 1)
        };
        current
    };
    (
        counter(rusqlite::ffi::SQLITE_DBSTATUS_CACHE_HIT),
        counter(rusqlite::ffi::SQLITE_DBSTATUS_CACHE_MISS),
    )
}

/// Moves every page into the database file and drops the file from the OS
/// page cache, so the next reads come from the drive. Pages on tmpfs stay
/// in memory.
#[cfg(target_os = "linux")]
fn evict(path: &Path) -> sqlive::Result<()> {
    use std::os::fd::AsRawFd;
    unsafe extern "C" {
        fn posix_fadvise(fd: i32, offset: i64, len: i64, advice: i32) -> i32;
    }
    const POSIX_FADV_DONTNEED: i32 = 4;
    // Queries read pages left in the WAL from the WAL, and the OS drops
    // only pages already on the drive.
    Connection::open(path)?.execute_batch("PRAGMA wal_checkpoint(TRUNCATE)")?;
    let file = std::fs::File::open(path).expect("the database");
    file.sync_all().expect("the database syncs");
    // SAFETY: the descriptor stays open for the call.
    let status = unsafe { posix_fadvise(file.as_raw_fd(), 0, 0, POSIX_FADV_DONTNEED) };
    assert_eq!(status, 0, "posix_fadvise");
    Ok(())
}

/// The first query of each room, with the OS page cache emptied first and
/// fresh connections, through SqLive and through plain rusqlite.
#[cfg(target_os = "linux")]
fn cold(path: &Path) -> sqlive::Result<(Duration, Duration)> {
    let first_of_each = |query: &dyn Fn(usize)| {
        let started = Instant::now();
        (0..ROOMS as usize).for_each(query);
        started.elapsed()
    };
    evict(path)?;
    let db = LiveDb::open_with(
        path.to_string_lossy(),
        Options {
            readers: 1,
            ..Options::default()
        },
    )?;
    let live = first_of_each(&|i| {
        black_box(db.query(QUERY, [room(i)], chat).expect("a query"));
    });
    drop(db);
    evict(path)?;
    let plain = Connection::open(path)?;
    let plain = first_of_each(&|i| {
        black_box(plain_query(&plain, QUERY, [room(i)]).expect("a query"));
    });
    Ok((live, plain))
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
    // rusqlite's statement cache clears a statement's bindings as it takes
    // the statement back, which makes SQLite prepare this one again on its
    // next run; a statement the caller holds skips that.
    let mut held = plain.prepare(QUERY)?;
    pages(&plain);
    let plain_held = timed(|i| {
        let rows = held.query_map([room(i)], chat).expect("a query");
        black_box(rows.collect::<rusqlite::Result<Vec<Chat>>>().expect("rows"));
    });
    let (cached, fetched) = pages(&plain);
    drop(held);
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
        "| 1 thread, rusqlite holding its statement | | | {:.1} | {:.0} |",
        micros(plain_held, QUERIES),
        per_second(QUERIES, plain_held)
    );
    println!(
        "| `query`, {THREADS} threads | | {:.0} | | {:.0} |",
        per_second(QUERIES, live_many),
        per_second(QUERIES, plain_many)
    );
    println!(
        "\nThe {:.1} MB database is hot. Holding its statement, rusqlite fetched {:.1} \
         pages per query from SQLite's page cache and {:.1} from the OS.",
        std::fs::metadata(&path).expect("the database").len() as f64 / 1e6,
        f64::from(cached) / QUERIES as f64,
        f64::from(fetched) / QUERIES as f64
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

    #[cfg(target_os = "linux")]
    {
        drop(plain);
        drop(db);
        let (live_cold, plain_cold) = cold(&path)?;
        let rooms = ROOMS as usize;
        println!("\n| OS page cache emptied first | SqLive µs | rusqlite µs |");
        println!("| --- | ---: | ---: |");
        println!(
            "| `query`, first of each room, 1 thread | {:.1} | {:.1} |",
            micros(live_cold, rooms),
            micros(plain_cold, rooms)
        );
    }
    Ok(())
}
