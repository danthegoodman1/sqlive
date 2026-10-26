//! Memory: the Rust heap each chat subscription's read set takes in a
//! `ReadSetIndex`, the heap each event takes in the retained log, and
//! SQLite's own heap.

use crate::{BODY, CHATS, QUERY, fill, fresh_dir, heap, sqlite_heap};
use sqlive::{LiveDb, ReadSetIndex};

const ROOMS: i64 = 10_000;
const SUBSCRIPTIONS: u32 = 100_000;
/// The default log capacity, so the log ends full.
const EVENTS: usize = 4_096;

fn megabytes(bytes: usize) -> f64 {
    bytes as f64 / (1024.0 * 1024.0)
}

/// A field of `/proc/self/status`, in kilobytes.
fn status_kb(field: &str) -> Option<usize> {
    let status = std::fs::read_to_string("/proc/self/status").ok()?;
    let line = status.lines().find(|line| line.starts_with(field))?;
    line.split_whitespace().nth(1)?.parse().ok()
}

pub fn run() -> sqlive::Result<()> {
    let dir = fresh_dir("memory");
    let db = LiveDb::open(dir.join("bench.db").to_string_lossy())?;
    db.write(|tx| {
        tx.execute_batch(CHATS)?;
        fill(tx, ROOMS)
    })?;
    // Warm the reader's caches, so they count toward neither figure.
    db.query(QUERY, [0], |_| Ok(()))?;

    let before = heap();
    let mut held = Vec::with_capacity(SUBSCRIPTIONS as usize);
    for subscription in 0..SUBSCRIPTIONS {
        let room = i64::from(subscription) % ROOMS;
        held.push(db.query(QUERY, [room], |_| Ok(()))?.read_set);
    }
    let read_set_bytes = heap() - before;
    drop(held);

    let before = heap();
    let mut index = ReadSetIndex::new();
    for subscription in 0..SUBSCRIPTIONS {
        let room = i64::from(subscription) % ROOMS;
        let result = db.query(QUERY, [room], |_| Ok(()))?;
        index.insert(subscription, result.read_set);
    }
    let index_bytes = heap() - before;

    let before = heap();
    for i in 0..EVENTS {
        db.write(|tx| {
            tx.execute(
                "INSERT INTO chats (room, created_at, body) VALUES (?1, ?2, ?3)",
                (i as i64 % ROOMS, 100 + i as i64, BODY),
            )
        })?;
    }
    let log_bytes = heap() - before;

    println!("\n## memory\n");
    println!("| Held | Count | Rust heap | Per item |");
    println!("| --- | ---: | ---: | ---: |");
    println!(
        "| Chat-query read sets, held alone | {SUBSCRIPTIONS} | {:.1} MB | {} bytes |",
        megabytes(read_set_bytes),
        read_set_bytes / SUBSCRIPTIONS as usize
    );
    println!(
        "| Chat-query read sets in a `ReadSetIndex` | {SUBSCRIPTIONS} | {:.1} MB | {} bytes |",
        megabytes(index_bytes),
        index_bytes / SUBSCRIPTIONS as usize
    );
    println!(
        "| Retained events, each inserting one chat | {EVENTS} | {:.1} MB | {} bytes |",
        megabytes(log_bytes),
        log_bytes / EVENTS
    );
    println!(
        "\nSQLite's heap: {:.1} MB. Process RSS: {:.0} MB (peak {:.0} MB).",
        megabytes(sqlite_heap() as usize),
        status_kb("VmRSS:").unwrap_or(0) as f64 / 1024.0,
        status_kb("VmHWM:").unwrap_or(0) as f64 / 1024.0
    );
    drop(index);
    Ok(())
}
