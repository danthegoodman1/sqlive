//! Times `ReadSetIndex::affected` against testing every read set, for
//! 100,000 subscriptions to the chat query over 10,000 rooms.
//!
//!     cargo run --release -p sqlive-bench

use sqlive::{CommitEvent, LiveDb, ReadSet, ReadSetIndex};
use std::time::{Duration, Instant};

const ROOMS: i64 = 10_000;
const SUBSCRIPTIONS: u32 = 100_000;
const EVENTS: i64 = 2_000;
const QUERY: &str = "SELECT * FROM chats WHERE room = ?1 ORDER BY created_at DESC LIMIT 10";

fn per_second(count: usize, elapsed: Duration) -> f64 {
    count as f64 / elapsed.as_secs_f64()
}

fn main() -> sqlive::Result<()> {
    let dir = std::env::temp_dir().join(format!("sqlive-bench-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).expect("a temporary directory");
    let db = LiveDb::open(dir.join("bench.db").to_string_lossy())?;
    db.write(|tx| {
        tx.execute_batch(
            "CREATE TABLE chats (id INTEGER PRIMARY KEY, room INTEGER, created_at INTEGER, body TEXT);
             CREATE INDEX chats_by_room ON chats (room, created_at);",
        )?;
        // Twelve chats per room, so each room's window of ten is full.
        let mut insert =
            tx.prepare("INSERT INTO chats (room, created_at, body) VALUES (?1, ?2, 'hello')")?;
        for room in 0..ROOMS {
            for created_at in 0..12 {
                insert.execute((room, created_at))?;
            }
        }
        Ok(())
    })?;

    let started = Instant::now();
    let mut index = ReadSetIndex::new();
    let mut read_sets: Vec<ReadSet> = Vec::new();
    for subscription in 0..SUBSCRIPTIONS {
        let room = i64::from(subscription) % ROOMS;
        let read_set = db.query(QUERY, [room], |_| Ok(()))?.read_set;
        index.insert(subscription, read_set.clone());
        read_sets.push(read_set);
    }
    let built = started.elapsed();

    let mut events: Vec<CommitEvent> = Vec::new();
    for i in 0..EVENTS {
        let room = (i * 7919) % ROOMS;
        let ((), version) = db.write(|tx| {
            tx.execute(
                "INSERT INTO chats (room, created_at, body) VALUES (?1, ?2, 'new')",
                (room, 100 + i),
            )
            .map(|_| ())
        })?;
        let event = db
            .changes_since(version - 1)?
            .try_next()
            .expect("committed")?;
        events.push(event.as_ref().clone());
    }

    let started = Instant::now();
    let indexed: usize = events
        .iter()
        .map(|event| index.affected(event).count())
        .sum();
    let with_index = started.elapsed();
    let started = Instant::now();
    let tested: usize = events
        .iter()
        .map(|event| read_sets.iter().filter(|r| r.intersects(event)).count())
        .sum();
    let without_index = started.elapsed();
    assert_eq!(indexed, tested, "the index and brute force disagree");

    println!("{SUBSCRIPTIONS} subscriptions over {ROOMS} rooms, built in {built:.2?}");
    println!(
        "index:       {:>10.0} events/s ({} affected per event)",
        per_second(events.len(), with_index),
        indexed / events.len()
    );
    println!(
        "brute force: {:>10.0} events/s",
        per_second(events.len(), without_index)
    );
    let _ = std::fs::remove_dir_all(&dir);
    Ok(())
}
