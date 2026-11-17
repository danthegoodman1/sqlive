//! `ReadSetIndex::affected` against testing every read set, for 100,000
//! subscriptions to the chat query over 10,000 rooms: for events that
//! insert one chat, and for events that insert a chat into each of many
//! rooms.

use crate::{CHATS, QUERY, fill, fresh_dir, per_second};
use sqlive::{CommitEvent, LiveDb, ReadSet, ReadSetIndex};
use std::sync::Arc;
use std::time::Instant;

const ROOMS: i64 = 10_000;
const SUBSCRIPTIONS: u32 = 100_000;
const EVENTS: i64 = 2_000;
/// Rooms one batch event inserts a chat into.
const BATCHES: [i64; 4] = [1, 100, 1_000, 5_000];
/// The changes each batch size is measured over: as many events as total
/// this many, and two at least, each looked up once.
const LOOKUPS: i64 = 2_000;

pub fn run() -> sqlive::Result<()> {
    let dir = fresh_dir("index");
    let db = LiveDb::open(dir.join("bench.db").to_string_lossy())?;
    db.write(|tx| {
        tx.execute_batch(CHATS)?;
        fill(tx, ROOMS)
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

    println!(
        "\n## index: {SUBSCRIPTIONS} subscriptions over {ROOMS} rooms, built in {built:.2?}\n"
    );
    println!("| Method | Events per second | Affected per event |");
    println!("| --- | ---: | ---: |");
    println!(
        "| `ReadSetIndex::affected` | {:.0} | {} |",
        per_second(events.len(), with_index),
        indexed / events.len()
    );
    println!(
        "| `intersects` on every read set | {:.0} | {} |",
        per_second(events.len(), without_index),
        tested / events.len()
    );

    println!(
        "\n| Changes in one event | Events | µs per event | µs per change | Affected per event |"
    );
    println!("| ---: | ---: | ---: | ---: | ---: |");
    let mut next_room = 0;
    for rooms in BATCHES {
        // The events walk the rooms, so each lookup reaches read sets the
        // one before it left alone.
        let mut batches: Vec<Arc<CommitEvent>> = Vec::new();
        for _ in 0..(LOOKUPS / rooms).max(2) {
            let ((), version) = db.write(|tx| {
                let mut insert = tx.prepare(
                    "INSERT INTO chats (room, created_at, body) VALUES (?1, 1000000, 'batch')",
                )?;
                for _ in 0..rooms {
                    insert.execute([(next_room * 7919) % ROOMS])?;
                    next_room += 1;
                }
                Ok(())
            })?;
            let event = db
                .changes_since(version - 1)?
                .try_next()
                .expect("committed")?;
            assert_eq!(event.changes.len() as i64, rooms);
            batches.push(event);
        }
        let started = Instant::now();
        let affected: usize = batches
            .iter()
            .map(|event| index.affected(event).count())
            .sum();
        let micros = started.elapsed().as_secs_f64() * 1e6 / batches.len() as f64;
        let tested = read_sets
            .iter()
            .filter(|r| r.intersects(&batches[0]))
            .count();
        assert_eq!(
            index.affected(&batches[0]).count(),
            tested,
            "the index and brute force disagree"
        );
        println!(
            "| {rooms} | {} | {micros:.1} | {:.2} | {} |",
            batches.len(),
            micros / rooms as f64,
            affected / batches.len()
        );
    }
    Ok(())
}
