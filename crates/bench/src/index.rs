//! `ReadSetIndex::affected` against testing every read set, for 100,000
//! subscriptions to the chat query over 10,000 rooms.

use crate::{CHATS, QUERY, fill, fresh_dir, per_second};
use sqlive::{CommitEvent, LiveDb, ReadSet, ReadSetIndex};
use std::time::Instant;

const ROOMS: i64 = 10_000;
const SUBSCRIPTIONS: u32 = 100_000;
const EVENTS: i64 = 2_000;

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
    Ok(())
}
