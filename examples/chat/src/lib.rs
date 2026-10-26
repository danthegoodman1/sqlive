//! A chat server whose rooms update live. Each room shows the result of one
//! SqLive query; a follower re-runs it whenever a commit intersects the
//! query's read set, and pushes the new rows to everyone in the room.

use axum::Router;
use axum::extract::{Json, Path, State};
use axum::http::StatusCode;
use axum::response::sse::{Event, KeepAlive, Sse};
use axum::response::{Html, IntoResponse};
use axum::routing::{get, post};
use futures_util::Stream;
use serde::{Deserialize, Serialize};
use sqlive::rusqlite::Row;
use sqlive::{CommitEvent, Error, LiveDb, Options, ReadSetIndex, Version};
use std::collections::{BTreeMap, BTreeSet};
use std::convert::Infallible;
use std::sync::{Arc, Mutex, Weak};
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::sync::watch;

/// The live query each room shows.
pub const QUERY: &str = "SELECT * FROM chats WHERE room = ?1 ORDER BY created_at DESC LIMIT 10";

const SCHEMA: &str = "
    CREATE TABLE IF NOT EXISTS chats (
        id INTEGER PRIMARY KEY,
        room TEXT NOT NULL,
        author TEXT NOT NULL,
        body TEXT NOT NULL,
        created_at INTEGER NOT NULL
    );
    CREATE UNIQUE INDEX IF NOT EXISTS chats_by_room ON chats (room, created_at);
";

#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct ChatRow {
    pub id: i64,
    pub room: String,
    pub author: String,
    pub body: String,
    /// Milliseconds since the Unix epoch, unique within the room.
    pub created_at: i64,
}

impl ChatRow {
    fn from_row(row: &Row<'_>) -> sqlive::rusqlite::Result<ChatRow> {
        Ok(ChatRow {
            id: row.get("id")?,
            room: row.get("room")?,
            author: row.get("author")?,
            body: row.get("body")?,
            created_at: row.get("created_at")?,
        })
    }
}

/// A room's live query result.
#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct RoomResult {
    pub version: Version,
    /// Newest first, as the query orders them.
    pub chats: Vec<ChatRow>,
    pub read_set: String,
}

/// What the follower did for a room while it was open.
#[derive(Clone, Debug, Default, PartialEq, Serialize)]
pub struct RoomStats {
    /// Re-runs of the room's query that pushed a newer result, after a commit
    /// intersected its read set or the follower fell behind the log.
    pub reruns: u64,
    /// Commits that left the room's read set untouched, so nothing ran.
    pub skips: u64,
}

struct Room {
    viewers: usize,
    results: watch::Sender<RoomResult>,
    stats: watch::Sender<RoomStats>,
}

/// The rooms someone is watching and the index of their read sets.
struct Hub {
    rooms: BTreeMap<String, Room>,
    index: ReadSetIndex<String>,
    /// Rooms whose last re-run failed; the follower retries them on the
    /// next commit.
    retry: BTreeSet<String>,
}

pub struct Chat {
    db: Arc<LiveDb>,
    hub: Arc<Mutex<Hub>>,
}

/// One viewer of a room; the room closes when its last viewer drops.
pub struct Viewer {
    pub room: String,
    pub results: watch::Receiver<RoomResult>,
    pub stats: watch::Receiver<RoomStats>,
    hub: Weak<Mutex<Hub>>,
}

impl Drop for Viewer {
    fn drop(&mut self) {
        if let Some(hub) = self.hub.upgrade() {
            let mut hub = lock(&hub);
            let close = hub.rooms.get_mut(&self.room).is_some_and(|room| {
                room.viewers -= 1;
                room.viewers == 0
            });
            if close {
                hub.rooms.remove(&self.room);
                hub.index.remove(&self.room);
            }
        }
    }
}

fn lock<T>(mutex: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    mutex
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

fn query(db: &LiveDb, room: &str) -> sqlive::Result<(RoomResult, sqlive::ReadSet)> {
    let result = db.query(QUERY, [room], ChatRow::from_row)?;
    let room_result = RoomResult {
        version: result.version,
        chats: result.rows,
        read_set: result.read_set.to_string(),
    };
    Ok((room_result, result.read_set))
}

impl Chat {
    /// Opens the database at `path`, creating the schema, and starts the
    /// follower thread, which ends when the database closes.
    pub fn open(path: &str) -> sqlive::Result<Chat> {
        Chat::open_with(path, Options::default())
    }

    pub fn open_with(path: &str, options: Options) -> sqlive::Result<Chat> {
        let db = Arc::new(LiveDb::open_with(path, options)?);
        db.write(|tx| tx.execute_batch(SCHEMA))?;
        let hub = Arc::new(Mutex::new(Hub {
            rooms: BTreeMap::new(),
            index: ReadSetIndex::new(),
            retry: BTreeSet::new(),
        }));
        let changes = db.changes()?;
        let (weak_db, weak_hub) = (Arc::downgrade(&db), Arc::downgrade(&hub));
        std::thread::Builder::new()
            .name("chat-follower".to_owned())
            .spawn(move || follow(changes, weak_db, weak_hub))
            .expect("starting the follower thread");
        Ok(Chat { db, hub })
    }

    /// The database, for writes the page does not make.
    pub fn db(&self) -> &LiveDb {
        &self.db
    }

    /// Posts a chat and returns its commit version.
    pub fn post(&self, room: &str, author: &str, body: &str) -> sqlive::Result<Version> {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_or(0, |d| d.as_millis() as i64);
        let ((), version) = self.db.write(|tx| {
            // One past the room's newest chat when the clock has not moved
            // past it, so every chat sorts after the ones before it.
            tx.execute(
                "INSERT INTO chats (room, author, body, created_at)
                 VALUES (?1, ?2, ?3, max(?4, (SELECT coalesce(max(created_at), 0) + 1 FROM chats WHERE room = ?1)))",
                (room, author, body, now),
            )
            .map(|_| ())
        })?;
        Ok(version)
    }

    /// Joins a room, querying it if no one is watching it yet.
    pub fn watch(&self, room: &str) -> sqlive::Result<Viewer> {
        // Querying under the hub's lock keeps the follower from checking a
        // commit between this query and the room joining the index.
        let mut hub = lock(&self.hub);
        if !hub.rooms.contains_key(room) {
            let (result, read_set) = query(&self.db, room)?;
            hub.index.insert(room.to_owned(), read_set);
            hub.rooms.insert(
                room.to_owned(),
                Room {
                    viewers: 0,
                    results: watch::Sender::new(result),
                    stats: watch::Sender::new(RoomStats::default()),
                },
            );
        }
        let entry = hub.rooms.get_mut(room).expect("just inserted");
        entry.viewers += 1;
        Ok(Viewer {
            room: room.to_owned(),
            results: entry.results.subscribe(),
            stats: entry.stats.subscribe(),
            hub: Arc::downgrade(&self.hub),
        })
    }
}

/// Checks every commit against the rooms' read sets and re-runs the
/// queries it intersects. Ends when the database closes.
fn follow(mut changes: sqlive::Changes, db: Weak<LiveDb>, hub: Weak<Mutex<Hub>>) {
    while let Some(item) = changes.next() {
        let (Some(db), Some(hub)) = (db.upgrade(), hub.upgrade()) else {
            return;
        };
        let rooms = match item {
            Ok(event) => affected(&mut lock(&hub), &event),
            Err(Error::Lagged { .. }) => {
                // The follower fell behind the log: start over from now and
                // query every room again.
                let Ok(fresh) = db.changes() else { return };
                changes = fresh;
                let mut hub = lock(&hub);
                hub.retry.clear();
                hub.rooms.keys().cloned().collect()
            }
            Err(_) => return,
        };
        // The queries run outside the hub's lock, so viewers join and leave
        // rooms meanwhile.
        for room in rooms {
            rerun(&db, &hub, &room);
        }
    }
}

/// Counts a skip for each room the event leaves alone, and returns the rooms
/// to re-run: those it affects, and those whose last re-run failed.
fn affected(hub: &mut Hub, event: &CommitEvent) -> BTreeSet<String> {
    let mut rooms: BTreeSet<String> = hub.index.affected(event).collect();
    rooms.append(&mut hub.retry);
    for (name, room) in &hub.rooms {
        if !rooms.contains(name) {
            room.stats.send_modify(|stats| stats.skips += 1);
        }
    }
    rooms
}

fn rerun(db: &LiveDb, hub: &Mutex<Hub>, room: &str) {
    let fresh = query(db, room);
    let mut hub = lock(hub);
    let Hub {
        rooms,
        index,
        retry,
    } = &mut *hub;
    // The room may have closed during the query.
    let Some(entry) = rooms.get(room) else {
        return;
    };
    let shown = entry.results.borrow().version;
    match fresh {
        // The room may also have closed and reopened with a newer result.
        Ok((result, read_set)) if result.version > shown => {
            index.insert(room.to_owned(), read_set);
            entry.results.send_replace(result);
            entry.stats.send_modify(|stats| stats.reruns += 1);
        }
        Ok(_) => {}
        Err(error) => {
            eprintln!("re-running room {room:?}: {error}");
            retry.insert(room.to_owned());
        }
    }
}

/// The HTTP routes: the page, posting a chat, and a room's live result as
/// server-sent events.
pub fn router(chat: Arc<Chat>) -> Router {
    Router::new()
        .route("/", get(page))
        .route("/rooms/{room}/chats", post(post_chat))
        .route("/rooms/{room}/live", get(live))
        .with_state(chat)
}

async fn page() -> Html<&'static str> {
    Html(include_str!("../static/index.html"))
}

#[derive(Deserialize)]
struct NewChat {
    author: String,
    body: String,
}

#[derive(Serialize)]
struct Posted {
    version: Version,
}

type Failure = (StatusCode, String);

/// Whether `text` has 1 to `most` characters. The page's `maxlength` counts
/// UTF-16 units, at least one per character, so what it accepts fits.
fn fits(text: &str, most: usize) -> bool {
    !text.is_empty() && text.chars().count() <= most
}

fn check_room(room: &str) -> Result<(), Failure> {
    if room.trim().is_empty() || !fits(room, 64) {
        return Err((
            StatusCode::BAD_REQUEST,
            "a room id has 1 to 64 characters".to_owned(),
        ));
    }
    Ok(())
}

fn server_error(error: impl std::fmt::Display) -> Failure {
    (StatusCode::INTERNAL_SERVER_ERROR, error.to_string())
}

async fn post_chat(
    State(chat): State<Arc<Chat>>,
    Path(room): Path<String>,
    Json(new): Json<NewChat>,
) -> Result<Json<Posted>, Failure> {
    check_room(&room)?;
    let (author, body) = (new.author.trim().to_owned(), new.body.trim().to_owned());
    if !fits(&author, 40) || !fits(&body, 1000) {
        return Err((
            StatusCode::BAD_REQUEST,
            "a chat needs a name of 1 to 40 characters and a message of 1 to 1000".to_owned(),
        ));
    }
    let version = tokio::task::spawn_blocking(move || chat.post(&room, &author, &body))
        .await
        .map_err(server_error)?
        .map_err(server_error)?;
    Ok(Json(Posted { version }))
}

fn result_event(result: &RoomResult) -> Event {
    Event::default()
        .event("result")
        .json_data(result)
        .expect("a result serializes")
}

fn stats_event(stats: &RoomStats) -> Event {
    Event::default()
        .event("stats")
        .json_data(stats)
        .expect("stats serialize")
}

async fn live(
    State(chat): State<Arc<Chat>>,
    Path(room): Path<String>,
) -> Result<impl IntoResponse, Failure> {
    check_room(&room)?;
    let mut viewer = tokio::task::spawn_blocking(move || chat.watch(&room))
        .await
        .map_err(server_error)?
        .map_err(server_error)?;
    let first = [
        result_event(&viewer.results.borrow_and_update()),
        stats_event(&viewer.stats.borrow_and_update()),
    ];
    Ok(Sse::new(updates(first, viewer)).keep_alive(KeepAlive::default()))
}

/// The room's current result and stats, then each change to either, until
/// the room closes or the client leaves and drops the viewer.
fn updates(first: [Event; 2], viewer: Viewer) -> impl Stream<Item = Result<Event, Infallible>> {
    use futures_util::StreamExt;
    let later = futures_util::stream::unfold(viewer, |mut viewer| async move {
        let event = tokio::select! {
            changed = viewer.results.changed() => {
                changed.ok()?;
                result_event(&viewer.results.borrow_and_update())
            }
            changed = viewer.stats.changed() => {
                changed.ok()?;
                stats_event(&viewer.stats.borrow_and_update())
            }
        };
        Some((event, viewer))
    });
    futures_util::stream::iter(first).chain(later).map(Ok)
}
