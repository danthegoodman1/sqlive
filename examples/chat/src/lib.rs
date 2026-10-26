//! A chat server whose rooms update live. Each room shows the result of one
//! SqLive query; a follower re-runs it whenever a commit intersects the
//! query's read set, and pushes the new rows to everyone in the room when
//! their hash changed. A room keeps only the version and hash of the rows
//! its viewers hold.

use axum::Router;
use axum::extract::{Json, Path, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::sse::{Event, KeepAlive, Sse};
use axum::response::{Html, IntoResponse};
use axum::routing::{get, post};
use futures_util::Stream;
use serde::{Deserialize, Serialize};
use sqlive::rusqlite::Row;
use sqlive::{
    CommitEvent, Error, LiveDb, Options, QueryResult, ReadSet, ReadSetIndex, ResultHash, Version,
};
use std::collections::{BTreeMap, BTreeSet};
use std::convert::Infallible;
use std::sync::{Arc, Mutex, Weak};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::sync::{Notify, watch};

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

/// How soon a browser reconnects after its stream ends, as at shutdown.
const RECONNECT: Duration = Duration::from_secs(1);

/// The least time between a viewer's counter updates. Every commit changes
/// every room's counters, so each viewer gets the latest counts at this pace
/// instead of one event per commit.
const STATS_EVERY: Duration = Duration::from_millis(500);

/// A room's live query result.
#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct RoomResult {
    pub version: Version,
    /// Newest first, as the query orders them.
    pub chats: Vec<ChatRow>,
    pub read_set: String,
    /// The rows' hash, sent as the event's id. A reconnecting browser sends
    /// it back as `Last-Event-ID`.
    #[serde(skip)]
    pub hash: ResultHash,
}

/// The room's version, and what the follower did for the room while it
/// was open.
#[derive(Clone, Debug, Default, PartialEq, Serialize)]
pub struct RoomStats {
    /// The version of the room's latest query; the rows every viewer holds
    /// are current as of it.
    pub version: Version,
    /// Re-runs of the room's query, after a commit intersected its read set
    /// or the follower fell behind the log.
    pub reruns: u64,
    /// Re-runs whose rows hashed the same as the room's, so nothing was
    /// pushed.
    pub unchanged: u64,
    /// Commits that left the room's read set untouched, so nothing ran.
    pub skips: u64,
}

/// The newest result a viewer has yet to send. A newer result replaces an
/// unsent one, so a slow viewer skips to the latest rows and holds at most
/// one result.
#[derive(Default)]
struct Mailbox {
    slot: Mutex<Option<Arc<RoomResult>>>,
    ready: Notify,
}

impl Mailbox {
    fn put(&self, result: Arc<RoomResult>) {
        *lock(&self.slot) = Some(result);
        self.ready.notify_one();
    }

    fn take(&self) -> Option<Arc<RoomResult>> {
        lock(&self.slot).take()
    }

    async fn next(&self) -> Arc<RoomResult> {
        loop {
            if let Some(result) = self.take() {
                return result;
            }
            // `notify_one` leaves a permit when no one waits, so a result
            // put after the check above still wakes this wait.
            self.ready.notified().await;
        }
    }
}

struct Room {
    viewers: BTreeMap<u64, Arc<Mailbox>>,
    /// The version and hash of the rows every viewer holds.
    version: Version,
    hash: ResultHash,
    stats: Arc<watch::Sender<RoomStats>>,
}

/// The rooms someone is watching and the index of their read sets.
struct Hub {
    rooms: BTreeMap<String, Room>,
    index: ReadSetIndex<String>,
    /// Rooms whose last re-run failed; the follower retries them on the
    /// next commit.
    retry: BTreeSet<String>,
    next_viewer: u64,
}

pub struct Chat {
    db: Arc<LiveDb>,
    hub: Arc<Mutex<Hub>>,
    closing: watch::Sender<bool>,
}

/// One viewer of a room; the room closes when its last viewer drops.
pub struct Viewer {
    pub room: String,
    pub stats: watch::Receiver<RoomStats>,
    mailbox: Arc<Mailbox>,
    closing: watch::Receiver<bool>,
    id: u64,
    hub: Weak<Mutex<Hub>>,
}

impl Viewer {
    /// Waits for the room's next result. The first is the result the viewer
    /// joined with.
    pub async fn next_result(&self) -> Arc<RoomResult> {
        self.mailbox.next().await
    }

    /// The room's next result, if one is waiting.
    pub fn try_result(&self) -> Option<Arc<RoomResult>> {
        self.mailbox.take()
    }
}

impl Drop for Viewer {
    fn drop(&mut self) {
        if let Some(hub) = self.hub.upgrade() {
            let mut hub = lock(&hub);
            let close = hub.rooms.get_mut(&self.room).is_some_and(|room| {
                room.viewers.remove(&self.id);
                room.viewers.is_empty()
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

/// Splits a result into what viewers see and the read set the index keeps.
fn split(result: QueryResult<ChatRow>) -> (RoomResult, ReadSet) {
    let shown = RoomResult {
        version: result.version,
        chats: result.rows,
        read_set: result.read_set.to_string(),
        hash: result.hash,
    };
    (shown, result.read_set)
}

/// Hands a result to each of the room's viewers.
fn push(room: &Room, result: &Arc<RoomResult>) {
    for mailbox in room.viewers.values() {
        mailbox.put(result.clone());
    }
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
            next_viewer: 0,
        }));
        let changes = db.changes()?;
        let (weak_db, weak_hub) = (Arc::downgrade(&db), Arc::downgrade(&hub));
        std::thread::Builder::new()
            .name("chat-follower".to_owned())
            .spawn(move || follow(changes, weak_db, weak_hub))
            .expect("starting the follower thread");
        Ok(Chat {
            db,
            hub,
            closing: watch::Sender::new(false),
        })
    }

    /// Ends every live stream, as the server does before it exits; browsers
    /// then reconnect, to this server or the one that replaces it.
    pub fn close(&self) {
        self.closing.send_replace(true);
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

    /// Joins a room and queries it for the new viewer.
    pub fn watch(&self, room: &str) -> sqlive::Result<Viewer> {
        self.join(room, None)
    }

    /// Joins a room for a viewer who holds the result with hash `held`, as
    /// a reconnecting browser does. The viewer gets the room's rows only
    /// once they differ from the ones it holds.
    pub fn resume(&self, room: &str, held: ResultHash) -> sqlive::Result<Viewer> {
        self.join(room, Some(held))
    }

    fn join(&self, room: &str, held: Option<ResultHash>) -> sqlive::Result<Viewer> {
        // Querying under the hub's lock keeps the follower from checking a
        // commit between this query and the room joining the index.
        let mut hub = lock(&self.hub);
        let result = self.db.query(QUERY, [room], ChatRow::from_row)?;
        let (version, hash) = (result.version, result.hash);
        let (shown, read_set) = split(result);
        let shown = Arc::new(shown);
        hub.index.insert(room.to_owned(), read_set);
        let id = hub.next_viewer;
        hub.next_viewer += 1;
        let entry = hub.rooms.entry(room.to_owned()).or_insert_with(|| Room {
            viewers: BTreeMap::new(),
            version,
            hash,
            stats: Arc::new(watch::Sender::new(RoomStats {
                version,
                ..RoomStats::default()
            })),
        });
        // This query may see a commit the follower has not reached. The
        // room's viewers then get these rows too, so every viewer holds the
        // rows the room's hash describes.
        if entry.hash != hash {
            push(entry, &shown);
        }
        entry.version = version;
        entry.hash = hash;
        let mailbox = Arc::new(Mailbox::default());
        if held != Some(hash) {
            mailbox.put(shown);
        }
        entry.viewers.insert(id, mailbox.clone());
        let stats = entry.stats.clone();
        drop(hub);
        let receiver = stats.subscribe();
        // Counted outside the hub's lock, so a slow reader of the counters
        // holds up only this join; a newer version may have landed first.
        stats.send_if_modified(|stats| {
            let moved = version > stats.version;
            stats.version = stats.version.max(version);
            moved
        });
        Ok(Viewer {
            room: room.to_owned(),
            stats: receiver,
            mailbox,
            closing: self.closing.subscribe(),
            id,
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
        let (rooms, skipped) = match item {
            Ok(event) => affected(&mut lock(&hub), &event),
            Err(Error::Lagged { .. }) => {
                // The follower fell behind the log: start over from now and
                // query every room again.
                let Ok(fresh) = db.changes() else { return };
                changes = fresh;
                let mut hub = lock(&hub);
                hub.retry.clear();
                (hub.rooms.keys().cloned().collect(), Vec::new())
            }
            Err(_) => return,
        };
        // Counted outside the hub's lock, so a slow reader of the counters
        // holds up only the follower.
        for stats in skipped {
            stats.send_modify(|stats| stats.skips += 1);
        }
        for room in rooms {
            rerun(&db, &hub, &room);
        }
    }
}

/// The rooms to re-run, those the event affects and those whose last re-run
/// failed, and the counters of the rooms it leaves alone.
fn affected(
    hub: &mut Hub,
    event: &CommitEvent,
) -> (BTreeSet<String>, Vec<Arc<watch::Sender<RoomStats>>>) {
    let mut rooms: BTreeSet<String> = hub.index.affected(event).collect();
    rooms.append(&mut hub.retry);
    let skipped = hub
        .rooms
        .iter()
        .filter(|(name, _)| !rooms.contains(*name))
        .map(|(_, room)| room.stats.clone())
        .collect();
    (rooms, skipped)
}

/// Re-runs a room's query against the hash of the rows its viewers hold,
/// and pushes the rows only if they changed.
fn rerun(db: &LiveDb, hub: &Mutex<Hub>, room: &str) {
    loop {
        let Some(hash) = lock(hub).rooms.get(room).map(|entry| entry.hash) else {
            return;
        };
        // The query runs outside the hub's lock, so viewers join and leave
        // rooms meanwhile.
        let fresh = db.rerun(QUERY, [room], hash, ChatRow::from_row);
        let mut guard = lock(hub);
        let Hub {
            rooms,
            index,
            retry,
            ..
        } = &mut *guard;
        // The room may have closed during the query.
        let Some(entry) = rooms.get_mut(room) else {
            return;
        };
        let counted = match fresh {
            // A viewer who joined meanwhile read this version or a newer one.
            Ok(result) if result.version <= entry.version => None,
            // A viewer who joined meanwhile moved the room to other rows, and
            // an unchanged result has none to bring the others along.
            Ok(result) if result.unchanged && result.hash != entry.hash => continue,
            Ok(result) => {
                let QueryResult {
                    rows,
                    version,
                    read_set,
                    hash,
                    ..
                } = result;
                let changed = hash != entry.hash;
                if changed {
                    let shown = RoomResult {
                        version,
                        chats: rows,
                        read_set: read_set.to_string(),
                        hash,
                    };
                    push(entry, &Arc::new(shown));
                }
                index.insert(room.to_owned(), read_set);
                entry.version = version;
                entry.hash = hash;
                Some((entry.stats.clone(), version, changed))
            }
            Err(error) => {
                eprintln!("re-running room {room:?}: {error}");
                retry.insert(room.to_owned());
                None
            }
        };
        drop(guard);
        // Counted outside the hub's lock, so a slow reader of the counters
        // holds up only the follower; a newer version may have landed first.
        if let Some((stats, version, changed)) = counted {
            stats.send_modify(|stats| {
                stats.version = stats.version.max(version);
                stats.reruns += 1;
                stats.unchanged += u64::from(!changed);
            });
        }
        return;
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
        .id(result.hash.to_string())
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
    headers: HeaderMap,
) -> Result<impl IntoResponse, Failure> {
    check_room(&room)?;
    // A reconnecting browser names the last result it received.
    let held = headers
        .get("last-event-id")
        .and_then(|id| id.to_str().ok())
        .and_then(ResultHash::from_hex);
    let mut viewer = tokio::task::spawn_blocking(move || match held {
        Some(held) => chat.resume(&room, held),
        None => chat.watch(&room),
    })
    .await
    .map_err(server_error)?
    .map_err(server_error)?;
    let first = stats_event(&viewer.stats.borrow_and_update()).retry(RECONNECT);
    Ok(Sse::new(updates(first, viewer)).keep_alive(KeepAlive::default()))
}

/// The room's stats, then each result and the latest stats at most every
/// `STATS_EVERY`, until the chat closes or the client leaves and drops the
/// viewer.
fn updates(first: Event, viewer: Viewer) -> impl Stream<Item = Result<Event, Infallible>> {
    use futures_util::StreamExt;
    let next_stats = tokio::time::Instant::now() + STATS_EVERY;
    let later = futures_util::stream::unfold(
        (viewer, next_stats),
        |(mut viewer, mut next_stats)| async move {
            let Viewer {
                stats,
                mailbox,
                closing,
                ..
            } = &mut viewer;
            let event = tokio::select! {
                _ = closing.wait_for(|closing| *closing) => return None,
                result = mailbox.next() => result_event(&result),
                changed = async {
                    tokio::time::sleep_until(next_stats).await;
                    stats.changed().await
                } => {
                    changed.ok()?;
                    next_stats = tokio::time::Instant::now() + STATS_EVERY;
                    stats_event(&stats.borrow_and_update())
                }
            };
            Some((event, (viewer, next_stats)))
        },
    );
    futures_util::stream::iter([first]).chain(later).map(Ok)
}
