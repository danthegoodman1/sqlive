//! The retained log of recent commit events, and the streams that read it.

use crate::error::{Error, Result};
use crate::event::{CommitEvent, Version};
use std::collections::VecDeque;
use std::sync::{Arc, Condvar, Mutex};

pub(crate) struct Log {
    state: Mutex<LogState>,
    published: Condvar,
}

struct LogState {
    /// The events after `base`, through `latest`.
    events: VecDeque<Arc<CommitEvent>>,
    base: Version,
    latest: Version,
    capacity: usize,
    /// Bumped when the log loses track of the database; every stream opened
    /// before then lags.
    epoch: u64,
    /// A commit is under way: its version may be visible to readers before
    /// it is published.
    committing: bool,
    closed: bool,
}

impl Log {
    pub fn new(latest: Version, capacity: usize) -> Log {
        Log {
            state: Mutex::new(LogState {
                events: VecDeque::new(),
                base: latest,
                latest,
                capacity: capacity.max(1),
                epoch: 0,
                committing: false,
                closed: false,
            }),
            published: Condvar::new(),
        }
    }

    pub fn latest(&self) -> Version {
        crate::lock(&self.state).latest
    }

    pub fn publish(&self, event: CommitEvent) {
        let mut state = crate::lock(&self.state);
        if event.version != state.latest + 1 {
            // The database moved without the log: no stream can continue.
            state.events.clear();
            state.base = event.version - 1;
            state.epoch += 1;
        }
        state.latest = event.version;
        state.events.push_back(Arc::new(event));
        while state.events.len() > state.capacity {
            state.events.pop_front();
            state.base += 1;
        }
        drop(state);
        self.published.notify_all();
    }

    /// Forgets every retained event and continues from `latest`, which the
    /// database reports. Every open stream lags.
    pub fn reset(&self, latest: Version) {
        let mut state = crate::lock(&self.state);
        state.events.clear();
        state.base = latest;
        state.latest = latest;
        state.epoch += 1;
        drop(state);
        self.published.notify_all();
    }

    /// Marks a commit under way until the guard drops, after the commit's
    /// event is published or abandoned.
    pub fn begin_commit(&self) -> Committing<'_> {
        crate::lock(&self.state).committing = true;
        Committing { log: self }
    }

    /// Brings the log up to `version`, which a reader saw in the database:
    /// waits for a commit under way to publish, then, if the database is
    /// still ahead, as after a commit that took effect without an event,
    /// moves the log to `version`. Every open stream then lags.
    pub fn catch_up(&self, version: Version) {
        let mut state = crate::lock(&self.state);
        while state.latest < version && state.committing {
            state = self
                .published
                .wait(state)
                .unwrap_or_else(|poisoned| poisoned.into_inner());
        }
        if version > state.latest {
            state.events.clear();
            state.base = version;
            state.latest = version;
            state.epoch += 1;
            drop(state);
            self.published.notify_all();
        }
    }

    pub fn close(&self) {
        crate::lock(&self.state).closed = true;
        self.published.notify_all();
    }

    /// A stream of the events after `since`.
    pub fn since(self: &Arc<Self>, since: Version) -> Result<Changes> {
        let state = crate::lock(&self.state);
        if since > state.latest {
            return Err(Error::FutureVersion {
                requested: since,
                latest: state.latest,
            });
        }
        Ok(Changes {
            log: self.clone(),
            next: since + 1,
            epoch: state.epoch,
            done: false,
        })
    }

    /// A stream of the events after the latest one.
    pub fn from_latest(self: &Arc<Self>) -> Changes {
        let state = crate::lock(&self.state);
        Changes {
            log: self.clone(),
            next: state.latest + 1,
            epoch: state.epoch,
            done: false,
        }
    }
}

/// Commit events in version order, from [`crate::LiveDb::changes`] or
/// [`crate::LiveDb::changes_since`].
///
/// Iterating blocks until the next event commits, and ends when the
/// database closes. A stream that falls behind the retained log yields
/// [`Error::Lagged`] once and then ends: query again and open a new stream
/// from the new result's version.
pub struct Changes {
    log: Arc<Log>,
    next: Version,
    epoch: u64,
    done: bool,
}

enum Poll {
    Ready(Result<Arc<CommitEvent>>),
    Pending,
    Closed,
}

/// A commit under way; see [`Log::begin_commit`].
pub(crate) struct Committing<'a> {
    log: &'a Log,
}

impl Drop for Committing<'_> {
    fn drop(&mut self) {
        crate::lock(&self.log.state).committing = false;
        self.log.published.notify_all();
    }
}

impl Changes {
    /// The version of the event this stream yields next.
    pub fn next_version(&self) -> Version {
        self.next
    }

    /// The next event if one has committed, without blocking.
    pub fn try_next(&mut self) -> Option<Result<Arc<CommitEvent>>> {
        if self.done {
            return None;
        }
        let log = self.log.clone();
        let state = crate::lock(&log.state);
        match self.poll(&state) {
            Poll::Ready(item) => Some(item),
            Poll::Pending | Poll::Closed => None,
        }
    }

    fn poll(&mut self, state: &LogState) -> Poll {
        if state.epoch != self.epoch || self.next <= state.base {
            self.done = true;
            return Poll::Ready(Err(Error::Lagged {
                requested: self.next - 1,
                oldest: state.base,
            }));
        }
        if self.next <= state.latest {
            let event = state.events[(self.next - state.base - 1) as usize].clone();
            self.next += 1;
            return Poll::Ready(Ok(event));
        }
        if state.closed {
            self.done = true;
            return Poll::Closed;
        }
        Poll::Pending
    }
}

impl Iterator for Changes {
    type Item = Result<Arc<CommitEvent>>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.done {
            return None;
        }
        let log = self.log.clone();
        let mut state = crate::lock(&log.state);
        loop {
            match self.poll(&state) {
                Poll::Ready(item) => return Some(item),
                Poll::Closed => return None,
                Poll::Pending => {
                    state = log
                        .published
                        .wait(state)
                        .unwrap_or_else(|poisoned| poisoned.into_inner());
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn event(version: Version) -> CommitEvent {
        CommitEvent {
            version,
            schema_changed: false,
            changes: Vec::new(),
        }
    }

    #[test]
    fn a_stream_yields_events_after_its_version_in_order() {
        let log = Arc::new(Log::new(10, 8));
        for v in 11..=14 {
            log.publish(event(v));
        }
        let mut stream = log.since(12).unwrap();
        assert_eq!(stream.try_next().unwrap().unwrap().version, 13);
        assert_eq!(stream.try_next().unwrap().unwrap().version, 14);
        assert!(stream.try_next().is_none());
        log.publish(event(15));
        assert_eq!(stream.try_next().unwrap().unwrap().version, 15);
    }

    #[test]
    fn a_stream_behind_the_log_lags_once() {
        let log = Arc::new(Log::new(0, 2));
        let mut stream = log.since(0).unwrap();
        for v in 1..=3 {
            log.publish(event(v));
        }
        assert!(matches!(
            stream.try_next(),
            Some(Err(Error::Lagged {
                requested: 0,
                oldest: 1
            }))
        ));
        assert!(stream.try_next().is_none());
        let mut fresh = log.since(1).unwrap();
        assert_eq!(fresh.try_next().unwrap().unwrap().version, 2);
    }

    #[test]
    fn a_reset_lags_every_open_stream() {
        let log = Arc::new(Log::new(5, 8));
        let mut stream = log.since(5).unwrap();
        log.reset(7);
        assert!(matches!(stream.try_next(), Some(Err(Error::Lagged { .. }))));
        let mut fresh = log.since(7).unwrap();
        log.publish(event(8));
        assert_eq!(fresh.try_next().unwrap().unwrap().version, 8);
    }

    #[test]
    fn a_future_version_is_an_error() {
        let log = Arc::new(Log::new(3, 8));
        assert!(matches!(
            log.since(4),
            Err(Error::FutureVersion {
                requested: 4,
                latest: 3
            })
        ));
    }

    #[test]
    fn iteration_ends_when_the_log_closes() {
        let log = Arc::new(Log::new(0, 8));
        let mut stream = log.from_latest();
        log.publish(event(1));
        log.close();
        assert_eq!(stream.next().unwrap().unwrap().version, 1);
        assert!(stream.next().is_none());
    }
}
