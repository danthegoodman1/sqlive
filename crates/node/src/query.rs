//! Queries: synchronous ones on the JavaScript thread, and asynchronous ones
//! that read and pack their rows on libuv's thread pool.

use crate::errors::{Fail, Result, closed, raise};
use crate::events::Raw;
use crate::read_set::{self, Key, NativeIndex};
use crate::rows::{self, Packed};
use crate::values::{Integers, check, params, string};
use napi::bindgen_prelude::*;
use napi::sys;
use napi_derive::napi;
use sqlive::rusqlite;
use sqlive::{LiveDb, ParamValues, QueryResult, ResultHash, Snapshot};
use std::ptr;
use std::sync::Arc;

pub(crate) fn previous_hash(previous: Option<String>) -> Result<Option<ResultHash>> {
    previous
        .map(|hex| {
            ResultHash::from_hex(&hex)
                .ok_or_else(|| Fail::Type("a result hash is 64 hexadecimal digits".to_owned()))
        })
        .transpose()
}

/// Where a query runs: a fresh snapshot of the database, or one the caller
/// holds.
pub(crate) enum Source<'a> {
    Db(&'a LiveDb),
    Snapshot(&'a Snapshot<'static>),
}

type Map<'m> = &'m mut dyn FnMut(&rusqlite::Row<'_>) -> rusqlite::Result<()>;

impl Source<'_> {
    fn run(
        &self,
        sql: &str,
        params: ParamValues,
        previous: Option<ResultHash>,
        map: Map<'_>,
    ) -> sqlive::Result<QueryResult<()>> {
        match (self, previous) {
            (Source::Db(db), None) => db.query(sql, params, map),
            (Source::Db(db), Some(hash)) => db.rerun(sql, params, hash, map),
            (Source::Snapshot(snapshot), None) => snapshot.query(sql, params, map),
            (Source::Snapshot(snapshot), Some(hash)) => snapshot.rerun(sql, params, hash, map),
        }
    }
}

/// Runs a query on this thread, packing each row as SQLite returns it.
pub(crate) unsafe fn query_sync(
    env: sys::napi_env,
    source: Source<'_>,
    integers: Integers,
    sql: &str,
    js_params: sys::napi_value,
    previous: Option<String>,
    scratch: sys::napi_value,
) -> Result<sys::napi_value> {
    unsafe {
        let (rows, result) = run_sync(env, source, integers, sql, js_params, previous)?;
        let scratch = rows::scratch(env, scratch)?;
        result_array(env, rows, scratch, result, None)
    }
}

/// Runs a query on this thread and packs its rows, dropping them when the
/// result is unchanged.
pub(crate) unsafe fn run_sync(
    env: sys::napi_env,
    source: Source<'_>,
    integers: Integers,
    sql: &str,
    js_params: sys::napi_value,
    previous: Option<String>,
) -> Result<(Packed, QueryResult<()>)> {
    let values = unsafe { params(env, sql, js_params)? };
    let previous = previous_hash(previous)?;
    let mut rows = Packed::new(integers);
    let result = source.run(sql, values, previous, &mut |row| rows.push(row))?;
    if result.unchanged {
        rows.clear();
    }
    Ok((rows, result))
}

/// Lowercase hex, written without the formatting machinery.
fn hex(hash: &ResultHash) -> String {
    const DIGITS: &[u8; 16] = b"0123456789abcdef";
    let mut out = String::with_capacity(64);
    for byte in hash.as_bytes() {
        out.push(char::from(DIGITS[usize::from(byte >> 4)]));
        out.push(char::from(DIGITS[usize::from(byte & 15)]));
    }
    out
}

/// `[buffer, chunks, columns, blobs, hash, readSet]`, which `index.js`
/// turns into a result object; the buffer's header holds the version and
/// whether the result is unchanged. With `into`, the read set goes into
/// that index under that key instead, and `readSet` is `undefined`.
pub(crate) unsafe fn result_array(
    env: sys::napi_env,
    rows: Packed,
    scratch: Option<(*mut u8, usize)>,
    result: QueryResult<()>,
    into: Option<(&mut NativeIndex, f64)>,
) -> Result<sys::napi_value> {
    unsafe {
        let extra = [result.version as f64, f64::from(u8::from(result.unchanged))];
        let [buffer, chunks, key, blobs] = rows.finish(env, scratch, extra)?;
        let read_set = match into {
            Some((index, key)) => {
                index.inner.insert(Key::Number(key), result.read_set);
                crate::values::undefined(env)?
            }
            None => read_set::to_js(env, result.read_set)?,
        };
        let parts = [
            buffer,
            chunks,
            key,
            blobs,
            string(env, &hex(&result.hash))?,
            read_set,
        ];
        let mut array = ptr::null_mut();
        check(sys::napi_create_array_with_length(
            env,
            parts.len(),
            &mut array,
        ))?;
        for (i, part) in parts.into_iter().enumerate() {
            check(sys::napi_set_element(env, array, i as u32, part))?;
        }
        Ok(array)
    }
}

/// A query that reads on libuv's thread pool.
pub struct QueryTask {
    pub(crate) db: Arc<LiveDb>,
    pub(crate) sql: String,
    pub(crate) params: Option<ParamValues>,
    pub(crate) previous: Option<ResultHash>,
    pub(crate) integers: Integers,
    /// The query's error, which converts on the JavaScript thread.
    pub(crate) failure: Option<sqlive::Error>,
}

impl Task for QueryTask {
    type Output = (Packed, QueryResult<()>);
    type JsValue = Raw;

    fn compute(&mut self) -> napi::Result<Self::Output> {
        let mut rows = Packed::new(self.integers);
        let params = self.params.take().unwrap_or_default();
        let result =
            Source::Db(&self.db).run(&self.sql, params, self.previous, &mut |row| rows.push(row));
        match result {
            Ok(result) => {
                if result.unchanged {
                    rows.clear();
                }
                Ok((rows, result))
            }
            Err(error) => {
                self.failure = Some(error);
                Err(napi::Error::from_reason("the query failed"))
            }
        }
    }

    fn resolve(&mut self, env: Env, (rows, result): Self::Output) -> napi::Result<Raw> {
        let env = env.raw();
        unsafe {
            result_array(env, rows, None, result, None)
                .map(Raw)
                .map_err(|fail| raise(env, fail))
        }
    }

    fn reject(&mut self, env: Env, error: napi::Error) -> napi::Result<Raw> {
        match self.failure.take() {
            Some(failure) => Err(raise(env.raw(), Fail::Live(failure))),
            None => Err(error),
        }
    }
}

/// A read transaction on one version of the database.
#[napi]
pub struct NativeSnapshot {
    /// Dropped before `db`, which it borrows.
    snapshot: Option<Snapshot<'static>>,
    db: Arc<LiveDb>,
    integers: Integers,
}

impl NativeSnapshot {
    pub(crate) fn open(db: Arc<LiveDb>, integers: Integers) -> Result<NativeSnapshot> {
        let snapshot = db.snapshot()?;
        // SAFETY: the snapshot borrows the database `db` keeps alive, and
        // drops first.
        let snapshot = unsafe { std::mem::transmute::<Snapshot<'_>, Snapshot<'static>>(snapshot) };
        Ok(NativeSnapshot {
            snapshot: Some(snapshot),
            db,
            integers,
        })
    }
}

#[napi]
impl NativeSnapshot {
    #[napi(getter)]
    pub fn version(&self) -> napi::Result<f64> {
        match &self.snapshot {
            Some(snapshot) => Ok(snapshot.version() as f64),
            None => Ok(f64::NAN),
        }
    }

    #[napi]
    pub fn query(
        &self,
        env: &Env,
        sql: String,
        params: Unknown<'_>,
        previous: Option<String>,
        scratch: Unknown<'_>,
    ) -> napi::Result<Raw> {
        let env = env.raw();
        let Some(snapshot) = &self.snapshot else {
            return Err(raise(
                env,
                Fail::State(c"ERR_SQLIVE_CLOSED", "the snapshot is closed".to_owned()),
            ));
        };
        let _ = &self.db;
        unsafe {
            query_sync(
                env,
                Source::Snapshot(snapshot),
                self.integers,
                &sql,
                params.raw(),
                previous,
                scratch.raw(),
            )
            .map(Raw)
            .map_err(|fail| raise(env, fail))
        }
    }

    /// Ends the read transaction and returns its connection to the pool.
    #[napi]
    pub fn close(&mut self) {
        self.snapshot = None;
    }
}

pub(crate) fn open_db(db: &Option<Arc<LiveDb>>) -> Result<&Arc<LiveDb>> {
    db.as_ref().ok_or_else(closed)
}
