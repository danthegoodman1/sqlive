//! The database. Reads run on the calling thread or on libuv's thread
//! pool. An asynchronous write's callback runs on the JavaScript thread,
//! which uses the transaction while a writer thread, one per database,
//! waits for it to finish and then commits, so the commit's fsync never
//! blocks the event loop.

use crate::errors::{Fail, Result, closed, raise};
use crate::events::{ChangeStream, Raw};
use crate::query::{
    NativeSnapshot, QueryTask, Source, open_db, previous_hash, query_sync, result_array, run_sync,
};
use crate::read_set::NativeIndex;
use crate::rows::Packed;
use crate::values::{Integers, check, integer, number, undefined};
use napi::bindgen_prelude::*;
use napi::sys;
use napi_derive::napi;
use sqlive::rusqlite::{self, Statement, Transaction};
use sqlive::{LiveDb, Options, ParamValues, Version};
use std::cell::{Cell, RefCell};
use std::ffi::c_void;
use std::ptr;
use std::sync::atomic::{AtomicBool, AtomicPtr, AtomicU32, Ordering};
use std::sync::{Arc, Condvar, Mutex, mpsc};
use std::time::Duration;

/// How `new LiveDb` opens a database; every field is optional.
#[napi(object)]
pub struct OpenOptions {
    pub readers: Option<u32>,
    pub log_capacity: Option<f64>,
    pub log_bytes: Option<f64>,
    pub table_changes: Option<f64>,
    /// Milliseconds.
    pub busy_timeout: Option<f64>,
    pub checkpoint_pages: Option<u32>,
    pub vfs: Option<String>,
    pub safe_integers: Option<bool>,
}

/// A count option: a non-negative integer, or `Infinity` for no bound.
fn count(value: Option<f64>, default: usize, name: &str) -> Result<usize> {
    match value {
        None => Ok(default),
        Some(f64::INFINITY) => Ok(usize::MAX),
        Some(v) if v >= 0.0 && v.fract() == 0.0 && v < 2f64.powi(53) => Ok(v as usize),
        Some(v) => Err(Fail::Range(format!(
            "{name} must be a non-negative integer or Infinity, not {v}"
        ))),
    }
}

fn version_arg(version: f64) -> Result<Version> {
    if version >= 0.0 && version.fract() == 0.0 && version < 2f64.powi(53) {
        Ok(version as Version)
    } else {
        Err(Fail::Range(format!(
            "a version is a non-negative integer, not {version}"
        )))
    }
}

/// The transaction a write's callback may use, shared with the writer
/// thread.
struct Slot {
    /// The open `Transaction`, null outside a write's callback.
    tx: AtomicPtr<c_void>,
    /// The write whose callback may use `tx`; 0 outside one.
    job: AtomicU32,
    /// Set when the callback finishes: whether it succeeded.
    gate: Mutex<Option<bool>>,
    finished: Condvar,
    /// The writer's statement cache has room for a write's statements.
    sized: AtomicBool,
}

impl Slot {
    fn open(&self, job: u32, tx: &Transaction<'_>) {
        if !self.sized.swap(true, Ordering::Relaxed) {
            tx.set_prepared_statement_cache_capacity(256);
        }
        self.tx
            .store(ptr::from_ref(tx).cast_mut().cast(), Ordering::Release);
        self.job.store(job, Ordering::Release);
    }

    fn close(&self) {
        self.job.store(0, Ordering::Release);
        self.tx.store(ptr::null_mut(), Ordering::Release);
    }

    /// Ends the open callback, if any, so its writer commits or rolls back.
    fn finish(&self, succeeded: bool) {
        self.close();
        *crate::lock(&self.gate) = Some(succeeded);
        self.finished.notify_one();
    }
}

enum Job {
    Write(u32),
    Vacuum(u32),
}

/// What the writer thread tells the JavaScript thread.
enum Message {
    /// The write's transaction is open: run its callback.
    Begin(u32),
    /// The write or vacuum finished.
    Done(u32, sqlive::Result<Version>),
}

/// A thread-safe function, which Node-API lets any thread call and release.
struct Tsfn(sys::napi_threadsafe_function);

// SAFETY: Node-API's thread-safe functions may be called and released from
// any thread while the environment lives.
unsafe impl Send for Tsfn {}

impl Drop for Tsfn {
    fn drop(&mut self) {
        unsafe {
            sys::napi_release_threadsafe_function(
                self.0,
                sys::ThreadsafeFunctionReleaseMode::release,
            )
        };
    }
}

/// What the writer thread, the JavaScript thread and the environment's
/// cleanup hook share. Each thread holds its own count on the dispatcher
/// and releases it once done with it. Node destroys a thread-safe function
/// as its environment tears down, so the cleanup hook, which runs first,
/// releases both counts and takes the dispatcher away from the writer
/// thread.
struct Writer {
    slot: Arc<Slot>,
    /// The writer thread's count, released when the thread ends.
    dispatch: Mutex<Option<Tsfn>>,
    /// The JavaScript thread's count, released when the database is
    /// collected; while it holds, that thread may refer and unrefer the
    /// dispatcher.
    js: Mutex<Option<Tsfn>>,
    /// The writer thread's queue; dropping it ends the thread once the
    /// queue drains.
    jobs: Mutex<Option<mpsc::Sender<Job>>>,
    /// Set once the cleanup hook has run, which freed its reference.
    /// Node reports success for removing a hook that already ran.
    torn_down: AtomicBool,
}

impl Writer {
    /// Queues `message` for the JavaScript thread; false once the
    /// environment is tearing down.
    fn send(&self, message: Message) -> bool {
        let dispatch = crate::lock(&self.dispatch);
        let Some(tsfn) = dispatch.as_ref() else {
            return false;
        };
        let data = Box::into_raw(Box::new(message));
        let status = unsafe {
            sys::napi_call_threadsafe_function(
                tsfn.0,
                data.cast(),
                sys::ThreadsafeFunctionCallMode::nonblocking,
            )
        };
        if status != sys::Status::napi_ok {
            drop(unsafe { Box::from_raw(data) });
            return false;
        }
        true
    }

    /// Stops every use of the environment: the queue closes, the dispatcher
    /// is released, and a write whose callback can no longer finish rolls
    /// back.
    fn shut(&self) {
        crate::lock(&self.jobs).take();
        drop(crate::lock(&self.dispatch).take());
        drop(crate::lock(&self.js).take());
        if self.slot.job.load(Ordering::Acquire) != 0 {
            self.slot.finish(false);
        }
    }
}

/// Runs as the environment tears down, as when Node exits or a worker
/// terminates, before Node destroys the dispatcher.
unsafe extern "C" fn teardown(writer: *mut c_void) {
    let writer = unsafe { Arc::from_raw(writer.cast_const().cast::<Writer>()) };
    writer.torn_down.store(true, Ordering::Release);
    writer.shut();
}

/// Calls `dispatch(kind, job, payload)` on the JavaScript thread: kind 0
/// opens a write's callback, 1 resolves a job with its version, and 2
/// rejects it with an error.
unsafe extern "C" fn call_js(
    env: sys::napi_env,
    dispatch: sys::napi_value,
    _context: *mut c_void,
    data: *mut c_void,
) {
    let message = unsafe { Box::from_raw(data.cast::<Message>()) };
    if env.is_null() || dispatch.is_null() {
        return;
    }
    unsafe {
        let args = match *message {
            Message::Begin(job) => [
                number(env, 0.0),
                number(env, f64::from(job)),
                undefined(env),
            ],
            Message::Done(job, Ok(version)) => [
                number(env, 1.0),
                number(env, f64::from(job)),
                number(env, version as f64),
            ],
            // Without its error object, the job still settles.
            Message::Done(job, Err(error)) => [
                number(env, 2.0),
                number(env, f64::from(job)),
                crate::errors::build(env, Fail::Live(error)).or_else(|_| undefined(env)),
            ],
        };
        let Ok(args) = args.into_iter().collect::<Result<Vec<_>>>() else {
            return;
        };
        let Ok(this) = undefined(env) else { return };
        let mut result = ptr::null_mut();
        // An exception the dispatcher throws reaches `uncaughtException`.
        sys::napi_call_function(env, this, dispatch, args.len(), args.as_ptr(), &mut result);
    }
}

/// Stands for a write callback that threw, whose error JavaScript holds.
fn callback_failed() -> rusqlite::Error {
    rusqlite::Error::ToSqlConversionFailure("the write callback failed".into())
}

/// Runs queued jobs until the queue closes, then releases the dispatcher.
/// The release happens here, on this thread: from the JavaScript thread it
/// could run inside Node's own teardown of the dispatcher, which holds the
/// lock a release takes.
fn run_writer(db: Arc<LiveDb>, writer: Arc<Writer>, jobs: mpsc::Receiver<Job>) {
    for job in jobs {
        let id = match job {
            Job::Write(id) | Job::Vacuum(id) => id,
        };
        let run = std::panic::AssertUnwindSafe(|| run_job(&db, &writer, job));
        let message = std::panic::catch_unwind(run).unwrap_or_else(|_| {
            writer.slot.close();
            Message::Done(
                id,
                Err(sqlive::Error::Unsupported(
                    "the writer thread panicked".to_owned(),
                )),
            )
        });
        writer.send(message);
    }
    drop(crate::lock(&writer.dispatch).take());
}

fn run_job(db: &LiveDb, writer: &Writer, job: Job) -> Message {
    let slot = &writer.slot;
    match job {
        Job::Write(id) => {
            let outcome = db.write(|tx| {
                *crate::lock(&slot.gate) = None;
                slot.open(id, tx);
                if !writer.send(Message::Begin(id)) {
                    slot.close();
                    return Err(callback_failed());
                }
                let mut gate = crate::lock(&slot.gate);
                let succeeded = loop {
                    match *gate {
                        Some(succeeded) => break succeeded,
                        None => {
                            gate = slot
                                .finished
                                .wait(gate)
                                .unwrap_or_else(|poisoned| poisoned.into_inner());
                        }
                    }
                };
                if succeeded {
                    Ok(())
                } else {
                    Err(callback_failed())
                }
            });
            Message::Done(id, outcome.map(|((), version)| version))
        }
        Job::Vacuum(id) => Message::Done(id, db.vacuum()),
    }
}

/// Calls a JavaScript function with no arguments, keeping what it throws.
unsafe fn call(env: sys::napi_env, function: sys::napi_value) -> napi::Result<()> {
    unsafe {
        let mut this = ptr::null_mut();
        sys::napi_get_undefined(env, &mut this);
        let mut result = ptr::null_mut();
        let status = sys::napi_call_function(env, this, function, 0, ptr::null(), &mut result);
        if status == sys::Status::napi_pending_exception {
            let mut thrown = ptr::null_mut();
            sys::napi_get_and_clear_last_exception(env, &mut thrown);
            return Err(napi::Error::from(Unknown::from_raw_unchecked(env, thrown)));
        }
        if status != sys::Status::napi_ok {
            return Err(napi::Error::new(
                napi::Status::from(status),
                "calling the write callback failed",
            ));
        }
        Ok(())
    }
}

/// Binds every value `values` holds to a statement whose parameters are all
/// unbound, as a cached statement's are.
fn bind(statement: &mut Statement<'_>, values: &ParamValues) -> rusqlite::Result<()> {
    let expected = statement.parameter_count();
    if values.positional.len() > expected
        || (values.named.is_empty() && values.positional.len() != expected)
    {
        return Err(rusqlite::Error::InvalidParameterCount(
            values.positional.len(),
            expected,
        ));
    }
    for (i, value) in values.positional.iter().enumerate() {
        statement.raw_bind_parameter(i + 1, value)?;
    }
    for (name, value) in &values.named {
        let index = statement
            .parameter_index(name)?
            .ok_or_else(|| rusqlite::Error::InvalidParameterName(name.clone()))?;
        statement.raw_bind_parameter(index, value)?;
    }
    Ok(())
}

#[napi]
pub struct NativeDb {
    db: RefCell<Option<Arc<LiveDb>>>,
    path: String,
    integers: Integers,
    slot: Arc<Slot>,
    /// The writer thread, started by the first asynchronous write.
    writer: RefCell<Option<WriterHandle>>,
    /// Asynchronous writes and vacuums queued and not yet settled in
    /// JavaScript.
    outstanding: Cell<usize>,
}

struct WriterHandle {
    env: sys::napi_env,
    writer: Arc<Writer>,
    /// The dispatcher, which this thread refers and unrefers while
    /// `writer.js` holds its count.
    dispatch: sys::napi_threadsafe_function,
    /// The dispatcher keeps the event loop alive while jobs are outstanding.
    referenced: bool,
    /// The strong reference the cleanup hook owns while registered.
    hook: *const Writer,
}

impl WriterHandle {
    /// Whether this thread may still refer and unrefer the dispatcher.
    fn live(&self) -> bool {
        crate::lock(&self.writer.js).is_some()
    }
}

impl Drop for WriterHandle {
    /// Closes the queue, releases this thread's count on the dispatcher, and
    /// unregisters the cleanup hook. Teardown may have run the hook
    /// already, which then released both counts.
    fn drop(&mut self) {
        crate::lock(&self.writer.jobs).take();
        drop(crate::lock(&self.writer.js).take());
        if self.writer.torn_down.load(Ordering::Acquire) {
            return;
        }
        unsafe {
            sys::napi_remove_env_cleanup_hook(
                self.env,
                Some(teardown),
                self.hook.cast_mut().cast(),
            );
            drop(Arc::from_raw(self.hook));
        }
    }
}

impl NativeDb {
    fn db(&self) -> Result<Arc<LiveDb>> {
        open_db(&self.db.borrow()).cloned()
    }

    /// The open transaction of write `job`, as its callback sees it.
    fn tx(&self, job: u32) -> Result<&Transaction<'static>> {
        let tx = self.slot.tx.load(Ordering::Acquire);
        if job == 0 || self.slot.job.load(Ordering::Acquire) != job || tx.is_null() {
            return Err(Fail::State(
                c"ERR_SQLIVE_TRANSACTION_ENDED",
                "this transaction has ended; use the tx its write passed to the callback"
                    .to_owned(),
            ));
        }
        // SAFETY: the writer that opened the transaction waits until the
        // callback finishes, and `close` clears the pointer first.
        Ok(unsafe { &*tx.cast::<Transaction<'static>>() })
    }

    fn queue(&self, env: &Env, job: Job, dispatch: Unknown<'_>) -> Result<()> {
        let db = self.db()?;
        let mut writer = self.writer.borrow_mut();
        if writer.is_none() {
            *writer = Some(start_writer(env, db, self.slot.clone(), dispatch)?);
        }
        let Some(handle) = writer.as_mut() else {
            return Err(closed());
        };
        let sent = crate::lock(&handle.writer.jobs)
            .as_ref()
            .is_some_and(|jobs| jobs.send(job).is_ok());
        if !sent {
            return Err(closed());
        }
        self.outstanding.set(self.outstanding.get() + 1);
        if !handle.referenced && handle.live() {
            check(unsafe { sys::napi_ref_threadsafe_function(env.raw(), handle.dispatch) })?;
            handle.referenced = true;
        }
        Ok(())
    }
}

/// Creates the dispatcher, owned by a new writer thread, and leaves it
/// unreferenced so an idle database lets the event loop exit.
fn start_writer(
    env: &Env,
    db: Arc<LiveDb>,
    slot: Arc<Slot>,
    dispatch: Unknown<'_>,
) -> Result<WriterHandle> {
    let raw = env.raw();
    let mut tsfn = ptr::null_mut();
    unsafe {
        let name = crate::values::string(raw, "sqlive write")?;
        check(sys::napi_create_threadsafe_function(
            raw,
            dispatch.raw(),
            ptr::null_mut(),
            name,
            0,
            1,
            ptr::null_mut(),
            None,
            ptr::null_mut(),
            Some(call_js),
            &mut tsfn,
        ))?;
        check(sys::napi_unref_threadsafe_function(raw, tsfn))?;
    }
    // The creation's count is this thread's; the writer thread takes a
    // second.
    let js = Tsfn(tsfn);
    check(unsafe { sys::napi_acquire_threadsafe_function(tsfn) })?;
    let (jobs, receive) = mpsc::channel();
    let writer = Arc::new(Writer {
        slot,
        dispatch: Mutex::new(Some(Tsfn(tsfn))),
        js: Mutex::new(Some(js)),
        jobs: Mutex::new(Some(jobs)),
        torn_down: AtomicBool::new(false),
    });
    // Registered after the dispatcher, so it runs before Node destroys it:
    // cleanup hooks run newest first.
    let hook = Arc::into_raw(writer.clone());
    if let Err(fail) = check(unsafe {
        sys::napi_add_env_cleanup_hook(raw, Some(teardown), hook.cast_mut().cast())
    }) {
        drop(unsafe { Arc::from_raw(hook) });
        writer.shut();
        return Err(fail);
    }
    let handle = WriterHandle {
        env: raw,
        writer: writer.clone(),
        dispatch: tsfn,
        referenced: false,
        hook,
    };
    std::thread::Builder::new()
        .name("sqlive-writer".to_owned())
        .spawn(move || run_writer(db, writer, receive))
        .map_err(|error| {
            Fail::State(
                c"ERR_SQLIVE_WRITER",
                format!("the writer thread did not start: {error}"),
            )
        })?;
    Ok(handle)
}

#[napi]
impl NativeDb {
    #[napi(constructor)]
    pub fn new(env: &Env, path: String, options: Option<OpenOptions>) -> napi::Result<NativeDb> {
        Self::open(path, options).map_err(|fail| raise(env.raw(), fail))
    }

    #[napi(getter)]
    pub fn path(&self) -> String {
        self.path.clone()
    }

    #[napi(getter)]
    pub fn version(&self, env: &Env) -> napi::Result<f64> {
        self.db()
            .map(|db| db.version() as f64)
            .map_err(|fail| raise(env.raw(), fail))
    }

    #[napi(getter)]
    pub fn closed(&self) -> bool {
        self.db.borrow().is_none()
    }

    /// Closes the database once its queued writes finish.
    #[napi]
    pub fn close(&self) {
        self.db.borrow_mut().take();
        // The writer thread finishes the queued jobs; the handle stays to
        // let the event loop go once they settle.
        if let Some(handle) = self.writer.borrow().as_ref() {
            crate::lock(&handle.writer.jobs).take();
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
        self.db()
            .and_then(|db| unsafe {
                query_sync(
                    env,
                    Source::Db(&db),
                    self.integers,
                    &sql,
                    params.raw(),
                    previous,
                    scratch.raw(),
                )
            })
            .map(Raw)
            .map_err(|fail| raise(env, fail))
    }

    /// Runs a query, or re-runs it given `previous`, and stores its read set
    /// in `index` under `key`, as a subscription does.
    #[allow(clippy::too_many_arguments)]
    #[napi]
    pub fn query_into(
        &self,
        env: &Env,
        index: &mut NativeIndex,
        key: f64,
        sql: String,
        params: Unknown<'_>,
        previous: Option<String>,
        scratch: Unknown<'_>,
    ) -> napi::Result<Raw> {
        let raw = env.raw();
        (|| unsafe {
            let db = self.db()?;
            let (rows, result) = run_sync(
                raw,
                Source::Db(&db),
                self.integers,
                &sql,
                params.raw(),
                previous,
            )?;
            let scratch = crate::rows::scratch(raw, scratch.raw())?;
            result_array(raw, rows, scratch, result, Some((index, key)))
        })()
        .map(Raw)
        .map_err(|fail| raise(raw, fail))
    }

    #[napi(ts_return_type = "Promise<unknown>")]
    pub fn query_async(
        &self,
        env: &Env,
        sql: String,
        params: Unknown<'_>,
        previous: Option<String>,
    ) -> napi::Result<AsyncTask<QueryTask>> {
        let raw = env.raw();
        (|| {
            Ok(AsyncTask::new(QueryTask {
                db: self.db()?,
                params: Some(unsafe { crate::values::params(raw, &sql, params.raw())? }),
                sql,
                previous: previous_hash(previous)?,
                integers: self.integers,
                failure: None,
            }))
        })()
        .map_err(|fail| raise(raw, fail))
    }

    #[napi]
    pub fn snapshot(&self, env: &Env) -> napi::Result<NativeSnapshot> {
        self.db()
            .and_then(|db| NativeSnapshot::open(db, self.integers))
            .map_err(|fail| raise(env.raw(), fail))
    }

    #[napi]
    pub fn changes(&self, env: &Env) -> napi::Result<ChangeStream> {
        self.db()
            .and_then(|db| Ok(db.changes()?))
            .map(|inner| ChangeStream {
                inner,
                integers: self.integers,
            })
            .map_err(|fail| raise(env.raw(), fail))
    }

    #[napi]
    pub fn changes_since(&self, env: &Env, version: f64) -> napi::Result<ChangeStream> {
        (|| Ok(self.db()?.changes_since(version_arg(version)?)?))()
            .map(|inner| ChangeStream {
                inner,
                integers: self.integers,
            })
            .map_err(|fail: Fail| raise(env.raw(), fail))
    }

    /// Queues write `job`. `dispatch(kind, job, payload)` opens its callback
    /// and later settles it; the first queued job's `dispatch` serves them
    /// all.
    #[napi]
    pub fn queue_write(&self, env: &Env, job: u32, dispatch: Unknown<'_>) -> napi::Result<()> {
        self.queue(env, Job::Write(job), dispatch)
            .map_err(|fail| raise(env.raw(), fail))
    }

    #[napi]
    pub fn queue_vacuum(&self, env: &Env, job: u32, dispatch: Unknown<'_>) -> napi::Result<()> {
        self.queue(env, Job::Vacuum(job), dispatch)
            .map_err(|fail| raise(env.raw(), fail))
    }

    /// Ends write `job`'s callback; the writer commits if it succeeded and
    /// rolls back otherwise.
    #[napi]
    pub fn finish_write(&self, job: u32, succeeded: bool) {
        if self.slot.job.load(Ordering::Acquire) == job {
            self.slot.finish(succeeded);
        }
    }

    /// Records that JavaScript settled a job, and lets the event loop exit
    /// once none is outstanding.
    #[napi]
    pub fn settled(&self, env: &Env) -> napi::Result<()> {
        self.outstanding
            .set(self.outstanding.get().saturating_sub(1));
        let mut writer = self.writer.borrow_mut();
        if let Some(handle) = writer.as_mut()
            && handle.referenced
            && self.outstanding.get() == 0
            && handle.live()
        {
            check(unsafe { sys::napi_unref_threadsafe_function(env.raw(), handle.dispatch) })
                .map_err(|fail| raise(env.raw(), fail))?;
            handle.referenced = false;
        }
        Ok(())
    }

    /// Runs write `job` on this thread, calling `callback` inside the
    /// transaction.
    #[napi]
    pub fn write_sync(&self, env: &Env, job: u32, callback: Unknown<'_>) -> napi::Result<f64> {
        let raw = env.raw();
        if self.outstanding.get() != 0 || self.slot.job.load(Ordering::Acquire) != 0 {
            return Err(raise(
                raw,
                Fail::State(
                    c"ERR_SQLIVE_WRITE_PENDING",
                    "writeSync cannot run inside another write or while an asynchronous write or vacuum is pending; await it first".to_owned(),
                ),
            ));
        }
        let db = self.db().map_err(|fail| raise(raw, fail))?;
        let mut thrown = None;
        let outcome = db.write(|tx| {
            self.slot.open(job, tx);
            let called = unsafe { call(raw, callback.raw()) };
            self.slot.close();
            called.map_err(|error| {
                thrown = Some(error);
                callback_failed()
            })
        });
        match (outcome, thrown) {
            (_, Some(thrown)) => Err(thrown),
            (Ok(((), version)), None) => Ok(version as f64),
            (Err(error), None) => Err(raise(raw, error.into())),
        }
    }

    /// Runs one statement in write `job`'s transaction, stepping through any
    /// rows it returns.
    #[napi]
    pub fn tx_run(
        &self,
        env: &Env,
        job: u32,
        sql: String,
        params: Unknown<'_>,
    ) -> napi::Result<Raw> {
        let raw = env.raw();
        (|| unsafe {
            let tx = self.tx(job)?;
            let values = crate::values::params(raw, &sql, params.raw())?;
            let mut statement = tx.prepare_cached(&sql)?;
            bind(&mut statement, &values)?;
            let mut rows = statement.raw_query();
            while rows.next()?.is_some() {}
            drop(rows);
            let mut object = ptr::null_mut();
            check(sys::napi_create_object(raw, &mut object))?;
            check(sys::napi_set_named_property(
                raw,
                object,
                c"changes".as_ptr(),
                number(raw, tx.changes() as f64)?,
            ))?;
            check(sys::napi_set_named_property(
                raw,
                object,
                c"lastInsertRowid".as_ptr(),
                integer(raw, tx.last_insert_rowid(), self.integers)?,
            ))?;
            Ok(object)
        })()
        .map(Raw)
        .map_err(|fail| raise(raw, fail))
    }

    /// Runs a query in write `job`'s transaction, with at most `limit` rows:
    /// `[buffer, chunks, columns, blobs]`, as a result's rows come.
    #[napi]
    pub fn tx_query(
        &self,
        env: &Env,
        job: u32,
        sql: String,
        params: Unknown<'_>,
        limit: Option<u32>,
        scratch: Unknown<'_>,
    ) -> napi::Result<Raw> {
        let raw = env.raw();
        (|| unsafe {
            let tx = self.tx(job)?;
            let values = crate::values::params(raw, &sql, params.raw())?;
            let mut statement = tx.prepare_cached(&sql)?;
            bind(&mut statement, &values)?;
            let mut packed = Packed::new(self.integers);
            let mut rows = statement.raw_query();
            let mut left = limit.unwrap_or(u32::MAX);
            while left > 0 {
                let Some(row) = rows.next()? else { break };
                packed.push(row)?;
                left -= 1;
            }
            drop(rows);
            let parts = packed.finish(raw, crate::rows::scratch(raw, scratch.raw())?, [0.0; 2])?;
            let mut array = ptr::null_mut();
            check(sys::napi_create_array_with_length(
                raw,
                parts.len(),
                &mut array,
            ))?;
            for (i, part) in parts.into_iter().enumerate() {
                check(sys::napi_set_element(raw, array, i as u32, part))?;
            }
            Ok(array)
        })()
        .map(Raw)
        .map_err(|fail| raise(raw, fail))
    }

    /// Runs a script of statements in write `job`'s transaction.
    #[napi]
    pub fn tx_exec(&self, env: &Env, job: u32, sql: String) -> napi::Result<()> {
        (|| Ok(self.tx(job)?.execute_batch(&sql)?))().map_err(|fail: Fail| raise(env.raw(), fail))
    }
}

impl NativeDb {
    fn open(path: String, options: Option<OpenOptions>) -> Result<NativeDb> {
        let defaults = Options::default();
        let options = options.unwrap_or(OpenOptions {
            readers: None,
            log_capacity: None,
            log_bytes: None,
            table_changes: None,
            busy_timeout: None,
            checkpoint_pages: None,
            vfs: None,
            safe_integers: None,
        });
        let busy_timeout = match options.busy_timeout {
            None => defaults.busy_timeout,
            Some(ms) if let Ok(timeout) = Duration::try_from_secs_f64(ms / 1000.0) => timeout,
            Some(ms) => {
                return Err(Fail::Range(format!(
                    "busyTimeout must be a non-negative number of milliseconds, not {ms}"
                )));
            }
        };
        let live = Options {
            vfs: options.vfs,
            readers: options.readers.map_or(defaults.readers, |n| n as usize),
            log_capacity: count(options.log_capacity, defaults.log_capacity, "logCapacity")?,
            log_bytes: count(options.log_bytes, defaults.log_bytes, "logBytes")?,
            table_changes: count(
                options.table_changes,
                defaults.table_changes,
                "tableChanges",
            )?,
            busy_timeout,
            checkpoint_pages: options
                .checkpoint_pages
                .unwrap_or(defaults.checkpoint_pages),
        };
        let db = LiveDb::open_with(&path, live)?;
        Ok(NativeDb {
            db: RefCell::new(Some(Arc::new(db))),
            path,
            integers: Integers {
                big: options.safe_integers.unwrap_or(false),
            },
            slot: Arc::new(Slot {
                tx: AtomicPtr::new(ptr::null_mut()),
                job: AtomicU32::new(0),
                gate: Mutex::new(None),
                finished: Condvar::new(),
                sized: AtomicBool::new(false),
            }),
            writer: RefCell::new(None),
            outstanding: Cell::new(0),
        })
    }
}
