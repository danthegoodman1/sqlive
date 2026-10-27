//! A deterministic in-memory VFS that fails and crashes like a real disk.
//!
//! Each file holds the bytes reads see and the bytes a crash keeps for sure.
//! Writes and truncations since the file's last sync wait in a pending list;
//! a sync makes them durable. [`SimVfs::finish_crash`] replays the pending
//! list sector by sector, keeping each sector with a seeded probability, which
//! produces lost and torn writes but never touches bytes outside a write, so
//! the VFS may claim `SQLITE_IOCAP_POWERSAFE_OVERWRITE`. Reads, writes, syncs
//! and lock requests fail with seeded probabilities.
//!
//! Locks follow SQLite's five levels across the connections of one VFS, and
//! the `xShm*` methods share WAL-index regions between them, so WAL mode works
//! with a writer and several readers. A handle opened read-only takes only
//! shared locks and never writes, like a read-only file descriptor, so a
//! read-only connection that closes last leaves the WAL in place. File
//! creation and deletion take effect at once and survive crashes.
//!
//! Time comes from a simulated clock that only `xSleep` advances, and
//! `xRandomness` from the seed. SQLite seeds its own PRNG, which picks WAL
//! salts, from the default VFS instead; [`crate::Simulator::run`] reseeds it
//! from the run's seed, so a single run replays byte for byte. Runs that share
//! a process in a parallel sweep share that PRNG, which changes salt bytes but
//! never a run's logical outcome.

use crate::prng::Prng;
use rusqlite::ffi;
use std::collections::{BTreeMap, BTreeSet};
use std::ffi::{CStr, CString, c_char, c_int, c_void};
use std::ptr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Mutex, MutexGuard};

/// Probabilities of each injected fault, per operation.
#[derive(Clone, Debug)]
pub struct FaultConfig {
    pub read_error: f64,
    pub write_error: f64,
    pub sync_error: f64,
    /// A lock request answered `SQLITE_BUSY` although no one holds the lock.
    pub lock_busy: f64,
    /// The granularity at which a crash tears a write.
    pub sector_size: u64,
    /// The probability that an unsynced sector write survives a crash.
    pub crash_keep_sector: f64,
    pub powersafe_overwrite: bool,
}

impl FaultConfig {
    pub fn none() -> FaultConfig {
        FaultConfig {
            read_error: 0.0,
            write_error: 0.0,
            sync_error: 0.0,
            lock_busy: 0.0,
            sector_size: 512,
            crash_keep_sector: 0.5,
            powersafe_overwrite: true,
        }
    }

    /// Draws a configuration: about half the seeds inject errors.
    pub fn from_prng(prng: &mut Prng) -> FaultConfig {
        let mut faults = FaultConfig {
            sector_size: *prng.pick(&[512, 4096]),
            crash_keep_sector: *prng.pick(&[0.0, 0.5, 0.9, 1.0]),
            powersafe_overwrite: prng.chance(0.8),
            ..FaultConfig::none()
        };
        if prng.chance(0.5) {
            faults.read_error = *prng.pick(&[0.002, 0.02]);
        }
        if prng.chance(0.5) {
            faults.write_error = *prng.pick(&[0.002, 0.02]);
            faults.sync_error = *prng.pick(&[0.01, 0.05]);
        }
        if prng.chance(0.3) {
            faults.lock_busy = *prng.pick(&[0.01, 0.05]);
        }
        faults
    }
}

#[derive(Clone, Debug, Default)]
pub struct IoStats {
    pub reads: u64,
    pub writes: u64,
    pub syncs: u64,
    pub read_errors: u64,
    pub write_errors: u64,
    pub sync_errors: u64,
    pub busy: u64,
    pub crashes: u64,
    pub sectors_lost: u64,
}

enum Pending {
    Write { offset: usize, data: Vec<u8> },
    Truncate(usize),
}

#[derive(Default)]
struct FileData {
    current: Vec<u8>,
    durable: Vec<u8>,
    pending: Vec<Pending>,
}

#[derive(Default)]
struct FileLock {
    shared: BTreeSet<u64>,
    reserved: Option<u64>,
    pending: Option<u64>,
    exclusive: Option<u64>,
}

const SHM_LOCKS: usize = ffi::SQLITE_SHM_NLOCK as usize;

#[derive(Default)]
struct Shm {
    /// Boxed so a region stays put while SQLite holds pointers into it.
    regions: Vec<Box<[u8]>>,
    mappers: BTreeSet<u64>,
    shared: [BTreeSet<u64>; SHM_LOCKS],
    exclusive: [Option<u64>; SHM_LOCKS],
}

struct State {
    faults: FaultConfig,
    faults_enabled: bool,
    fault_prng: Prng,
    crash_prng: Prng,
    random: Prng,
    clock_us: i64,
    /// Handles opened before the latest crash belong to earlier generations;
    /// every operation on them fails.
    generation: u64,
    /// Between `begin_crash` and `finish_crash`, nothing reaches the files.
    frozen: bool,
    next_handle: u64,
    /// Syncs still to fail on purpose, for scripted scenarios.
    failing_syncs: u32,
    /// Reads left before one fails, whatever the fault configuration says.
    reads_until_failure: Option<u64>,
    files: BTreeMap<String, FileData>,
    temp: BTreeMap<u64, Vec<u8>>,
    locks: BTreeMap<String, FileLock>,
    /// WAL indexes by handle generation, then database file.
    shm: BTreeMap<u64, BTreeMap<String, Shm>>,
    stats: IoStats,
}

impl State {
    fn fault(&mut self, probability: f64) -> bool {
        self.faults_enabled && self.fault_prng.chance(probability)
    }
}

struct Inner {
    vfs: ffi::sqlite3_vfs,
    name: CString,
    state: Mutex<State>,
}

/// A registered VFS. Every connection that uses it must close before it drops.
pub struct SimVfs {
    inner: Box<Inner>,
}

static NEXT_VFS: AtomicU64 = AtomicU64::new(0);

/// Turns off SQLite's memory statistics before it initializes: they take a
/// process-wide mutex on every allocation, which serializes parallel sweeps.
/// Registering a VFS initializes SQLite, so [`SimVfs::new`] calls this first.
pub fn configure_sqlite() {
    static ONCE: std::sync::Once = std::sync::Once::new();
    ONCE.call_once(|| {
        // SAFETY: `sqlite3_config` before `sqlite3_initialize` is its
        // contract; afterwards it returns SQLITE_MISUSE and changes nothing.
        let rc = unsafe { ffi::sqlite3_config(ffi::SQLITE_CONFIG_MEMSTATUS, 0) };
        if rc != ffi::SQLITE_OK {
            eprintln!(
                "SQLite initialized before the simulator configured it (code {rc}); \
                 parallel runs will share its allocation mutex"
            );
        }
    });
}

impl SimVfs {
    pub fn new(seed: u64, faults: FaultConfig) -> SimVfs {
        configure_sqlite();
        let n = NEXT_VFS.fetch_add(1, Ordering::Relaxed);
        let name = CString::new(format!("sim-{seed:x}-{n}")).expect("no NUL in the name");
        let state = State {
            faults,
            faults_enabled: false,
            fault_prng: Prng::stream(seed, "vfs faults"),
            crash_prng: Prng::stream(seed, "vfs crashes"),
            random: Prng::stream(seed, "vfs randomness"),
            clock_us: 0,
            generation: 0,
            frozen: false,
            next_handle: 1,
            failing_syncs: 0,
            reads_until_failure: None,
            files: BTreeMap::new(),
            temp: BTreeMap::new(),
            locks: BTreeMap::new(),
            shm: BTreeMap::new(),
            stats: IoStats::default(),
        };
        let mut inner = Box::new(Inner {
            vfs: ffi::sqlite3_vfs {
                iVersion: 2,
                szOsFile: std::mem::size_of::<SimFile>() as c_int,
                mxPathname: 512,
                pNext: ptr::null_mut(),
                zName: ptr::null(),
                pAppData: ptr::null_mut(),
                xOpen: Some(x_open),
                xDelete: Some(x_delete),
                xAccess: Some(x_access),
                xFullPathname: Some(x_full_pathname),
                xDlOpen: None,
                xDlError: None,
                xDlSym: None,
                xDlClose: None,
                xRandomness: Some(x_randomness),
                xSleep: Some(x_sleep),
                xCurrentTime: Some(x_current_time),
                xGetLastError: Some(x_get_last_error),
                xCurrentTimeInt64: Some(x_current_time_int64),
                xSetSystemCall: None,
                xGetSystemCall: None,
                xNextSystemCall: None,
            },
            name,
            state: Mutex::new(state),
        });
        inner.vfs.zName = inner.name.as_ptr();
        inner.vfs.pAppData = ptr::from_ref::<Inner>(&inner).cast_mut().cast();
        // SAFETY: the box keeps `vfs` at a fixed address until `Drop`
        // unregisters it.
        let rc = unsafe { ffi::sqlite3_vfs_register(&mut inner.vfs, 0) };
        assert_eq!(rc, ffi::SQLITE_OK, "registering the VFS");
        SimVfs { inner }
    }

    pub fn name(&self) -> &str {
        self.inner.name.to_str().expect("the name is ASCII")
    }

    fn state(&self) -> MutexGuard<'_, State> {
        lock(&self.inner.state)
    }

    /// Turns fault injection on or off; it starts off.
    pub fn set_faults(&self, enabled: bool) {
        self.state().faults_enabled = enabled;
    }

    pub fn faults(&self) -> bool {
        self.state().faults_enabled
    }

    /// Runs `check` with fault injection suspended, so checks never fail on
    /// an injected error.
    pub fn quietly<R>(&self, check: impl FnOnce() -> R) -> R {
        let enabled = self.faults();
        self.set_faults(false);
        let result = check();
        self.set_faults(enabled);
        result
    }

    pub fn stats(&self) -> IoStats {
        self.state().stats.clone()
    }

    /// Makes the next sync of a database file fail, whatever the fault
    /// configuration says.
    pub fn fail_next_sync(&self) {
        self.state().failing_syncs += 1;
    }

    /// Makes the read `reads` reads from now fail: `0` fails the next one.
    /// `None` cancels a failure that has not happened.
    pub fn fail_read_after(&self, reads: Option<u64>) {
        self.state().reads_until_failure = reads;
    }

    /// Cuts the power: every open handle stops reaching the files. Close
    /// every connection, then call [`SimVfs::finish_crash`].
    pub fn begin_crash(&self) {
        let mut state = self.state();
        state.generation += 1;
        state.frozen = true;
        state.stats.crashes += 1;
    }

    /// Decides which unsynced writes reached the media and restarts the disk.
    pub fn finish_crash(&self) {
        let mut guard = self.state();
        let state = &mut *guard;
        let sector = state.faults.sector_size as usize;
        let keep = state.faults.crash_keep_sector;
        for file in state.files.values_mut() {
            let mut image = std::mem::take(&mut file.durable);
            for op in file.pending.drain(..) {
                match op {
                    Pending::Truncate(size) => {
                        if state.crash_prng.chance(keep) {
                            image.truncate(size);
                        }
                    }
                    Pending::Write { offset, data } => {
                        let mut start = offset;
                        let end = offset + data.len();
                        while start < end {
                            let stop = ((start / sector + 1) * sector).min(end);
                            if state.crash_prng.chance(keep) {
                                if image.len() < stop {
                                    image.resize(stop, 0);
                                }
                                image[start..stop]
                                    .copy_from_slice(&data[start - offset..stop - offset]);
                            } else {
                                state.stats.sectors_lost += 1;
                            }
                            start = stop;
                        }
                    }
                }
            }
            file.current = image.clone();
            file.durable = image;
        }
        state.locks.clear();
        let generation = state.generation;
        state.shm.retain(|g, indexes| {
            indexes.retain(|_, shm| *g == generation || !shm.mappers.is_empty());
            *g == generation || !indexes.is_empty()
        });
        state.temp.clear();
        state.frozen = false;
    }

    pub fn file_names(&self) -> Vec<String> {
        self.state().files.keys().cloned().collect()
    }
}

impl Drop for SimVfs {
    fn drop(&mut self) {
        // SAFETY: registered in `new`; every connection using it has closed.
        unsafe {
            ffi::sqlite3_vfs_unregister(&mut self.inner.vfs);
        }
    }
}

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

#[repr(C)]
struct SimFile {
    base: ffi::sqlite3_file,
    inner: *const Inner,
    /// Null for a temporary file.
    name: *mut String,
    handle: u64,
    generation: u64,
    lock: c_int,
    read_only: bool,
    delete_on_close: bool,
    shm_mapped: bool,
}

impl SimFile {
    /// The file's name, borrowed apart from the handle so the handle stays
    /// mutable.
    fn name<'a>(&self) -> Option<&'a str> {
        // SAFETY: set from `Box::into_raw` in `x_open`, freed in `x_close`
        // after the last use.
        unsafe { self.name.as_ref() }.map(String::as_str)
    }

    /// The VFS state, borrowed apart from the handle so the handle stays
    /// mutable.
    fn state<'a>(&self) -> MutexGuard<'a, State> {
        // SAFETY: the VFS outlives its handles.
        lock(unsafe { &(*self.inner).state })
    }
}

static METHODS: ffi::sqlite3_io_methods = ffi::sqlite3_io_methods {
    iVersion: 2,
    xClose: Some(x_close),
    xRead: Some(x_read),
    xWrite: Some(x_write),
    xTruncate: Some(x_truncate),
    xSync: Some(x_sync),
    xFileSize: Some(x_file_size),
    xLock: Some(x_lock),
    xUnlock: Some(x_unlock),
    xCheckReservedLock: Some(x_check_reserved_lock),
    xFileControl: Some(x_file_control),
    xSectorSize: Some(x_sector_size),
    xDeviceCharacteristics: Some(x_device_characteristics),
    xShmMap: Some(x_shm_map),
    xShmLock: Some(x_shm_lock),
    xShmBarrier: Some(x_shm_barrier),
    xShmUnmap: Some(x_shm_unmap),
    xFetch: None,
    xUnfetch: None,
};

/// # Safety
/// `p` must be a file this VFS opened.
unsafe fn file<'a>(p: *mut ffi::sqlite3_file) -> &'a mut SimFile {
    unsafe { &mut *p.cast::<SimFile>() }
}

/// # Safety
/// `vfs` must be a VFS registered by [`SimVfs::new`].
unsafe fn inner<'a>(vfs: *mut ffi::sqlite3_vfs) -> &'a Inner {
    unsafe { &*(*vfs).pAppData.cast::<Inner>() }
}

unsafe fn c_str<'a>(p: *const c_char) -> &'a str {
    unsafe { CStr::from_ptr(p) }.to_str().unwrap_or("")
}

unsafe extern "C" fn x_open(
    vfs: *mut ffi::sqlite3_vfs,
    name: ffi::sqlite3_filename,
    out: *mut ffi::sqlite3_file,
    flags: c_int,
    out_flags: *mut c_int,
) -> c_int {
    let inner = unsafe { inner(vfs) };
    let mut state = lock(&inner.state);
    unsafe { (*out).pMethods = ptr::null() };
    if state.frozen {
        return ffi::SQLITE_CANTOPEN;
    }
    let handle = state.next_handle;
    state.next_handle += 1;
    let name = if name.is_null() {
        state.temp.insert(handle, Vec::new());
        ptr::null_mut()
    } else {
        let name = unsafe { c_str(name) }.to_owned();
        if !state.files.contains_key(&name) {
            if flags & ffi::SQLITE_OPEN_CREATE == 0 {
                return ffi::SQLITE_CANTOPEN;
            }
            state.files.insert(name.clone(), FileData::default());
        }
        Box::into_raw(Box::new(name))
    };
    let file = SimFile {
        base: ffi::sqlite3_file { pMethods: &METHODS },
        inner,
        name,
        handle,
        generation: state.generation,
        lock: ffi::SQLITE_LOCK_NONE,
        read_only: flags & ffi::SQLITE_OPEN_READONLY != 0,
        delete_on_close: flags & ffi::SQLITE_OPEN_DELETEONCLOSE != 0,
        shm_mapped: false,
    };
    // SAFETY: SQLite hands over `szOsFile` bytes for the file.
    unsafe { ptr::write(out.cast::<SimFile>(), file) };
    if !out_flags.is_null() {
        unsafe { *out_flags = flags };
    }
    ffi::SQLITE_OK
}

unsafe extern "C" fn x_delete(
    vfs: *mut ffi::sqlite3_vfs,
    name: *const c_char,
    _sync_dir: c_int,
) -> c_int {
    let inner = unsafe { inner(vfs) };
    let mut state = lock(&inner.state);
    if !state.frozen {
        state.files.remove(unsafe { c_str(name) });
    }
    ffi::SQLITE_OK
}

unsafe extern "C" fn x_access(
    vfs: *mut ffi::sqlite3_vfs,
    name: *const c_char,
    flags: c_int,
    out: *mut c_int,
) -> c_int {
    let inner = unsafe { inner(vfs) };
    let state = lock(&inner.state);
    let file = state.files.get(unsafe { c_str(name) });
    // Like the unix VFS, an empty file does not exist.
    let found = match flags {
        ffi::SQLITE_ACCESS_EXISTS => file.is_some_and(|f| !f.current.is_empty()),
        _ => file.is_some(),
    };
    unsafe { *out = c_int::from(found) };
    ffi::SQLITE_OK
}

unsafe extern "C" fn x_full_pathname(
    _vfs: *mut ffi::sqlite3_vfs,
    name: *const c_char,
    n_out: c_int,
    out: *mut c_char,
) -> c_int {
    let bytes = unsafe { CStr::from_ptr(name) }.to_bytes_with_nul();
    if bytes.len() > n_out as usize {
        return ffi::SQLITE_CANTOPEN;
    }
    unsafe { ptr::copy_nonoverlapping(bytes.as_ptr().cast(), out, bytes.len()) };
    ffi::SQLITE_OK
}

unsafe extern "C" fn x_randomness(vfs: *mut ffi::sqlite3_vfs, n: c_int, out: *mut c_char) -> c_int {
    let inner = unsafe { inner(vfs) };
    let mut state = lock(&inner.state);
    let buf = unsafe { std::slice::from_raw_parts_mut(out.cast::<u8>(), n as usize) };
    state.random.fill(buf);
    n
}

unsafe extern "C" fn x_sleep(vfs: *mut ffi::sqlite3_vfs, us: c_int) -> c_int {
    let inner = unsafe { inner(vfs) };
    lock(&inner.state).clock_us += i64::from(us);
    us
}

/// 2026-01-01 as milliseconds since the Julian epoch.
const EPOCH_JULIAN_MS: i64 = 210_866_760_000_000 + 1_767_225_600_000;

unsafe extern "C" fn x_current_time_int64(vfs: *mut ffi::sqlite3_vfs, out: *mut i64) -> c_int {
    let inner = unsafe { inner(vfs) };
    let us = lock(&inner.state).clock_us;
    unsafe { *out = EPOCH_JULIAN_MS + us / 1000 };
    ffi::SQLITE_OK
}

unsafe extern "C" fn x_current_time(vfs: *mut ffi::sqlite3_vfs, out: *mut f64) -> c_int {
    let mut ms = 0i64;
    unsafe { x_current_time_int64(vfs, &mut ms) };
    unsafe { *out = ms as f64 / 86_400_000.0 };
    ffi::SQLITE_OK
}

unsafe extern "C" fn x_get_last_error(
    _vfs: *mut ffi::sqlite3_vfs,
    _n: c_int,
    _out: *mut c_char,
) -> c_int {
    0
}

unsafe extern "C" fn x_close(p: *mut ffi::sqlite3_file) -> c_int {
    let file = unsafe { file(p) };
    {
        let mut guard = file.state();
        let state = &mut *guard;
        let live = file.generation == state.generation;
        match file.name() {
            None => {
                state.temp.remove(&file.handle);
            }
            Some(name) => {
                if live {
                    if let Some(locks) = state.locks.get_mut(name) {
                        release_file_lock(locks, file.handle, ffi::SQLITE_LOCK_NONE);
                    }
                    if file.delete_on_close && !state.frozen {
                        state.files.remove(name);
                    }
                }
                if file.shm_mapped {
                    unmap_shm(state, name, file.generation, file.handle, false);
                }
            }
        }
    }
    if !file.name.is_null() {
        // SAFETY: from `Box::into_raw` in `x_open`.
        drop(unsafe { Box::from_raw(file.name) });
        file.name = ptr::null_mut();
    }
    ffi::SQLITE_OK
}

unsafe extern "C" fn x_read(
    p: *mut ffi::sqlite3_file,
    buf: *mut c_void,
    amt: c_int,
    offset: i64,
) -> c_int {
    let file = unsafe { file(p) };
    let mut guard = file.state();
    let state = &mut *guard;
    if file.generation != state.generation {
        return ffi::SQLITE_IOERR_READ;
    }
    state.stats.reads += 1;
    match state.reads_until_failure {
        Some(0) => {
            state.reads_until_failure = None;
            state.stats.read_errors += 1;
            return ffi::SQLITE_IOERR_READ;
        }
        Some(n) => state.reads_until_failure = Some(n - 1),
        None => {}
    }
    if state.fault(state.faults.read_error) {
        state.stats.read_errors += 1;
        return ffi::SQLITE_IOERR_READ;
    }
    let data = match file.name() {
        None => state.temp.get(&file.handle).map(Vec::as_slice),
        Some(name) => state.files.get(name).map(|f| f.current.as_slice()),
    }
    .unwrap_or(&[]);
    let out = unsafe { std::slice::from_raw_parts_mut(buf.cast::<u8>(), amt as usize) };
    let offset = offset as usize;
    let available = data.len().saturating_sub(offset).min(out.len());
    if available > 0 {
        out[..available].copy_from_slice(&data[offset..offset + available]);
    }
    if available < out.len() {
        out[available..].fill(0);
        return ffi::SQLITE_IOERR_SHORT_READ;
    }
    ffi::SQLITE_OK
}

fn write_at(data: &mut Vec<u8>, offset: usize, bytes: &[u8]) {
    if data.len() < offset + bytes.len() {
        data.resize(offset + bytes.len(), 0);
    }
    data[offset..offset + bytes.len()].copy_from_slice(bytes);
}

unsafe extern "C" fn x_write(
    p: *mut ffi::sqlite3_file,
    buf: *const c_void,
    amt: c_int,
    offset: i64,
) -> c_int {
    let file = unsafe { file(p) };
    let mut guard = file.state();
    let state = &mut *guard;
    if file.generation != state.generation || state.frozen || file.read_only {
        return ffi::SQLITE_IOERR_WRITE;
    }
    state.stats.writes += 1;
    if state.fault(state.faults.write_error) {
        state.stats.write_errors += 1;
        return ffi::SQLITE_IOERR_WRITE;
    }
    let bytes = unsafe { std::slice::from_raw_parts(buf.cast::<u8>(), amt as usize) };
    let offset = offset as usize;
    match file.name() {
        None => {
            if let Some(data) = state.temp.get_mut(&file.handle) {
                write_at(data, offset, bytes);
            }
        }
        Some(name) => {
            if let Some(data) = state.files.get_mut(name) {
                write_at(&mut data.current, offset, bytes);
                data.pending.push(Pending::Write {
                    offset,
                    data: bytes.to_vec(),
                });
            }
        }
    }
    ffi::SQLITE_OK
}

unsafe extern "C" fn x_truncate(p: *mut ffi::sqlite3_file, size: i64) -> c_int {
    let file = unsafe { file(p) };
    let mut guard = file.state();
    let state = &mut *guard;
    if file.generation != state.generation || state.frozen || file.read_only {
        return ffi::SQLITE_IOERR_TRUNCATE;
    }
    let size = size as usize;
    match file.name() {
        None => {
            if let Some(data) = state.temp.get_mut(&file.handle) {
                data.truncate(size);
            }
        }
        Some(name) => {
            if let Some(data) = state.files.get_mut(name) {
                data.current.truncate(size);
                data.pending.push(Pending::Truncate(size));
            }
        }
    }
    ffi::SQLITE_OK
}

unsafe extern "C" fn x_sync(p: *mut ffi::sqlite3_file, _flags: c_int) -> c_int {
    let file = unsafe { file(p) };
    let mut guard = file.state();
    let state = &mut *guard;
    if file.generation != state.generation || state.frozen {
        return ffi::SQLITE_IOERR_FSYNC;
    }
    let Some(name) = file.name() else {
        return ffi::SQLITE_OK;
    };
    state.stats.syncs += 1;
    if state.failing_syncs > 0 {
        state.failing_syncs -= 1;
        state.stats.sync_errors += 1;
        return ffi::SQLITE_IOERR_FSYNC;
    }
    if state.fault(state.faults.sync_error) {
        state.stats.sync_errors += 1;
        return ffi::SQLITE_IOERR_FSYNC;
    }
    if let Some(data) = state.files.get_mut(name) {
        for op in data.pending.drain(..) {
            match op {
                Pending::Write {
                    offset,
                    data: bytes,
                } => write_at(&mut data.durable, offset, &bytes),
                Pending::Truncate(size) => data.durable.truncate(size),
            }
        }
    }
    ffi::SQLITE_OK
}

unsafe extern "C" fn x_file_size(p: *mut ffi::sqlite3_file, out: *mut i64) -> c_int {
    let file = unsafe { file(p) };
    let state = file.state();
    if file.generation != state.generation {
        return ffi::SQLITE_IOERR_FSTAT;
    }
    let size = match file.name() {
        None => state.temp.get(&file.handle).map_or(0, Vec::len),
        Some(name) => state.files.get(name).map_or(0, |f| f.current.len()),
    };
    unsafe { *out = size as i64 };
    ffi::SQLITE_OK
}

unsafe extern "C" fn x_lock(p: *mut ffi::sqlite3_file, level: c_int) -> c_int {
    let file = unsafe { file(p) };
    let mut guard = file.state();
    let state = &mut *guard;
    if file.generation != state.generation || state.frozen {
        return ffi::SQLITE_IOERR_LOCK;
    }
    if file.lock >= level {
        return ffi::SQLITE_OK;
    }
    if file.read_only && level > ffi::SQLITE_LOCK_SHARED {
        return ffi::SQLITE_IOERR_LOCK;
    }
    let Some(name) = file.name() else {
        file.lock = level;
        return ffi::SQLITE_OK;
    };
    if level <= ffi::SQLITE_LOCK_RESERVED && state.fault(state.faults.lock_busy) {
        state.stats.busy += 1;
        return ffi::SQLITE_BUSY;
    }
    let me = file.handle;
    if !state.locks.contains_key(name) {
        state.locks.insert(name.to_owned(), FileLock::default());
    }
    let locks = state.locks.get_mut(name).expect("inserted above");
    let other = |holder: Option<u64>| holder.is_some_and(|h| h != me);
    match level {
        ffi::SQLITE_LOCK_SHARED => {
            if other(locks.pending) || other(locks.exclusive) {
                return ffi::SQLITE_BUSY;
            }
            locks.shared.insert(me);
        }
        ffi::SQLITE_LOCK_RESERVED => {
            if other(locks.reserved) || other(locks.pending) || other(locks.exclusive) {
                return ffi::SQLITE_BUSY;
            }
            locks.reserved = Some(me);
        }
        ffi::SQLITE_LOCK_EXCLUSIVE => {
            if other(locks.pending) || other(locks.exclusive) {
                return ffi::SQLITE_BUSY;
            }
            locks.pending = Some(me);
            if locks.shared.iter().any(|&h| h != me) {
                file.lock = ffi::SQLITE_LOCK_PENDING;
                return ffi::SQLITE_BUSY;
            }
            locks.exclusive = Some(me);
        }
        _ => return ffi::SQLITE_IOERR_LOCK,
    }
    file.lock = level;
    ffi::SQLITE_OK
}

fn release_file_lock(locks: &mut FileLock, me: u64, level: c_int) {
    if level < ffi::SQLITE_LOCK_RESERVED {
        for holder in [
            &mut locks.reserved,
            &mut locks.pending,
            &mut locks.exclusive,
        ] {
            if *holder == Some(me) {
                *holder = None;
            }
        }
    }
    if level == ffi::SQLITE_LOCK_NONE {
        locks.shared.remove(&me);
    }
}

unsafe extern "C" fn x_unlock(p: *mut ffi::sqlite3_file, level: c_int) -> c_int {
    let file = unsafe { file(p) };
    let mut guard = file.state();
    let state = &mut *guard;
    if file.lock <= level {
        return ffi::SQLITE_OK;
    }
    if file.generation == state.generation
        && let Some(name) = file.name()
        && let Some(locks) = state.locks.get_mut(name)
    {
        release_file_lock(locks, file.handle, level);
    }
    file.lock = level;
    ffi::SQLITE_OK
}

unsafe extern "C" fn x_check_reserved_lock(p: *mut ffi::sqlite3_file, out: *mut c_int) -> c_int {
    let file = unsafe { file(p) };
    let state = file.state();
    if file.generation != state.generation {
        return ffi::SQLITE_IOERR_CHECKRESERVEDLOCK;
    }
    let held = file
        .name()
        .and_then(|name| state.locks.get(name))
        .is_some_and(|l| l.reserved.is_some() || l.pending.is_some() || l.exclusive.is_some());
    unsafe { *out = c_int::from(held) };
    ffi::SQLITE_OK
}

unsafe extern "C" fn x_file_control(
    _p: *mut ffi::sqlite3_file,
    _op: c_int,
    _arg: *mut c_void,
) -> c_int {
    ffi::SQLITE_NOTFOUND
}

unsafe extern "C" fn x_sector_size(p: *mut ffi::sqlite3_file) -> c_int {
    let file = unsafe { file(p) };
    file.state().faults.sector_size as c_int
}

unsafe extern "C" fn x_device_characteristics(p: *mut ffi::sqlite3_file) -> c_int {
    let file = unsafe { file(p) };
    if file.state().faults.powersafe_overwrite {
        ffi::SQLITE_IOCAP_POWERSAFE_OVERWRITE
    } else {
        0
    }
}

unsafe extern "C" fn x_shm_map(
    p: *mut ffi::sqlite3_file,
    region: c_int,
    size: c_int,
    extend: c_int,
    out: *mut *mut c_void,
) -> c_int {
    let file = unsafe { file(p) };
    unsafe { *out = ptr::null_mut() };
    let Some(name) = file.name() else {
        return ffi::SQLITE_IOERR_SHMMAP;
    };
    let mut guard = file.state();
    let state = &mut *guard;
    if file.generation != state.generation || state.frozen {
        return ffi::SQLITE_IOERR_SHMMAP;
    }
    let indexes = state.shm.entry(file.generation).or_default();
    if !indexes.contains_key(name) {
        indexes.insert(name.to_owned(), Shm::default());
    }
    let shm = indexes.get_mut(name).expect("inserted above");
    if !file.shm_mapped {
        // The first connection to map the WAL index finds it empty, as the
        // unix VFS's dead-man switch truncates a -shm file no one has open.
        if shm.mappers.is_empty() {
            shm.regions.clear();
        }
        shm.mappers.insert(file.handle);
        file.shm_mapped = true;
    }
    let region = region as usize;
    if region >= shm.regions.len() {
        if extend == 0 {
            return ffi::SQLITE_OK;
        }
        while shm.regions.len() <= region {
            shm.regions
                .push(vec![0u8; size as usize].into_boxed_slice());
        }
    }
    unsafe { *out = shm.regions[region].as_mut_ptr().cast() };
    ffi::SQLITE_OK
}

unsafe extern "C" fn x_shm_lock(
    p: *mut ffi::sqlite3_file,
    offset: c_int,
    n: c_int,
    flags: c_int,
) -> c_int {
    let file = unsafe { file(p) };
    let Some(name) = file.name() else {
        return ffi::SQLITE_IOERR_SHMLOCK;
    };
    let mut guard = file.state();
    let state = &mut *guard;
    let unlock = flags & ffi::SQLITE_SHM_UNLOCK != 0;
    if !unlock && (file.generation != state.generation || state.frozen) {
        return ffi::SQLITE_IOERR_SHMLOCK;
    }
    let Some(shm) = state
        .shm
        .get_mut(&file.generation)
        .and_then(|indexes| indexes.get_mut(name))
    else {
        return if unlock {
            ffi::SQLITE_OK
        } else {
            ffi::SQLITE_IOERR_SHMLOCK
        };
    };
    let me = file.handle;
    let slots = offset as usize..(offset + n) as usize;
    let shared = flags & ffi::SQLITE_SHM_SHARED != 0;
    if unlock {
        for slot in slots {
            if shared {
                shm.shared[slot].remove(&me);
            } else if shm.exclusive[slot] == Some(me) {
                shm.exclusive[slot] = None;
            }
        }
        return ffi::SQLITE_OK;
    }
    let free = slots.clone().all(|slot| {
        shm.exclusive[slot].is_none_or(|h| h == me)
            && (shared || shm.shared[slot].iter().all(|&h| h == me))
    });
    if !free {
        return ffi::SQLITE_BUSY;
    }
    for slot in slots {
        if shared {
            shm.shared[slot].insert(me);
        } else {
            shm.exclusive[slot] = Some(me);
        }
    }
    ffi::SQLITE_OK
}

unsafe extern "C" fn x_shm_barrier(_p: *mut ffi::sqlite3_file) {
    std::sync::atomic::fence(Ordering::SeqCst);
}

fn unmap_shm(state: &mut State, name: &str, generation: u64, me: u64, delete: bool) {
    let current = state.generation;
    let Some(indexes) = state.shm.get_mut(&generation) else {
        return;
    };
    let Some(shm) = indexes.get_mut(name) else {
        return;
    };
    shm.mappers.remove(&me);
    for slot in 0..SHM_LOCKS {
        shm.shared[slot].remove(&me);
        if shm.exclusive[slot] == Some(me) {
            shm.exclusive[slot] = None;
        }
    }
    if shm.mappers.is_empty() && (delete || generation != current) {
        indexes.remove(name);
    }
    if indexes.is_empty() && generation != current {
        state.shm.remove(&generation);
    }
}

unsafe extern "C" fn x_shm_unmap(p: *mut ffi::sqlite3_file, delete: c_int) -> c_int {
    let file = unsafe { file(p) };
    if !file.shm_mapped {
        return ffi::SQLITE_OK;
    }
    if let Some(name) = file.name() {
        let mut guard = file.state();
        unmap_shm(&mut guard, name, file.generation, file.handle, delete != 0);
    }
    file.shm_mapped = false;
    ffi::SQLITE_OK
}

#[cfg(test)]
mod tests {
    use super::*;
    use rusqlite::{Connection, OpenFlags};

    fn open(vfs: &SimVfs, name: &str) -> Connection {
        let conn = Connection::open_with_flags_and_vfs(
            name,
            OpenFlags::SQLITE_OPEN_READ_WRITE | OpenFlags::SQLITE_OPEN_CREATE,
            vfs.name(),
        )
        .expect("open");
        conn.execute_batch("PRAGMA journal_mode = WAL; PRAGMA synchronous = FULL;")
            .expect("pragmas");
        conn
    }

    #[test]
    fn wal_readers_keep_their_snapshot_while_the_writer_commits() {
        let vfs = SimVfs::new(1, FaultConfig::none());
        let writer = open(&vfs, "db");
        writer
            .execute_batch("CREATE TABLE t(x); INSERT INTO t VALUES (1);")
            .unwrap();
        let reader = open(&vfs, "db");
        let other = open(&vfs, "db");
        reader.execute_batch("BEGIN").unwrap();
        let count = |c: &Connection| -> i64 {
            c.query_row("SELECT count(*) FROM t", [], |r| r.get(0))
                .unwrap()
        };
        assert_eq!(count(&reader), 1);
        writer.execute_batch("INSERT INTO t VALUES (2)").unwrap();
        assert_eq!(count(&reader), 1, "the open snapshot ignores the commit");
        assert_eq!(count(&other), 2);
        reader.execute_batch("COMMIT").unwrap();
        assert_eq!(count(&reader), 2);
        drop((reader, other, writer));
        assert!(vfs.file_names().contains(&"db".to_owned()));
    }

    #[test]
    fn the_last_connection_to_close_removes_the_wal_unless_it_is_read_only() {
        let vfs = SimVfs::new(5, FaultConfig::none());
        let writer = open(&vfs, "db");
        writer
            .execute_batch("CREATE TABLE t(x); INSERT INTO t VALUES (1);")
            .unwrap();
        let reader =
            Connection::open_with_flags_and_vfs("db", OpenFlags::SQLITE_OPEN_READ_ONLY, vfs.name())
                .unwrap();
        let count: i64 = reader
            .query_row("SELECT count(*) FROM t", [], |r| r.get(0))
            .unwrap();
        assert_eq!(count, 1);
        let wal = || vfs.file_names().contains(&"db-wal".to_owned());
        drop(writer);
        drop(reader);
        assert!(wal(), "a read-only connection cannot checkpoint");
        drop(open(&vfs, "db"));
        assert!(
            !wal(),
            "a read-write connection checkpoints and deletes the WAL"
        );
    }

    #[test]
    fn shm_locks_exclude_a_second_exclusive_holder() {
        let vfs = SimVfs::new(2, FaultConfig::none());
        let a = open(&vfs, "db");
        a.execute_batch("CREATE TABLE t(x)").unwrap();
        let b = open(&vfs, "db");
        a.execute_batch("BEGIN IMMEDIATE; INSERT INTO t VALUES (1);")
            .unwrap();
        let err = b.execute_batch("BEGIN IMMEDIATE").unwrap_err();
        assert_eq!(
            err.sqlite_error_code(),
            Some(rusqlite::ErrorCode::DatabaseBusy)
        );
        a.execute_batch("COMMIT").unwrap();
        b.execute_batch("BEGIN IMMEDIATE; COMMIT;").unwrap();
    }

    #[test]
    fn a_crash_keeps_synced_commits() {
        let mut faults = FaultConfig::none();
        faults.crash_keep_sector = 0.0;
        let vfs = SimVfs::new(3, faults);
        let conn = open(&vfs, "db");
        conn.execute_batch("CREATE TABLE t(x); INSERT INTO t VALUES (1), (2);")
            .unwrap();
        vfs.begin_crash();
        drop(conn);
        vfs.finish_crash();
        let conn = open(&vfs, "db");
        let count: i64 = conn
            .query_row("SELECT count(*) FROM t", [], |r| r.get(0))
            .unwrap();
        assert_eq!(count, 2);
        let check: String = conn
            .query_row("PRAGMA integrity_check", [], |r| r.get(0))
            .unwrap();
        assert_eq!(check, "ok");
    }

    #[test]
    fn a_crash_drops_unsynced_writes() {
        let mut faults = FaultConfig::none();
        faults.crash_keep_sector = 0.0;
        let vfs = SimVfs::new(4, faults);
        let conn = open(&vfs, "db");
        conn.execute_batch(
            "PRAGMA synchronous = OFF; CREATE TABLE t(x); INSERT INTO t VALUES (1);",
        )
        .unwrap();
        vfs.begin_crash();
        drop(conn);
        vfs.finish_crash();
        let conn = open(&vfs, "db");
        let tables: i64 = conn
            .query_row(
                "SELECT count(*) FROM sqlite_schema WHERE name = 't'",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(tables, 0, "nothing was synced, so nothing survives");
    }
}
