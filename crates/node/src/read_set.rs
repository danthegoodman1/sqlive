//! Read sets and the index that finds which of many a commit affects.
//!
//! A query's read set crosses to JavaScript as a type-tagged external,
//! which `index.js` wraps in its `ReadSet` class: an external costs less to
//! create and collect than a class instance, and every query returns one.

use crate::errors::{Fail, Result, raise};
use crate::events::CommitEvent;
use crate::values::check;
use napi::bindgen_prelude::*;
use napi::sys;
use napi_derive::napi;
use std::cmp::Ordering;
use std::ffi::c_void;
use std::ptr;

/// Marks the externals that hold a `sqlive::ReadSet`.
const TAG: sys::napi_type_tag = sys::napi_type_tag {
    lower: 0x7371_6c69_7665_5253,
    upper: 0x5265_6164_5365_7431,
};

unsafe extern "C" fn finalize(_env: sys::napi_env, data: *mut c_void, _hint: *mut c_void) {
    drop(unsafe { Box::from_raw(data.cast::<sqlive::ReadSet>()) });
}

/// A read set as a tagged external.
pub(crate) unsafe fn to_js(
    env: sys::napi_env,
    read_set: sqlive::ReadSet,
) -> Result<sys::napi_value> {
    unsafe {
        let data = Box::into_raw(Box::new(read_set));
        let mut value = ptr::null_mut();
        if let Err(fail) = check(sys::napi_create_external(
            env,
            data.cast(),
            Some(finalize),
            ptr::null_mut(),
            &mut value,
        )) {
            drop(Box::from_raw(data));
            return Err(fail);
        }
        check(sys::napi_type_tag_object(env, value, &TAG))?;
        Ok(value)
    }
}

/// The read set a tagged external holds, alive while the external is.
unsafe fn from_js<'a>(env: sys::napi_env, value: sys::napi_value) -> Result<&'a sqlive::ReadSet> {
    unsafe {
        let mut tagged = false;
        let status = sys::napi_check_object_type_tag(env, value, &TAG, &mut tagged);
        if status != sys::Status::napi_ok || !tagged {
            return Err(Fail::Type(
                "expected a ReadSet from a query result".to_owned(),
            ));
        }
        let mut data = ptr::null_mut();
        check(sys::napi_get_value_external(env, value, &mut data))?;
        Ok(&*data.cast::<sqlive::ReadSet>())
    }
}

fn with<T>(
    env: &Env,
    read_set: Unknown<'_>,
    f: impl FnOnce(&sqlive::ReadSet) -> T,
) -> napi::Result<T> {
    unsafe { from_js(env.raw(), read_set.raw()) }
        .map(f)
        .map_err(|fail| raise(env.raw(), fail))
}

/// One table a read set covers.
#[napi(object)]
pub struct TableRead {
    pub name: String,
    /// The columns read, in lowercase.
    pub columns: Vec<String>,
    /// An update to any column counts.
    pub every_column: bool,
    /// The rows the result depends on, as SQL; `TRUE` for every row.
    pub filter: String,
}

#[napi]
pub fn read_set_version(env: &Env, read_set: Unknown<'_>) -> napi::Result<f64> {
    with(env, read_set, |read_set| read_set.version() as f64)
}

#[napi]
pub fn read_set_every_commit(env: &Env, read_set: Unknown<'_>) -> napi::Result<bool> {
    with(env, read_set, sqlive::ReadSet::every_commit)
}

#[napi]
pub fn read_set_tables(env: &Env, read_set: Unknown<'_>) -> napi::Result<Vec<TableRead>> {
    with(env, read_set, |read_set| {
        read_set
            .tables()
            .map(|table| TableRead {
                name: table.name().to_owned(),
                columns: table.columns().map(str::to_owned).collect(),
                every_column: table.every_column(),
                filter: table.filter().to_string(),
            })
            .collect()
    })
}

#[napi]
pub fn read_set_intersects(
    env: &Env,
    read_set: Unknown<'_>,
    event: &CommitEvent,
) -> napi::Result<bool> {
    with(env, read_set, |read_set| read_set.intersects(&event.inner))
}

#[napi]
pub fn read_set_reads_rows_of(
    env: &Env,
    read_set: Unknown<'_>,
    table: String,
) -> napi::Result<bool> {
    with(env, read_set, |read_set| read_set.reads_rows_of(&table))
}

#[napi]
pub fn read_set_display(env: &Env, read_set: Unknown<'_>) -> napi::Result<String> {
    with(env, read_set, ToString::to_string)
}

/// A key a `ReadSetIndex` stores a read set under: a number or a string.
#[derive(Clone, Debug)]
pub(crate) enum Key {
    Number(f64),
    Text(String),
}

impl From<Either<f64, String>> for Key {
    fn from(key: Either<f64, String>) -> Key {
        match key {
            Either::A(number) => Key::Number(number),
            Either::B(text) => Key::Text(text),
        }
    }
}

impl From<Key> for Either<f64, String> {
    fn from(key: Key) -> Either<f64, String> {
        match key {
            Key::Number(number) => Either::A(number),
            Key::Text(text) => Either::B(text),
        }
    }
}

impl Ord for Key {
    fn cmp(&self, other: &Key) -> Ordering {
        match (self, other) {
            (Key::Number(a), Key::Number(b)) => a.total_cmp(b),
            (Key::Text(a), Key::Text(b)) => a.cmp(b),
            (Key::Number(_), Key::Text(_)) => Ordering::Less,
            (Key::Text(_), Key::Number(_)) => Ordering::Greater,
        }
    }
}

impl PartialOrd for Key {
    fn partial_cmp(&self, other: &Key) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl PartialEq for Key {
    fn eq(&self, other: &Key) -> bool {
        self.cmp(other) == Ordering::Equal
    }
}

impl Eq for Key {}

/// Read sets stored under keys; `index.js` wraps it as `ReadSetIndex`.
#[napi]
pub struct NativeIndex {
    pub(crate) inner: sqlive::ReadSetIndex<Key>,
}

#[napi]
impl NativeIndex {
    #[napi(constructor)]
    #[allow(clippy::new_without_default)]
    pub fn new() -> NativeIndex {
        NativeIndex {
            inner: sqlive::ReadSetIndex::new(),
        }
    }

    #[napi(getter)]
    pub fn size(&self) -> u32 {
        self.inner.len() as u32
    }

    /// Stores a read set under `key`, replacing the key's previous one.
    #[napi]
    pub fn insert(
        &mut self,
        env: &Env,
        key: Either<f64, String>,
        read_set: Unknown<'_>,
    ) -> napi::Result<()> {
        let read_set = with(env, read_set, Clone::clone)?;
        self.inner.insert(key.into(), read_set);
        Ok(())
    }

    /// Drops the key's read set; false if the key held none.
    #[napi]
    pub fn delete(&mut self, key: Either<f64, String>) -> bool {
        self.inner.remove(&key.into()).is_some()
    }

    #[napi]
    pub fn has(&self, key: Either<f64, String>) -> bool {
        self.inner.get(&key.into()).is_some()
    }

    #[napi(ts_return_type = "unknown")]
    pub fn get(&self, env: &Env, key: Either<f64, String>) -> napi::Result<crate::events::Raw> {
        let raw = env.raw();
        let value = match self.inner.get(&key.into()) {
            Some(read_set) => unsafe { to_js(raw, read_set.clone()) },
            None => unsafe { crate::values::undefined(raw) },
        };
        value
            .map(crate::events::Raw)
            .map_err(|fail| raise(raw, fail))
    }

    /// The keys whose queries `event` may change, in key order: numbers
    /// first, then strings.
    #[napi]
    pub fn affected(&self, event: &CommitEvent) -> Vec<Either<f64, String>> {
        self.inner
            .affected(&event.inner)
            .map(Either::from)
            .collect()
    }
}
