//! Commit events and the streams that deliver them. An event stays in Rust
//! until JavaScript reads its rows, so `ReadSetIndex.affected` and
//! `ReadSet.intersects` take it without converting a value.

use crate::errors::{Result, raise};
use crate::values::{Integers, check, integer, null, owned_to_js, string};
use napi::bindgen_prelude::*;
use napi::sys;
use napi_derive::napi;
use sqlive::{Op, Row};
use std::ptr;
use std::sync::Arc;

/// The net row changes of one committed transaction.
#[napi]
pub struct CommitEvent {
    pub(crate) inner: Arc<sqlive::CommitEvent>,
    pub(crate) integers: Integers,
}

#[napi]
impl CommitEvent {
    #[napi(getter)]
    pub fn version(&self) -> f64 {
        self.inner.version as f64
    }

    #[napi(getter)]
    pub fn schema_changed(&self) -> bool {
        self.inner.schema_changed
    }

    #[napi(getter)]
    pub fn whole_tables(&self) -> Vec<String> {
        self.inner.whole_tables.clone()
    }

    /// The event's row changes as plain objects; `index.js` caches them as
    /// `changes`.
    #[napi]
    pub fn read_changes(&self, env: &Env) -> napi::Result<Raw> {
        let env = env.raw();
        unsafe { changes(env, &self.inner, self.integers) }
            .map(Raw)
            .map_err(|fail| raise(env, fail))
    }
}

/// A Node-API value handed back as is.
pub struct Raw(pub(crate) sys::napi_value);

impl ToNapiValue for Raw {
    unsafe fn to_napi_value(_env: sys::napi_env, value: Raw) -> napi::Result<sys::napi_value> {
        Ok(value.0)
    }
}

impl TypeName for Raw {
    fn type_name() -> &'static str {
        "unknown"
    }

    fn value_type() -> ValueType {
        ValueType::Unknown
    }
}

unsafe fn set(
    env: sys::napi_env,
    object: sys::napi_value,
    name: &std::ffi::CStr,
    value: sys::napi_value,
) -> Result<()> {
    check(unsafe { sys::napi_set_named_property(env, object, name.as_ptr(), value) })
}

unsafe fn changes(
    env: sys::napi_env,
    event: &sqlive::CommitEvent,
    integers: Integers,
) -> Result<sys::napi_value> {
    unsafe {
        let mut array = ptr::null_mut();
        check(sys::napi_create_array_with_length(
            env,
            event.changes.len(),
            &mut array,
        ))?;
        let (insert, update, delete) = (
            string(env, "insert")?,
            string(env, "update")?,
            string(env, "delete")?,
        );
        // Every row of a table shares its column names, so their keys are
        // made once for each run of changes to one table.
        let mut keys = Keys::default();
        for (i, change) in event.changes.iter().enumerate() {
            let mut object = ptr::null_mut();
            check(sys::napi_create_object(env, &mut object))?;
            set(env, object, c"table", string(env, &change.table)?)?;
            let op = match change.op {
                Op::Insert => insert,
                Op::Update => update,
                Op::Delete => delete,
            };
            set(env, object, c"op", op)?;
            let rowid = match change.rowid {
                Some(rowid) => integer(env, rowid, integers)?,
                None => null(env)?,
            };
            set(env, object, c"rowid", rowid)?;
            set(
                env,
                object,
                c"old",
                keys.row(env, change.old.as_ref(), integers)?,
            )?;
            set(
                env,
                object,
                c"new",
                keys.row(env, change.new.as_ref(), integers)?,
            )?;
            check(sys::napi_set_element(env, array, i as u32, object))?;
        }
        Ok(array)
    }
}

#[derive(Default)]
struct Keys {
    columns: *const String,
    keys: Vec<sys::napi_value>,
}

impl Keys {
    unsafe fn row(
        &mut self,
        env: sys::napi_env,
        row: Option<&Row>,
        integers: Integers,
    ) -> Result<sys::napi_value> {
        let Some(row) = row else {
            return unsafe { null(env) };
        };
        unsafe {
            if !ptr::eq(self.columns, row.columns().as_ptr()) {
                self.keys = row
                    .columns()
                    .iter()
                    .map(|column| string(env, column))
                    .collect::<Result<_>>()?;
                self.columns = row.columns().as_ptr();
            }
            let mut object = ptr::null_mut();
            check(sys::napi_create_object(env, &mut object))?;
            for (key, value) in self.keys.iter().zip(row.values()) {
                check(sys::napi_set_property(
                    env,
                    object,
                    *key,
                    owned_to_js(env, value, integers)?,
                ))?;
            }
            Ok(object)
        }
    }
}

/// Commit events in version order.
#[napi]
pub struct ChangeStream {
    pub(crate) inner: sqlive::Changes,
    pub(crate) integers: Integers,
}

#[napi]
impl ChangeStream {
    /// The next event if one has committed. A stream that has fallen behind
    /// the retained log throws `ERR_SQLIVE_LAGGED` once, then ends.
    #[napi]
    pub fn try_next(&mut self, env: &Env) -> napi::Result<Option<CommitEvent>> {
        match self.inner.try_next() {
            None => Ok(None),
            Some(Ok(event)) => Ok(Some(CommitEvent {
                inner: event,
                integers: self.integers,
            })),
            Some(Err(error)) => Err(raise(env.raw(), error.into())),
        }
    }

    #[napi(getter)]
    pub fn next_version(&self) -> f64 {
        self.inner.next_version() as f64
    }
}
