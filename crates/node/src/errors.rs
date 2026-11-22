//! Errors as JavaScript sees them. Each carries a `code`: SQLite's own name
//! for a SQLite failure, such as `SQLITE_CONSTRAINT_UNIQUE`, or an
//! `ERR_SQLIVE_*` code for SqLive's errors, with their fields alongside.

use napi::bindgen_prelude::Unknown;
use napi::sys;
use sqlive::rusqlite::{self, ffi};
use std::ffi::{CStr, c_char};
use std::ptr;

pub(crate) type Result<T> = std::result::Result<T, Fail>;

pub(crate) enum Fail {
    /// A Node-API failure, or a JavaScript exception to rethrow as it was.
    Napi(napi::Error),
    /// A `TypeError` for an argument of the wrong type.
    Type(String),
    /// A `RangeError` for a value out of range.
    Range(String),
    Live(sqlive::Error),
    /// An `Error` with an `ERR_SQLIVE_*` code the binding raises itself.
    State(&'static CStr, String),
}

impl From<napi::Error> for Fail {
    fn from(error: napi::Error) -> Fail {
        Fail::Napi(error)
    }
}

impl From<sqlive::Error> for Fail {
    fn from(error: sqlive::Error) -> Fail {
        Fail::Live(error)
    }
}

impl From<rusqlite::Error> for Fail {
    fn from(error: rusqlite::Error) -> Fail {
        Fail::Live(sqlive::Error::Sqlite(error))
    }
}

pub(crate) fn closed() -> Fail {
    Fail::State(c"ERR_SQLIVE_CLOSED", "the database is closed".to_owned())
}

/// Turns a failure into the JavaScript error napi-rs throws.
pub(crate) fn raise(env: sys::napi_env, fail: Fail) -> napi::Error {
    match fail {
        Fail::Napi(error) => error,
        fail => match unsafe { build(env, fail) } {
            Ok(value) => napi::Error::from(unsafe { Unknown::from_raw_unchecked(env, value) }),
            Err(error) => error,
        },
    }
}

/// The JavaScript error object for a failure other than `Fail::Napi`.
pub(crate) unsafe fn build(env: sys::napi_env, fail: Fail) -> napi::Result<sys::napi_value> {
    unsafe {
        match fail {
            Fail::Napi(error) => Err(error),
            Fail::Type(message) => create(env, Kind::Type, c"ERR_INVALID_ARG_TYPE", &message),
            Fail::Range(message) => create(env, Kind::Range, c"ERR_OUT_OF_RANGE", &message),
            Fail::State(code, message) => create(env, Kind::Error, code, &message),
            Fail::Live(error) => live(env, error),
        }
    }
}

#[derive(Clone, Copy)]
enum Kind {
    Error,
    Type,
    Range,
}

unsafe fn create(
    env: sys::napi_env,
    kind: Kind,
    code: &CStr,
    message: &str,
) -> napi::Result<sys::napi_value> {
    unsafe {
        let mut code_value = ptr::null_mut();
        check(sys::napi_create_string_utf8(
            env,
            code.as_ptr(),
            code.count_bytes() as isize,
            &mut code_value,
        ))?;
        let mut message_value = ptr::null_mut();
        check(sys::napi_create_string_utf8(
            env,
            message.as_ptr().cast::<c_char>(),
            message.len() as isize,
            &mut message_value,
        ))?;
        let mut error = ptr::null_mut();
        check(match kind {
            Kind::Error => sys::napi_create_error(env, code_value, message_value, &mut error),
            Kind::Type => sys::napi_create_type_error(env, code_value, message_value, &mut error),
            Kind::Range => sys::napi_create_range_error(env, code_value, message_value, &mut error),
        })?;
        Ok(error)
    }
}

unsafe fn set_number(
    env: sys::napi_env,
    object: sys::napi_value,
    name: &CStr,
    value: u64,
) -> napi::Result<()> {
    unsafe {
        let mut number = ptr::null_mut();
        check(sys::napi_create_double(env, value as f64, &mut number))?;
        check(sys::napi_set_named_property(
            env,
            object,
            name.as_ptr(),
            number,
        ))
    }
}

fn check(status: sys::napi_status) -> napi::Result<()> {
    if status == sys::Status::napi_ok {
        Ok(())
    } else {
        Err(napi::Error::new(
            napi::Status::from(status),
            "a Node-API call failed",
        ))
    }
}

unsafe fn live(env: sys::napi_env, error: sqlive::Error) -> napi::Result<sys::napi_value> {
    let message = error.to_string();
    unsafe {
        match error {
            sqlive::Error::Lagged { requested, oldest } => {
                let object = create(env, Kind::Error, c"ERR_SQLIVE_LAGGED", &message)?;
                set_number(env, object, c"requested", requested)?;
                set_number(env, object, c"oldest", oldest)?;
                Ok(object)
            }
            sqlive::Error::FutureVersion { requested, latest } => {
                let object = create(env, Kind::Error, c"ERR_SQLIVE_FUTURE_VERSION", &message)?;
                set_number(env, object, c"requested", requested)?;
                set_number(env, object, c"latest", latest)?;
                Ok(object)
            }
            sqlive::Error::RolledBack => {
                create(env, Kind::Error, c"ERR_SQLIVE_ROLLED_BACK", &message)
            }
            sqlive::Error::SnapshotEnded => {
                create(env, Kind::Error, c"ERR_SQLIVE_SNAPSHOT_ENDED", &message)
            }
            sqlive::Error::Unsupported(_) => {
                create(env, Kind::Error, c"ERR_SQLIVE_UNSUPPORTED", &message)
            }
            sqlive::Error::Sqlite(
                rusqlite::Error::SqliteFailure(failure, _)
                | rusqlite::Error::SqlInputError { error: failure, .. },
            ) => {
                let code = sqlite_code(failure.extended_code);
                create(env, Kind::Error, code, &message)
            }
            sqlive::Error::Sqlite(
                rusqlite::Error::InvalidParameterCount(..)
                | rusqlite::Error::InvalidParameterName(_)
                | rusqlite::Error::ToSqlConversionFailure(_),
            ) => create(env, Kind::Range, c"ERR_SQLIVE_PARAMETERS", &message),
            sqlive::Error::Sqlite(_) => create(env, Kind::Error, c"ERR_SQLIVE_SQLITE", &message),
            // `sqlive::Error` may grow variants.
            #[allow(unreachable_patterns)]
            _ => create(env, Kind::Error, c"ERR_SQLIVE", &message),
        }
    }
}

macro_rules! codes {
    ($code:expr; $($name:ident),* $(,)?) => {
        match $code {
            $(ffi::$name => {
                const NAME: &CStr = match CStr::from_bytes_with_nul(concat!(stringify!($name), "\0").as_bytes()) {
                    Ok(name) => name,
                    Err(_) => panic!("a code name holds no NUL"),
                };
                NAME
            })*
            _ => c"SQLITE_ERROR",
        }
    };
}

/// SQLite's name for an extended result code, or for its primary code when
/// the extended one is unknown.
fn sqlite_code(code: i32) -> &'static CStr {
    let extended = extended_code(code);
    if extended != c"SQLITE_ERROR" || code == ffi::SQLITE_ERROR {
        return extended;
    }
    extended_code(code & 0xff)
}

fn extended_code(code: i32) -> &'static CStr {
    codes!(code;
        SQLITE_ERROR, SQLITE_INTERNAL, SQLITE_PERM, SQLITE_ABORT, SQLITE_BUSY, SQLITE_LOCKED,
        SQLITE_NOMEM, SQLITE_READONLY, SQLITE_INTERRUPT, SQLITE_IOERR, SQLITE_CORRUPT,
        SQLITE_NOTFOUND, SQLITE_FULL, SQLITE_CANTOPEN, SQLITE_PROTOCOL, SQLITE_EMPTY,
        SQLITE_SCHEMA, SQLITE_TOOBIG, SQLITE_CONSTRAINT, SQLITE_MISMATCH, SQLITE_MISUSE,
        SQLITE_NOLFS, SQLITE_AUTH, SQLITE_FORMAT, SQLITE_RANGE, SQLITE_NOTADB, SQLITE_NOTICE,
        SQLITE_WARNING,
        SQLITE_ERROR_MISSING_COLLSEQ, SQLITE_ERROR_RETRY, SQLITE_ERROR_SNAPSHOT,
        SQLITE_IOERR_READ, SQLITE_IOERR_SHORT_READ, SQLITE_IOERR_WRITE, SQLITE_IOERR_FSYNC,
        SQLITE_IOERR_DIR_FSYNC, SQLITE_IOERR_TRUNCATE, SQLITE_IOERR_FSTAT, SQLITE_IOERR_UNLOCK,
        SQLITE_IOERR_RDLOCK, SQLITE_IOERR_DELETE, SQLITE_IOERR_BLOCKED, SQLITE_IOERR_NOMEM,
        SQLITE_IOERR_ACCESS, SQLITE_IOERR_CHECKRESERVEDLOCK, SQLITE_IOERR_LOCK,
        SQLITE_IOERR_CLOSE, SQLITE_IOERR_DIR_CLOSE, SQLITE_IOERR_SHMOPEN, SQLITE_IOERR_SHMSIZE,
        SQLITE_IOERR_SHMLOCK, SQLITE_IOERR_SHMMAP, SQLITE_IOERR_SEEK, SQLITE_IOERR_DELETE_NOENT,
        SQLITE_IOERR_MMAP, SQLITE_IOERR_GETTEMPPATH, SQLITE_IOERR_CONVPATH, SQLITE_IOERR_VNODE,
        SQLITE_IOERR_AUTH, SQLITE_IOERR_BEGIN_ATOMIC, SQLITE_IOERR_COMMIT_ATOMIC,
        SQLITE_IOERR_ROLLBACK_ATOMIC, SQLITE_IOERR_DATA, SQLITE_IOERR_CORRUPTFS,
        SQLITE_LOCKED_SHAREDCACHE, SQLITE_LOCKED_VTAB,
        SQLITE_BUSY_RECOVERY, SQLITE_BUSY_SNAPSHOT, SQLITE_BUSY_TIMEOUT,
        SQLITE_CANTOPEN_NOTEMPDIR, SQLITE_CANTOPEN_ISDIR, SQLITE_CANTOPEN_FULLPATH,
        SQLITE_CANTOPEN_CONVPATH, SQLITE_CANTOPEN_SYMLINK,
        SQLITE_CORRUPT_VTAB, SQLITE_CORRUPT_SEQUENCE, SQLITE_CORRUPT_INDEX,
        SQLITE_READONLY_RECOVERY, SQLITE_READONLY_CANTLOCK, SQLITE_READONLY_ROLLBACK,
        SQLITE_READONLY_DBMOVED, SQLITE_READONLY_CANTINIT, SQLITE_READONLY_DIRECTORY,
        SQLITE_ABORT_ROLLBACK,
        SQLITE_CONSTRAINT_CHECK, SQLITE_CONSTRAINT_COMMITHOOK, SQLITE_CONSTRAINT_FOREIGNKEY,
        SQLITE_CONSTRAINT_FUNCTION, SQLITE_CONSTRAINT_NOTNULL, SQLITE_CONSTRAINT_PRIMARYKEY,
        SQLITE_CONSTRAINT_TRIGGER, SQLITE_CONSTRAINT_UNIQUE, SQLITE_CONSTRAINT_VTAB,
        SQLITE_CONSTRAINT_ROWID, SQLITE_CONSTRAINT_PINNED, SQLITE_CONSTRAINT_DATATYPE,
        SQLITE_NOTICE_RECOVER_WAL, SQLITE_NOTICE_RECOVER_ROLLBACK,
        SQLITE_WARNING_AUTOINDEX, SQLITE_AUTH_USER,
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn codes_name_the_extended_code_or_fall_back_to_the_primary() {
        assert_eq!(
            sqlite_code(ffi::SQLITE_CONSTRAINT_UNIQUE),
            c"SQLITE_CONSTRAINT_UNIQUE"
        );
        assert_eq!(sqlite_code(ffi::SQLITE_BUSY), c"SQLITE_BUSY");
        assert_eq!(sqlite_code(ffi::SQLITE_ERROR), c"SQLITE_ERROR");
        // An extended code this table lacks names its primary code.
        assert_eq!(sqlite_code(ffi::SQLITE_IOERR | (200 << 8)), c"SQLITE_IOERR");
    }
}
