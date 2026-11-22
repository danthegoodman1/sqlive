//! Conversions between JavaScript values and SQLite values, written against
//! the raw Node-API so a query's rows cost one call per value.

use crate::errors::{Fail, Result};
use napi::sys;
use sqlive::rusqlite::types::ValueRef;
use sqlive::{ParamValues, Value};
use std::ffi::c_char;
use std::ptr;

/// The largest integer a JavaScript number holds exactly.
const MAX_SAFE: i64 = (1 << 53) - 1;

pub(crate) fn check(status: sys::napi_status) -> Result<()> {
    if status == sys::Status::napi_ok {
        Ok(())
    } else {
        Err(Fail::Napi(napi::Error::new(
            napi::Status::from(status),
            "a Node-API call failed",
        )))
    }
}

pub(crate) fn type_error(message: impl Into<String>) -> Fail {
    Fail::Type(message.into())
}

/// How integers come out of the database: as numbers, which must fit in 53
/// bits, or always as bigints.
#[derive(Clone, Copy)]
pub struct Integers {
    pub big: bool,
}

pub(crate) unsafe fn null(env: sys::napi_env) -> Result<sys::napi_value> {
    let mut out = ptr::null_mut();
    check(unsafe { sys::napi_get_null(env, &mut out) })?;
    Ok(out)
}

pub(crate) unsafe fn undefined(env: sys::napi_env) -> Result<sys::napi_value> {
    let mut out = ptr::null_mut();
    check(unsafe { sys::napi_get_undefined(env, &mut out) })?;
    Ok(out)
}

pub(crate) unsafe fn string(env: sys::napi_env, text: &str) -> Result<sys::napi_value> {
    unsafe { utf8(env, text.as_bytes()) }
}

/// A string from UTF-8 bytes. V8 replaces invalid sequences with U+FFFD.
unsafe fn utf8(env: sys::napi_env, bytes: &[u8]) -> Result<sys::napi_value> {
    let mut out = ptr::null_mut();
    check(unsafe {
        sys::napi_create_string_utf8(
            env,
            bytes.as_ptr().cast::<c_char>(),
            bytes.len() as isize,
            &mut out,
        )
    })?;
    Ok(out)
}

pub(crate) unsafe fn number(env: sys::napi_env, value: f64) -> Result<sys::napi_value> {
    let mut out = ptr::null_mut();
    check(unsafe { sys::napi_create_double(env, value, &mut out) })?;
    Ok(out)
}

/// An integer as a number when it fits in 53 bits, as a bigint when
/// `integers.big` asks for one, and a `RangeError` otherwise.
pub(crate) unsafe fn integer(
    env: sys::napi_env,
    value: i64,
    integers: Integers,
) -> Result<sys::napi_value> {
    let mut out = ptr::null_mut();
    if integers.big {
        check(unsafe { sys::napi_create_bigint_int64(env, value, &mut out) })?;
    } else if (-MAX_SAFE..=MAX_SAFE).contains(&value) {
        check(unsafe { sys::napi_create_int64(env, value, &mut out) })?;
    } else {
        return Err(Fail::Range(format!(
            "the integer {value} does not fit in a JavaScript number; \
             open the database with `safeIntegers: true` to read integers as bigints"
        )));
    }
    Ok(out)
}

/// A SQLite value as JavaScript: `null`, a number or bigint, a string, or a
/// `Buffer`.
pub(crate) unsafe fn to_js(
    env: sys::napi_env,
    value: ValueRef<'_>,
    integers: Integers,
) -> Result<sys::napi_value> {
    unsafe {
        match value {
            ValueRef::Null => null(env),
            ValueRef::Integer(i) => integer(env, i, integers),
            ValueRef::Real(r) => number(env, r),
            ValueRef::Text(bytes) => utf8(env, bytes),
            ValueRef::Blob(bytes) => {
                let mut out = ptr::null_mut();
                let mut data = ptr::null_mut();
                check(sys::napi_create_buffer_copy(
                    env,
                    bytes.len(),
                    bytes.as_ptr().cast(),
                    &mut data,
                    &mut out,
                ))?;
                Ok(out)
            }
        }
    }
}

pub(crate) unsafe fn owned_to_js(
    env: sys::napi_env,
    value: &Value,
    integers: Integers,
) -> Result<sys::napi_value> {
    unsafe { to_js(env, ValueRef::from(value), integers) }
}

pub(crate) unsafe fn type_of(
    env: sys::napi_env,
    value: sys::napi_value,
) -> Result<sys::napi_valuetype> {
    let mut kind = 0;
    check(unsafe { sys::napi_typeof(env, value, &mut kind) })?;
    Ok(kind)
}

pub(crate) unsafe fn get_string(env: sys::napi_env, value: sys::napi_value) -> Result<String> {
    let mut len = 0;
    check(unsafe { sys::napi_get_value_string_utf8(env, value, ptr::null_mut(), 0, &mut len) })?;
    let mut bytes = vec![0u8; len + 1];
    let mut written = 0;
    check(unsafe {
        sys::napi_get_value_string_utf8(
            env,
            value,
            bytes.as_mut_ptr().cast::<c_char>(),
            bytes.len(),
            &mut written,
        )
    })?;
    bytes.truncate(written);
    // SAFETY: Node writes UTF-8, replacing lone surrogates.
    Ok(unsafe { String::from_utf8_unchecked(bytes) })
}

/// The bytes of a `Buffer`, typed array, `DataView` or `ArrayBuffer`, or
/// `None` for any other object.
unsafe fn bytes_of(env: sys::napi_env, value: sys::napi_value) -> Result<Option<Vec<u8>>> {
    let mut is = false;
    let mut data: *mut std::ffi::c_void = ptr::null_mut();
    let mut len = 0usize;
    unsafe {
        check(sys::napi_is_typedarray(env, value, &mut is))?;
        if is {
            let mut kind = 0;
            let mut buffer = ptr::null_mut();
            let mut offset = 0;
            check(sys::napi_get_typedarray_info(
                env,
                value,
                &mut kind,
                &mut len,
                &mut data,
                &mut buffer,
                &mut offset,
            ))?;
            len *= element_size(kind);
            return Ok(Some(copy(data, len)));
        }
        check(sys::napi_is_dataview(env, value, &mut is))?;
        if is {
            let mut buffer = ptr::null_mut();
            let mut offset = 0;
            check(sys::napi_get_dataview_info(
                env,
                value,
                &mut len,
                &mut data,
                &mut buffer,
                &mut offset,
            ))?;
            return Ok(Some(copy(data, len)));
        }
        check(sys::napi_is_arraybuffer(env, value, &mut is))?;
        if is {
            check(sys::napi_get_arraybuffer_info(
                env, value, &mut data, &mut len,
            ))?;
            return Ok(Some(copy(data, len)));
        }
    }
    Ok(None)
}

fn element_size(kind: sys::napi_typedarray_type) -> usize {
    use sys::TypedarrayType as T;
    match kind {
        T::int8_array | T::uint8_array | T::uint8_clamped_array => 1,
        T::int16_array | T::uint16_array => 2,
        T::int32_array | T::uint32_array | T::float32_array => 4,
        _ => 8,
    }
}

unsafe fn copy(data: *const std::ffi::c_void, len: usize) -> Vec<u8> {
    if len == 0 || data.is_null() {
        return Vec::new();
    }
    unsafe { std::slice::from_raw_parts(data.cast::<u8>(), len) }.to_vec()
}

/// A JavaScript value as a SQLite value. Integral numbers within 53 bits
/// bind as integers, other numbers as reals; `-0` stays a real, so its sign
/// survives.
pub(crate) unsafe fn from_js(env: sys::napi_env, value: sys::napi_value) -> Result<Value> {
    unsafe {
        Ok(match type_of(env, value)? {
            sys::ValueType::napi_undefined | sys::ValueType::napi_null => Value::Null,
            sys::ValueType::napi_number => {
                let mut n = 0f64;
                check(sys::napi_get_value_double(env, value, &mut n))?;
                if n.fract() == 0.0
                    && n.abs() <= MAX_SAFE as f64
                    && !(n == 0.0 && n.is_sign_negative())
                {
                    Value::Integer(n as i64)
                } else {
                    Value::Real(n)
                }
            }
            sys::ValueType::napi_boolean => {
                let mut b = false;
                check(sys::napi_get_value_bool(env, value, &mut b))?;
                Value::Integer(i64::from(b))
            }
            sys::ValueType::napi_string => Value::Text(get_string(env, value)?),
            sys::ValueType::napi_bigint => {
                let mut i = 0i64;
                let mut lossless = false;
                check(sys::napi_get_value_bigint_int64(
                    env,
                    value,
                    &mut i,
                    &mut lossless,
                ))?;
                if !lossless {
                    return Err(Fail::Range(
                        "a bigint parameter does not fit in SQLite's 64-bit integers".to_owned(),
                    ));
                }
                Value::Integer(i)
            }
            sys::ValueType::napi_object => match bytes_of(env, value)? {
                Some(bytes) => Value::Blob(bytes),
                None => {
                    return Err(type_error(
                        "SQLite binds null, numbers, bigints, strings, booleans and byte arrays; \
                         convert other objects first",
                    ));
                }
            },
            _ => {
                return Err(type_error(
                    "SQLite binds null, numbers, bigints, strings, booleans and byte arrays",
                ));
            }
        })
    }
}

/// A query's parameters: `undefined` for none, an array for `?`, `?N`
/// positions, or an object for named parameters. An object key may carry
/// its prefix (`:`, `@` or `$`); a bare key takes the prefix the SQL uses
/// for it.
pub(crate) unsafe fn params(
    env: sys::napi_env,
    sql: &str,
    value: sys::napi_value,
) -> Result<ParamValues> {
    unsafe {
        match type_of(env, value)? {
            sys::ValueType::napi_undefined | sys::ValueType::napi_null => {
                return Ok(ParamValues::default());
            }
            sys::ValueType::napi_object => {}
            _ => {
                return Err(type_error(
                    "parameters must be an array of positional values or an object of named ones",
                ));
            }
        }
        let mut is_array = false;
        check(sys::napi_is_array(env, value, &mut is_array))?;
        if is_array {
            let mut len = 0u32;
            check(sys::napi_get_array_length(env, value, &mut len))?;
            let mut positional = Vec::with_capacity(len as usize);
            for i in 0..len {
                let mut element = ptr::null_mut();
                check(sys::napi_get_element(env, value, i, &mut element))?;
                positional.push(from_js(env, element)?);
            }
            return Ok(ParamValues {
                positional,
                named: Vec::new(),
            });
        }
        let mut keys = ptr::null_mut();
        check(sys::napi_get_all_property_names(
            env,
            value,
            sys::KeyCollectionMode::own_only,
            sys::KeyFilter::enumerable | sys::KeyFilter::skip_symbols,
            sys::KeyConversion::numbers_to_strings,
            &mut keys,
        ))?;
        let mut len = 0u32;
        check(sys::napi_get_array_length(env, keys, &mut len))?;
        let mut named = Vec::with_capacity(len as usize);
        for i in 0..len {
            let mut key = ptr::null_mut();
            check(sys::napi_get_element(env, keys, i, &mut key))?;
            let mut element = ptr::null_mut();
            check(sys::napi_get_property(env, value, key, &mut element))?;
            let name = get_string(env, key)?;
            named.push((prefixed(sql, name), from_js(env, element)?));
        }
        Ok(ParamValues {
            positional: Vec::new(),
            named,
        })
    }
}

/// The parameter name `key` stands for in `sql`: `key` itself if it starts
/// with a prefix, otherwise `key` with the first prefix the SQL uses it
/// with, or `:` if the SQL uses none.
fn prefixed(sql: &str, key: String) -> String {
    if key.starts_with([':', '@', '$']) {
        return key;
    }
    let bytes = sql.as_bytes();
    let first = (*b":@$")
        .into_iter()
        .filter_map(|prefix| {
            let mut from = 0;
            while let Some(at) = find(&bytes[from..], prefix, key.as_bytes()) {
                let at = from + at;
                if bytes
                    .get(at + 1 + key.len())
                    .is_none_or(|b| !is_name_byte(*b))
                {
                    return Some((at, prefix));
                }
                from = at + 1;
            }
            None
        })
        .min();
    let prefix = first.map_or(':', |(_, prefix)| char::from(prefix));
    format!("{prefix}{key}")
}

fn find(haystack: &[u8], prefix: u8, name: &[u8]) -> Option<usize> {
    haystack
        .windows(name.len() + 1)
        .position(|window| window[0] == prefix && &window[1..] == name)
}

fn is_name_byte(b: u8) -> bool {
    b.is_ascii_alphanumeric() || b == b'_' || b == b'$' || b >= 0x80
}

#[cfg(test)]
mod tests {
    use super::prefixed;

    #[test]
    fn a_bare_key_takes_the_prefix_the_sql_uses() {
        let sql = "SELECT * FROM t WHERE a = @list AND b = $room AND c = :name";
        assert_eq!(prefixed(sql, "list".into()), "@list");
        assert_eq!(prefixed(sql, "room".into()), "$room");
        assert_eq!(prefixed(sql, "name".into()), ":name");
        assert_eq!(prefixed(sql, "missing".into()), ":missing");
        assert_eq!(prefixed(sql, "@kept".into()), "@kept");
        // A longer name that starts with the key does not count.
        assert_eq!(prefixed("SELECT :listing, @list", "list".into()), "@list");
    }
}
