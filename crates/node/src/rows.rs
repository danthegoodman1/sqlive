//! Query rows packed for JavaScript in a few Node-API calls, whatever their
//! count. A tag per value says where it lives: numbers and bigints in an
//! 8-byte slot of a buffer, text in shared strings that `index.js` slices by
//! each value's length, and blobs in a side array. Text packs into strings
//! of at most `CHUNK` UTF-16 units, so a slice JavaScript keeps retains at
//! most one chunk.

use crate::errors::{Fail, Result};
use crate::values::{Integers, check, string};
use napi::sys;
use sqlive::rusqlite::{self, types::ValueRef};
use std::ffi::{c_char, c_void};
use std::ptr;

/// The largest integer a JavaScript number holds exactly.
const MAX_SAFE: i64 = (1 << 53) - 1;

/// UTF-16 units a text chunk holds, unless one value alone is longer.
const CHUNK: usize = 4096;

/// Tags, as `index.js` reads them.
const NULL: u8 = 0;
const NUMBER: u8 = 1;
const TEXT: u8 = 2;
const BLOB: u8 = 3;
const BIGINT: u8 = 4;

/// The buffer's header: cells, slots, texts and columns, then two numbers
/// the caller passes, each as a float64.
const HEADER: usize = 6;

/// Rows packed on any thread; `finish` hands them to JavaScript. Each
/// thread keeps one `Packed` to reuse, so a query's buffers seldom
/// allocate.
#[derive(Default)]
pub struct Packed {
    big: bool,
    columns: usize,
    keyed: bool,
    key: String,
    tags: Vec<u8>,
    /// Numbers as float64 bits, or integers as int64 when `big`.
    slots: Vec<u64>,
    lengths: Vec<u32>,
    /// The first `used` hold this result's text; the rest are spare.
    chunks: Vec<Chunk>,
    used: usize,
    blobs: Vec<Vec<u8>>,
    /// An integer too large for a number, which fails the conversion.
    out_of_range: Option<i64>,
}

/// The bytes a reused `Packed` may keep, so one large result does not hold
/// its memory for good.
const KEEP: usize = 1 << 20;

thread_local! {
    static SPARE: std::cell::RefCell<Option<Packed>> = const { std::cell::RefCell::new(None) };
}

struct Chunk {
    text: String,
    units: usize,
    ascii: bool,
}

impl Chunk {
    unsafe fn to_js(&self, env: sys::napi_env) -> Result<sys::napi_value> {
        let bytes = self.text.as_bytes();
        let mut text = ptr::null_mut();
        check(unsafe {
            if self.ascii {
                sys::napi_create_string_latin1(
                    env,
                    bytes.as_ptr().cast::<c_char>(),
                    bytes.len() as isize,
                    &mut text,
                )
            } else {
                sys::napi_create_string_utf8(
                    env,
                    bytes.as_ptr().cast::<c_char>(),
                    bytes.len() as isize,
                    &mut text,
                )
            }
        })?;
        Ok(text)
    }
}

impl Packed {
    /// This thread's spare `Packed`, or a new one.
    pub(crate) fn new(integers: Integers) -> Packed {
        let mut packed = SPARE
            .with(|spare| spare.borrow_mut().take())
            .unwrap_or_default();
        packed.big = integers.big;
        packed
    }

    fn capacity(&self) -> usize {
        self.tags.capacity()
            + self.slots.capacity() * 8
            + self.lengths.capacity() * 4
            + self
                .chunks
                .iter()
                .map(|chunk| chunk.text.capacity())
                .sum::<usize>()
    }

    pub(crate) fn push(&mut self, row: &rusqlite::Row<'_>) -> rusqlite::Result<()> {
        if !self.keyed {
            // Read once the statement has stepped: a statement prepared
            // under an older schema reports its columns until then.
            let statement = row.as_ref();
            self.columns = statement.column_count();
            for i in 0..self.columns {
                if i > 0 {
                    self.key.push('\0');
                }
                self.key.push_str(statement.column_name(i)?);
            }
            self.keyed = true;
        }
        for i in 0..self.columns {
            self.value(row.get_ref(i)?);
        }
        Ok(())
    }

    fn value(&mut self, value: ValueRef<'_>) {
        match value {
            ValueRef::Null => self.tags.push(NULL),
            ValueRef::Integer(i) if self.big => {
                self.tags.push(BIGINT);
                self.slots.push(i as u64);
            }
            ValueRef::Integer(i) => {
                if !(-MAX_SAFE..=MAX_SAFE).contains(&i) {
                    self.out_of_range.get_or_insert(i);
                }
                self.tags.push(NUMBER);
                self.slots.push((i as f64).to_bits());
            }
            ValueRef::Real(r) => {
                self.tags.push(NUMBER);
                self.slots.push(r.to_bits());
            }
            ValueRef::Text(bytes) => {
                self.tags.push(TEXT);
                self.text(bytes);
            }
            ValueRef::Blob(bytes) => {
                self.tags.push(BLOB);
                self.blobs.push(bytes.to_vec());
            }
        }
    }

    fn text(&mut self, bytes: &[u8]) {
        let lossy;
        let (text, units, ascii) = if bytes.is_ascii() {
            // SAFETY: ASCII is UTF-8.
            (
                unsafe { std::str::from_utf8_unchecked(bytes) },
                bytes.len(),
                true,
            )
        } else {
            lossy = String::from_utf8_lossy(bytes);
            (lossy.as_ref(), lossy.encode_utf16().count(), false)
        };
        // An empty value fits any chunk, so no chunk holds nothing but
        // empty values after the first.
        let fits =
            self.used > 0 && (units == 0 || self.chunks[self.used - 1].units + units <= CHUNK);
        if !fits {
            if self.used == self.chunks.len() {
                self.chunks.push(Chunk {
                    text: String::new(),
                    units: 0,
                    ascii: true,
                });
            }
            let chunk = &mut self.chunks[self.used];
            chunk.text.clear();
            chunk.units = 0;
            chunk.ascii = true;
            self.used += 1;
        }
        let chunk = &mut self.chunks[self.used - 1];
        chunk.text.push_str(text);
        chunk.units += units;
        chunk.ascii &= ascii;
        self.lengths.push(units as u32);
    }

    /// Drops the packed rows, as an unchanged re-run does.
    pub(crate) fn clear(&mut self) {
        self.keyed = false;
        self.columns = 0;
        self.key.clear();
        self.tags.clear();
        self.slots.clear();
        self.lengths.clear();
        self.used = 0;
        self.blobs.clear();
        self.out_of_range = None;
    }

    /// Hands the rows to JavaScript as `[buffer, chunks, columns, blobs]`:
    /// `chunks` is `undefined` without text, a string for one chunk, and an
    /// array for more.
    /// The buffer is `undefined` when the rows fit in `scratch`; otherwise a
    /// new `ArrayBuffer` that `index.js` keeps as its next scratch. `extra`
    /// fills the header's last three slots.
    pub(crate) unsafe fn finish(
        mut self,
        env: sys::napi_env,
        scratch: Option<(*mut u8, usize)>,
        extra: [f64; 2],
    ) -> Result<[sys::napi_value; 4]> {
        if let Some(i) = self.out_of_range {
            return Err(Fail::Range(format!(
                "the integer {i} does not fit in a JavaScript number; \
                 open the database with `safeIntegers: true` to read integers as bigints"
            )));
        }
        let size = (HEADER + self.slots.len()) * 8 + self.lengths.len() * 4 + self.tags.len();
        unsafe {
            let mut buffer = ptr::null_mut();
            let data = match scratch {
                Some((data, len)) if len >= size => {
                    check(sys::napi_get_undefined(env, &mut buffer))?;
                    data
                }
                _ => {
                    let mut data: *mut c_void = ptr::null_mut();
                    let len = size.next_power_of_two().max(4096);
                    check(sys::napi_create_arraybuffer(
                        env,
                        len,
                        &mut data,
                        &mut buffer,
                    ))?;
                    data.cast::<u8>()
                }
            };
            let header = [
                self.tags.len() as f64,
                self.slots.len() as f64,
                self.lengths.len() as f64,
                self.columns as f64,
                extra[0],
                extra[1],
            ];
            let mut at = 0;
            for value in header {
                ptr::copy_nonoverlapping(value.to_le_bytes().as_ptr(), data.add(at), 8);
                at += 8;
            }
            ptr::copy_nonoverlapping(
                self.slots.as_ptr().cast::<u8>(),
                data.add(at),
                self.slots.len() * 8,
            );
            at += self.slots.len() * 8;
            ptr::copy_nonoverlapping(
                self.lengths.as_ptr().cast::<u8>(),
                data.add(at),
                self.lengths.len() * 4,
            );
            at += self.lengths.len() * 4;
            ptr::copy_nonoverlapping(self.tags.as_ptr(), data.add(at), self.tags.len());

            // One chunk, the usual case, crosses as a bare string.
            let mut chunks = ptr::null_mut();
            match &self.chunks[..self.used] {
                [] => check(sys::napi_get_undefined(env, &mut chunks))?,
                [chunk] => chunks = chunk.to_js(env)?,
                all => {
                    check(sys::napi_create_array_with_length(
                        env,
                        all.len(),
                        &mut chunks,
                    ))?;
                    for (i, chunk) in all.iter().enumerate() {
                        check(sys::napi_set_element(
                            env,
                            chunks,
                            i as u32,
                            chunk.to_js(env)?,
                        ))?;
                    }
                }
            }
            let key = string(env, &self.key)?;
            let mut blobs = ptr::null_mut();
            if self.blobs.is_empty() {
                check(sys::napi_get_undefined(env, &mut blobs))?;
            } else {
                check(sys::napi_create_array_with_length(
                    env,
                    self.blobs.len(),
                    &mut blobs,
                ))?;
                for (i, blob) in self.blobs.iter().enumerate() {
                    let mut value = ptr::null_mut();
                    let mut copied = ptr::null_mut();
                    check(sys::napi_create_buffer_copy(
                        env,
                        blob.len(),
                        blob.as_ptr().cast(),
                        &mut copied,
                        &mut value,
                    ))?;
                    check(sys::napi_set_element(env, blobs, i as u32, value))?;
                }
            }
            self.clear();
            Ok([buffer, chunks, key, blobs])
        }
    }
}

/// Keeps the buffers as this thread's spare, unless it has one or they
/// grew past `KEEP`.
impl Drop for Packed {
    fn drop(&mut self) {
        if self.capacity() > KEEP {
            return;
        }
        let _ = SPARE.try_with(|spare| {
            if let Ok(mut spare) = spare.try_borrow_mut()
                && spare.is_none()
            {
                self.clear();
                *spare = Some(std::mem::take(self));
            }
        });
    }
}

/// The scratch `ArrayBuffer` `index.js` passes, as a pointer and length;
/// `None` for any other value.
pub(crate) unsafe fn scratch(
    env: sys::napi_env,
    buffer: sys::napi_value,
) -> Result<Option<(*mut u8, usize)>> {
    let mut is = false;
    unsafe {
        check(sys::napi_is_arraybuffer(env, buffer, &mut is))?;
        if !is {
            return Ok(None);
        }
        let mut data: *mut c_void = ptr::null_mut();
        let mut len = 0;
        check(sys::napi_get_arraybuffer_info(
            env, buffer, &mut data, &mut len,
        ))?;
        Ok(Some((data.cast(), len)))
    }
}
