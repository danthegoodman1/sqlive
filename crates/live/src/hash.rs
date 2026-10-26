//! Result hashes: a BLAKE3 digest of a query's rows, so an app can tell
//! whether a re-run changed its result without keeping the rows.

use rusqlite::types::ValueRef;
use std::fmt;

/// A digest of a query's rows: each row in order, and each value in it with
/// its storage class. Any difference in a value, its storage class, the
/// row order or the row count changes the hash, and the same rows hash the
/// same in every process.
#[derive(Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct ResultHash([u8; 32]);

impl ResultHash {
    pub fn from_bytes(bytes: [u8; 32]) -> ResultHash {
        ResultHash(bytes)
    }

    pub fn as_bytes(&self) -> &[u8; 32] {
        &self.0
    }

    /// Reads the 64 hex digits `Display` writes, in either case.
    pub fn from_hex(hex: &str) -> Option<ResultHash> {
        let digits = hex.as_bytes();
        if digits.len() != 64 {
            return None;
        }
        let mut bytes = [0u8; 32];
        for (byte, pair) in bytes.iter_mut().zip(digits.chunks(2)) {
            let high = char::from(pair[0]).to_digit(16)?;
            let low = char::from(pair[1]).to_digit(16)?;
            *byte = (high * 16 + low) as u8;
        }
        Some(ResultHash(bytes))
    }
}

/// Lowercase hex.
impl fmt::Display for ResultHash {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        for byte in self.0 {
            write!(f, "{byte:02x}")?;
        }
        Ok(())
    }
}

impl fmt::Debug for ResultHash {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "ResultHash({self})")
    }
}

/// Bytes gathered before each BLAKE3 update. BLAKE3 hashes a large input
/// several chunks at a time, and a small one a block at a time.
const BUFFER: usize = 16 * 1024;

/// Marks the start of a row, apart from the storage-class tags.
const ROW: u8 = 5;

/// Hashes a result as its rows stream out of SQLite.
pub(crate) struct RowHasher {
    hasher: blake3::Hasher,
    buffer: Vec<u8>,
}

impl RowHasher {
    /// Starts a hash that gathers bytes in `buffer`, which `finish` returns
    /// for the next result.
    pub(crate) fn new(mut buffer: Vec<u8>) -> RowHasher {
        buffer.clear();
        RowHasher {
            hasher: blake3::Hasher::new_derive_key("sqlive 2026-10-26 query result hash"),
            buffer,
        }
    }

    pub(crate) fn row(&mut self, row: &rusqlite::Row<'_>) -> rusqlite::Result<()> {
        self.buffer.push(ROW);
        for i in 0..row.as_ref().column_count() {
            self.value(row.get_ref(i)?);
        }
        Ok(())
    }

    /// Writes a storage-class tag and the value; text and blobs carry their
    /// length, so every value ends where the encoding says it does.
    fn value(&mut self, value: ValueRef<'_>) {
        match value {
            ValueRef::Null => self.buffer.push(0),
            ValueRef::Integer(i) => {
                self.buffer.push(1);
                self.buffer.extend_from_slice(&i.to_le_bytes());
            }
            // By bits, so 0.0 and -0.0 differ, as they do in commit events.
            ValueRef::Real(r) => {
                self.buffer.push(2);
                self.buffer.extend_from_slice(&r.to_bits().to_le_bytes());
            }
            ValueRef::Text(bytes) => self.bytes(3, bytes),
            ValueRef::Blob(bytes) => self.bytes(4, bytes),
        }
        if self.buffer.len() >= BUFFER {
            self.flush();
        }
    }

    fn bytes(&mut self, tag: u8, bytes: &[u8]) {
        self.buffer.push(tag);
        self.buffer
            .extend_from_slice(&(bytes.len() as u64).to_le_bytes());
        if bytes.len() >= BUFFER {
            // A large value goes to BLAKE3 directly instead of being copied.
            self.flush();
            self.hasher.update(bytes);
        } else {
            self.buffer.extend_from_slice(bytes);
        }
    }

    fn flush(&mut self) {
        self.hasher.update(&self.buffer);
        self.buffer.clear();
    }

    pub(crate) fn finish(mut self) -> (ResultHash, Vec<u8>) {
        self.flush();
        (ResultHash(*self.hasher.finalize().as_bytes()), self.buffer)
    }
}
