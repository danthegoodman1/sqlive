//! Result hashes: a BLAKE3 digest of a query's rows, so an app can tell
//! whether a re-run changed its result without keeping the rows.

use rusqlite::types::ValueRef;
use std::fmt;

/// A digest of a query's result: its column names, then each row in order,
/// and each value in it with its storage class. Any difference in a column
/// name, a value, its storage class, the row order or the row count changes
/// the hash, and the same result hashes the same in every process.
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

/// Hashes a result as its rows stream out of SQLite.
pub(crate) struct RowHasher {
    hasher: blake3::Hasher,
    buffer: Vec<u8>,
    /// The column count, once the header holding it and the names is
    /// written; the count marks where each row ends.
    columns: Option<usize>,
}

impl RowHasher {
    /// Starts a hash that gathers bytes in `buffer`, which `finish` returns
    /// for the next result.
    pub(crate) fn new(mut buffer: Vec<u8>) -> RowHasher {
        buffer.clear();
        RowHasher {
            hasher: blake3::Hasher::new_derive_key("sqlive 2026-10-26 query result hash"),
            buffer,
            columns: None,
        }
    }

    /// Writes the column count and each name with its length. Called once
    /// the statement has stepped: a statement prepared under an older
    /// schema reports that schema's columns until its first step.
    fn header(&mut self, statement: &rusqlite::Statement<'_>) -> rusqlite::Result<usize> {
        let count = statement.column_count();
        self.buffer.extend_from_slice(&(count as u64).to_le_bytes());
        for i in 0..count {
            let name = statement.column_name(i)?;
            self.buffer
                .extend_from_slice(&(name.len() as u64).to_le_bytes());
            self.buffer.extend_from_slice(name.as_bytes());
        }
        self.columns = Some(count);
        Ok(count)
    }

    pub(crate) fn row(&mut self, row: &rusqlite::Row<'_>) -> rusqlite::Result<()> {
        let columns = match self.columns {
            Some(columns) => columns,
            None => self.header(row.as_ref())?,
        };
        for i in 0..columns {
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

    /// Ends the hash of `statement`'s result, which has stepped to its end.
    pub(crate) fn finish(
        mut self,
        statement: &rusqlite::Statement<'_>,
    ) -> rusqlite::Result<(ResultHash, Vec<u8>)> {
        if self.columns.is_none() {
            self.header(statement)?;
        }
        self.flush();
        Ok((ResultHash(*self.hasher.finalize().as_bytes()), self.buffer))
    }
}
