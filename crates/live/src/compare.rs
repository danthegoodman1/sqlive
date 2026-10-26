//! SQLite's rules for comparing values: equality under the built-in
//! collations, exact integer-to-real equality, and column affinity.

use rusqlite::types::ValueRef;

/// A collating sequence. SqLive applies the built-in ones exactly; a read
/// set treats a column with any other collation as unconstrained.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum Collation {
    Binary,
    NoCase,
    Rtrim,
    Other(String),
}

impl Collation {
    pub fn from_name(name: &str) -> Collation {
        if name.eq_ignore_ascii_case("BINARY") {
            Collation::Binary
        } else if name.eq_ignore_ascii_case("NOCASE") {
            Collation::NoCase
        } else if name.eq_ignore_ascii_case("RTRIM") {
            Collation::Rtrim
        } else {
            Collation::Other(name.to_owned())
        }
    }

    pub fn name(&self) -> &str {
        match self {
            Collation::Binary => "BINARY",
            Collation::NoCase => "NOCASE",
            Collation::Rtrim => "RTRIM",
            Collation::Other(name) => name,
        }
    }

    pub fn is_builtin(&self) -> bool {
        !matches!(self, Collation::Other(_))
    }
}

/// A column's type affinity.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum Affinity {
    /// No affinity: values are stored and compared as they come.
    Blob,
    Text,
    Numeric,
    Integer,
    Real,
}

impl Affinity {
    /// The affinity SQLite gives a column declared with this type.
    pub fn from_declared(declared: &str, strict: bool) -> Affinity {
        let upper = declared.to_ascii_uppercase();
        if strict && upper == "ANY" {
            return Affinity::Blob;
        }
        if upper.contains("INT") {
            Affinity::Integer
        } else if upper.contains("CHAR") || upper.contains("CLOB") || upper.contains("TEXT") {
            Affinity::Text
        } else if upper.contains("BLOB") || upper.is_empty() {
            Affinity::Blob
        } else if upper.contains("REAL") || upper.contains("FLOA") || upper.contains("DOUB") {
            Affinity::Real
        } else {
            Affinity::Numeric
        }
    }

    pub fn is_numeric(self) -> bool {
        matches!(self, Affinity::Numeric | Affinity::Integer | Affinity::Real)
    }
}

/// Text bytes as a collation sees them: NOCASE folds ASCII letters only,
/// and RTRIM drops trailing spaces.
fn fold(text: &[u8], collation: &Collation) -> Vec<u8> {
    match collation {
        Collation::NoCase => text.iter().map(u8::to_ascii_lowercase).collect(),
        Collation::Rtrim => {
            let end = text.iter().rposition(|&b| b != b' ').map_or(0, |i| i + 1);
            text[..end].to_vec()
        }
        Collation::Binary | Collation::Other(_) => text.to_vec(),
    }
}

/// A value reduced to what SQLite's equality sees under a collation: two
/// values compare equal exactly when their keys are equal.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub(crate) enum EqKey {
    Null,
    Int(i64),
    /// A real with a fraction, or outside the integer range, by its bits.
    Real(u64),
    Text(Vec<u8>),
    Blob(Vec<u8>),
}

pub(crate) fn eq_key(value: ValueRef<'_>, collation: &Collation) -> EqKey {
    match value {
        ValueRef::Null => EqKey::Null,
        ValueRef::Integer(i) => EqKey::Int(i),
        ValueRef::Real(r) => {
            if r.fract() == 0.0
                && (-9_223_372_036_854_775_808.0..9_223_372_036_854_775_808.0).contains(&r)
            {
                EqKey::Int(r as i64)
            } else {
                EqKey::Real(r.to_bits())
            }
        }
        ValueRef::Text(text) => EqKey::Text(fold(text, collation)),
        ValueRef::Blob(bytes) => EqKey::Blob(bytes.to_vec()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rusqlite::types::Value;

    #[test]
    fn equal_keys_follow_sqlite_equality() {
        let key = |v: Value, c: Collation| eq_key(ValueRef::from(&v), &c);
        assert_eq!(
            key(Value::Real(5.0), Collation::Binary),
            key(Value::Integer(5), Collation::Binary)
        );
        assert_ne!(
            key(Value::Real(9_007_199_254_740_992.0), Collation::Binary),
            key(Value::Integer(9_007_199_254_740_993), Collation::Binary)
        );
        assert_eq!(key(Value::Real(-0.0), Collation::Binary), EqKey::Int(0));
        assert_eq!(
            key(Value::Text("ABC".into()), Collation::NoCase),
            key(Value::Text("abc".into()), Collation::NoCase)
        );
        assert_ne!(
            key(Value::Text("ABC".into()), Collation::Binary),
            key(Value::Text("abc".into()), Collation::Binary)
        );
        assert_eq!(
            key(Value::Text("abc  ".into()), Collation::Rtrim),
            key(Value::Text("abc".into()), Collation::Rtrim)
        );
        assert_ne!(
            key(Value::Text("1".into()), Collation::Binary),
            key(Value::Integer(1), Collation::Binary)
        );
    }

    #[test]
    fn declared_types_map_to_sqlite_affinities() {
        for (declared, affinity) in [
            ("INTEGER", Affinity::Integer),
            ("BIGINT", Affinity::Integer),
            ("VARCHAR(10)", Affinity::Text),
            ("CLOB", Affinity::Text),
            ("", Affinity::Blob),
            ("BLOB", Affinity::Blob),
            ("DOUBLE PRECISION", Affinity::Real),
            ("FLOATING POINT", Affinity::Integer),
            ("DECIMAL(10,5)", Affinity::Numeric),
            ("STRING", Affinity::Numeric),
        ] {
            assert_eq!(
                Affinity::from_declared(declared, false),
                affinity,
                "{declared}"
            );
        }
        assert_eq!(Affinity::from_declared("ANY", true), Affinity::Blob);
        assert_eq!(Affinity::from_declared("ANY", false), Affinity::Numeric);
    }
}
