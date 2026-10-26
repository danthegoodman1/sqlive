//! Random schemas: rowid tables with and without `INTEGER PRIMARY KEY`,
//! composite keys, `WITHOUT ROWID` and `STRICT` tables, generated columns,
//! declared affinities and collations, unique constraints, foreign keys with
//! cascades, triggers, and indexes.

use crate::prng::Prng;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TableKind {
    /// A rowid table with no declared key.
    Rowid,
    /// `id INTEGER PRIMARY KEY`, an alias for the rowid.
    IntegerKey,
    /// A rowid table with a composite `PRIMARY KEY`.
    CompositeKey,
    WithoutRowid,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Generated {
    No,
    Virtual,
    Stored,
}

#[derive(Clone, Debug)]
pub struct ColumnSpec {
    pub name: String,
    /// The declared type, possibly empty.
    pub declared: &'static str,
    pub collation: Option<&'static str>,
    pub generated: Generated,
    pub not_null: bool,
    /// Holds the parent's `id` when set.
    pub references: Option<usize>,
}

#[derive(Clone, Debug)]
pub struct TableSpec {
    pub name: String,
    pub kind: TableKind,
    pub strict: bool,
    pub columns: Vec<ColumnSpec>,
    /// Indexes into `columns`.
    pub key: Vec<usize>,
    /// A collation for each key column in `PRIMARY KEY (...)`, which can
    /// differ from the column's own.
    pub key_collations: Vec<Option<&'static str>>,
    pub unique: Option<usize>,
}

impl TableSpec {
    pub fn has_rowid(&self) -> bool {
        self.kind != TableKind::WithoutRowid
    }

    /// Columns a statement may write: every column except generated ones.
    pub fn writable(&self) -> Vec<usize> {
        (0..self.columns.len())
            .filter(|&i| self.columns[i].generated == Generated::No)
            .collect()
    }

    pub fn ddl(&self) -> String {
        let mut parts = Vec::new();
        for (i, column) in self.columns.iter().enumerate() {
            let mut part = column.name.clone();
            if !column.declared.is_empty() {
                part.push(' ');
                part.push_str(column.declared);
            }
            if self.kind == TableKind::IntegerKey && i == 0 {
                part.push_str(" PRIMARY KEY");
            }
            if column.not_null {
                part.push_str(" NOT NULL");
            }
            if let Some(collation) = column.collation {
                part.push_str(" COLLATE ");
                part.push_str(collation);
            }
            if self.unique == Some(i) {
                part.push_str(" UNIQUE");
            }
            match column.generated {
                Generated::No => {}
                Generated::Virtual => part.push_str(&format!(
                    " GENERATED ALWAYS AS ({}) VIRTUAL",
                    self.expression(i)
                )),
                Generated::Stored => part.push_str(&format!(
                    " GENERATED ALWAYS AS ({}) STORED",
                    self.expression(i)
                )),
            }
            if let Some(parent) = column.references {
                part.push_str(&format!(" REFERENCES t{parent} (id) ON DELETE CASCADE"));
            }
            parts.push(part);
        }
        if matches!(self.kind, TableKind::CompositeKey | TableKind::WithoutRowid) {
            let key: Vec<String> = self
                .key
                .iter()
                .zip(&self.key_collations)
                .map(|(&i, collation)| match collation {
                    Some(collation) => format!("{} COLLATE {collation}", self.columns[i].name),
                    None => self.columns[i].name.clone(),
                })
                .collect();
            parts.push(format!("PRIMARY KEY ({})", key.join(", ")));
        }
        let mut options = Vec::new();
        if self.kind == TableKind::WithoutRowid {
            options.push("WITHOUT ROWID");
        }
        if self.strict {
            options.push("STRICT");
        }
        format!(
            "CREATE TABLE {} ({}) {};",
            self.name,
            parts.join(", "),
            options.join(", ")
        )
    }

    /// A generated column's expression, over the first plain column.
    fn expression(&self, i: usize) -> String {
        let base = &self.columns[self.writable()[0]].name;
        if i % 2 == 0 {
            format!("coalesce({base}, '') || '!'")
        } else {
            format!("length({base})")
        }
    }
}

#[derive(Clone, Debug)]
pub struct SchemaSpec {
    pub tables: Vec<TableSpec>,
    pub extra: Vec<String>,
}

impl SchemaSpec {
    pub fn ddl(&self) -> String {
        let mut ddl: Vec<String> = self.tables.iter().map(TableSpec::ddl).collect();
        ddl.extend(self.extra.iter().cloned());
        ddl.join("\n")
    }
}

const TYPES: [&str; 6] = ["INTEGER", "TEXT", "REAL", "NUMERIC", "BLOB", ""];
const STRICT_TYPES: [&str; 5] = ["INTEGER", "TEXT", "REAL", "BLOB", "ANY"];

pub fn generate(prng: &mut Prng) -> SchemaSpec {
    let count = prng.range(1..=4) as usize;
    let mut tables: Vec<TableSpec> = Vec::new();
    for t in 0..count {
        let kind = *prng.pick(&[
            TableKind::Rowid,
            TableKind::IntegerKey,
            TableKind::IntegerKey,
            TableKind::CompositeKey,
            TableKind::WithoutRowid,
        ]);
        let strict = prng.chance(0.25);
        let mut columns = Vec::new();
        if kind == TableKind::IntegerKey {
            columns.push(ColumnSpec {
                name: "id".into(),
                declared: "INTEGER",
                collation: None,
                generated: Generated::No,
                not_null: false,
                references: None,
            });
        }
        for c in 0..prng.range(2..=4) {
            let declared = if strict {
                *prng.pick(&STRICT_TYPES)
            } else {
                *prng.pick(&TYPES)
            };
            let collation = if matches!(declared, "TEXT" | "" | "ANY") && prng.chance(0.5) {
                Some(*prng.pick(&["NOCASE", "RTRIM", "BINARY"]))
            } else {
                None
            };
            columns.push(ColumnSpec {
                name: format!("c{c}"),
                declared,
                collation,
                generated: Generated::No,
                not_null: false,
                references: None,
            });
        }
        // A foreign key to an earlier table with an integer key.
        let parents: Vec<usize> = (0..t)
            .filter(|&p| tables[p].kind == TableKind::IntegerKey)
            .collect();
        if !parents.is_empty() && prng.chance(0.5) {
            columns.push(ColumnSpec {
                name: "parent".into(),
                declared: "INTEGER",
                collation: None,
                generated: Generated::No,
                not_null: false,
                references: Some(*prng.pick(&parents)),
            });
        }
        let first_plain = usize::from(kind == TableKind::IntegerKey);
        let key = match kind {
            TableKind::CompositeKey | TableKind::WithoutRowid => {
                let width = prng.range(1..=2) as usize;
                (first_plain..first_plain + width).collect()
            }
            TableKind::IntegerKey => vec![0],
            TableKind::Rowid => Vec::new(),
        };
        if kind == TableKind::WithoutRowid {
            for &k in &key {
                columns[k].not_null = true;
            }
        }
        let generated_type = if strict { "ANY" } else { "" };
        if prng.chance(0.3) {
            let n = columns.len();
            columns.push(ColumnSpec {
                name: format!("g{n}"),
                declared: generated_type,
                collation: None,
                generated: Generated::Virtual,
                not_null: false,
                references: None,
            });
        }
        if prng.chance(0.3) {
            let n = columns.len();
            columns.push(ColumnSpec {
                name: format!("s{n}"),
                declared: generated_type,
                collation: None,
                generated: Generated::Stored,
                not_null: false,
                references: None,
            });
        }
        let key_collations = key
            .iter()
            .map(|_| {
                prng.chance(0.3)
                    .then(|| *prng.pick(&["NOCASE", "BINARY", "RTRIM"]))
            })
            .collect();
        let candidates: Vec<usize> = (first_plain..columns.len())
            .filter(|&i| {
                columns[i].generated == Generated::No
                    && !key.contains(&i)
                    && columns[i].references.is_none()
            })
            .collect();
        let unique = (!candidates.is_empty() && prng.chance(0.4)).then(|| *prng.pick(&candidates));
        tables.push(TableSpec {
            name: format!("t{t}"),
            kind,
            strict,
            columns,
            key,
            key_collations,
            unique,
        });
    }
    let mut extra = Vec::new();
    for (t, table) in tables.iter().enumerate() {
        if prng.chance(0.5) {
            let column = &table.columns[*prng.pick(&table.writable())].name;
            extra.push(format!(
                "CREATE INDEX t{t}_{column} ON {} ({column});",
                table.name
            ));
        }
    }
    // A trigger copying inserts into a rowid table, and one cascading deletes.
    if tables.len() >= 2 && prng.chance(0.5) {
        let source = prng.index(tables.len());
        let target = (source + 1) % tables.len();
        if tables[target].has_rowid() {
            let from = &tables[source].columns[tables[source].writable()[0]].name;
            let to = &tables[target].columns
                [*tables[target].writable().last().expect("a writable column")]
            .name;
            extra.push(format!(
                "CREATE TRIGGER copy_{source} AFTER INSERT ON t{source} BEGIN \
                 INSERT OR IGNORE INTO t{target} ({to}) VALUES (new.{from}); END;"
            ));
        }
    }
    if tables.len() >= 2 && prng.chance(0.4) {
        let source = prng.index(tables.len());
        let target = (source + 1) % tables.len();
        let from = &tables[source].columns[tables[source].writable()[0]].name;
        let to = &tables[target].columns[tables[target].writable()[0]].name;
        extra.push(format!(
            "CREATE TRIGGER purge_{source} AFTER DELETE ON t{source} BEGIN \
             DELETE FROM t{target} WHERE {to} = old.{from}; END;"
        ));
    }
    SchemaSpec { tables, extra }
}
