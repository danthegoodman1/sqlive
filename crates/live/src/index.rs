//! An index of read sets that finds the ones an event may affect without
//! testing each one.
//!
//! Each conjunction of a table's filter registers under one anchor
//! constraint: an equality goes in a map keyed by SQLite's equality key under
//! its collation, a bound goes in a map ordered by SQLite's comparison, and a
//! table without a usable constraint goes in the table's bucket. A change's
//! old and new rows look up each indexed column, and every candidate is then
//! checked with [`ReadSet::intersects`], so the index only narrows the work.
//!
//! A two-sided range registers under its lower bound, so a value above many
//! such ranges finds each of them as a candidate.

use crate::compare::{Collation, EqKey, compare, eq_key};
use crate::constraints::{Condition, Conjunction, Constraint, Filter};
use crate::event::{CommitEvent, Row};
use crate::read_set::ReadSet;
use crate::schema::lowercase;
use rusqlite::types::{Value, ValueRef};
use std::cmp::Ordering;
use std::collections::{BTreeMap, BTreeSet};

/// Read sets by key, such as a subscription id.
pub struct ReadSetIndex<K> {
    entries: BTreeMap<K, Entry>,
    /// Keys whose read set every commit affects.
    every_commit: BTreeSet<K>,
    /// By lowercase table name.
    tables: BTreeMap<String, TableIndex<K>>,
}

struct Entry {
    read_set: ReadSet,
    /// Where the key is registered, so removing it touches nothing else.
    places: Vec<(String, Place)>,
}

/// One registration within a table's index.
#[derive(Clone)]
enum Place {
    Whole,
    Equal(ColumnKey, EqKey),
    From(ColumnKey, Ordered),
    Upto(ColumnKey, Ordered),
    Null(ColumnKey),
}

/// A lowercase column name and the collation its conditions compare with.
type ColumnKey = (String, Collation);

struct TableIndex<K> {
    /// Keys that depend on any row of the table.
    whole: BTreeSet<K>,
    columns: BTreeMap<ColumnKey, ColumnIndex<K>>,
}

struct ColumnIndex<K> {
    /// Conditions an equal value may meet.
    equal: BTreeMap<EqKey, BTreeSet<K>>,
    /// Conditions a value at or above the bound may meet.
    from: BTreeMap<Ordered, BTreeSet<K>>,
    /// Conditions a value at or below the bound may meet.
    upto: BTreeMap<Ordered, BTreeSet<K>>,
    /// Conditions a `NULL` may meet, beyond those under `EqKey::Null`.
    null: BTreeSet<K>,
}

/// A value ordered by SQLite's comparison under a built-in collation; the
/// keys of one map share their collation.
#[derive(Clone, Debug)]
struct Ordered {
    value: Value,
    collation: Collation,
}

impl PartialEq for Ordered {
    fn eq(&self, other: &Ordered) -> bool {
        self.cmp(other).is_eq()
    }
}

impl Eq for Ordered {}

impl PartialOrd for Ordered {
    fn partial_cmp(&self, other: &Ordered) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for Ordered {
    fn cmp(&self, other: &Ordered) -> Ordering {
        compare(&self.value, &other.value, &self.collation)
    }
}

/// Where one conjunction registers: under its first equality on a value,
/// else its first `IS NULL`, else its first bound, else the whole table.
/// `None` when the conjunction matches no row.
fn places(conjunction: &Conjunction) -> Option<Vec<Place>> {
    let column =
        |constraint: &Constraint| (constraint.column.clone(), constraint.collation.clone());
    let ordered = |constraint: &Constraint, value: &Value| Ordered {
        value: value.clone(),
        collation: constraint.collation.clone(),
    };
    let mut bounded = None;
    let mut null = None;
    for constraint in &conjunction.constraints {
        let key = |value: &Value| eq_key(ValueRef::from(value), &constraint.collation);
        match &constraint.condition {
            Condition::Never => return None,
            Condition::OneOf(values) => {
                return Some(
                    values
                        .iter()
                        .map(|value| Place::Equal(column(constraint), key(value)))
                        .collect(),
                );
            }
            Condition::Is(value) => {
                return Some(vec![Place::Equal(column(constraint), key(value))]);
            }
            Condition::IsNull if null.is_none() => {
                null = Some(vec![Place::Equal(column(constraint), EqKey::Null)]);
            }
            Condition::Range { low, high } if bounded.is_none() => {
                bounded = match (low, high) {
                    (Some((low, _)), _) => Some(vec![Place::From(
                        column(constraint),
                        ordered(constraint, low),
                    )]),
                    (None, Some((high, _))) => Some(vec![Place::Upto(
                        column(constraint),
                        ordered(constraint, high),
                    )]),
                    (None, None) => None,
                };
            }
            Condition::Window {
                boundary,
                descending,
                nulls_first,
            } if bounded.is_none() && !matches!(boundary, Value::Null) => {
                let bound = ordered(constraint, boundary);
                let mut window = vec![if *descending {
                    Place::From(column(constraint), bound)
                } else {
                    Place::Upto(column(constraint), bound)
                }];
                if *nulls_first {
                    window.push(Place::Null(column(constraint)));
                }
                bounded = Some(window);
            }
            _ => {}
        }
    }
    Some(null.or(bounded).unwrap_or_else(|| vec![Place::Whole]))
}

impl<K: Ord + Clone> Default for ReadSetIndex<K> {
    fn default() -> ReadSetIndex<K> {
        ReadSetIndex::new()
    }
}

impl<K: Ord + Clone> ReadSetIndex<K> {
    pub fn new() -> ReadSetIndex<K> {
        ReadSetIndex {
            entries: BTreeMap::new(),
            every_commit: BTreeSet::new(),
            tables: BTreeMap::new(),
        }
    }

    pub fn len(&self) -> usize {
        self.entries.len()
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    pub fn get(&self, key: &K) -> Option<&ReadSet> {
        self.entries.get(key).map(|entry| &entry.read_set)
    }

    /// Adds `key`'s read set, replacing any it had.
    pub fn insert(&mut self, key: K, read_set: ReadSet) {
        self.remove(&key);
        if read_set.every_commit() {
            self.every_commit.insert(key.clone());
        }
        let mut registered = Vec::new();
        for table in read_set.tables() {
            let name = table.name().to_ascii_lowercase();
            let mut table_places = Vec::new();
            match table.filter() {
                Filter::All => table_places.push(Place::Whole),
                Filter::AnyOf(conjunctions) => {
                    for conjunction in conjunctions {
                        table_places.extend(places(conjunction).into_iter().flatten());
                    }
                }
            }
            if table_places.is_empty() {
                continue;
            }
            let index = self
                .tables
                .entry(name.clone())
                .or_insert_with(|| TableIndex {
                    whole: BTreeSet::new(),
                    columns: BTreeMap::new(),
                });
            for place in table_places {
                index.add(&key, &place);
                registered.push((name.clone(), place));
            }
        }
        self.entries.insert(
            key,
            Entry {
                read_set,
                places: registered,
            },
        );
    }

    /// Removes `key`'s read set, returning it.
    pub fn remove(&mut self, key: &K) -> Option<ReadSet> {
        let entry = self.entries.remove(key)?;
        self.every_commit.remove(key);
        for (table, place) in &entry.places {
            if let Some(index) = self.tables.get_mut(table) {
                index.take(key, place);
                if index.whole.is_empty() && index.columns.is_empty() {
                    self.tables.remove(table);
                }
            }
        }
        Some(entry.read_set)
    }

    /// The keys whose read sets `event` intersects, in key order.
    pub fn affected<'a>(&'a self, event: &'a CommitEvent) -> impl Iterator<Item = K> + 'a {
        let candidates: Box<dyn Iterator<Item = &K>> = if event.schema_changed {
            Box::new(self.entries.keys())
        } else {
            let mut found: BTreeSet<&K> = self.every_commit.iter().collect();
            for change in &event.changes {
                let Some(index) = self.tables.get(lowercase(&change.table).as_ref()) else {
                    continue;
                };
                found.extend(&index.whole);
                for ((column, collation), keys) in &index.columns {
                    for row in [&change.old, &change.new].into_iter().flatten() {
                        keys.candidates(row, column, collation, &mut found);
                    }
                }
            }
            Box::new(found.into_iter())
        };
        candidates
            .filter(|key| self.entries[*key].read_set.intersects(event))
            .cloned()
    }
}

impl<K: Ord + Clone> TableIndex<K> {
    fn column(&mut self, column: &ColumnKey) -> &mut ColumnIndex<K> {
        self.columns
            .entry(column.clone())
            .or_insert_with(|| ColumnIndex {
                equal: BTreeMap::new(),
                from: BTreeMap::new(),
                upto: BTreeMap::new(),
                null: BTreeSet::new(),
            })
    }

    fn add(&mut self, key: &K, place: &Place) {
        let key = key.clone();
        match place {
            Place::Whole => {
                self.whole.insert(key);
            }
            Place::Equal(column, eq) => {
                self.column(column)
                    .equal
                    .entry(eq.clone())
                    .or_default()
                    .insert(key);
            }
            Place::From(column, bound) => {
                self.column(column)
                    .from
                    .entry(bound.clone())
                    .or_default()
                    .insert(key);
            }
            Place::Upto(column, bound) => {
                self.column(column)
                    .upto
                    .entry(bound.clone())
                    .or_default()
                    .insert(key);
            }
            Place::Null(column) => {
                self.column(column).null.insert(key);
            }
        }
    }

    fn take(&mut self, key: &K, place: &Place) {
        fn take_from<M: Ord, K: Ord>(map: &mut BTreeMap<M, BTreeSet<K>>, at: &M, key: &K) {
            if let Some(keys) = map.get_mut(at) {
                keys.remove(key);
                if keys.is_empty() {
                    map.remove(at);
                }
            }
        }
        let column = match place {
            Place::Whole => {
                self.whole.remove(key);
                return;
            }
            Place::Equal(column, _)
            | Place::From(column, _)
            | Place::Upto(column, _)
            | Place::Null(column) => column,
        };
        let Some(index) = self.columns.get_mut(column) else {
            return;
        };
        match place {
            Place::Equal(_, eq) => take_from(&mut index.equal, eq, key),
            Place::From(_, bound) => take_from(&mut index.from, bound, key),
            Place::Upto(_, bound) => take_from(&mut index.upto, bound, key),
            Place::Null(_) => {
                index.null.remove(key);
            }
            Place::Whole => {}
        }
        if index.equal.is_empty()
            && index.from.is_empty()
            && index.upto.is_empty()
            && index.null.is_empty()
        {
            self.columns.remove(column);
        }
    }
}

impl<K: Ord> ColumnIndex<K> {
    /// Adds the keys whose conditions on `column`, under `collation`, the
    /// row's value may meet.
    fn candidates<'a>(
        &'a self,
        row: &Row,
        column: &str,
        collation: &Collation,
        found: &mut BTreeSet<&'a K>,
    ) {
        let value = match row.get(column) {
            // A row without the column, or text that converted lossily, may
            // meet any condition.
            None => None,
            Some(Value::Text(text)) if text.contains('\u{FFFD}') => None,
            Some(value) => Some(value),
        };
        let Some(value) = value else {
            found.extend(self.equal.values().flatten());
            found.extend(self.from.values().flatten());
            found.extend(self.upto.values().flatten());
            found.extend(&self.null);
            return;
        };
        if let Some(keys) = self.equal.get(&eq_key(ValueRef::from(value), collation)) {
            found.extend(keys);
        }
        if matches!(value, Value::Null) {
            found.extend(&self.null);
            return;
        }
        let ordered = Ordered {
            value: value.clone(),
            collation: collation.clone(),
        };
        found.extend(self.from.range(..=&ordered).flat_map(|(_, keys)| keys));
        found.extend(self.upto.range(&ordered..).flat_map(|(_, keys)| keys));
    }
}
