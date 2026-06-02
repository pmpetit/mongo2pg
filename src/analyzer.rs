//! Probabilistic schema inference from BSON/MongoDB documents.
//!
//! # Design
//! * Every BSON value is mapped to an internal *type name* string (see [`bson_type_name`]).
//! * Per-field type-distribution counters are accumulated via [`Analyzer`].
//! * After processing all documents [`Analyzer::finish`] computes probabilities,
//!   injects implicit `Undefined` entries, and sorts fields (`_id` first, then
//!   case-insensitive alphabetical order).
//! * Values are kept via reservoir sampling; the reservoir capacity is 100 for
//!   `String`, `Binary`, `JavaScriptCode`, `JavaScriptCodeWithScope`, and 10 000
//!   for all other types.

use std::collections::{HashMap, HashSet};

use bson::Bson;
use indexmap::IndexMap;
use rand::rngs::SmallRng;
use rand::{Rng, SeedableRng};
use serde::{Deserialize, Serialize};

fn is_false(b: &bool) -> bool {
    !b
}

// ──────────────────────────────────────────────────────────────────────────────
// Internal type-name constants
// ──────────────────────────────────────────────────────────────────────────────

pub const TYPE_NUMBER: &str = "Number";
pub const TYPE_DOUBLE: &str = "Double";
pub const TYPE_INT32: &str = "Int32";
pub const TYPE_INT64: &str = "Int64";
pub const TYPE_STRING: &str = "String";
pub const TYPE_BOOLEAN: &str = "Boolean";
pub const TYPE_DATE: &str = "Date";
pub const TYPE_OBJECTID: &str = "ObjectId";
pub const TYPE_NULL: &str = "Null";
pub const TYPE_BINARY: &str = "Binary";
pub const TYPE_ARRAY: &str = "Array";
pub const TYPE_OBJECT: &str = "Object";
pub const TYPE_DECIMAL128: &str = "Decimal128";
pub const TYPE_REGEX: &str = "RegularExpression";
pub const TYPE_CODE: &str = "JavaScriptCode";
pub const TYPE_CODE_W_SCOPE: &str = "JavaScriptCodeWithScope";
pub const TYPE_TIMESTAMP: &str = "Timestamp";
pub const TYPE_SYMBOL: &str = "Symbol";
pub const TYPE_DBPOINTER: &str = "DbPointer";
pub const TYPE_MAXKEY: &str = "MaxKey";
pub const TYPE_MINKEY: &str = "MinKey";
pub const TYPE_UNDEFINED: &str = "Undefined";

// ──────────────────────────────────────────────────────────────────────────────
// Schema data structures (internal representation)
// ──────────────────────────────────────────────────────────────────────────────

/// Top-level schema for a sampled collection.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CollectionSchema {
    /// Total number of documents in the collection.
    pub count: u64,
    /// Number of documents actually sampled and analysed.
    pub sampled: u64,
    /// Field schemas, ordered: `_id` first then case-insensitive alphabetical.
    pub object: IndexMap<String, FieldSchema>,
}

/// Schema for a single field (across all observed documents).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FieldSchema {
    /// `count / total_docs` – probability the field is present.
    pub probability: f64,
    /// Type distribution for this field.
    pub types: IndexMap<String, TypeSchema>,
}

/// Schema for one BSON type within a field.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TypeSchema {
    /// `count / field.count` – probability of this type given the field exists.
    pub probability: f64,
    /// Number of documents/items in which this type was observed (denominator for nested probabilities).
    pub sampled: u64,
    /// When `true`, `to-pg` emits a `JSONB` column for this Object field
    /// instead of creating a separate child table. Set by `infer --jsonb`.
    #[serde(default, skip_serializing_if = "is_false")]
    pub as_jsonb: bool,
    /// Average number of array elements per document (only set when `type_name == "Array"`).
    /// Equivalent to PostgreSQL `n_distinct` when positive: an absolute average cardinality.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ndistinct: Option<f64>,
    /// Sub-document schema (present when `type_name == "Object"`).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub object: Option<IndexMap<String, FieldSchema>>,
    /// Array-items schema (present when `type_name == "Array"`).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub array: Option<Box<FieldSchema>>,
    /// Reservoir-sampled values (present when value collection is enabled).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub values: Option<Vec<serde_json::Value>>,
    /// Maximum sampled string length observed for this type.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_length: Option<usize>,
    /// Effective VARCHAR length chosen by the DDL sizing heuristic for this string type.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub varchar_length: Option<usize>,
}

// ──────────────────────────────────────────────────────────────────────────────
// Internal accumulation helpers (not serialized)
// ──────────────────────────────────────────────────────────────────────────────

struct ValueReservoir {
    reservoir: Vec<serde_json::Value>,
    capacity: usize,
    seen: u64,
    rng: SmallRng,
}

impl ValueReservoir {
    fn new(capacity: usize) -> Self {
        Self {
            reservoir: Vec::with_capacity(capacity.min(64)),
            capacity,
            seen: 0,
            rng: SmallRng::from_entropy(),
        }
    }

    fn add(&mut self, value: serde_json::Value) {
        self.seen += 1;
        if self.reservoir.len() < self.capacity {
            self.reservoir.push(value);
        } else {
            let idx = self.rng.gen_range(0..self.seen) as usize;
            if idx < self.capacity {
                self.reservoir[idx] = value;
            }
        }
    }

    fn into_values(self) -> Vec<serde_json::Value> {
        self.reservoir
    }
}

/// Maximum number of distinct values tracked per type before we stop counting.
const DISTINCT_CAP: usize = 1_000;

struct TypeAcc {
    count: u64,
    nested_object: Option<ObjectAcc>,
    array_items: Option<Box<FieldAcc>>,
    values: Option<ValueReservoir>,
    max_string_length: Option<usize>,
    /// Distinct serialized values seen for scalar types (capped at [`DISTINCT_CAP`]).
    distinct_values: HashSet<String>,
    /// First 20 distinct values in order of first appearance (scalar types only).
    first_distinct_values: Vec<serde_json::Value>,
}

impl TypeAcc {
    fn new(type_name: &str, collect_values: bool) -> Self {
        let values = if collect_values {
            let cap = reservoir_capacity(type_name);
            Some(ValueReservoir::new(cap))
        } else {
            None
        };
        Self {
            count: 0,
            nested_object: None,
            array_items: None,
            values,
            max_string_length: None,
            distinct_values: HashSet::new(),
            first_distinct_values: Vec::new(),
        }
    }
}

struct FieldAcc {
    count: u64,
    types: HashMap<String, TypeAcc>,
    collect_values: bool,
}

impl FieldAcc {
    fn new(collect_values: bool) -> Self {
        Self {
            count: 0,
            types: HashMap::new(),
            collect_values,
        }
    }

    fn observe_value(&mut self, bson: &Bson) {
        if matches!(bson, Bson::Array(arr) if arr.is_empty()) {
            return;
        }

        self.count += 1;
        let type_name = bson_type_name(bson);
        let acc = self
            .types
            .entry(type_name.to_owned())
            .or_insert_with(|| TypeAcc::new(type_name, self.collect_values));
        acc.count += 1;

        // Collect sample value and track distinct values for scalar types.
        if let Some(v) = bson_to_json_value(bson) {
            if let Some(len) = v.as_str().map(str::len) {
                acc.max_string_length = Some(acc.max_string_length.unwrap_or(0).max(len));
            }
            if let Some(reservoir) = acc.values.as_mut() {
                reservoir.add(v.clone());
            }
            // Track distinct values for non-Object, non-Array types.
            if acc.distinct_values.len() < DISTINCT_CAP {
                if let Ok(s) = serde_json::to_string(&v) {
                    let is_new = acc.distinct_values.insert(s);
                    if is_new && acc.first_distinct_values.len() < 20 {
                        acc.first_distinct_values.push(v);
                    }
                }
            }
        }

        match bson {
            Bson::Document(doc) => {
                let nested = acc.nested_object.get_or_insert_with(ObjectAcc::new);
                for (k, v) in doc {
                    nested.observe_field(k, v, self.collect_values);
                }
            }
            Bson::Array(arr) => {
                let items = acc
                    .array_items
                    .get_or_insert_with(|| Box::new(FieldAcc::new(self.collect_values)));
                for v in arr {
                    items.observe_value(v);
                }
            }
            _ => {}
        }
    }
}

struct ObjectAcc {
    fields: HashMap<String, FieldAcc>,
}

impl ObjectAcc {
    fn new() -> Self {
        Self {
            fields: HashMap::new(),
        }
    }

    fn observe_field(&mut self, key: &str, value: &Bson, collect_values: bool) {
        let acc = self
            .fields
            .entry(key.to_owned())
            .or_insert_with(|| FieldAcc::new(collect_values));
        acc.observe_value(value);
    }
}

// ──────────────────────────────────────────────────────────────────────────────
// Public Analyzer
// ──────────────────────────────────────────────────────────────────────────────

/// Accumulates BSON documents and produces a [`CollectionSchema`].
pub struct Analyzer {
    total_docs: u64,
    root: ObjectAcc,
    collect_values: bool,
}

impl Analyzer {
    /// Create a new analyzer.
    ///
    /// * `collect_values` – whether to reservoir-sample field values.
    pub fn new(collect_values: bool) -> Self {
        Self {
            total_docs: 0,
            root: ObjectAcc::new(),
            collect_values,
        }
    }

    /// Feed one BSON document into the analyzer.
    pub fn process_document(&mut self, doc: &bson::Document) {
        self.total_docs += 1;
        for (key, value) in doc {
            self.root.observe_field(key, value, self.collect_values);
        }
    }

    /// Finalize and return the inferred [`CollectionSchema`].
    pub fn finish(self) -> CollectionSchema {
        let total = self.total_docs;
        let object = build_field_map(self.root, total);
        CollectionSchema {
            count: total,
            sampled: total,
            object,
        }
    }
}

// ──────────────────────────────────────────────────────────────────────────────
// Schema-building helpers
// ──────────────────────────────────────────────────────────────────────────────

fn build_field_map(acc: ObjectAcc, total_docs: u64) -> IndexMap<String, FieldSchema> {
    let mut entries: Vec<(String, FieldSchema)> = acc
        .fields
        .into_iter()
        .map(|(name, fa)| {
            let schema = build_field_schema(fa, total_docs);
            (name, schema)
        })
        .collect();

    // Sort: _id first, then case-insensitive alphabetical
    entries.sort_by(|(a, _), (b, _)| {
        if a == "_id" {
            std::cmp::Ordering::Less
        } else if b == "_id" {
            std::cmp::Ordering::Greater
        } else {
            a.to_lowercase().cmp(&b.to_lowercase())
        }
    });

    let mut map = IndexMap::with_capacity(entries.len());
    for (k, v) in entries {
        map.insert(k, v);
    }
    map
}

fn build_field_schema(fa: FieldAcc, total_docs: u64) -> FieldSchema {
    let field_count = fa.count;
    let probability = if total_docs > 0 {
        field_count as f64 / total_docs as f64
    } else {
        0.0
    };

    let mut type_entries: Vec<(String, TypeSchema)> = fa
        .types
        .into_iter()
        .map(|(tname, ta)| {
            let schema = build_type_schema(&tname, ta, field_count, total_docs);
            (tname, schema)
        })
        .collect();

    // Add implicit Undefined if field was missing from some docs
    let undefined_count = total_docs.saturating_sub(field_count);
    if undefined_count > 0 {
        let undef_schema = TypeSchema {
            probability: undefined_count as f64 / total_docs as f64,
            sampled: undefined_count,
            as_jsonb: false,
            ndistinct: None,
            object: None,
            array: None,
            values: None,
            max_length: None,
            varchar_length: None,
        };
        type_entries.push((TYPE_UNDEFINED.to_owned(), undef_schema));
    }

    // Sort type entries by descending count for determinism
    type_entries.sort_by(|(an, _), (bn, _)| {
        if an == TYPE_UNDEFINED {
            std::cmp::Ordering::Greater
        } else if bn == TYPE_UNDEFINED {
            std::cmp::Ordering::Less
        } else {
            an.cmp(bn)
        }
    });

    let mut types = IndexMap::with_capacity(type_entries.len());
    for (k, v) in type_entries {
        types.insert(k, v);
    }

    FieldSchema { probability, types }
}

fn inferred_varchar_length(max_length: usize) -> Option<usize> {
    let max_length = max_length.max(1);
    if max_length <= 5 {
        Some(max_length)
    } else if max_length <= 20 {
        Some(20)
    } else {
        None
    }
}

fn build_type_schema(
    type_name: &str,
    ta: TypeAcc,
    field_count: u64,
    total_docs: u64,
) -> TypeSchema {
    let probability = if field_count > 0 {
        ta.count as f64 / field_count as f64
    } else {
        0.0
    };

    // Compute ndistinct:
    // - Array:  average number of elements per document (avg cardinality).
    // - Object: None (ndistinct is meaningless for sub-documents).
    // - Scalar: number of distinct values observed (capped at DISTINCT_CAP).
    let has_object = ta.nested_object.is_some();
    let has_array = ta.array_items.is_some();

    let object = ta
        .nested_object
        .map(|nested| build_field_map(nested, ta.count));

    let ndistinct = if has_array {
        ta.array_items.as_ref().map(|items_fa| {
            if total_docs > 0 {
                items_fa.count as f64 / total_docs as f64
            } else {
                0.0
            }
        })
    } else if has_object {
        None
    } else {
        Some(ta.distinct_values.len() as f64)
    };

    // For the items FieldSchema, use ta.count (number of array occurrences) as the
    // denominator so that items.probability = avg elements per array occurrence.
    let array_count = ta.count;
    let array = ta
        .array_items
        .map(|items_fa| Box::new(build_field_schema(*items_fa, array_count)));

    // For scalar types, output the first 20 distinct values in order of first appearance.
    // For Object/Array types (where bson_to_json_value returns None), fall back to
    // the reservoir samples (truncated to 20).
    let values = if !ta.first_distinct_values.is_empty() {
        Some(ta.first_distinct_values).filter(|v| !v.is_empty())
    } else {
        ta.values
            .map(|r| {
                let mut v = r.into_values();
                v.truncate(20);
                v
            })
            .filter(|v| !v.is_empty())
    };

    let max_length = if type_name == TYPE_STRING {
        ta.max_string_length
    } else {
        None
    };
    let varchar_length = max_length.and_then(inferred_varchar_length);

    TypeSchema {
        probability,
        sampled: ta.count,
        as_jsonb: false,
        ndistinct,
        object,
        array,
        values,
        max_length,
        varchar_length,
    }
}

// ──────────────────────────────────────────────────────────────────────────────
// JSONB marking
// ──────────────────────────────────────────────────────────────────────────────

impl CollectionSchema {
    /// Mark every `Object`-typed field in the schema with `as_jsonb = true`.
    /// `to-pg` will then emit a `JSONB` column instead of a child table.
    /// Array-of-objects fields are left untouched (they still become child tables),
    /// but any Object sub-fields *inside* array items are also marked.
    pub fn mark_objects_as_jsonb(&mut self) {
        for field in self.object.values_mut() {
            mark_field_as_jsonb(field);
        }
    }
}

fn mark_field_as_jsonb(field: &mut FieldSchema) {
    for (type_name, type_schema) in field.types.iter_mut() {
        if type_name == TYPE_OBJECT {
            type_schema.as_jsonb = true;
            // No need to recurse: once the field is JSONB the sub-schema
            // is ignored by to-pg.
        } else if type_name == TYPE_ARRAY {
            // Recurse into array items so any Object sub-fields within
            // array rows are also marked.
            if let Some(items) = type_schema.array.as_mut() {
                mark_field_as_jsonb(items);
            }
        }
    }
}

// ──────────────────────────────────────────────────────────────────────────────
// BSON helpers
// ──────────────────────────────────────────────────────────────────────────────

/// Map a BSON value to an internal type-name string.
///
/// Numeric BSON subtypes are kept distinct so schema JSON can report
/// subtype-specific statistics.
/// Legacy schema files may still contain `"Number"`.
pub fn bson_type_name(bson: &Bson) -> &'static str {
    match bson {
        Bson::Double(_) => TYPE_DOUBLE,
        Bson::Int32(_) => TYPE_INT32,
        Bson::Int64(_) => TYPE_INT64,
        Bson::String(_) => TYPE_STRING,
        Bson::Document(_) => TYPE_OBJECT,
        Bson::Array(_) => TYPE_ARRAY,
        Bson::Binary(_) => TYPE_BINARY,
        Bson::ObjectId(_) => TYPE_OBJECTID,
        Bson::Boolean(_) => TYPE_BOOLEAN,
        Bson::DateTime(_) => TYPE_DATE,
        Bson::Null => TYPE_NULL,
        Bson::RegularExpression(_) => TYPE_REGEX,
        Bson::JavaScriptCode(_) => TYPE_CODE,
        Bson::JavaScriptCodeWithScope(_) => TYPE_CODE_W_SCOPE,
        Bson::Symbol(_) => TYPE_SYMBOL,
        Bson::Timestamp(_) => TYPE_TIMESTAMP,
        Bson::Decimal128(_) => TYPE_DECIMAL128,
        Bson::MaxKey => TYPE_MAXKEY,
        Bson::MinKey => TYPE_MINKEY,
        Bson::DbPointer(_) => TYPE_DBPOINTER,
        Bson::Undefined => TYPE_UNDEFINED,
    }
}

/// Reservoir capacity for a given internal type name.
fn reservoir_capacity(type_name: &str) -> usize {
    match type_name {
        TYPE_STRING | TYPE_BINARY | TYPE_CODE | TYPE_CODE_W_SCOPE => 100,
        _ => 10_000,
    }
}

/// Convert a BSON value to a JSON-compatible value for sample storage.
/// Returns `None` for values that cannot be meaningfully represented.
pub fn bson_to_json_value(bson: &Bson) -> Option<serde_json::Value> {
    match bson {
        Bson::Double(v) => Some(serde_json::json!(v)),
        Bson::Int32(v) => Some(serde_json::json!(v)),
        Bson::Int64(v) => Some(serde_json::json!(v)),
        Bson::String(s) => Some(serde_json::Value::String(s.clone())),
        Bson::Boolean(b) => Some(serde_json::Value::Bool(*b)),
        Bson::Null => Some(serde_json::Value::Null),
        Bson::ObjectId(oid) => Some(serde_json::Value::String(oid.to_hex())),
        Bson::DateTime(dt) => Some(serde_json::Value::String(dt.to_string())),
        Bson::Decimal128(d) => Some(serde_json::Value::String(d.to_string())),
        Bson::Binary(b) => {
            // Represent binary as hex string for sampling purposes
            let hex: String = b.bytes.iter().map(|byte| format!("{byte:02x}")).collect();
            Some(serde_json::Value::String(hex))
        }
        Bson::Document(doc) => {
            let mut map = serde_json::Map::new();
            for (k, v) in doc {
                if let Some(jv) = bson_to_json_value(v) {
                    map.insert(k.clone(), jv);
                }
            }
            Some(serde_json::Value::Object(map))
        }
        Bson::Array(arr) => {
            let vals: Vec<serde_json::Value> = arr.iter().filter_map(bson_to_json_value).collect();
            Some(serde_json::Value::Array(vals))
        }
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::{Analyzer, TYPE_DOUBLE, TYPE_INT32, TYPE_INT64, TYPE_STRING};
    use bson::doc;

    #[test]
    fn empty_arrays_do_not_count_as_present_for_probability() {
        let docs = vec![
            doc! { "advices": [] },
            doc! { "advices": [ { "advice": "keep" } ] },
            doc! { "advices": [ { "advice": "check" } ] },
        ];

        let mut analyzer = Analyzer::new(true);
        for doc in &docs {
            analyzer.process_document(doc);
        }
        let schema = analyzer.finish();

        let advices = schema
            .object
            .get("advices")
            .expect("advices field should exist");
        assert!(
            (advices.probability - (2.0 / 3.0)).abs() < f64::EPSILON,
            "empty arrays should not count as present"
        );
        let array_type = advices
            .types
            .get("Array")
            .expect("advices should be typed as array");
        assert_eq!(array_type.sampled, 2);
    }

    #[test]
    fn numeric_bson_subtypes_are_kept_separate() {
        let docs = vec![
            doc! { "value": 1_i32 },
            doc! { "value": 2_i64 },
            doc! { "value": 3.5_f64 },
        ];

        let mut analyzer = Analyzer::new(true);
        for doc in &docs {
            analyzer.process_document(doc);
        }
        let schema = analyzer.finish();

        let value = schema
            .object
            .get("value")
            .expect("value field should exist");

        assert!(value.types.contains_key(TYPE_INT32));
        assert!(value.types.contains_key(TYPE_INT64));
        assert!(value.types.contains_key(TYPE_DOUBLE));
    }

    #[test]
    fn string_types_include_max_and_varchar_lengths() {
        let docs = vec![doc! { "name": "abcdefgh" }, doc! { "name": "abc" }];

        let mut analyzer = Analyzer::new(true);
        for doc in &docs {
            analyzer.process_document(doc);
        }
        let schema = analyzer.finish();

        let string_type = schema
            .object
            .get("name")
            .and_then(|field| field.types.get(TYPE_STRING))
            .expect("name should be inferred as String");

        assert_eq!(string_type.max_length, Some(8));
        assert_eq!(string_type.varchar_length, Some(20));
    }

    #[test]
    fn string_max_length_is_not_limited_to_first_distinct_preview_values() {
        let mut docs = Vec::new();
        for idx in 0..20 {
            docs.push(doc! { "name": format!("short-{idx:02}") });
        }
        docs.push(doc! { "name": "mongodb-einrichchmugue" });

        let mut analyzer = Analyzer::new(true);
        for doc in &docs {
            analyzer.process_document(doc);
        }
        let schema = analyzer.finish();

        let string_type = schema
            .object
            .get("name")
            .and_then(|field| field.types.get(TYPE_STRING))
            .expect("name should be inferred as String");

        assert_eq!(string_type.max_length, Some(22));
        assert_eq!(string_type.varchar_length, None);
        assert_eq!(string_type.values.as_ref().map(Vec::len), Some(20));
    }
}
