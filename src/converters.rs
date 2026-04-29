//! Schema converters: internal [`CollectionSchema`] → JSON Schema variants.
//!
//! Three public entry points:
//!
//! | Function | Output dialect |
//! |---|---|
//! | [`to_mongodb_schema`] | MongoDB JSON Schema (`bsonType`, `properties`, `required`, `anyOf`) |
//! | [`to_json_schema`] | Standard JSON Schema draft 2020-12 (`$schema`, `$defs`) |
//! | [`to_expanded_schema`] | Extended: `x-bsonType`, `x-metadata`, `x-sampleValues` |

use indexmap::IndexMap;
use serde_json::{json, Map, Value};

use crate::analyzer::{
    CollectionSchema, FieldSchema, TypeSchema, TYPE_ARRAY, TYPE_BOOLEAN, TYPE_DATE,
    TYPE_DECIMAL128, TYPE_NULL, TYPE_NUMBER, TYPE_OBJECT, TYPE_OBJECTID, TYPE_REGEX,
    TYPE_STRING, TYPE_SYMBOL, TYPE_TIMESTAMP, TYPE_UNDEFINED, TYPE_BINARY, TYPE_CODE,
    TYPE_CODE_W_SCOPE, TYPE_DBPOINTER, TYPE_MAXKEY, TYPE_MINKEY,
};

// ──────────────────────────────────────────────────────────────────────────────
// MongoDB JSON Schema converter
// ──────────────────────────────────────────────────────────────────────────────

/// Convert the internal schema to a MongoDB JSON Schema object.
///
/// * Uses `bsonType` instead of `type`.
/// * Adds a `required` array for fields present in all documents
///   (`prop_in_object == 1.0`).
/// * Uses `anyOf` when a field has multiple non-`Undefined` types.
pub fn to_mongodb_schema(schema: &CollectionSchema) -> Value {
    let mut root = Map::new();
    root.insert("bsonType".into(), json!("object"));

    let props = object_to_mongodb_properties(&schema.object);
    root.insert("properties".into(), Value::Object(props));

    let required: Vec<Value> = schema
        .object
        .iter()
        .filter(|(_, f)| {
            (f.prop_in_object - 1.0).abs() < f64::EPSILON
                && !f.types.contains_key(TYPE_UNDEFINED)
        })
        .map(|(name, _)| Value::String(name.clone()))
        .collect();

    if !required.is_empty() {
        root.insert("required".into(), Value::Array(required));
    }

    Value::Object(root)
}

fn object_to_mongodb_properties(
    fields: &IndexMap<String, FieldSchema>,
) -> Map<String, Value> {
    let mut props = Map::new();
    for (name, field) in fields {
        props.insert(name.clone(), field_to_mongodb_schema(field));
    }
    props
}

fn field_to_mongodb_schema(field: &FieldSchema) -> Value {
    let non_undef: Vec<(&String, &TypeSchema)> = field
        .types
        .iter()
        .filter(|(t, _)| t.as_str() != TYPE_UNDEFINED)
        .collect();

    if non_undef.is_empty() {
        return json!({});
    }

    if non_undef.len() == 1 {
        return type_to_mongodb_schema(non_undef[0].0, non_undef[0].1);
    }

    // Multiple types → anyOf
    let any_of: Vec<Value> = non_undef
        .into_iter()
        .map(|(t, ts)| type_to_mongodb_schema(t, ts))
        .collect();
    json!({ "anyOf": any_of })
}

fn type_to_mongodb_schema(type_name: &str, ts: &TypeSchema) -> Value {
    let bson_type = internal_to_bson_type(type_name);
    let mut obj = Map::new();
    obj.insert("bsonType".into(), Value::String(bson_type.to_owned()));

    if type_name == TYPE_OBJECT {
        if let Some(nested) = &ts.object {
            let props = object_to_mongodb_properties(nested);
            obj.insert("properties".into(), Value::Object(props));

            let required: Vec<Value> = nested
                .iter()
                .filter(|(_, f)| {
                    (f.prop_in_object - 1.0).abs() < f64::EPSILON
                        && !f.types.contains_key(TYPE_UNDEFINED)
                })
                .map(|(n, _)| Value::String(n.clone()))
                .collect();
            if !required.is_empty() {
                obj.insert("required".into(), Value::Array(required));
            }
        }
    }

    if type_name == TYPE_ARRAY {
        if let Some(items_field) = &ts.array {
            obj.insert("items".into(), field_to_mongodb_schema(items_field));
        }
    }

    Value::Object(obj)
}

// ──────────────────────────────────────────────────────────────────────────────
// Standard JSON Schema draft 2020-12 converter
// ──────────────────────────────────────────────────────────────────────────────

const JSON_SCHEMA_DRAFT: &str = "https://json-schema.org/draft/2020-12/schema";

/// Convert to standard JSON Schema draft 2020-12.
///
/// BSON-specific types that have no direct JSON Schema equivalent are
/// represented via `$ref` pointing to definitions in `$defs`.
pub fn to_json_schema(schema: &CollectionSchema) -> Value {
    let mut root = Map::new();
    root.insert("$schema".into(), Value::String(JSON_SCHEMA_DRAFT.to_owned()));
    root.insert("type".into(), json!("object"));

    let props = object_to_json_properties(&schema.object);
    root.insert("properties".into(), Value::Object(props));

    let required: Vec<Value> = schema
        .object
        .iter()
        .filter(|(_, f)| {
            (f.prop_in_object - 1.0).abs() < f64::EPSILON
                && !f.types.contains_key(TYPE_UNDEFINED)
        })
        .map(|(name, _)| Value::String(name.clone()))
        .collect();

    if !required.is_empty() {
        root.insert("required".into(), Value::Array(required));
    }

    root.insert("$defs".into(), bson_defs());

    Value::Object(root)
}

fn object_to_json_properties(fields: &IndexMap<String, FieldSchema>) -> Map<String, Value> {
    let mut props = Map::new();
    for (name, field) in fields {
        props.insert(name.clone(), field_to_json_schema(field));
    }
    props
}

fn field_to_json_schema(field: &FieldSchema) -> Value {
    let non_undef: Vec<(&String, &TypeSchema)> = field
        .types
        .iter()
        .filter(|(t, _)| t.as_str() != TYPE_UNDEFINED)
        .collect();

    if non_undef.is_empty() {
        return json!({});
    }

    if non_undef.len() == 1 {
        return type_to_json_schema(non_undef[0].0, non_undef[0].1);
    }

    let any_of: Vec<Value> = non_undef
        .into_iter()
        .map(|(t, ts)| type_to_json_schema(t, ts))
        .collect();
    json!({ "anyOf": any_of })
}

fn type_to_json_schema(type_name: &str, ts: &TypeSchema) -> Value {
    let json_type = internal_to_json_type(type_name);
    let mut obj = Map::new();

    match json_type {
        JsonType::Primitive(t) => {
            obj.insert("type".into(), Value::String(t.to_owned()));
        }
        JsonType::Ref(r) => {
            obj.insert("$ref".into(), Value::String(format!("#/$defs/{r}")));
        }
    }

    if type_name == TYPE_OBJECT {
        if let Some(nested) = &ts.object {
            let props = object_to_json_properties(nested);
            obj.insert("properties".into(), Value::Object(props));

            let required: Vec<Value> = nested
                .iter()
                .filter(|(_, f)| {
                    (f.prop_in_object - 1.0).abs() < f64::EPSILON
                        && !f.types.contains_key(TYPE_UNDEFINED)
                })
                .map(|(n, _)| Value::String(n.clone()))
                .collect();
            if !required.is_empty() {
                obj.insert("required".into(), Value::Array(required));
            }
        }
    }

    if type_name == TYPE_ARRAY {
        if let Some(items_field) = &ts.array {
            obj.insert("items".into(), field_to_json_schema(items_field));
        }
    }

    Value::Object(obj)
}

enum JsonType {
    Primitive(&'static str),
    Ref(&'static str),
}

fn internal_to_json_type(type_name: &str) -> JsonType {
    match type_name {
        TYPE_NUMBER => JsonType::Primitive("number"),
        TYPE_STRING => JsonType::Primitive("string"),
        TYPE_BOOLEAN => JsonType::Primitive("boolean"),
        TYPE_NULL => JsonType::Primitive("null"),
        TYPE_OBJECT => JsonType::Primitive("object"),
        TYPE_ARRAY => JsonType::Primitive("array"),
        TYPE_OBJECTID => JsonType::Ref("ObjectId"),
        TYPE_DATE => JsonType::Ref("Date"),
        TYPE_DECIMAL128 => JsonType::Ref("Decimal128"),
        TYPE_BINARY => JsonType::Ref("Binary"),
        TYPE_REGEX => JsonType::Ref("RegularExpression"),
        TYPE_CODE => JsonType::Ref("JavaScriptCode"),
        TYPE_CODE_W_SCOPE => JsonType::Ref("JavaScriptCodeWithScope"),
        TYPE_TIMESTAMP => JsonType::Ref("Timestamp"),
        TYPE_SYMBOL => JsonType::Ref("Symbol"),
        TYPE_DBPOINTER => JsonType::Ref("DbPointer"),
        TYPE_MAXKEY => JsonType::Ref("MaxKey"),
        TYPE_MINKEY => JsonType::Ref("MinKey"),
        _ => JsonType::Primitive("string"),
    }
}

/// `$defs` block with relaxed EJSON-style definitions for BSON types.
fn bson_defs() -> Value {
    json!({
        "ObjectId": { "type": "object", "properties": { "$oid": { "type": "string" } } },
        "Date": { "oneOf": [{ "type": "string", "format": "date-time" }, { "type": "object", "properties": { "$date": { "type": "string" } } }] },
        "Decimal128": { "type": "object", "properties": { "$numberDecimal": { "type": "string" } } },
        "Binary": { "type": "object", "properties": { "$binary": { "type": "object", "properties": { "base64": { "type": "string" }, "subType": { "type": "string" } } } } },
        "RegularExpression": { "type": "object", "properties": { "$regularExpression": { "type": "object" } } },
        "JavaScriptCode": { "type": "object", "properties": { "$code": { "type": "string" } } },
        "JavaScriptCodeWithScope": { "type": "object", "properties": { "$code": { "type": "string" }, "$scope": { "type": "object" } } },
        "Timestamp": { "type": "object", "properties": { "$timestamp": { "type": "object" } } },
        "Symbol": { "type": "object", "properties": { "$symbol": { "type": "string" } } },
        "DbPointer": { "type": "object" },
        "MaxKey": { "type": "object", "properties": { "$maxKey": { "type": "integer", "enum": [1] } } },
        "MinKey": { "type": "object", "properties": { "$minKey": { "type": "integer", "enum": [1] } } }
    })
}

// ──────────────────────────────────────────────────────────────────────────────
// Expanded JSON Schema converter
// ──────────────────────────────────────────────────────────────────────────────

/// Convert to expanded JSON Schema.
///
/// Like [`to_json_schema`] but adds:
/// * `x-bsonType` – the internal BSON type name.
/// * `x-metadata` – `{ "count", "prob" }` per field/type.
/// * `x-sampleValues` – reservoir-sampled values when available.
/// * `x-semanticType` – detected semantic type when available.
///
/// No `$schema` declaration is emitted.
pub fn to_expanded_schema(schema: &CollectionSchema) -> Value {
    let mut root = Map::new();
    root.insert("type".into(), json!("object"));

    let props = object_to_expanded_properties(&schema.object, schema.count);
    root.insert("properties".into(), Value::Object(props));

    let required: Vec<Value> = schema
        .object
        .iter()
        .filter(|(_, f)| {
            (f.prop_in_object - 1.0).abs() < f64::EPSILON
                && !f.types.contains_key(TYPE_UNDEFINED)
        })
        .map(|(name, _)| Value::String(name.clone()))
        .collect();

    if !required.is_empty() {
        root.insert("required".into(), Value::Array(required));
    }

    root.insert(
        "x-metadata".into(),
        json!({ "count": schema.count }),
    );

    Value::Object(root)
}

fn object_to_expanded_properties(
    fields: &IndexMap<String, FieldSchema>,
    total_docs: u64,
) -> Map<String, Value> {
    let mut props = Map::new();
    for (name, field) in fields {
        props.insert(
            name.clone(),
            field_to_expanded_schema(field, total_docs),
        );
    }
    props
}

fn field_to_expanded_schema(field: &FieldSchema, total_docs: u64) -> Value {
    let non_undef: Vec<(&String, &TypeSchema)> = field
        .types
        .iter()
        .filter(|(t, _)| t.as_str() != TYPE_UNDEFINED)
        .collect();

    let mut obj = Map::new();

    obj.insert(
        "x-metadata".into(),
        json!({
            "count": field.count,
            "prob": field.prop_in_object,
        }),
    );

    if non_undef.is_empty() {
        return Value::Object(obj);
    }

    if non_undef.len() == 1 {
        let (type_name, ts) = non_undef[0];
        let type_node = type_to_expanded_schema(type_name, ts, total_docs);
        // Merge type node into obj
        if let Value::Object(m) = type_node {
            for (k, v) in m {
                obj.insert(k, v);
            }
        }
    } else {
        let any_of: Vec<Value> = non_undef
            .into_iter()
            .map(|(t, ts)| type_to_expanded_schema(t, ts, total_docs))
            .collect();
        obj.insert("anyOf".into(), Value::Array(any_of));
    }

    Value::Object(obj)
}

fn type_to_expanded_schema(type_name: &str, ts: &TypeSchema, total_docs: u64) -> Value {
    let json_type = internal_to_json_type(type_name);
    let mut obj = Map::new();

    match json_type {
        JsonType::Primitive(t) => {
            obj.insert("type".into(), Value::String(t.to_owned()));
        }
        JsonType::Ref(r) => {
            obj.insert("$ref".into(), Value::String(format!("#/$defs/{r}")));
        }
    }

    obj.insert("x-bsonType".into(), Value::String(type_name.to_owned()));
    obj.insert(
        "x-metadata".into(),
        json!({
            "count": ts.count,
            "prob": ts.prop_in_types,
        }),
    );

    if let Some(values) = &ts.values {
        if !values.is_empty() {
            obj.insert("x-sampleValues".into(), Value::Array(values.clone()));
        }
    }

    if type_name == TYPE_OBJECT {
        if let Some(nested) = &ts.object {
            let props = object_to_expanded_properties(nested, ts.count);
            obj.insert("properties".into(), Value::Object(props));

            let required: Vec<Value> = nested
                .iter()
                .filter(|(_, f)| {
                    (f.prop_in_object - 1.0).abs() < f64::EPSILON
                        && !f.types.contains_key(TYPE_UNDEFINED)
                })
                .map(|(n, _)| Value::String(n.clone()))
                .collect();
            if !required.is_empty() {
                obj.insert("required".into(), Value::Array(required));
            }
        }
    }

    if type_name == TYPE_ARRAY {
        if let Some(items_field) = &ts.array {
            obj.insert(
                "items".into(),
                field_to_expanded_schema(items_field, total_docs),
            );
        }
    }

    Value::Object(obj)
}

// ──────────────────────────────────────────────────────────────────────────────
// BSON type name → MongoDB bsonType string
// ──────────────────────────────────────────────────────────────────────────────

fn internal_to_bson_type(type_name: &str) -> &'static str {
    match type_name {
        TYPE_NUMBER => "number",
        TYPE_STRING => "string",
        TYPE_BOOLEAN => "bool",
        TYPE_DATE => "date",
        TYPE_OBJECTID => "objectId",
        TYPE_NULL => "null",
        TYPE_BINARY => "binData",
        TYPE_ARRAY => "array",
        TYPE_OBJECT => "object",
        TYPE_DECIMAL128 => "decimal",
        TYPE_REGEX => "regex",
        TYPE_CODE => "javascript",
        TYPE_CODE_W_SCOPE => "javascriptWithScope",
        TYPE_TIMESTAMP => "timestamp",
        TYPE_SYMBOL => "symbol",
        TYPE_DBPOINTER => "dbPointer",
        TYPE_MAXKEY => "maxKey",
        TYPE_MINKEY => "minKey",
        _ => "string",
    }
}
