//! Integration tests for schema inference and conversion.
//!
//! These tests exercise the library with in-memory BSON documents so that no
//! live MongoDB connection is required.

use bson::{doc, Bson};
use mongo2pg::analyzer::{Analyzer, CollectionSchema};
use mongo2pg::converters::to_expanded_schema;
use mongo2pg::stats::SchemaStats;

// ──────────────────────────────────────────────────────────────────────────────
// Helpers
// ──────────────────────────────────────────────────────────────────────────────

/// Build a small in-memory schema by feeding documents through the analyzer.
fn analyze_docs(docs: &[bson::Document]) -> CollectionSchema {
    let mut analyzer = Analyzer::new(true, None);
    for doc in docs {
        analyzer.process_document(doc);
    }
    analyzer.finish()
}

// ──────────────────────────────────────────────────────────────────────────────
// Analyzer tests
// ──────────────────────────────────────────────────────────────────────────────

#[test]
fn test_basic_field_count() {
    let docs = vec![
        doc! { "_id": 1, "name": "Alice" },
        doc! { "_id": 2, "name": "Bob" },
        doc! { "_id": 3, "name": "Carol" },
    ];
    let schema = analyze_docs(&docs);
    assert_eq!(schema.count, 3);
    assert!(schema.object.contains_key("_id"));
    assert!(schema.object.contains_key("name"));
}

#[test]
fn test_id_sorted_first() {
    let docs = vec![
        doc! { "zebra": "z", "_id": 1, "apple": "a" },
    ];
    let schema = analyze_docs(&docs);
    let keys: Vec<&str> = schema.object.keys().map(|s: &String| s.as_str()).collect();
    assert_eq!(keys[0], "_id", "_id must be the first key");
}

#[test]
fn test_numeric_type_mapped_to_number() {
    let docs = vec![
        doc! { "_id": 1, "score": 42_i32 },
        doc! { "_id": 2, "score": 3.14_f64 },
    ];
    let schema = analyze_docs(&docs);
    let score = schema.object.get("score").expect("score field missing");
    assert!(
        score.types.contains_key("Number"),
        "Int32 and f64 should both map to 'Number'"
    );
}

#[test]
fn test_decimal128_distinct_from_number() {
    use bson::Decimal128;
    let d128: Decimal128 = "3.14".parse().expect("valid Decimal128 literal");
    let docs = vec![doc! { "_id": 1, "amount": Bson::Decimal128(d128) }];
    let schema = analyze_docs(&docs);
    let amount = schema.object.get("amount").expect("amount missing");
    assert!(amount.types.contains_key("Decimal128"), "Decimal128 must be its own type");
    assert!(!amount.types.contains_key("Number"), "Decimal128 must NOT be 'Number'");
}

#[test]
fn test_undefined_injected_for_missing_fields() {
    let docs = vec![
        doc! { "_id": 1, "optional": "present" },
        doc! { "_id": 2 },
    ];
    let schema = analyze_docs(&docs);
    let field = schema.object.get("optional").expect("optional missing");
    assert!(
        field.types.contains_key("Undefined"),
        "Undefined must be injected when field is absent in some docs"
    );
}

#[test]
fn test_prop_in_object_computed_correctly() {
    let docs = vec![
        doc! { "_id": 1, "x": 1 },
        doc! { "_id": 2, "x": 2 },
        doc! { "_id": 3 },
    ];
    let schema = analyze_docs(&docs);
    let x = schema.object.get("x").unwrap();
    let expected = 2.0 / 3.0;
    assert!(
        (x.prop_in_object - expected).abs() < 1e-9,
        "prop_in_object should be {expected} but was {}",
        x.prop_in_object
    );
}

#[test]
fn test_nested_object_schema() {
    let docs = vec![doc! {
        "_id": 1,
        "address": { "city": "Paris", "zip": "75001" }
    }];
    let schema = analyze_docs(&docs);
    let address = schema.object.get("address").expect("address missing");
    let obj_type = address.types.get("Object").expect("Object type missing");
    let nested = obj_type.object.as_ref().expect("nested object schema missing");
    assert!(nested.contains_key("city"), "nested city field missing");
    assert!(nested.contains_key("zip"), "nested zip field missing");
}

#[test]
fn test_array_type() {
    let docs = vec![doc! { "_id": 1, "tags": ["rust", "mongodb"] }];
    let schema = analyze_docs(&docs);
    let tags = schema.object.get("tags").expect("tags missing");
    assert!(tags.types.contains_key("Array"), "Array type expected");
    let arr_type = tags.types.get("Array").unwrap();
    assert!(arr_type.array.is_some(), "array items schema should be present");
}

#[test]
fn test_sample_values_collected() {
    let docs: Vec<bson::Document> = (0..5)
        .map(|i| doc! { "_id": i, "name": format!("user{i}") })
        .collect();
    let schema = analyze_docs(&docs);
    let name = schema.object.get("name").unwrap();
    let str_type = name.types.get("String").unwrap();
    let values = str_type.values.as_ref().expect("values should be collected");
    assert!(!values.is_empty(), "should have sampled values");
}

#[test]
fn test_alphabetical_sort_excluding_id() {
    let docs = vec![doc! { "_id": 1, "zoo": 1, "alpha": 2, "beta": 3 }];
    let schema = analyze_docs(&docs);
    let keys: Vec<&str> = schema.object.keys().map(|s: &String| s.as_str()).collect();
    assert_eq!(keys[0], "_id");
    assert_eq!(keys[1], "alpha");
    assert_eq!(keys[2], "beta");
    assert_eq!(keys[3], "zoo");
}

// ──────────────────────────────────────────────────────────────────────────────
// Expanded JSON Schema converter tests
// ──────────────────────────────────────────────────────────────────────────────

#[test]
fn test_expanded_has_x_bson_type() {
    let docs = vec![doc! { "_id": 1, "name": "Alice" }];
    let schema = analyze_docs(&docs);
    let value = to_expanded_schema(&schema);
    let name_prop = &value["properties"]["name"];
    assert!(
        name_prop.get("x-bsonType").is_some(),
        "expanded schema should have x-bsonType"
    );
}

#[test]
fn test_expanded_has_x_metadata() {
    let docs = vec![doc! { "_id": 1, "score": 99_i32 }];
    let schema = analyze_docs(&docs);
    let value = to_expanded_schema(&schema);
    assert!(
        value["x-metadata"]["count"].is_number(),
        "root x-metadata.count should be a number"
    );
    let score_prop = &value["properties"]["score"];
    assert!(
        score_prop["x-metadata"]["count"].is_number(),
        "field x-metadata.count should be a number"
    );
    assert!(
        score_prop["x-metadata"]["prob"].is_number(),
        "field x-metadata.prob should be a number"
    );
}

#[test]
fn test_expanded_has_x_sample_values() {
    let docs: Vec<bson::Document> = (0..5)
        .map(|i| doc! { "_id": i, "tag": format!("t{i}") })
        .collect();
    let schema = analyze_docs(&docs);
    let value = to_expanded_schema(&schema);
    let tag_prop = &value["properties"]["tag"];
    assert!(
        tag_prop.get("x-sampleValues").is_some(),
        "expanded schema should include x-sampleValues for string fields"
    );
    let samples = tag_prop["x-sampleValues"].as_array().unwrap();
    assert!(!samples.is_empty());
}

#[test]
fn test_expanded_no_schema_keyword() {
    let docs = vec![doc! { "_id": 1 }];
    let schema = analyze_docs(&docs);
    let value = to_expanded_schema(&schema);
    assert!(
        value.get("$schema").is_none(),
        "expanded format should not have $schema"
    );
}

#[test]
fn test_expanded_objectid_ref() {
    use bson::oid::ObjectId;
    let docs = vec![doc! { "_id": ObjectId::new() }];
    let schema = analyze_docs(&docs);
    let value = to_expanded_schema(&schema);
    // _id has ObjectId type → x-bsonType should be "ObjectId"
    let id_prop = &value["properties"]["_id"];
    assert_eq!(id_prop["x-bsonType"], "ObjectId");
}

// ──────────────────────────────────────────────────────────────────────────────
// Stats tests
// ──────────────────────────────────────────────────────────────────────────────

#[test]
fn test_stats_width() {
    let docs = vec![doc! { "_id": 1, "a": 1, "b": 2 }];
    let schema = analyze_docs(&docs);
    let stats = SchemaStats::compute(&schema);
    assert_eq!(stats.width, 3); // _id, a, b
}

#[test]
fn test_stats_depth_nested() {
    let docs = vec![doc! {
        "_id": 1,
        "outer": { "inner": { "deep": "value" } }
    }];
    let schema = analyze_docs(&docs);
    let stats = SchemaStats::compute(&schema);
    assert!(stats.depth >= 3, "depth should be at least 3 for triple-nested doc");
}

#[test]
fn test_stats_branch_count() {
    let docs = vec![doc! { "_id": 1, "a": 1, "b": 2, "c": 3 }];
    let schema = analyze_docs(&docs);
    let stats = SchemaStats::compute(&schema);
    assert_eq!(stats.branch, 4); // _id, a, b, c
}

// ──────────────────────────────────────────────────────────────────────────────
// Semantic types tests
// ──────────────────────────────────────────────────────────────────────────────

#[test]
fn test_semantic_type_email_detected() {
    use mongo2pg::semantic_types::SemanticDetector;
    let det = SemanticDetector::new();
    let values = vec![
        "alice@example.com",
        "bob@company.org",
        "carol@test.net",
        "dave@foo.io",
    ];
    let result = det.detect(&values);
    assert_eq!(result, Some("Email".to_owned()));
}

#[test]
fn test_semantic_type_with_analyzer() {
    use mongo2pg::semantic_types::SemanticDetector;
    let docs: Vec<bson::Document> = (0..10)
        .map(|i| doc! { "_id": i, "email": format!("user{i}@example.com") })
        .collect();
    let mut analyzer = Analyzer::new(true, Some(SemanticDetector::new()));
    for d in &docs {
        analyzer.process_document(d);
    }
    let schema = analyzer.finish();
    let email_field = schema.object.get("email").expect("email field missing");
    assert_eq!(
        email_field.semantic_type.as_deref(),
        Some("Email"),
        "email field should have semantic type Email"
    );
}
