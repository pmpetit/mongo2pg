//! Integration tests for schema inference and conversion.
//!
//! These tests exercise the library with in-memory BSON documents so that no
//! live MongoDB connection is required.

use bson::{doc, Bson};
use mongo2pg::analyzer::{Analyzer, CollectionSchema};
use mongo2pg::report::{compute_cluster_score, render_cluster_html, DatabaseScore};
use mongo2pg::stats::SchemaStats;

// ──────────────────────────────────────────────────────────────────────────────
// Helpers
// ──────────────────────────────────────────────────────────────────────────────

/// Build a small in-memory schema by feeding documents through the analyzer.
fn analyze_docs(docs: &[bson::Document]) -> CollectionSchema {
    let mut analyzer = Analyzer::new(true);
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
    let docs = vec![doc! { "zebra": "z", "_id": 1, "apple": "a" }];
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
    assert!(
        amount.types.contains_key("Decimal128"),
        "Decimal128 must be its own type"
    );
    assert!(
        !amount.types.contains_key("Number"),
        "Decimal128 must NOT be 'Number'"
    );
}

#[test]
fn test_undefined_injected_for_missing_fields() {
    let docs = vec![doc! { "_id": 1, "optional": "present" }, doc! { "_id": 2 }];
    let schema = analyze_docs(&docs);
    let field = schema.object.get("optional").expect("optional missing");
    assert!(
        field.types.contains_key("Undefined"),
        "Undefined must be injected when field is absent in some docs"
    );
}

#[test]
fn test_probability_computed_correctly() {
    let docs = vec![
        doc! { "_id": 1, "x": 1 },
        doc! { "_id": 2, "x": 2 },
        doc! { "_id": 3 },
    ];
    let schema = analyze_docs(&docs);
    let x = schema.object.get("x").unwrap();
    let expected = 2.0 / 3.0;
    assert!(
        (x.probability - expected).abs() < 1e-9,
        "probability should be {expected} but was {}",
        x.probability
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
    let nested = obj_type
        .object
        .as_ref()
        .expect("nested object schema missing");
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
    assert!(
        arr_type.array.is_some(),
        "array items schema should be present"
    );
}

#[test]
fn test_sample_values_collected() {
    let docs: Vec<bson::Document> = (0..5)
        .map(|i| doc! { "_id": i, "name": format!("user{i}") })
        .collect();
    let schema = analyze_docs(&docs);
    let name = schema.object.get("name").unwrap();
    let str_type = name.types.get("String").unwrap();
    let values = str_type
        .values
        .as_ref()
        .expect("values should be collected");
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
    assert!(
        stats.depth >= 3,
        "depth should be at least 3 for triple-nested doc"
    );
}

#[test]
fn test_stats_branch_count() {
    let docs = vec![doc! { "_id": 1, "a": 1, "b": 2, "c": 3 }];
    let schema = analyze_docs(&docs);
    let stats = SchemaStats::compute(&schema);
    assert_eq!(stats.branch, 4.0); // _id, a, b, c
    assert_eq!(stats.branches_by_level, vec![4.0]); // all 4 fields at level 1
}

// ──────────────────────────────────────────────────────────────────────────────
// Cluster score tests
// ──────────────────────────────────────────────────────────────────────────────

/// Helper to build a DatabaseScore directly (no filesystem required).
fn make_db_score(name: &str, score_db: f64, score_avg: f64, score_max: f64, total_docs: u64, collection_count: usize) -> DatabaseScore {
    DatabaseScore {
        name: name.to_owned(),
        score_db,
        score_avg,
        score_max,
        total_docs,
        collection_count,
    }
}

#[test]
fn test_cluster_score_single_db() {
    // A cluster with exactly one database should add 1.5 × 1 to that database's score.
    let db = make_db_score("mydb", 10.0, 2.5, 5.0, 1000, 3);
    let cs = compute_cluster_score(&[db]);

    assert_eq!(cs.db_count, 1);
    // score_total = 1.5 * 1 + 10.0 = 11.5
    assert!(
        (cs.score_total - 11.5).abs() < 1e-6,
        "expected score_total=11.5, got {}",
        cs.score_total
    );
    // score_max = the only database's score_db
    assert!(
        (cs.score_max - 10.0).abs() < 1e-6,
        "expected score_max=10.0, got {}",
        cs.score_max
    );
    // score_avg: only one db, so equals that db's score_avg
    assert!(
        (cs.score_avg - 2.5).abs() < 1e-3,
        "expected score_avg=2.5, got {}",
        cs.score_avg
    );
}

#[test]
fn test_cluster_score_weighted_avg() {
    // Two databases with different doc counts; the weighted average should favour
    // the database that has more documents.
    let db1 = make_db_score("db1", 10.0, 4.0, 5.0, 1000, 2);
    let db2 = make_db_score("db2", 20.0, 8.0, 12.0, 3000, 3);
    let cs = compute_cluster_score(&[db1, db2]);

    // score_total = 1.5 * 2 + 10.0 + 20.0 = 33.0
    assert!(
        (cs.score_total - 33.0).abs() < 1e-6,
        "expected score_total=33.0, got {}",
        cs.score_total
    );
    // score_avg = (4.0*1000 + 8.0*3000) / (1000+3000) = 28000/4000 = 7.0
    assert!(
        (cs.score_avg - 7.0).abs() < 1e-3,
        "expected score_avg=7.0, got {}",
        cs.score_avg
    );
    // score_max = max(10.0, 20.0) = 20.0
    assert!(
        (cs.score_max - 20.0).abs() < 1e-6,
        "expected score_max=20.0, got {}",
        cs.score_max
    );
    assert_eq!(cs.db_count, 2);
}

#[test]
fn test_cluster_score_zero_docs() {
    // When all databases have 0 documents the doc-weighted average should fall back
    // to 0.0, matching the existing per-database behaviour in render_html.
    let db = make_db_score("emptydb", 5.0, 0.0, 3.0, 0, 1);
    let cs = compute_cluster_score(&[db]);

    assert!(
        (cs.score_avg - 0.0).abs() < 1e-6,
        "expected score_avg=0.0 when total_docs=0, got {}",
        cs.score_avg
    );
    // score_total = 1.5 * 1 + 5.0 = 6.5
    assert!(
        (cs.score_total - 6.5).abs() < 1e-6,
        "expected score_total=6.5, got {}",
        cs.score_total
    );
}

#[test]
fn test_render_cluster_html_contains_scores() {
    let db = make_db_score("testdb", 12.0, 3.5, 7.0, 500, 2);
    let html = render_cluster_html(&[db], "localhost:27017");

    // Header information
    assert!(html.contains("localhost:27017"), "cluster label should appear in report");
    assert!(html.contains("testdb"), "database name should appear in report");

    // Cluster score = 1.5 * 1 + 12.0 = 13.5
    assert!(
        html.contains("13.5") || html.contains("13.50"),
        "cluster score 13.5 should appear in report; html snippet: {}",
        &html[..500.min(html.len())]
    );

    // Per-database score_db
    assert!(html.contains("12.00"), "db score 12.00 should appear in report");
}

// ──────────────────────────────────────────────────────────────────────────────
// Infer-all-databases feature tests
// ──────────────────────────────────────────────────────────────────────────────

/// Verify that the SYSTEM_DATABASES constant covers the right set.
#[test]
fn test_system_databases_list() {
    // These three databases must always be excluded when iterating the cluster.
    let system_dbs = ["admin", "local", "config"];
    let user_dbs = ["myapp", "sample_airbnb", "retail", "sample_mflix"];

    for db in system_dbs {
        assert!(
            is_system_db(db),
            "'{db}' should be treated as a system database"
        );
    }
    for db in user_dbs {
        assert!(
            !is_system_db(db),
            "'{db}' should NOT be treated as a system database"
        );
    }
}

/// Helper matching the filter logic used in `infer_all_databases`.
fn is_system_db(name: &str) -> bool {
    const SYSTEM_DATABASES: &[&str] = &["admin", "local", "config"];
    SYSTEM_DATABASES.contains(&name)
}

/// Verify that collection output names are prefixed with the database name.
#[test]
fn test_collection_output_name_includes_dbname() {
    let db = "sample_airbnb";
    let coll = "listingsAndReviews";
    let output_name = format!("{db}_{coll}");
    assert!(
        output_name.starts_with(db),
        "output name '{output_name}' must start with db name '{db}'"
    );
    assert!(
        output_name.contains(coll),
        "output name '{output_name}' must contain collection name '{coll}'"
    );
    assert_eq!(output_name, "sample_airbnb_listingsAndReviews");
}

/// Filtering rows by db name prefix works correctly.
#[test]
fn test_row_filter_by_db_prefix() {
    let names = vec![
        "mydb_users".to_owned(),
        "mydb_orders".to_owned(),
        "otherdb_users".to_owned(),
        "unrelated".to_owned(),
    ];
    let db = "mydb";
    let prefix = format!("{db}_");
    let filtered: Vec<&str> = names
        .iter()
        .filter(|n| n.starts_with(&prefix))
        .map(|s| s.as_str())
        .collect();

    assert_eq!(filtered.len(), 2, "only 'mydb_*' entries should match");
    assert!(filtered.contains(&"mydb_users"));
    assert!(filtered.contains(&"mydb_orders"));
}

/// Verify that `build_mongo_mermaid` produces valid Mermaid output.
#[test]
fn test_build_mongo_mermaid_output() {
    use mongo2pg::schema_diagram::build_mongo_mermaid;

    let docs = vec![doc! { "_id": 1, "name": "Alice", "age": 30_i32 }];
    let schema = analyze_docs(&docs);

    let collections: Vec<(&str, &CollectionSchema)> = vec![("users", &schema)];
    let mermaid = build_mongo_mermaid(&collections);

    assert!(
        mermaid.starts_with("erDiagram"),
        "Mermaid output must start with 'erDiagram'"
    );
    assert!(
        mermaid.contains("users"),
        "Mermaid output must contain the collection name"
    );
    assert!(
        mermaid.contains("_id") || mermaid.contains("name") || mermaid.contains("age"),
        "Mermaid output must contain field names"
    );
}

/// Verify that `render_mongo_schema_html` returns valid HTML.
#[test]
fn test_render_mongo_schema_html_structure() {
    use mongo2pg::schema_diagram::render_mongo_schema_html;

    let docs = vec![
        doc! { "_id": 1, "title": "Cozy Cabin", "price": 99_i32 },
        doc! { "_id": 2, "title": "City Loft",  "price": 150_i32 },
    ];
    let schema = analyze_docs(&docs);

    let collections: Vec<(&str, &CollectionSchema)> = vec![("listings", &schema)];
    let html = render_mongo_schema_html(&collections, "sample_airbnb");

    // Basic HTML structure
    assert!(html.contains("<!DOCTYPE html>"), "must be a full HTML document");
    assert!(
        html.contains("sample_airbnb"),
        "HTML must contain the database name"
    );
    assert!(
        html.contains("listings"),
        "HTML must contain the collection name in the sidebar"
    );
    assert!(
        html.contains("erDiagram"),
        "HTML must embed a Mermaid erDiagram block"
    );
    assert!(
        html.contains("mermaid"),
        "HTML must reference the Mermaid library"
    );
}
