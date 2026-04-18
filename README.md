# mongodb_to_pg

A Rust library and CLI tool for **MongoDB schema inference and conversion**. It samples documents from a MongoDB collection, infers a probabilistic schema, and exports it in multiple JSON Schema dialects.

---

## Features

- **MongoDB sampling** – connects to any MongoDB URI, samples documents via `$sample` (default) or sequential `find/limit`
- **Probabilistic schema inference** – tracks per-field counts, type distributions, and probabilities
- **Three output formats**:
  - `expanded` (default) – extended JSON Schema with `x-bsonType`, `x-metadata`, `x-sampleValues`
  - `mongodb` – MongoDB JSON Schema dialect (`bsonType`, `properties`, `required`, `anyOf`)
  - `standard` – JSON Schema draft 2020-12 (`$schema`, `$defs`)
- **Stats** – width / depth / branch counts printed to stderr with `--stats`
- **Semantic type detection** – e.g. detects email fields with `--semantic-types`
- **Reservoir sampling** of field values (100 samples for strings/binary/code, 10 000 otherwise)
- **Output renderers**: JSON, YAML, ASCII table

---

## Installation

```bash
cargo install --path .
```

Or build from source:

```bash
git clone https://github.com/pmpetit/mongodb_to_pg
cd mongodb_to_pg
cargo build --release
# Binary at: ./target/release/mongodb_to_pg
```

---

## CLI Usage

```
mongodb_to_pg <URI> <DB.COLLECTION> [OPTIONS]

Arguments:
  <URI>            MongoDB connection URI (e.g. mongodb://localhost:27017)
  <DB.COLLECTION>  Namespace in the form <db>.<collection>

Options:
  -n, --number <N>      Number of documents to sample [default: 1000]
  -f, --format <FMT>    Output format [default: expanded]
                          expanded  – x-bsonType + x-metadata + x-sampleValues
                          mongodb   – MongoDB JSON Schema (bsonType)
                          standard  – JSON Schema draft 2020-12
                          json      – expanded rendered as JSON
                          yaml      – expanded rendered as YAML
                          table     – ASCII table of top-level fields
  -s, --stats           Print width/depth/branch stats to stderr
  -t, --semantic-types  Enable semantic-type detection (email, …)
      --values          Collect and include sample values [default]
      --no-values       Disable sample-value collection
      --sampling        Use $sample aggregation [default]
      --no-sampling     Use sequential find/limit instead of $sample
  -h, --help            Print help
  -V, --version         Print version
```

### Examples

```bash
# Infer schema in expanded format (default), pretty-printed JSON
mongodb_to_pg mongodb://localhost:27017 mydb.users

# MongoDB JSON Schema dialect, sample 500 docs
mongodb_to_pg mongodb://localhost:27017 mydb.users -n 500 -f mongodb

# Standard JSON Schema + stats on stderr
mongodb_to_pg mongodb://localhost:27017 mydb.orders -f standard --stats

# YAML output with semantic-type detection
mongodb_to_pg mongodb://localhost:27017 mydb.customers -f yaml -t

# ASCII table of top-level fields
mongodb_to_pg mongodb://localhost:27017 mydb.products -f table

# No sampling (sequential scan), no value collection
mongodb_to_pg mongodb://localhost:27017 mydb.events --no-sampling --no-values
```

---

## Library Usage

```rust
use mongodb_to_pg::analyzer::Analyzer;
use mongodb_to_pg::converters::{to_expanded_schema, to_mongodb_schema, to_json_schema};

// Feed BSON documents (from any source)
let mut analyzer = Analyzer::new(/*collect_values=*/true, /*semantic_detector=*/None);
for doc in my_documents {
    analyzer.process_document(&doc);
}
let schema = analyzer.finish();

// Convert to desired dialect
let expanded  = to_expanded_schema(&schema);
let mongo_js  = to_mongodb_schema(&schema);
let standard  = to_json_schema(&schema);

println!("{}", serde_json::to_string_pretty(&expanded).unwrap());
```

---

## Running Tests

```bash
cargo test
```

Tests cover:
- Field count and sorting (`_id` first, then alphabetical)
- Numeric type normalisation (`Number` for int/float, `Decimal128` distinct)
- Implicit `Undefined` injection for optional fields
- Nested object and array schema
- Reservoir value sampling
- All three schema converters (MongoDB, standard, expanded)
- `x-metadata`, `x-bsonType`, `x-sampleValues` presence in expanded output
- Stats (width / depth / branch)
- Semantic type detection (email)

---

## License

Apache-2.0 – see [LICENSE](LICENSE).
