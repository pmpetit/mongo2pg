# mongo2pg

A Rust library and CLI tool for **MongoDB schema inference and conversion**. It samples documents from a MongoDB collection, infers a probabilistic schema, and exports it in multiple JSON Schema dialects.

---

## Motivation

In teams without a dedicated DBA, MongoDB is sometimes chosen for the wrong reasons:

- **"MongoDB doesn't need data modelling"** – In reality, every non-trivial application benefits from a well-thought-out data model. Skipping this step in MongoDB typically leads to inconsistent documents, duplicated data, and increasingly painful application code.
- **"MongoDB lets you avoid managing relations and constraints"** – Relations don't disappear; they just move into application code, where they are harder to enforce and easier to get wrong.
- **"It's easy to spin one up, so why not?"** – Modern cloud platforms (AWS DocumentDB / Atlas, Azure Cosmos DB, GCP Firestore, …) make provisioning a MongoDB-compatible cluster a matter of a few clicks or a single Terraform resource. This low friction is a feature, but it also means clusters get created without anyone asking whether a document store is actually the right fit. A dedicated DBA — or a platform-engineering guild — would typically catch these cases early and steer teams toward the most appropriate storage technology.

MongoDB *does* have genuinely good use-cases, and it is an excellent choice when:

- Documents are naturally self-contained and deeply nested (e.g. event logs, content management, product catalogues with highly variable attributes).
- Schema flexibility is a real requirement, not just a convenience (e.g. rapid prototyping, heterogeneous data ingestion, IoT telemetry).
- You need horizontal write-scaling or geo-distributed deployments that map naturally to MongoDB's sharding model.
- You are storing large volumes of time-series or unstructured data where a document model genuinely fits.

In practice, however, some MongoDB databases end up looking surprisingly relational: collections reference each other with manual foreign keys, data is normalised across collections, and documents have very shallow nesting with only scalar fields at the top level. These databases would be a natural fit for PostgreSQL and its mature relational tooling.

There is also a **cost dimension**: managed MongoDB clusters can cost up to **~10× more** than equivalent PostgreSQL deployments for comparable workloads. Being able to detect databases that do not actually need MongoDB's document model — and that could migrate to PostgreSQL without significant re-design — can therefore lead to substantial infrastructure savings.

`mongo2pg` helps you make that assessment quickly: it samples a collection, infers its probabilistic schema, and surfaces the structural metrics (depth, width, branching factor) that reveal whether a collection is a good migration candidate.

> **Inspiration:** This project is inspired by [`mongodb-schema`](https://github.com/mongodb-js/mongodb-schema), with modifications to the values that are output.

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
git clone https://github.com/pmpetit/mongo2pg
cd mongo2pg
cargo build --release
# Binary at: ./target/release/mongo2pg
```

---

## CLI Usage

```
mongo2pg <URI> <DB.COLLECTION> [OPTIONS]

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
mongo2pg mongodb://localhost:27017 mydb.users

# MongoDB JSON Schema dialect, sample 500 docs
mongo2pg mongodb://localhost:27017 mydb.users -n 500 -f mongodb

# Standard JSON Schema + stats on stderr
mongo2pg mongodb://localhost:27017 mydb.orders -f standard --stats

# YAML output with semantic-type detection
mongo2pg mongodb://localhost:27017 mydb.customers -f yaml -t

# ASCII table of top-level fields
mongo2pg mongodb://localhost:27017 mydb.products -f table

# No sampling (sequential scan), no value collection
mongo2pg mongodb://localhost:27017 mydb.events --no-sampling --no-values
```

---

## Library Usage

```rust
use mongo2pg::analyzer::Analyzer;
use mongo2pg::converters::{to_expanded_schema, to_mongodb_schema, to_json_schema};

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
