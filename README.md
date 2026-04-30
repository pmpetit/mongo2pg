# mongo2pg

A Rust library and CLI tool for **MongoDB schema inference**. It samples documents from a MongoDB collection, infers a probabilistic schema, and exports it in the **expanded** JSON Schema dialect.

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

Just as tool exist to migrate from relational databases to document stores, a tool to support the reverse journey — from document to relational — should exist too.

> **Inspiration:** This project is inspired by [`mongodb-schema`](https://github.com/mongodb-js/mongodb-schema), with modifications to the values that are output.

---

## Features

- **MongoDB sampling** – connects to any MongoDB URI, samples documents via `$sample` (default) or sequential `find/limit`
- **Probabilistic schema inference** – tracks per-field counts, type distributions, and probabilities
- **Expanded output** – extended JSON Schema with `x-bsonType`, `x-metadata`, `x-sampleValues`
- **Stats** – width / depth / branch counts printed to stderr with `--stats`
- **Reservoir sampling** of field values (100 samples for strings/binary/code, 10 000 otherwise)
- **Output renderers**: JSON (default), YAML, ASCII table

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

`mongo2pg` has two subcommands. Running it without a subcommand defaults to `infer`.

### `infer` – sample a collection and output its JSON Schema

```
mongo2pg [infer] <URI> <DB.COLLECTION> [OPTIONS]

Arguments:
  <URI>            MongoDB connection URI (e.g. mongodb://localhost:27017)
  <DB.COLLECTION>  Namespace in the form <db>.<collection>

Options:
  -n, --number <N>      Number of documents to sample [default: 1000]
                          (mutually exclusive with --percent)
  -p, --percent <PCT>   Percentage of the collection to sample, e.g. 10 for 10%
                          (mutually exclusive with --number)
      --no-output       Suppress schema output to stdout
  -h, --help            Print help
  -V, --version         Print version
```

### `to-pg` – convert an inferred schema to PostgreSQL DDL

```
mongo2pg to-pg <SCHEMA_FILE> [OPTIONS]

Arguments:
  <SCHEMA_FILE>    Path to a schema JSON file produced by `mongo2pg infer`

Options:
  -t, --table <NAME>    Root table name (defaults to the schema file stem)
  -h, --help            Print help
```

### Examples

```bash
# Infer schema (pretty-printed JSON to stdout)
mongo2pg mongodb://localhost:27017 mydb.users

# Sample 500 docs
mongo2pg mongodb://localhost:27017 mydb.users -n 500

# Sample 10% of the collection
mongo2pg mongodb://localhost:27017 mydb.users -p 10

# Infer schema and pipe directly to to-pg
mongo2pg mongodb://localhost:27017 mydb.orders > orders.json
mongo2pg to-pg orders.json --table orders

# Suppress schema output (e.g. when only interested in side-effects)
mongo2pg mongodb://localhost:27017 mydb.logs --no-output
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
- Expanded schema converter (`x-metadata`, `x-bsonType`, `x-sampleValues`)
- Stats (width / depth / branch)

---

## License

Apache-2.0 – see [LICENSE](LICENSE).

## Disclaimer

This project is not affiliated with, endorsed by, or software from MongoDB, Inc. or the PostgreSQL Global Development Group. "MongoDB" and "PostgreSQL" are trademarks of their respective owners.
