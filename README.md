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

> **Inspiration:** This project is inspired by [`mongodb-schema`](https://github.com/mongodb-js/mongodb-schema), with modifications to the values that are output. The overall migration-project workflow (init → analyse → convert → report) is inspired by the [ora2pg](https://ora2pg.darold.net/) approach to Oracle → PostgreSQL migrations.

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

See **[USAGE.md](USAGE.md)** for the full command reference and workflow.

The migration-project workflow (init → analyse → convert → report) is inspired by the [ora2pg](https://ora2pg.darold.net/) approach to Oracle → PostgreSQL migrations.

Quick start:

```bash
# 1. Create a project
mongo2pg init --project-base /app/migration --project-name retail \
  --uri "mongodb://user:pass@localhost:27017"

# 2. Infer schemas, generate SQL, generate reports
mongo2pg infer  -c /app/migration/retail/config/retail.conf
mongo2pg to-pg  -c /app/migration/retail/config/retail.conf
mongo2pg report -c /app/migration/retail/config/retail.conf
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
