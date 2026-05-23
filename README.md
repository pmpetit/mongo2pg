# mongo2pg

A Rust library and CLI tool for **MongoDB schema inference and migration**. It samples documents from a MongoDB collection, infers a probabilistic schema, generates PostgreSQL DDL, exports data to gzipped CSV files, and produces HTML reports.

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

In practice, however, some MongoDB databases end up looking surprisingly relational: collections reference each other with manual foreign keys, data is normalised across collections, and documents have very shallow nesting with only scalar fields at the top level. These databases would be a natural fit for PostgreSQL and its mature relational tooling. Also consider that sometimes, instead of creating a mongo with 3 documents or postgres with 3 rows, a .env file is enough.

There is also a **cost dimension**: managed MongoDB clusters costs more than its equivalent PostgreSQL for comparable workloads. Being able to detect databases that do not actually need MongoDB's document model — and that could migrate to PostgreSQL without significant re-design — can therefore lead to substantial infrastructure savings.

`mongo2pg` helps you make that assessment quickly: it samples a collection, infers its probabilistic schema, and surfaces the structural metrics (depth, width, branching factor) that reveal whether a collection is a good migration candidate.

Just as tool exist to migrate from relational databases to document stores, a tool to support the reverse journey — from document to relational — should exist too.

> **Inspiration:** This project is inspired by [`mongodb-schema`](https://github.com/mongodb-js/mongodb-schema), with modifications to the values that are output. The overall migration-project workflow (init → analyse → convert → report) is inspired by the [ora2pg](https://ora2pg.darold.net/) approach to Oracle → PostgreSQL migrations.

---

## Features

- **MongoDB sampling** – connects to any MongoDB URI, samples documents via `$sample` (default) or sequential `find/limit`
- **Probabilistic schema inference** – tracks per-field counts, type distributions, and probabilities
- **PostgreSQL DDL generation** – flattens nested objects and arrays into child tables with FK constraints
- **Data export** – streams full collections to gzipped CSV files, one per generated SQL table
- **Stats** – width / depth / branch counts per collection
- **HTML reports** – collection stats overview and entity-relationship diagram

---

## CLI Usage

See **[USAGE.md](USAGE.md)** for the full command reference and workflow.

The migration-project workflow (init → analyse → convert → report) is inspired by the [ora2pg](https://ora2pg.darold.net/) approach to Oracle → PostgreSQL migrations.

Quick start:

```bash
# 1. Create a project
mongo2pg init --project-base /app/migration --project-name retail \
  --source-uri "mongodb://user:pass@localhost:27017"

# 2. Infer schemas, generate SQL, export data, generate reports
mongo2pg infer  -c /app/migration/retail/config/retail.conf
mongo2pg to-pg  -c /app/migration/retail/config/retail.conf
mongo2pg export -c /app/migration/retail/config/retail.conf
mongo2pg report -c /app/migration/retail/config/retail.conf
```

---

## Running Tests

```bash
cargo test
```

## Reports

for example an analytic database, you can see some migration reports here

summarized report : [static](https://pmpetit.github.io/mongo2pg/results/infer/sample_analytics/reports/sample_analytics.html)

graphic representation: [graphical](https://pmpetit.github.io/mongo2pg/results/infer/sample_analytics/reports/sample_analytics.schema.html)

Tests cover:

- Field count and sorting (`_id` first, then alphabetical)
- Numeric type normalisation (`Number` for int/float, `Decimal128` distinct)
- Implicit `Undefined` injection for optional fields
- Nested object and array schema
- Reservoir value sampling
- Stats (width / depth / branch)

---

## Migration score

Each collection gets a **complexity score** that estimates how much effort its migration to PostgreSQL will require:

$$C_i = \frac{depth_{max}}{2} + array\_fields + \frac{distinct\_fields}{avg\_fields\_per\_doc}$$

| Term | Meaning |
|------|---------|
| $depth_{max} / 2$ | Penalises nesting depth (halved so it doesn't dominate). Every level of nesting typically requires a `JOIN` in SQL. |
| $array\_fields$ | Number of top-level fields whose type is `Array`. Each array field produces a child table with a foreign key. |
| $distinct\_fields / avg\_fields\_per\_doc$ | **Polymorphism ratio.** `1.0` means every field appears in every document (perfectly flat). Values `> 5` indicate a sparse or highly polymorphic schema where many columns will be `NULL`. |

The three individual terms are then rolled up to a **database-level score**:

$$C_{db} = 1.5 \times N_{collections} + \sum_i C_i$$

The $1.5 \times N$ factor accounts for the baseline coordination cost of migrating multiple collections (foreign-key wiring, join views, load ordering, …).

Three summary metrics are shown in the HTML report:

| Metric | Description |
|--------|-------------|
| **Score (total)** | $C_{db}$ – primary complexity indicator for the whole database |
| **Score (avg weighted)** | $C_i$ weighted by document count – shows where the bulk of the data sits |
| **Score (max collection)** | The single hardest collection to migrate |

**Thresholds:**

| Label | Range | Meaning |
|-------|-------|---------|
| 🟢 Easy | $C_{db} < 30$ | Mostly flat, scalar documents – migration is straightforward |
| 🟠 Medium | $30 \le C_{db} < 80$ | Some nesting or arrays – migration needs care but is tractable |
| 🔴 Hard | $C_{db} \ge 80$ | Deep nesting, many arrays, or high polymorphism – significant schema redesign likely required |

> **⚠️ Scores are strategy-dependent and not cross-comparable.**
> Running `infer` with `--jsonb` lowers the $depth_{max}$ term because nested Object fields become a single JSONB column — their internal nesting is never traversed relationally and is therefore not penalised. A lower score obtained with `--jsonb` does **not** mean the database is inherently simpler; it means you are trading relational depth for opaque JSON storage. Compare scores only within the same strategy (both with or both without `--jsonb`).

---

## Naming convention

`mongo2pg` flattens nested objects and arrays into child tables. Child table names are formed by concatenating the **full ancestor chain** with underscores:

```
<collection>_<field>[_<nested_field>…]
```

For example, a collection `orders` with an array field `products` that itself contains an array field `images` produces:

| MongoDB path | PostgreSQL table |
|---|---|
| `orders` | `orders` |
| `orders[].products` | `orders_products` |
| `orders[].products[].images` | `orders_products_images` |

**Why the full prefix matters:** if two different collections both have an array field called `tags`, they would both try to create a table named `tags` — causing a conflict in the shared `schema/tables/` directory and preventing `CREATE TABLE` from succeeding. By always prepending the full ancestor path, every generated table name is unique across the entire database regardless of how many collections share identically-named sub-fields.

> **Tip:** keep collection names reasonably short. Very deep nesting combined with long field names can produce table names that exceed PostgreSQL's 63-character identifier limit (`NAMEDATALEN - 1`). PostgreSQL will silently truncate them, which can cause duplicate-table errors. If you hit this, consider using `--jsonb` on deeply-nested collections to collapse those branches into a single JSONB column instead.

---

## License

Apache-2.0 – see [LICENSE](LICENSE).

## Disclaimer

This project is not affiliated with, endorsed by, or software from MongoDB, Inc. or the PostgreSQL Global Development Group. "MongoDB" and "PostgreSQL" are trademarks of their respective owners.
