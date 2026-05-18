# Installation

## Download a pre-built binary

Go to the [Releases page](https://github.com/pmpetit/mongo2pg/releases) and download the archive that matches your platform:

| Platform | File to download |
|---|---|
| Linux x86_64 | `mongo2pg-linux-x86_64` |
| Linux arm64 | `mongo2pg-linux-aarch64` |
| macOS Intel | `mongo2pg-macos-x86_64` |
| macOS Apple Silicon | `mongo2pg-macos-aarch64` |
| Windows x86_64 | `mongo2pg-windows-x86_64.exe` |
| Windows arm64 | `mongo2pg-windows-aarch64.exe` |

### Linux / macOS

```bash
# Replace <version> and <platform> with your values, e.g. v0.2.0 and linux-x86_64
version="v0.2.0"
platform="linux-x86_64"
curl -fL "https://github.com/pmpetit/mongo2pg/releases/download/${version}/mongo2pg-${platform}" \
  -o mongo2pg
chmod +x mongo2pg
sudo mv mongo2pg /usr/local/bin/
```

### Windows

Download the `.exe`, optionally rename it to `mongo2pg.exe`, and place it in a directory that is on your `PATH`.

---

## Build from source

Requires [Rust](https://rustup.rs) (stable).

```bash
git clone https://github.com/pmpetit/mongo2pg
cd mongo2pg
cargo build --release
# Binary at: ./target/release/mongo2pg
```

---

## Usage examples

### Infer the schema of a collection (1 000 documents sampled by default)

```bash
mongo2pg "mongodb://localhost:27017" mydb.mycollection
```

### Sample a fixed number of documents

```bash
mongo2pg "mongodb://localhost:27017" mydb.mycollection -n 5000
```

### Sample 10 % of the collection

```bash
mongo2pg "mongodb://localhost:27017" mydb.mycollection -p 10
```

### Print statistics only (suppress JSON output)

```bash
mongo2pg "mongodb://localhost:27017" mydb.mycollection --no-output
```

### Save the schema to a file

```bash
mongo2pg "mongodb://user:pass@host:27017" mydb.orders > orders-schema.json
```

### Disable sample-value collection (smaller output)

```bash
mongo2pg "mongodb://localhost:27017" mydb.mycollection --no-values
```

### With authentication and TLS

```bash
mongo2pg "mongodb://user:pass@host:27017/?authSource=admin&tls=true" mydb.mycollection
```

---

## Interpreting the output

`mongo2pg` produces two kinds of output: **stats** printed to stderr and a **JSON schema** on stdout.

### Stats (stderr)

```
Documents in collection : 45 231
Documents sampled       : 1 000
Width (top-level fields): 12
Depth (max nesting)     : 4
Branch (per level)      : L1:12  L2:8  L3:3  L4:1
Top-level types         : _id:ObjectId  name:String  address:Object  …
```

| Stat | What it means |
|---|---|
| **Documents in collection** | Total documents in the collection (from MongoDB metadata, fast estimate). |
| **Documents sampled** | How many documents were actually analysed. A higher number gives more reliable probabilities. |
| **Width** | Number of distinct top-level field names. A large width (> 30–40) often signals a denormalised or poorly modelled collection. |
| **Depth** | Maximum nesting level. Top-level fields are depth 1. An array containing an object with a field counts as depth 2. Depth > 3–4 suggests a heavily nested document model that may be hard to flatten. |
| **Branch (per level)** | Number of distinct fields at each nesting level. `L1:12  L2:8` means 12 fields at the top level and 8 fields inside nested objects one level down. A sharp drop-off (e.g. `L1:40 L2:5`) indicates most data is flat. |
| **Top-level types** | The dominant BSON type for each top-level field, useful for a quick sanity check. |

**What makes a good migration candidate?**

A collection is typically easy to migrate to PostgreSQL when:

- `Depth` is 1 or 2 (flat or only one level of nesting)
- `Width` is stable and small (< 20 fields)
- Per-level branch counts drop quickly (little nesting)
- Field presence probabilities are close to 1.0 (fields are present in most documents)

Collections with high depth, high width, or many low-probability fields represent schema flexibility that is harder to map to a fixed relational model.

---

The JSON output is **not** JSON Schema. It is a compact tree that describes:

- document/array nesting (`object`, `array`)
- field presence frequency (`count`, `probability`)- per-field cardinality or average array length (`ndistinct`)- per-field observed BSON types (`types`), including `Undefined` for “field missing”

At the root:

- `count` is the number of documents analysed.
- `object` contains the top-level fields.

#### Field stats: `count` and `probability`

For any field under an `object`:

- `count`: number of documents where the field exists
- `probability`: `count / parent.count` (so `1.0` means “always present”)

Example:

```json
"bedrooms": {
  "count": 5550,
  "probability": 0.9990999099909991,
  "types": {
    "Number": { "count": 5550, "probability": 1.0 },
    "Undefined": { "count": 5, "probability": 0.0009000900090009 }
  }
}
```

#### Types: `types`, `probability`, and `ndistinct`

For each BSON type observed for a field:

- `types.<BsonType>.count`: number of documents where the field had that BSON type
- `types.<BsonType>.probability`: proportion within the field occurrences (sums to `1.0` across all types)
- `types.<BsonType>.ndistinct` *(scalar types only)*: number of distinct values observed for that type (capped at 1 000). Absent for `Object` types (meaningless for sub-documents).
- `types.<BsonType>.values` *(when `--no-values` is not set)*: up to **20** reservoir-sampled values for that type.

`Undefined` is included when the field is absent. Its `count` is `parent.count - field.count`.

Example for a scalar type:

```json
"_id": {
  "count": 1000,
  "probability": 1.0,
  "types": {
    "ObjectId": {
      "count": 1000,
      "probability": 1.0,
      "ndistinct": 1000.0,
      "values": [ "572bb823...", "572bb822...", "..." ]
    }
  }
}
```

#### Nested objects: `object`

If a type is `Object`, it contains an `object` member describing sub-fields:

```json
"address": {
  "count": 5555,
  "probability": 1.0,
  "types": {
    "Object": {
      "count": 5555,
      "probability": 1.0,
      "object": {
        "country": {
          "count": 5555,
          "probability": 1.0,
          "types": { "String": { "count": 5555, "probability": 1.0 } }
        }
      }
    }
  }
}
```

#### Arrays: `array`

If a type is `Array`, it contains an `array` member that describes the array items.

The item schema has the same shape (it can have `types`, and when items are objects, an `object`).

Notes about array metrics:

- `array.count` is the **total number of array items** seen across all documents.
- `array.probability` is the **average items per array occurrence** (can be > 1.0).
- `types.Array.ndistinct` is the **average number of array elements per document** across all sampled documents (total items ÷ total docs). This is analogous to PostgreSQL's `n_distinct` when positive.

Example:

```json
"amenities": {
  "count": 5555,
  "probability": 1.0,
  "types": {
    "Array": {
      "count": 5555,
      "probability": 1.0,
      "array": {
        "count": 121402,
        "probability": 0.854545454545455,
        "types": {
          "String": { "count": 121402, "probability": 1.0 }
        }
      }
    }
  }
}
```

---

## Tutorial: run mongo2pg against MongoDB sample datasets

This tutorial walks you through starting a local MongoDB instance with Docker, loading the official MongoDB sample datasets, and running `mongo2pg` against each collection.

The main principe is

```bash
# default output = raw CollectionSchema (needed by to-pg)
mongo2pg "mongodb://..." db.col > schema.json
mongo2pg to-pg schema.json > schema.sql
```

### Prerequisites

- [Docker](https://docs.docker.com/get-docker/) installed and running
- `mongo2pg` installed (see above)
- `git` and `bash`

---

### Step 1 — Start MongoDB community in Docker

```bash
docker run --name mongodb -d \
  -p 2717:27017 \
  -e MONGO_INITDB_ROOT_USERNAME=user \
  -e MONGO_INITDB_ROOT_PASSWORD=pass \
  mongodb/mongodb-community-server
```

Verify it is running:

```bash
docker ps | grep mongodb
```

---

### Step 2 — Import the sample datasets

Clone the sample dataset repository and run its import script:

```bash
git clone https://github.com/neelabalan/mongodb-sample-dataset
cd mongodb-sample-dataset
```

The `start.sh` script uses `mongoimport` to load the data. It expects the MongoDB tools to be available. If you do not have `mongoimport` installed locally, run it inside the container:

```bash
# Copy the datasets into the container
docker cp . mongodb:/tmp/mongodb-sample-dataset

# Run the import from inside the container
docker exec -it mongodb bash -c "
  cd /tmp/mongodb-sample-dataset &&
  bash start.sh 'mongodb://user:pass@localhost:27017/?authSource=admin'
"
```

If `mongoimport` is available locally, you can pass the URI directly:

```bash
bash start.sh 'mongodb://user:pass@localhost:2717/?authSource=admin'
```

---

### Step 3 — Run mongo2pg against each collection

The URI for all commands below is:

```bash
mongodb://user:pass@localhost:2717/?authSource=admin
```

The --no-output option removes the `json` output and keep only statistics on collection.

#### sample_airbnb

```bash
# 5 555 documents – deeply nested, good stress test
mongo2pg "mongodb://user:pass@localhost:2717/?authSource=admin" \
  sample_airbnb.listingsAndReviews --no-output

Documents in collection : 5555
Documents sampled       : 1000
Width (top-level fields): 42
Depth (max nesting)     : 4
Branch (per level)      : L1:42  L2:40  L3:10  L4:1
Top-level types         : _id:String, access:String, accommodates:Number, address:Object, amenities:Array, availability:Object, bathrooms:Decimal128, bed_type:String, bedrooms:Number, beds:Number, calendar_last_scraped:Date, cancellation_policy:String, cleaning_fee:Decimal128, description:String, extra_people:Decimal128, first_review:Date, guests_included:Decimal128, host:Object, house_rules:String, images:Object, interaction:String, last_review:Date, last_scraped:Date, listing_url:String, maximum_nights:String, minimum_nights:String, monthly_price:Decimal128, name:String, neighborhood_overview:String, notes:String, number_of_reviews:Number, price:Decimal128, property_type:String, review_scores:Object, reviews:Array, reviews_per_month:Number, room_type:String, security_deposit:Decimal128, space:String, summary:String, transit:String, weekly_price:Decimal128  
```

#### sample_analytics

```bash
mongo2pg "mongodb://user:pass@localhost:2717/?authSource=admin" \
  sample_analytics.accounts --no-output

Documents in collection : 1746
Documents sampled       : 1000
Width (top-level fields): 4
Depth (max nesting)     : 2
Branch (per level)      : L1:4  L2:1
Top-level types         : _id:ObjectId, account_id:Number, limit:Number, products:Array  

mongo2pg "mongodb://user:pass@localhost:2717/?authSource=admin" \
  sample_analytics.customers --no-output

Documents in collection : 500
Documents sampled       : 500
Width (top-level fields): 9
Depth (max nesting)     : 4
Branch (per level)      : L1:9  L2:457  L3:1824  L4:456
Top-level types         : _id:ObjectId, accounts:Array, active:Boolean, address:String, birthdate:Date, email:String, name:String, tier_and_details:Object, username:String

mongo2pg "mongodb://user:pass@localhost:2717/?authSource=admin" \
  sample_analytics.transactions --no-output

Documents in collection : 1746
Documents sampled       : 1000
Width (top-level fields): 6
Depth (max nesting)     : 3
Branch (per level)      : L1:6  L2:1  L3:6
Top-level types         : _id:ObjectId, account_id:Number, bucket_end_date:Date, bucket_start_date:Date, transaction_count:Number, transactions:Array  
```

### Step 4 — Save schemas to files

To keep the inferred schemas for later analysis or create the postgres ddl.

```bash
URI="mongodb://user:pass@localhost:2717/?authSource=admin"

for ns in \
  sample_airbnb.listingsAndReviews \
  sample_analytics.accounts \
  sample_analytics.customers \
  sample_analytics.transactions \
  sample_geospatial.shipwrecks \
  sample_mflix.comments \
  sample_mflix.movies \
  sample_mflix.theaters \
  sample_mflix.users \
  sample_supplies.sales \
  sample_training.companies \
  sample_training.grades \
  sample_training.inspections \
  sample_training.routes \
  sample_training.trips \
  sample_training.zips \
  sample_weatherdata.data
do
  filename=$(echo "$ns" | tr '.' '_')
  echo "→ $ns"
  mongo2pg "$URI" "$ns" 2>${filename}.stats.txt > ${filename}.json
done
```

Each collection produces two files: `<db>_<collection>.json` (the schema) and `<db>_<collection>.stats.txt` (the stats lines).

---

### Step 5 — Generate pg DDL

```bash
for ns in \
  sample_airbnb.listingsAndReviews \
  sample_analytics_accounts \
  sample_analytics_customers \
  sample_analytics_transactions \
  sample_geospatial_shipwrecks \
  sample_mflix_comments \
  sample_mflix_movies \
  sample_mflix_theaters \
  sample_mflix_users \
  sample_supplies_sales \
  sample_training_companies \
  sample_training_grades \
  sample_training_inspections \
  sample_training_routes \
  sample_training_trips \
  sample_training_zips \
  sample_weatherdata_data
do
  filename=$(echo "$ns" | tr '.' '_')
  echo "→ $ns"
  mongo2pg to-pg ${filename}.json > ${filename}.sql
done
```

### Step 6 — Tear down

```bash
docker stop mongodb && docker rm mongodb
```

---

# Project-based workflow

`mongo2pg` provides four subcommands that mirror the [ora2pg](https://ora2pg.darold.net/) migration-project approach:

```
init   → create project structure + config file
infer  → sample MongoDB, infer schemas, write output files
to-pg  → convert inferred schemas to PostgreSQL DDL
report → generate HTML stats report and ERD diagram
```

---

## `init` – create a migration project

```
mongo2pg init --project-base <DIR> --project-name <NAME> [--uri <URI>]
```

Creates the project directory tree and writes a config file:

```
<project-base>/<project-name>/
    config/<project-name>.conf   ← BASE_DIR, PROJECT_DIR, URI, NAMESPACE
    schema/
        tables/                  ← generated SQL DDL files go here
    source/
        collections/             ← inferred JSON schemas + stats go here
    data/
    reports/                     ← HTML reports go here
```

**Options**

| Flag | Description |
|---|---|
| `--project-base <DIR>` | Parent directory that will contain the project folder |
| `--project-name <NAME>` | Name of the project (becomes the sub-directory name) |
| `--uri <URI>` | MongoDB connection URI – written into the config file |

---

## `infer` – sample a MongoDB collection and infer its schema

```
mongo2pg [infer] --uri <URI> [--namespace <NAMESPACE>] [OPTIONS]
mongo2pg [infer] -c <CONFIG> [OPTIONS]
```

**Arguments**

| Argument | Description |
|---|---|
| `--uri <URI>` | MongoDB connection URI – required unless `-c` is given |
| `--namespace <NAMESPACE>` | `<db>.<collection>` for a single collection, `<db>` for all collections in the database, or omit to infer **all user databases** on the server (see below). Can also be set via `NAMESPACE` in the config file. |

**Options**

| Flag | Description |
|---|---|
| `-n, --number <N>` | Number of documents to sample (default: 1000) |
| `-p, --percent <PCT>` | Percentage of the collection to sample |
| `-c, --config <FILE>` | Project config file – derives URI and output paths automatically |
| `-o, --output-dir <DIR>` | Write output files into `<dir>/` for each collection |
| `--no-output` | Suppress JSON schema output to stdout |

When `-c` or `-o` is given, each collection produces three files:

```
source/collections/<name>/
    <name>.json          ← inferred schema (JSON)
    <name>.stats.txt     ← human-readable stats
    <name>.stats.yaml    ← structured stats (consumed by report)
```

### Inferring all databases (no `--namespace`)

When `--namespace` is omitted (and `-c` is not given, or the config has no `NAMESPACE`),
`mongo2pg` automatically enumerates **all user databases** on the server, skipping
`admin`, `local`, and `config`.

For each database every non-system collection is inferred.  Output files (when `-o` is
given) are named `<dbname>_<collname>` to avoid collisions:

```
<output-dir>/
    <dbname>_<collname>/
        <dbname>_<collname>.json
        <dbname>_<collname>.stats.txt
        <dbname>_<collname>.stats.yaml
    reports/
        <dbname>.html          ← per-database migration stats report
        <dbname>.schema.html   ← Mermaid ER diagram of the MongoDB collections
```

Example:

```bash
mongo2pg infer --uri mongodb://localhost:27017 -o /tmp/output
```

---

## `to-pg` – convert inferred schemas to PostgreSQL DDL

```
mongo2pg to-pg [COLLECTION] [OPTIONS]
```

**Arguments**

| Argument | Description |
|---|---|
| `[COLLECTION]` | Optional collection name; omit to process all collections |

**Options**

| Flag | Description |
|---|---|
| `-c, --config <FILE>` | Project config file – reads `source/collections/`, writes `schema/tables/` |
| `-o, --output-dir <DIR>` | Directory to write `.sql` files into (overrides `-c`) |
| `-t, --table <NAME>` | Table name override (single collection only) |
| `--schema <NAME>` | Deploy all tables into the PostgreSQL schema `<NAME>` (see below) |
| `--schema-per-collection` | Like `--schema` but uses each collection name as its own schema (see below) |

Reads `source/collections/<name>/<name>.json` and writes `schema/tables/<name>.sql`.

---

### Why `--schema` and `--schema-per-collection`?

MongoDB collections are often deeply nested. When `to-pg` flattens nested documents
into relational tables, child table names are built by concatenating their parent
names with underscores:

```
salesorder
salesorder_lines
salesorder_lines_fulfillmentmaplines
salesorder_lines_fulfillmentmaplines_route
salesorder_lines_fulfillmentmaplines_route_sourcinglocation
salesorder_lines_fulfillmentmaplines_route_sourcinglocation_purchasecondition  ← 84 chars!
```

PostgreSQL silently truncates identifiers longer than **63 bytes**, which causes
name collisions in large schemas.  More practically, when you plan to deploy a
collection into its own dedicated PostgreSQL schema (one schema per collection
is a common pattern), the collection name prefix on every table name is redundant
noise.

`--schema` and `--schema-per-collection` address both problems:

1. **Each child table name has the `{schema}_` prefix stripped** before it is
   written, so names are shorter and collision-free.
2. **A `CREATE SCHEMA` preamble is prepended** to the generated SQL so the schema
   is created automatically when you run the file.
3. **`SET search_path`** is set at the top of the file so all `CREATE TABLE` and
   `REFERENCES` statements resolve within the schema without needing to be qualified.

**Before** (no flag):

```sql
CREATE TABLE salesorder ( … );
CREATE TABLE salesorder_lines ( … );
CREATE TABLE salesorder_lines_fulfillmentmaplines ( … );
CREATE TABLE salesorder_lines_fulfillmentmaplines_route ( … );
```

**After** (`--schema salesorder`):

```sql
CREATE SCHEMA IF NOT EXISTS salesorder;
SET search_path = salesorder;

CREATE TABLE salesorder ( … );   -- root table keeps its name
CREATE TABLE lines ( … );           -- prefix stripped
CREATE TABLE lines_fulfillmentmaplines ( … );
CREATE TABLE lines_fulfillmentmaplines_route ( … );
```

#### `--schema <NAME>`

Applies a single fixed schema name to the output. Useful when converting a single
collection, or when all collections in a project share one schema.

```bash
# Single collection deployed into its own schema
mongo2pg to-pg -c config/sofi.conf salesorder --schema salesorder
```

#### `--schema-per-collection`

Equivalent to running `--schema <collection_name>` for every collection processed.
Each output file gets its own `CREATE SCHEMA` preamble and its tables are named
without the collection prefix.  Mutually exclusive with `--schema`.

```bash
# Every collection gets deployed into its own schema
mongo2pg to-pg -c config/sofi.conf --schema-per-collection
```

This produces one `.sql` file per collection, each self-contained:

```sql
-- salesorder.sql
CREATE SCHEMA IF NOT EXISTS salesorder;
SET search_path = salesorder;

CREATE TABLE salesorder ( … );
CREATE TABLE lines ( … );
…
```

```sql
-- product.sql
CREATE SCHEMA IF NOT EXISTS product;
SET search_path = product;

CREATE TABLE product ( … );
CREATE TABLE product_variants ( … );
…
```

---

## `export` – export MongoDB data to gzipped CSV files

```
mongo2pg export [COLLECTION] [OPTIONS]
```

For each collection, reads the corresponding `schema/tables/<name>.sql` to understand
the table hierarchy, then streams **all documents** from MongoDB and writes one
`.csv.gz` file per SQL table into `data/<collection_name>/`.

Nested arrays and objects are expanded across child tables exactly as `to-pg` modelled
them, so the CSV files can be loaded directly into PostgreSQL with `\COPY`.

**Arguments**

| Argument | Description |
|---|---|
| `[COLLECTION]` | Optional collection name; omit to export all collections found in `schema/tables/` |

**Options**

| Flag | Description |
|---|---|
| `-c, --config <FILE>` | Project config file – derives URI, database name, `schema/tables/` and `data/` paths |
| `-o, --output-dir <DIR>` | Override the output directory for CSV files (default: `<project>/data/`) |

**Output layout**

```
data/
└── orders/
    ├── orders.csv.gz
    ├── orders_products.csv.gz
    ├── orders_products_image.csv.gz
    ├── orders_products_price.csv.gz
    └── orders_status_history.csv.gz
```

**Loading into PostgreSQL**

```sql
\COPY orders FROM PROGRAM 'gunzip -c orders.csv.gz' CSV HEADER;
\COPY orders_products FROM PROGRAM 'gunzip -c orders_products.csv.gz' CSV HEADER;
```

---

## `report` – generate HTML reports

```
mongo2pg report [OPTIONS]
```

**Options**

| Flag | Description |
|---|---|
| `-c, --config <FILE>` | Project config file |
| `--collections-dir <DIR>` | Path to `source/collections/` (overrides `-c`) |
| `-o, --output <FILE>` | Output path for the HTML report |
| `-n, --namespace <NS>` | Label shown in the report header |

Produces two files in `reports/`:

| File | Description |
|---|---|
| `<project>.html` | Collection stats report (documents, depth, width, field counts) |
| `<project>.schema.html` | Entity-relationship diagram generated from `schema/tables/*.sql` |

---

## Typical workflow

```bash
# 1. Create the project
mongo2pg init --project-base /app/migration --project-name retail \
  --uri "mongodb://user:pass@localhost:27017"

# 2. Optionally edit config to add the default namespace:
#    NAMESPACE = retail
#    /app/migration/retail/config/retail.conf

# 3. Infer all collection schemas
mongo2pg infer -c /app/migration/retail/config/retail.conf

# 4. Generate PostgreSQL DDL
mongo2pg to-pg -c /app/migration/retail/config/retail.conf

# 5. Export data to gzipped CSV files
mongo2pg export -c /app/migration/retail/config/retail.conf

# 6. Generate HTML reports + ERD diagram
mongo2pg report -c /app/migration/retail/config/retail.conf
```

---

## Examples

```bash
# Infer a single collection (stdout only)
mongo2pg mongodb://localhost:27017 mydb.users

# Infer all collections in a database, saving output files
mongo2pg infer mongodb://localhost:27017 mydb -o source/collections

# Sample 10 % of documents
mongo2pg infer mongodb://localhost:27017 mydb.orders -p 10

# Convert all inferred schemas to SQL (project-based)
mongo2pg to-pg -c config/retail.conf

# Convert a single collection only
mongo2pg to-pg -c config/retail.conf products

# Deploy each collection into its own PostgreSQL schema (strips collection prefix from child table names)
mongo2pg to-pg -c config/retail.conf --schema-per-collection

# Deploy a single collection into a named schema
mongo2pg to-pg -c config/retail.conf orders --schema orders

# Generate reports
mongo2pg report -c config/retail.conf

# Export all collections to gzipped CSV
mongo2pg export -c config/retail.conf

# Export a single collection
mongo2pg export -c config/retail.conf orders

# Export to a custom output directory
mongo2pg export -c config/retail.conf -o /tmp/csv
```
