# Contributing

Contributions are welcome, here is a starting process to help you to set up your env.

## Step 1 — Start the local Docker stack

```bash
cd docker
docker compose up -d
```

This stack starts:

- PostgreSQL
- MongoDB as a single-node replica set
- a one-shot `mongodb-init` container that initializes the replica set
- a one-shot `mongodb-seed` container that clones the sample dataset repository and imports it into MongoDB only when the sample datasets are not already present
- Kafka and Schema Registry

Verify the long-running services are up:

```bash
docker compose ps
```

If you want a clean restart that also recreates the MongoDB data volume and reloads the sample datasets, run:

```bash
docker compose down -v
docker compose up -d
```

---

## Step 2 — MongoDB sample data seeding details

The Compose stack now seeds MongoDB automatically with:

```bash
git clone https://github.com/neelabalan/mongodb-sample-dataset
cd mongodb-sample-dataset
bash script.sh 'mongodb://user:pass@mongodb:27017/?authSource=admin'
```

That command is executed by the `mongodb-seed` service inside Docker after the replica set is ready.
If `sample_mflix` is already present, the seeding container exits immediately without re-importing the sample datasets.

If you want to rerun the sample import manually outside Compose, clone the same repository and run:

```bash
bash script.sh localhost 2717 user pass
```

## Tasks

I have created some tasks, to help you run local migrations.

those 2 tasks will

🔍 Infer — samples your MongoDB collections and produces a probabilistic schema (field types, presence rates, nesting metrics)

📐 Assess — surfaces structural metrics (depth, width, branch factor) that reveal whether a collection truly needs a document model or would be fine in a relational schema

🛠️ Convert — generates PostgreSQL DDL from the inferred schema, flattening nested arrays and objects into child tables with foreign keys

📊 Report — produces an HTML stats report and a Mermaid entity-relationship diagram of the resulting schema

### infer

Runs infer against the previously imported databases. Results are stored in results/ folder.

```bash
task infer

(...)
📁 init sample_weatherdata
Project 'sample_weatherdata' initialised at results/infer/sample_weatherdata
  results/infer/sample_weatherdata/schema/tables
  results/infer/sample_weatherdata/source/collections
  results/infer/sample_weatherdata/data
  results/infer/sample_weatherdata/config
  results/infer/sample_weatherdata/reports
  results/infer/sample_weatherdata/config/sample_weatherdata.toml
📐 infer sample_weatherdata.data
🛠️  to-pg sample_weatherdata_data
tables : 44
columns: 169
SQL written to results/infer/sample_weatherdata/schema/tables/sample_weatherdata_data.sql
📊 report
Report written to results/infer/sample_weatherdata/reports/report.html
👉 results in results/infer
(...)
```

### infer-with-jsonb

It uses the `jsonb` pg column to store objects that are not array, for example address. It is here for testing, to see results.

```bash
task infer-with-jsonb

(...)
📁 init sample_weatherdata
Project 'sample_weatherdata' initialised at results/infer-with-jsonb/sample_weatherdata
  results/infer-with-jsonb/sample_weatherdata/schema/tables
  results/infer-with-jsonb/sample_weatherdata/source/collections
  results/infer-with-jsonb/sample_weatherdata/data
  results/infer-with-jsonb/sample_weatherdata/config
  results/infer-with-jsonb/sample_weatherdata/reports
  results/infer-with-jsonb/sample_weatherdata/config/sample_weatherdata.toml
📐 infer sample_weatherdata.data
🛠️ to-pg sample_weatherdata_data
tables : 11
columns: 58
SQL written to results/infer-with-jsonb/sample_weatherdata/schema/tables/sample_weatherdata_data.sql
📊 report
Report written to results/infer-with-jsonb/sample_weatherdata/reports/report.html
👉 results in results/infer-with-jsonb
(...)
```

I have added the results/ folder to give you an idea of the results.
