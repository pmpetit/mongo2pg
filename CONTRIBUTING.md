# Contributing

Contributions are welcome, here is a starting process to help you to set up your env.

## Step 1 — Start MongoDB community in Docker

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

## Step 2 — Import the sample datasets

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
  results/infer/sample_weatherdata/config/sample_weatherdata.conf
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
  results/infer-with-jsonb/sample_weatherdata/config/sample_weatherdata.conf
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
