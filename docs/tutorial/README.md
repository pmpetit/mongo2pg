# Tutorial — From MongoDB Collection to PostgreSQL Load Validation

This walkthrough takes you from a MongoDB sample dataset to inferred schemas,
generated PostgreSQL DDL, exported CSV files, and a post-import validation
report.

!!! tip "Using a config file"
    Every parameter shown in this tutorial can be stored in a project config
    file created by `mongo2pg init`. Once that file exists, pass `-c <path>`
    instead of repeating flags on every command.

---

## Prerequisites

- `mongo2pg` installed and on your `PATH` (see [Installation](../install.md))
- a running MongoDB instance
- a running PostgreSQL instance
- a dataset already loaded into MongoDB

This tutorial uses `sample_airbnb.listingsAndReviews` as the example
collection.

---

## Step 1 — Create a project

```bash
mongo2pg init \
  --project-base ./projects \
  --project-name sample_airbnb \
  --source-uri 'mongodb://user:pass@localhost:27017/?authSource=admin' \
  --target-uri 'postgres://postgres:x@localhost:5432/postgres' \
  --namespace sample_airbnb
```

This creates:

```text
projects/sample_airbnb/
  config/
    sample_airbnb.conf
  source/
    collections/
  schema/
    tables/
  data/
  reports/
```

Add PostgreSQL connectivity to the generated config file:

```properties
SOURCE_URI = mongodb://user:pass@localhost:27017/?authSource=admin
TARGET_URI = postgres://user:pass@localhost:5432/sample_airbnb?sslmode=require
NAMESPACE = sample_airbnb
```

---

## Step 2 — Infer schemas and statistics

```bash
mongo2pg infer -c ./projects/sample_airbnb/config/sample_airbnb.conf
```

This writes one folder per collection under `source/collections/` with:

- `<collection>.json`
- `<collection>.stats.txt`
- `<collection>.stats.yaml`
- `mapping_<collection>.yaml`

---

## Step 3 — Generate PostgreSQL DDL

```bash
mongo2pg to-pg -c ./projects/sample_airbnb/config/sample_airbnb.conf
```

Each collection generates a `.sql` file under `schema/tables/<db>/`.

By default, each collection is deployed into its own PostgreSQL schema.

---

## Step 4 - Generate pre-import report

Standard pre-migration report:

```bash
mongo2pg report -c ./projects/sample_airbnb/config/sample_airbnb.conf
```

This will generate an HTML report in reports/main.html

It shows a scoring, objects created.

## Step 5 - Adapt the DDL

Maybe the table name, schema name or database do not reflect your needs. You can modify those SQL files. They will be used during the next steps, generating export files.

For example, some table name can be over 64c, they should be renamed.

## Step 6 — Export data as relational CSV files

```bash
mongo2pg export -c ./projects/sample_airbnb/config/sample_airbnb.conf
```

Base on the existing SQL files in `schema/tables/<db>/*.sql`

This writes `.csv.gz` files under `data/<db>/<collection>/`, one per generated
PostgreSQL table.

---

## Step 7 — Load into PostgreSQL

Run the generated SQL files, then load the `.csv.gz` files with `\COPY`.

Example:

```bash
psql "$PGURI" -f ./projects/sample_airbnb/schema/tables/sample_airbnb/listingsandreviews.sql
```

```sql
\COPY listingsandreviews FROM PROGRAM 'gunzip -c listingsandreviews.csv.gz' CSV HEADER;
\COPY address FROM PROGRAM 'gunzip -c address.csv.gz' CSV HEADER;
```

### sample_airbnb

Create tables

```bash
export PGURI="postgres://postgres:x@localhost:5432/postgres"
find projects/sample_airbnb/schema/tables/sample_airbnb -maxdepth 1 -type f -name '*.sql' | while read -r f; do
  echo "executing psql -f $f"
  psql "$PGURI" -f $f
done
```

during this step, you can see some errors message about constraint violations (not null / null).

This is because during the infer process, sample value (100 default) was not enough.

For example, it read 100 documents, and in all documents a field `REVIEWER_NAME` was always present (probability 1).

The pg column `REVIEWER_NAME` has constraint set to `NOT NULL`.

But during the effective import all docs are read/write, and one of them did not have the `REVIEWER_NAME` fields.

The solution is : modify the column in the DDL sql file and remove the NOT NULL constraint.

and re-run the table creation :

Drop database

```bash
for dir in projects/sample_airbnb/data/*; do
  [[ -d "$dir" ]] || continue
  dbname=$(basename "$dir")
  psql "$PGURI" -c "DROP DATABASE IF EXISTS \"$dbname\";"
done
```

Re-create the table

Create tables

```bash
find projects/sample_airbnb/schema/tables/sample_airbnb -maxdepth 1 -type f -name '*.sql' | while read -r f; do
  echo "executing psql -f $f"
  psql "$PGURI" -f $f
done
```

Re-execute the import

Insert

```bash
export PGURIT=$(printf '%s\n' "$PGURI" | sed 's#/postgres?#/sample_airbnb?#')

{
  echo "BEGIN;"
  echo "SET CONSTRAINTS ALL DEFERRED;"

  find projects/sample_airbnb/data/sample_airbnb -mindepth 2 -maxdepth 2 -type f -name '*.csv.gz' | sort | while read -r f; do
    schema=$(basename "$(dirname "$f")")
    table=$(basename "$f" .csv.gz)
    printf "TRUNCATE TABLE %s.%s CASCADE;\n" "$schema" "$table"
  done

  find projects/sample_airbnb/data/sample_airbnb -mindepth 2 -maxdepth 2 -type f -name '*.csv.gz' | sort | while read -r f; do
    schema=$(basename "$(dirname "$f")")
    table=$(basename "$f" .csv.gz)
    printf "\\COPY %s.%s FROM PROGRAM 'gunzip -c %s' CSV HEADER;\n" "$schema" "$table" "$f"
  done

  echo "COMMIT;"
} | psql "$PGURIT"
```

---

## Step 8 — Generate post import report

Post-import validation report:

```bash
mongo2pg report \
  -c projects/sample_airbnb/config/sample_airbnb.conf \
  --post-import \
  --namespace sample_airbnb
```

The post-import report writes `reports/post_report.html` and compares:

- MongoDB top-level document counts
- MongoDB expanded nested occurrence counts
- PostgreSQL row counts per generated table

For nested nodes such as `address`, `reviews`, or `address.location.coordinates`,
the report shows the MongoDB occurrence count beside the matching PostgreSQL
table count so you can verify the relational expansion.
