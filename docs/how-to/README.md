# How-To Guides

Practical recipes for common `mongo2pg` tasks.

---

## How to set up a migration project

Use `mongo2pg init` to create a repeatable project directory:

```bash
mongo2pg init \
  --project-base ./projects \
  --project-name airbnb \
  --source-uri mongodb://localhost:27017 \
  --namespace sample_airbnb
```

This creates:

```text
projects/airbnb/
  config/
    airbnb.conf
  source/
    collections/
  schema/
    tables/
  data/
  reports/
```

You can then pass `-c projects/airbnb/config/airbnb.conf` to subsequent
commands instead of repeating all flags.

---

## How to infer schemas

### Single collection

```bash
mongo2pg infer \
  --source-uri mongodb://localhost:27017 \
  --namespace sample_airbnb.listingsAndReviews \
  --output-dir ./output \
  --number 2000
```

### All collections in a database

```bash
mongo2pg infer \
  --source-uri mongodb://localhost:27017 \
  --namespace sample_airbnb \
  --output-dir ./output
```

---

## How to generate PostgreSQL DDL

```bash
mongo2pg to-pg -c ./projects/airbnb/config/airbnb.conf
```

This reads inferred schemas from `source/collections/` and writes SQL to
`schema/tables/`.

---

## How to export relational CSV files

```bash
mongo2pg export -c ./projects/airbnb/config/airbnb.conf
```

This expands nested documents and arrays into one `.csv.gz` file per generated
PostgreSQL table.

---

## How to validate a PostgreSQL import

Set both `SOURCE_URI` and `TARGET_URI` in the config file, then run:

```bash
mongo2pg report \
  -c ./projects/airbnb/config/airbnb.conf \
  --post-import \
  --namespace sample_airbnb
```

This generates `reports/post_report.html`, which compares:

- MongoDB top-level document counts
- MongoDB expanded nested occurrence counts
- PostgreSQL row counts per generated table

The report highlights where counts match and where they differ.
