# How-To Guides

Practical recipes for common `mongo2pg` tasks.

---

## How to set up a migration project

Use `mongo2pg init` to create a repeatable project directory:

```bash
mongo2pg init \
  --project-base ./projects \
  --project-name airbnb \
  --cluster-name dev-cluster \
  --source-uri mongodb://localhost:27017 \
  --target-uri postgres://postgres:x@localhost:5432/postgres?sslmode=disable \
  --namespace sample_airbnb
```

This creates:

```text
projects/airbnb/
  dev-cluster/
    config/
      dev-cluster.toml
    source/
      collections/
    schema/
      tables/
    data/
    reports/
```

You can then pass `-c projects/airbnb/dev-cluster/config/dev-cluster.toml` to subsequent
commands instead of repeating all flags.

If you omit `--cluster-name`, layout stays `projects/<project-name>/...` and config file is
`config/<project-name>.toml`.

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

With `-c <config>`, `infer` also refreshes SQL files under `schema/tables/`
and the main reports under `reports/`.

When sampled data contains incompatible scalar families for same field (for
example numeric and string), `infer` stores warning details in
`<collection>.stats.yaml` and highlights affected collections in
`reports/main.html`.

---

## How to include or exclude collections from config

Set filters in the TOML config under `[source]`:

```toml
[source]
namespace = "sample_airbnb"
include = ["listingsAndReviews", "reviews"]
exclude = ["reviews"]
```

Filtering is applied consistently by `infer`, `to-pg`, `export`, and `import`.
If both lists are set, `exclude` takes precedence.

---

## How to export relational CSV files

```bash
mongo2pg export -c ./projects/airbnb/config/airbnb.toml
```

This expands nested documents and arrays into one `.csv.gz` file per generated
PostgreSQL table.

---

## How to load PostgreSQL objects and data

```bash
mongo2pg import -c ./projects/airbnb/config/airbnb.toml
```

This command:

- connects to PostgreSQL using `TARGET_URI`
- runs preflight to ensure the target database and schema exist (creates them when allowed)
- fails fast with actionable errors when database/schema creation lacks privileges
- stops early if destination tables already exist and asks operators to drop/clean them before retry
- executes `schema/tables/<db>/*.sql` only after preflight passes
- decompresses each exported `.csv.gz` file and loads it with `COPY`

---

## How to validate a PostgreSQL import

Set both `SOURCE_URI` and `TARGET_URI` in the config file, then run:

```bash
mongo2pg report \
  -c ./projects/airbnb/config/airbnb.toml \
  --post-import
```

This generates `reports/post_report.html`, which compares:

- MongoDB top-level document counts
- MongoDB expanded nested occurrence counts
- PostgreSQL row counts per generated table

The report highlights where counts match and where they differ.
