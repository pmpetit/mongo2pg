# Contributing

Contributions are welcome, here is a starting process to help you to set up your env.

## Step 0 — Bugfix/Feature workflow (branch + PR)

For every bug fix or feature, always start from `main` and open a Pull Request.

```bash
git checkout main
git pull --ff-only origin main

# pick clear branch name
git checkout -b fix/<short-description>
# or
git checkout -b feat/<short-description>
```

Then implement your changes, commit, and push:

```bash
git add -A
git commit -m "feat: <summary>"
# or: fix: <summary>
git push -u origin <your-branch>
```

Create a PR from `<your-branch>` into `main` and wait for CI + review.
After approvals, merge the PR.

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

---

## After PR merge — create a release

When your PR is merged, prepare the release with semantic versioning.
If you cannot push to `main` (protected branch), use a release branch + PR.

1. Update `CHANGELOG.md`:

- Move entries from `## [Unreleased]` to a new version section, e.g. `## [0.6.4] - YYYY-MM-DD`
- Keep the compare links at the bottom in sync

1. Remove pre-release references in docs:

- Replace any `0.0.0-pr.*` or preview tag examples with the final release tag (`vX.Y.Z`)
- Ensure install snippets and version links point to the final release

1. Update version in `Cargo.toml`:

- Set `version = "X.Y.Z"` to the release version (for example `0.6.4`)

1. Create a release preparation branch:

```bash
git checkout main
git pull --ff-only origin main
git checkout -b chore/release-vX.Y.Z
git add Cargo.toml CHANGELOG.md README.md docs/
git commit -m "chore(release): prepare vX.Y.Z"
git push -u origin chore/release-vX.Y.Z
```

1. Open a PR from `chore/release-vX.Y.Z` into `main` and merge it.

1. Tag the release (maintainer or anyone with tag permission):

```bash
git checkout main
git pull --ff-only origin main
git tag -a vX.Y.Z -m "Release vX.Y.Z"
git push origin vX.Y.Z
```

1. Publish GitHub release (UI or CLI, after tag is pushed):

```bash
gh release create vX.Y.Z --generate-notes
```

1. Verify release assets and install command in `README.md` are aligned with the new version.

If you do not have tag/release permissions, ask a maintainer to run the tag and GitHub release steps.
