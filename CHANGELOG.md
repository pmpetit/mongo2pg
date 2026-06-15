# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.5.3] - 2026-06-15

### Changed

- **Config file**: Added `include` and `exclude` parameters to the `[source]` section in TOML config files. These allow users to specify which collections to include or exclude during inference, export, and import steps. The `exclude` list takes precedence over `include` if both are specified.
- **Primary key and foreign key handling**: Now supports multi-column primary keys and corresponding foreign key relationships throughout the export and import logic. The internal representation and SQL generation have been updated to handle composite keys, and row extraction logic now correctly maps MongoDB `_id` fields to the appropriate SQL columns, including flattened object ID fields for composite PKs.
- **Type inference**: Simplified type inference for number fields, mapping MongoDB `ObjectId` to `TEXT` instead of `UUID`, and ensuring numeric strings remain `TEXT`.
- **Analyzer**: Empty arrays no longer count as present for probability calculations.
- **CLI**: `to-pg`, `infer`, `export`, and `import` commands now honor the `include` and `exclude` config parameters for collection filtering.
- **Infer warnings and HTML reports**: `infer` now emits warnings when a field mixes incompatible scalar families in the sampled source data, persists those warnings in stats YAML, and highlights affected collection names in `reports/main.html` with inline warning details and the first five distinct sampled examples for each observed type.
- **Tests**: Added comprehensive tests for multi-column PKs, collection filtering, and row extraction logic.

### Fixed

- **Composite PKs**: Fixed bugs in row extraction and SQL generation for tables with composite primary keys.
- **Collection filtering**: Fixed issues where excluded collections could still be processed.
- **Error reporting**: Improved error messages for missing SQL schemas and import/export failures, including line-level details for CSV import errors.
- **Child tables**: Child tables now drop redundant nested `id` fields instead of renaming them.
- **Post-import report**: Now filters collections using sanitized names and honors `include`/`exclude` lists.
- **`check-md5` normalization**: Canonicalized numeric JSON literals and normalized MongoDB values according to the target PostgreSQL column type before hashing and sorting, avoiding false mismatches such as `647.0` versus `647` and numeric Mongo values compared against PostgreSQL `TEXT` columns.
- **`check-md5` row ordering**: Comparison rows for nested exports such as `advisors_advices_earnings` now use the normalized target-side values consistently, so ordering no longer creates spurious mismatches.
- **Non-JSONB export schema lookup**: `export` now resolves collection schemas from both nested `source/collections/<db>/` layouts and older flat layouts, restoring CSV generation for collections such as `engine` in non-JSONB projects.

## [0.4.0] - 2026-05-25

### Added

- **`to-pg --schema <NAME>`**: deploy all tables for a collection into a dedicated PostgreSQL schema
  - Prepends `CREATE SCHEMA IF NOT EXISTS <name>; SET search_path = <name>;` to the generated SQL
  - Strips the `{schema}_` prefix from every child table name so names are shorter and schema-qualified references are unambiguous (e.g. `b2bsalesorder_lines` → `lines` inside schema `b2bsalesorder`)
  - Prefix matching is case-insensitive (sanitized before comparison) to handle mixed-case collection names
- **`to-pg` now uses one PostgreSQL schema per collection by default**: each generated SQL file gets its own self-contained schema preamble, and `--schema <NAME>` remains available to override that default with a fixed schema name
- **Output SQL filenames are now lowercased**: `B2BSalesOrder.sql` → `b2bsalesorder.sql`, consistent with PostgreSQL identifier folding
- **Integrated workflow**: `infer -c <config>` now refreshes generated SQL and the main HTML reports; post-import validation remains available through `report --post-import` after loading data into PostgreSQL
- **MongoDB connection inputs are now named `SOURCE_URI` / `--source-uri`** across the CLI and generated config files
- **Project config files now use TOML**: `mongo2pg init` generates `<project>.toml` with `[project]`, `[source]`, and `[target]` sections, while runtime parsing remains backward-compatible with older flat `.conf` files
- **`import` command**: creates PostgreSQL objects from `schema/tables/<db>/*.sql`, loads exported `.csv.gz` files with `COPY`, and targets the configured PostgreSQL database automatically
- **Automatic post-import validation**: `import -c <config>` now regenerates `reports/post_report.html` after a successful load; `report --post-import` remains available to rerun it later
- **Schema JSON output is now opt-in**: `infer` keeps the human-readable statistics/progress output by default, and the full inferred schema JSON is printed only with `--print-json`
- **Regression test `test_camelcase_collection_table_count_matches_report`**: verifies end-to-end that the number of `CREATE TABLE` statements produced by `to-pg` for a camelCase collection name equals the number of PG tables shown in the HTML report (catches the filename mismatch bug)
- **CI now runs `cargo test` on every PR**: a `test` job was added to `pr-preview.yml`; the build and preview-release jobs only proceed once all tests pass

### Fixed

- **HTML report no longer shows 0 PG tables for camelCase collection names**: `collect_rows` now lowercases the collection directory name when resolving the SQL file path (e.g. `listingsAndReviews/` → `listingsandreviews.sql`), consistent with how `to-pg` writes the file
- **`$sample` sort-memory failures no longer abort inference**: when `$sample` fails with a sort-memory-limit error (MongoDB error 292, common on Atlas shared/free tiers) or any other aggregation error, the tool now automatically falls back to `find().limit(<sample_size>)`, which has no sort stage and works on all tiers
- **Cursor deserialization errors trigger the same fallback**: if a `$sample` cursor emits a document-level error mid-iteration, the partial result is discarded and `find().limit()` is retried from scratch
- **Per-collection errors no longer abort database-wide or server-wide inference**: conversion errors on individual collections are now caught, a warning is printed to stderr, and the remaining collections continue to be processed
- **Per-database errors no longer abort server-wide inference**: if `list_collection_names()` fails for a database, that database is skipped with a warning and iteration continues
- **`estimated_document_count()` failure is now non-fatal**: falls back to the number of sampled documents instead of propagating the error
- **`SAMPLE_MAX_TIME` cap (120 s) added** to both the `$sample` aggregation and the `find()` fallback to prevent runaway queries
- **`export` now resolves SQL schemas by sanitized collection name only**: MongoDB collection names stay raw for querying, but schema lookup uses the lowercased sanitized filename (for example `listingsAndReviews` → `listingsandreviews.sql`), fixing mixed-case collection/export mismatches
- **`export --namespace` now overrides the config namespace consistently**: export reads SQL from `schema/tables/<db_name>/` with no fallback to older flat locations
- **Export output layout is now database-scoped**: generated `.csv.gz` files are written under `data/<database_name>/<sanitized_collection_name>/`
- **Export output now prints the configured `.csv.gz` path for each table**: row counts are logged alongside the project-relative file location during export instead of an absolute canonicalized path
- **Config parsing now uses `SOURCE_URI` and `TARGET_URI` keys** for MongoDB and PostgreSQL connection settings
- **`infer -c <config>` now writes DDL under `schema/tables/<db>/` for single-database projects too**: the chained `to-pg` step now prefixes flat collection layouts with the configured database name so downstream export/import/report steps use one consistent SQL layout
- **Single-database reports now resolve PostgreSQL tables from `schema/tables/<db>/`**: collection rows in `reports/main.html` once again show expandable PG tables after the per-database SQL layout change
- **`mongodb` dependency bumped** from 3.2.5 to 3.6.0

---

## [0.3.7] - 2026-05-07

### Added

- **Expandable DDL in HTML reports**: PG table pills in the per-collection detail row are now clickable
  - Clicking a table pill toggles a dark-themed `<pre>` block showing the full `CREATE TABLE` DDL
  - An animated `▶` arrow indicates the collapsed/expanded state
  - Works in both single-database and multi-database reports

---

## [0.3.6] - 2026-05-07

### Added

- **Database and server-level `infer` iteration**
  - `--namespace <db>` (no collection) now infers every collection in the database
  - Omitting `--namespace` entirely enumerates all user databases on the server and infers all their collections (`admin`, `local`, and `config` are always skipped)
  - Output files are named `<dbname>_<collname>.json` / `<dbname>_<collname>.stats.txt`
  - Per-database HTML report (`reports/<dbname>.html`) and Mermaid ER diagram (`reports/<dbname>.schema.html`) are generated automatically
- `build_mongo_mermaid` and `render_mongo_schema_html` helpers in `schema_diagram.rs` for ER diagram generation from inferred schemas
- `SYSTEM_DATABASES` constant exported from the library crate to avoid duplication

### Fixed

- `--uri` was erroneously accepted by `to-pg` (which does not connect to MongoDB) – option removed

---

## [0.3.5] - 2026-05-05

### Added

- Score surfaced in the cluster-level report

### Fixed

- Score computation was incorrectly limited to the first 20 sampled values – now uses all sampled values
- System views are excluded from collection inference
- Warning emitted when a referenced database is not found on the server

---

## [0.3.4] - 2026-05-05

### Added

- `cluster-report` subcommand: aggregates migration scores across multiple databases into a single cluster-level HTML report
  - Accepts one or more `--configs` paths (comma-separated or repeated)
  - Cluster label derived from the first config URI when `--cluster` is not provided
- `distinct_fields_over_avg_fields_per_doc` metric added to both text (`.stats.txt`) and YAML (`.stats.yaml`) stats outputs

---

## [0.3.3] - 2026-05-03

### Added

- **Migration complexity score** per collection and database in HTML report
  - Per-collection score: `C = depth_max/2 + array_fields + distinct_fields/avg_fields_per_doc`
  - DB-level score: `C_db = 1.5 × N_collections + Σ C_i`
  - Three summary metrics surfaced in the report header: total score, doc-weighted average, max collection
  - Easy / Medium / Hard badge with colour-coded thresholds (< 30 / 30–80 / > 80)
  - Per-row score column in the collections table, colour-coded green/orange/red
- Score is **JSONB-strategy-aware**: nested Object depth is not counted when `--jsonb` is active (those branches become a single opaque column, not relational depth)
- HTML report title and subtitle now show the **cluster host** and **database name** extracted from the connection URI
- `cluster_from_uri` public helper in `report.rs` to strip credentials from a URI for display

### Changed

- `render_html` signature gains a `cluster: &str` parameter

---

## [0.3.2] - 2026-05-03

### Added

- `CONTRIBUTING.md` with contribution guidelines
- `CHANGELOG.md`
- `--jsonb` flag on `infer` to emit Object fields as JSONB columns instead of child tables
- `sampled` field on `TypeSchema` at all nesting levels
- `Taskfile.yml` for build and test automation (`infer`, `infer-with-jsonb` tasks)
- `--uri` CLI override for commands that connect to MongoDB (`infer`, `export`)
- `--number` / `--percent` fallback from config file

### Fixed

- `--uri` was erroneously required on `to-pg`, `report`, and `schema` subcommands (which don't connect to MongoDB)

---

## [0.3.1] - 2026-05-02

### Added

- HTML report now shows the number of PostgreSQL tables created
- Tutorial documentation for the export workflow
- Default values written as comments in generated `.conf` file (`URI`, `NAMESPACE`, `NUMBER`, `PERCENT`)

### Fixed

- Dead code removed from analyzer

---

## [0.3.0] - 2026-05-01

### Added

- `export` subcommand to stream MongoDB documents to CSV/JSONL
- `report` subcommand to generate an HTML report from inferred schemas
- Layout and reports folder structure in project init

### Fixed

- Sample aggregation stats computation
- Removed unused worker parameters from CI

---

## [0.2.2] - 2026-04-30

### Added

- Distinct value sampling in schema output
- Apple Silicon runner in CI

### Changed

- Removed document count from tree view; replaced with sample count

---

## [0.2.1] - 2026-04-30

### Added

- `to-pg` subcommand to generate PostgreSQL DDL from inferred schemas
- Community MongoDB disclaimer in README

### Changed

- Renamed `prop_in_object` to `probability` throughout
- Improved INSTALL.md to match actual JSON output format

---

## [0.1.0] - 2026-04-29

### Added

- Initial release
- `infer` subcommand to sample MongoDB collections and output a JSON schema
- Schema analysis with type detection, nested objects, arrays, and probability scores
- CI pipeline with GitHub Actions

[Unreleased]: https://github.com/pmpetit/mongo2pg/compare/v0.5.3...HEAD
[0.5.3]: https://github.com/pmpetit/mongo2pg/compare/v0.4.0...v0.5.3
[0.4.0]: https://github.com/pmpetit/mongo2pg/compare/v0.3.7...v0.4.0
[0.3.7]: https://github.com/pmpetit/mongo2pg/compare/v0.3.6...v0.3.7
[0.3.6]: https://github.com/pmpetit/mongo2pg/compare/v0.3.5...v0.3.6
[0.3.5]: https://github.com/pmpetit/mongo2pg/compare/v0.3.4...v0.3.5
[0.3.4]: https://github.com/pmpetit/mongo2pg/compare/v0.3.3...v0.3.4
[0.3.3]: https://github.com/pmpetit/mongo2pg/compare/v0.3.2...v0.3.3
[0.3.2]: https://github.com/pmpetit/mongo2pg/compare/v0.3.1...v0.3.2
[0.3.1]: https://github.com/pmpetit/mongo2pg/compare/v0.3.0...v0.3.1
[0.3.0]: https://github.com/pmpetit/mongo2pg/compare/v0.2.2...v0.3.0
[0.2.2]: https://github.com/pmpetit/mongo2pg/compare/v0.2.1...v0.2.2
[0.2.1]: https://github.com/pmpetit/mongo2pg/compare/v0.1.0...v0.2.1
[0.1.0]: https://github.com/pmpetit/mongo2pg/releases/tag/v0.1.0
