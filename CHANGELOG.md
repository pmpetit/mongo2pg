# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.8.0](https://github.com/pmpetit/mongo2pg/compare/mongo2pg-v0.7.0...mongo2pg-v0.8.0) (2026-09-09)

### Fixed

* correct release metadata and preserve manual workflow triggers

## [0.7.0](https://github.com/pmpetit/mongo2pg/compare/mongo2pg-v0.6.0...mongo2pg-v0.7.0) (2026-09-08)

### Features

* add cluster-level migrability score and cluster-report subcommand ([d2594f1](https://github.com/pmpetit/mongo2pg/commit/d2594f180fbb2d4914c19c4472e5e22ef0949ad2))
* add docker compose ([93d541c](https://github.com/pmpetit/mongo2pg/commit/93d541c7d6d812bbc9c112305700e5aa139d9bd8))
* add docker compose for kafka ([d1404a5](https://github.com/pmpetit/mongo2pg/commit/d1404a527389c41fd47c0af993896e469577aa06))
* add docker compose for kafka ([8dd9964](https://github.com/pmpetit/mongo2pg/commit/8dd996478258d8840ac7a191a0b10496a7fc0e42))
* add exclude/include properties, fix number to double ([8732808](https://github.com/pmpetit/mongo2pg/commit/873280839c7af4f7d4c206ed418726cfb52ef6d7))
* add wf to build package ([79ee354](https://github.com/pmpetit/mongo2pg/commit/79ee354fb8882360e27d2602e59b70e4117bd927))
* add wf to build package ([25619ad](https://github.com/pmpetit/mongo2pg/commit/25619ad78e75f911a6cf1c866c6ead8fdd5fc914))
* add wf to build package ([571db32](https://github.com/pmpetit/mongo2pg/commit/571db32ef7530873d0dd9dfd5284fae73e091f94))
* **branch:** add count(*) ([8f3c5f3](https://github.com/pmpetit/mongo2pg/commit/8f3c5f3500d07dcffdd70ed3b75d80ea83302d4d))
* branching factors with tests ([1e75d31](https://github.com/pmpetit/mongo2pg/commit/1e75d3100861d8a6963eeb4f2fa8cdf7f5e7fc8a))
* branching factors with tests ([d212aac](https://github.com/pmpetit/mongo2pg/commit/d212aacb9818369f14777c44047002613155bc28))
* cluster-level migrability score and `cluster-report` subcommand ([f5a0da4](https://github.com/pmpetit/mongo2pg/commit/f5a0da4de044b147d1da43c062d275b06fdf3ee1))
* **export:** add export command ([3c3ce49](https://github.com/pmpetit/mongo2pg/commit/3c3ce49ca2259e258d467d81c5406a20ad6bdbad))
* infer all user databases when namespace is omitted ([9898a96](https://github.com/pmpetit/mongo2pg/commit/9898a96ae18d7eaf1eb218d9e86e15b6187e5608))
* **infer:** enumerate all user databases when --namespace is omitted ([b56c7d9](https://github.com/pmpetit/mongo2pg/commit/b56c7d95d570a359533dad89f23eed33b6144cfc))
* merge some report command ([c4b0025](https://github.com/pmpetit/mongo2pg/commit/c4b002531cc8476022594cfcb0de356e9cc72556))
* **reports:** ora2pg like ([7fb2a3e](https://github.com/pmpetit/mongo2pg/commit/7fb2a3e8c876e12b83b82b0408c21926dad5c30f))
* **score:** add collection score ([ccfb47e](https://github.com/pmpetit/mongo2pg/commit/ccfb47e66b2b23c0362519019c1150cd2e729ea4))
* **score:** to help cluster choice, add a complexity score ([d5a1e02](https://github.com/pmpetit/mongo2pg/commit/d5a1e02884ea6ce0261246b1e8eb13f995468172))
* **to-pg:** from json generate sql file ([6126c86](https://github.com/pmpetit/mongo2pg/commit/6126c86bdbcccb08e69c68400b3c3589cd357b9d))
* **version:** bump to 0.5.1 ([b8190c3](https://github.com/pmpetit/mongo2pg/commit/b8190c306bb9cb7ce3297694902be23caeb8b6f6))
* **warnings:** add warnings on fields ([2290439](https://github.com/pmpetit/mongo2pg/commit/2290439ecca510f5d4685db5d54506f542a15732))

### Bug Fixes

* add namespace to export cmd ([5c761b7](https://github.com/pmpetit/mongo2pg/commit/5c761b792d8ff1d15b5b23c3b008955a4fafb38f))
* convert error & sort on disk ([8cf59c7](https://github.com/pmpetit/mongo2pg/commit/8cf59c7857898860d689c2f11bdbfeca7dc3baf9))
* convert error & sort on disk ([2eecfb0](https://github.com/pmpetit/mongo2pg/commit/2eecfb07c2fb9d4e760136e4a8fcd422ee15b6ed))
* convert error & sort on disk ([87a23a0](https://github.com/pmpetit/mongo2pg/commit/87a23a035e965c44e73f8143c44906bbad39632c))
* error 292 ([30ccfa9](https://github.com/pmpetit/mongo2pg/commit/30ccfa984fdf9644fbd5f0b35a6a2b48ad9bea73))
* error 292 ([e75ab15](https://github.com/pmpetit/mongo2pg/commit/e75ab15e84c6ba1fdab537aa84c90c21c7ccf135))
* **export:** export command failure with CamelCase collection ([6622665](https://github.com/pmpetit/mongo2pg/commit/66226652ca46c12974db208224d492decd495e56))
* filename.sql in lowercase instead of CamelCase ([89aa56b](https://github.com/pmpetit/mongo2pg/commit/89aa56b027ef97a70387c95fef12b67cb5e80212))
* filename.sql in lowercase instead of CamelCase, modify the changelog ([583b701](https://github.com/pmpetit/mongo2pg/commit/583b701bdf815fb0ae18447d2c7a9e1cb245934b))
* **stats:** analytics stats, add warning about collection name ([6a8d993](https://github.com/pmpetit/mongo2pg/commit/6a8d99337781fa5d56af03b78006d085143c03d1))

## [Unreleased]

## [0.6.1] - 2026-09-08

### Changed

* **CI workflow sequencing**: PR preview pipeline now runs unit tests and reusable E2E validation before packaging preview artifacts.
* **CI workflow refresh**: updated workflow runner and build/release wiring in preview automation.
* **PR preview workflow job naming**: renamed the unit test job key from `test` to `tests-unit` and the reusable E2E job key from `e2e-tests` to `tests-e2e` for clearer CI graph naming.

### Fixed

* **PR preview release authorization**: preview release and cleanup steps now prefer `RELEASE_PAT` (with fallback to `GITHUB_TOKEN`) to avoid `HTTP 403: Resource not accessible by integration` on release API calls in restricted-token contexts.
* **Fork PR release failures**: preview release publication now runs only for same-repository pull requests, preventing release creation attempts in fork PR runs where release permissions are unavailable.

## [0.7.3] - 2026-09-07

### Changed

* **Command preflight connectivity checks**: `infer` now validates MongoDB source connectivity before schema sampling, `to-pg` validates PostgreSQL target connectivity before DDL generation (config mode), and `kafka-import` validates source + target + Kafka before processing.
* **Grouping gate behavior**: automatic collection grouping in `to-pg` now runs only when `source.add_grouped_key = true`; with default/false settings, versioned collections (for example `_v3` / `_v4`) remain separate SQL targets.
* **Ping failure diagnostics context**: backend ping failures now include safe connection context (`type`, `host`, `port`, `username`, redacted `url`) to speed up troubleshooting without exposing credentials.
* **Ping process network context**: ping failures now also include `process_ip`, showing the runtime egress/local process IP used to reach the target endpoint.

### Fixed

* **Export SQL lookup for grouped versioned collections**: when grouped mapping points to a versioned table name but only a shared base SQL file exists, export now falls back to that shared SQL schema instead of skipping the collection.

## [0.7.2] - 2026-09-04

### Added

* **GCS project folder markers**: `init` now creates visible markers for the standard project prefixes in GCS.
* **Normal report PG table details**: report collection rows can expand to show generated PostgreSQL tables and DDL from `schema/tables`.

### Changed

* **GCS infer pipeline**: chained `infer -c` now uploads inferred collection artifacts before running `to-pg`, then runs `report` afterward.
* **GCS staged artifact persistence**: `to-pg` and `report` upload generated schema and report artifacts from their temporary staging directories.
* **Schema SQL lookup**: report table discovery supports grouped table names, nested schema directories, case differences, and common `CREATE TABLE` formatting.

### Fixed

* **Fallback infer with GCS config**: fallback inference now uploads local artifacts before chained `to-pg` processing instead of searching an empty bucket prefix.
* **Init namespace logging**: runtime logs now report the namespace passed to `init` instead of `unknown`.

### Added

* **Kafka copy-mode tuning options**: new `[kafka]` settings for copy-mode imports (`copy_mode`, `transaction_batch_size`, `flush_batch_after`, `worker_count`, `stop_on_no_lag`, `group_id_log_suffix`).
* **Kafka write-mode diagnostics artifact**: post-import reporting now produces `kafka_import_write_mode.stats.yaml` and CI exposes it on failures.
* **`sample_events` e2e fixtures**: added local event datasets for deterministic online test coverage.
* **Datadog service override flag**: added global CLI option `--dd-service` to set runtime service name for telemetry output.
* **Datadog config section support**: added optional `[DATADOG]` config key `dd_service`.
* **Structured runtime JSON logs**: runtime logs now support JSON formatting suitable for log pipelines.

### Changed

* **Kafka import command layout**: moved the runtime handler into `src/commands/kafka_import.rs`, routed `KafkaImport` through the central command dispatcher, and replaced binary-relative wildcard imports with explicit module dependencies without changing CLI behavior.
* **Kafka import behavior in copy mode**: improved flushing and multi-worker runtime handling for long-running/large topic imports.
* **CI execution model**: refreshed self-hosted runner and release/e2e workflow wiring.
* **Trace volume in Kafka import**: removed per-message tracing in high-throughput path and kept stage-level spans to reduce cardinality and overhead.
* **Unified telemetry service naming**: trace and log service names now resolve through the same runtime logic (CLI override + config fallback).
* **Telemetry context enrichment**: `project_name` and `namespace` are now emitted on runtime JSON logs and attached to retained runtime spans.
* **Kafka tracing model**: replaced trace-file workflow with OTLP-only stage spans; no local/GCS Chrome trace artifacts are produced.

### Removed

* **Kafka trace-file CLI/runtime options**: removed `--trace`, `--duration`, and `--max-size` plus all related trace artifact persistence logic.
* **`[DATADOG].dd_service` runtime dependency**: service naming now resolves from CLI/env/fallback logic without requiring DATADOG config section.

### Fixed

* **CI PostgreSQL client availability**: added privilege-aware package installation and docker-backed `psql` shim fallback for non-privileged runners.
* **Debezium topic readiness checks**: fixed flaky/zero-count waits in e2e by using per-database prefixes and broker-compatible offset counting, with strict message-presence validation for online tests.
* **Kafka `stop_on_no_lag` transparency**: added clearer wait/reset heartbeat logs so stabilization delays are visible.
* **Kafka lag accuracy**: fixed false “stuck lag” situations when consumer position is temporarily unavailable.
* **Local e2e seeding**: `mongodb-seed-e2e` now skips cleanly when the selected local dataset folder is missing.
* **Observability test compatibility**: updated runtime log formatting unit tests after formatter signature expansion.

## [0.6.5] - 2026-07-29

### Added

* **force**: add force option to kafka-import to re-use the same table in sink connector

### Fixed

* **import**: can choose different pg database_name and mongo database_name. To prevent having pg database `mongdb-mydb`

## [0.6.4] - 2026-07-29

### Added

* **`ping` command**: added `mongo2pg ping -c <config> [--source] [--target] [--kafka]` to validate backend connectivity without running infer/export/import.
* **Per-backend ping status**: ping now reports pass/fail per selected backend and exits non-zero when any selected backend check fails.
* **Schema owner emission in generated SQL**: when `target.uri` contains a PostgreSQL username, generated DDL now includes `ALTER SCHEMA ... OWNER TO ...` after schema creation.

### Changed

* **Init config defaults**: `mongo2pg init` now writes `target.schema_name` as an active key (not commented) and defaults it to the same value as `target.database_name`.
* **Release docs for contributors**: contribution guide now includes branch/PR workflow from `main` and post-merge release steps.
* **Operational documentation refresh**: updated reference/how-to/tutorial docs to reflect import preflight behavior, Kafka topic precedence (`--topics` over `topic_prefix`), and the new `ping` command usage.
* **Scoring context marker (`search_node`)**: infer/report now detects MongoDB Search node capability (`$listSearchIndexes`) and adds a marker in score summaries to highlight features without direct PostgreSQL equivalent.

### Fixed

* **`to-pg` target database selection**: SQL preamble (`CREATE DATABASE` / `\connect`) now prefers `target.database_name` from config instead of deriving database name from `source.namespace` path layout.
* **Schema ownership mismatch**: generated schema no longer defaults to execution-role ownership when a target URI username is available; owner is explicitly set in generated SQL.
* **Connection attribution coverage**: connection-attribution specs now include ping command failures.

## [0.6.3] - 2026-07-24

### Added

* **Kafka import dead-letter queue (DLQ)**: when a Kafka message cannot be applied to PostgreSQL, the original message is now copied to topic `dlq_<source_topic>` with the original key/payload when available.
* **Kafka config `batch_log_messages`**: added `[kafka].batch_log_messages` to control progress log frequency during `kafka-import` (default remains 100 when unset).

### Changed

* **Kafka import nested mapping support**: Kafka upsert/delete logic now traverses nested mapping trees (child-of-child tables) instead of only root direct children, so mappings such as `.address.location` are applied correctly.
* **Kafka import progress logging cadence**: replaced hard-coded `100`-message logging intervals with configurable `batch_log_messages`.

### Fixed

* **CLI default dispatch panic**: removed brittle `expect("clap ensures args are present")` fallback and added safe handling for no-subcommand invocation.
* **CLI Infer argument duplication**: removed duplicate `--source-uri` definition in `infer` args that triggered Clap debug assertions.
* **Kafka nested insert cast for serial PKs**: normalized casts so PostgreSQL does not receive `CAST(... AS BIGSERIAL|SERIAL|SMALLSERIAL)` in recursive insert paths.
* **Post-import `check-md5` Mongo sort limit (error 13103)**: removed MongoDB-side sort-by-all-fields in MD5 collection flow and rely on local deterministic sorting to avoid "too many compound keys" failures.
* **Post-import `check-md5` type mismatch noise**: when a MongoDB source field is string-only but the mapped PostgreSQL target type is non-text (not `TEXT`/`VARCHAR`-family), that column is now excluded from MD5 comparison to prevent false mismatches (for example `_id` string vs `BIGSERIAL`, string dates vs `TIMESTAMP`).

## [0.5.3] - 2026-06-15

### Changed

* **Config file**: Added `include` and `exclude` parameters to the `[source]` section in TOML config files. These allow users to specify which collections to include or exclude during inference, export, and import steps. The `exclude` list takes precedence over `include` if both are specified.
* **Primary key and foreign key handling**: Now supports multi-column primary keys and corresponding foreign key relationships throughout the export and import logic. The internal representation and SQL generation have been updated to handle composite keys, and row extraction logic now correctly maps MongoDB `_id` fields to the appropriate SQL columns, including flattened object ID fields for composite PKs.
* **Type inference**: Simplified type inference for number fields, mapping MongoDB `ObjectId` to `TEXT` instead of `UUID`, and ensuring numeric strings remain `TEXT`.
* **Analyzer**: Empty arrays no longer count as present for probability calculations.
* **CLI**: `to-pg`, `infer`, `export`, and `import` commands now honor the `include` and `exclude` config parameters for collection filtering.
* **Infer warnings and HTML reports**: `infer` now emits warnings when a field mixes incompatible scalar families in the sampled source data, persists those warnings in stats YAML, and highlights affected collection names in `reports/main.html` with inline warning details and the first five distinct sampled examples for each observed type.
* **Tests**: Added comprehensive tests for multi-column PKs, collection filtering, and row extraction logic.

### Fixed

* **Composite PKs**: Fixed bugs in row extraction and SQL generation for tables with composite primary keys.
* **Collection filtering**: Fixed issues where excluded collections could still be processed.
* **Error reporting**: Improved error messages for missing SQL schemas and import/export failures, including line-level details for CSV import errors.
* **Child tables**: Child tables now drop redundant nested `id` fields instead of renaming them.
* **Post-import report**: Now filters collections using sanitized names and honors `include`/`exclude` lists.
* **`check-md5` normalization**: Canonicalized numeric JSON literals and normalized MongoDB values according to the target PostgreSQL column type before hashing and sorting, avoiding false mismatches such as `647.0` versus `647` and numeric Mongo values compared against PostgreSQL `TEXT` columns.
* **`check-md5` row ordering**: Comparison rows for nested exports such as `advisors_advices_earnings` now use the normalized target-side values consistently, so ordering no longer creates spurious mismatches.
* **Non-JSONB export schema lookup**: `export` now resolves collection schemas from both nested `source/collections/<db>/` layouts and older flat layouts, restoring CSV generation for collections such as `engine` in non-JSONB projects.

## [0.4.0] - 2026-05-25

### Added

* **`to-pg --schema <NAME>`**: deploy all tables for a collection into a dedicated PostgreSQL schema
  * Prepends `CREATE SCHEMA IF NOT EXISTS <name>; SET search_path = <name>;` to the generated SQL
  * Strips the `{schema}_` prefix from every child table name so names are shorter and schema-qualified references are unambiguous (e.g. `b2bsalesorder_lines` → `lines` inside schema `b2bsalesorder`)
  * Prefix matching is case-insensitive (sanitized before comparison) to handle mixed-case collection names
* **`to-pg` now uses one PostgreSQL schema per collection by default**: each generated SQL file gets its own self-contained schema preamble, and `--schema <NAME>` remains available to override that default with a fixed schema name
* **Output SQL filenames are now lowercased**: `B2BSalesOrder.sql` → `b2bsalesorder.sql`, consistent with PostgreSQL identifier folding
* **Integrated workflow**: `infer -c <config>` now refreshes generated SQL and the main HTML reports; post-import validation remains available through `report --post-import` after loading data into PostgreSQL
* **MongoDB connection inputs are now named `SOURCE_URI` / `--source-uri`** across the CLI and generated config files
* **Project config files now use TOML**: `mongo2pg init` generates `<project>.toml` with `[project]`, `[source]`, and `[target]` sections, while runtime parsing remains backward-compatible with older flat `.conf` files
* **`import` command**: creates PostgreSQL objects from `schema/tables/<db>/*.sql`, loads exported `.csv.gz` files with `COPY`, and targets the configured PostgreSQL database automatically
* **Automatic post-import validation**: `import -c <config>` now regenerates `reports/post_report.html` after a successful load; `report --post-import` remains available to rerun it later
* **Schema JSON output is now opt-in**: `infer` keeps the human-readable statistics/progress output by default, and the full inferred schema JSON is printed only with `--print-json`
* **Regression test `test_camelcase_collection_table_count_matches_report`**: verifies end-to-end that the number of `CREATE TABLE` statements produced by `to-pg` for a camelCase collection name equals the number of PG tables shown in the HTML report (catches the filename mismatch bug)
* **CI now runs `cargo test` on every PR**: a `test` job was added to `pr-preview.yml`; the build and preview-release jobs only proceed once all tests pass

### Fixed

* **HTML report no longer shows 0 PG tables for camelCase collection names**: `collect_rows` now lowercases the collection directory name when resolving the SQL file path (e.g. `listingsAndReviews/` → `listingsandreviews.sql`), consistent with how `to-pg` writes the file
* **`$sample` sort-memory failures no longer abort inference**: when `$sample` fails with a sort-memory-limit error (MongoDB error 292, common on Atlas shared/free tiers) or any other aggregation error, the tool now automatically falls back to `find().limit(<sample_size>)`, which has no sort stage and works on all tiers
* **Cursor deserialization errors trigger the same fallback**: if a `$sample` cursor emits a document-level error mid-iteration, the partial result is discarded and `find().limit()` is retried from scratch
* **Per-collection errors no longer abort database-wide or server-wide inference**: conversion errors on individual collections are now caught, a warning is printed to stderr, and the remaining collections continue to be processed
* **Per-database errors no longer abort server-wide inference**: if `list_collection_names()` fails for a database, that database is skipped with a warning and iteration continues
* **`estimated_document_count()` failure is now non-fatal**: falls back to the number of sampled documents instead of propagating the error
* **`SAMPLE_MAX_TIME` cap (120 s) added** to both the `$sample` aggregation and the `find()` fallback to prevent runaway queries
* **`export` now resolves SQL schemas by sanitized collection name only**: MongoDB collection names stay raw for querying, but schema lookup uses the lowercased sanitized filename (for example `listingsAndReviews` → `listingsandreviews.sql`), fixing mixed-case collection/export mismatches
* **`export --namespace` now overrides the config namespace consistently**: export reads SQL from `schema/tables/<db_name>/` with no fallback to older flat locations
* **Export output layout is now database-scoped**: generated `.csv.gz` files are written under `data/<database_name>/<sanitized_collection_name>/`
* **Export output now prints the configured `.csv.gz` path for each table**: row counts are logged alongside the project-relative file location during export instead of an absolute canonicalized path
* **Config parsing now uses `SOURCE_URI` and `TARGET_URI` keys** for MongoDB and PostgreSQL connection settings
* **`infer -c <config>` now writes DDL under `schema/tables/<db>/` for single-database projects too**: the chained `to-pg` step now prefixes flat collection layouts with the configured database name so downstream export/import/report steps use one consistent SQL layout
* **Single-database reports now resolve PostgreSQL tables from `schema/tables/<db>/`**: collection rows in `reports/main.html` once again show expandable PG tables after the per-database SQL layout change
* **`mongodb` dependency bumped** from 3.2.5 to 3.6.0

---

## [0.3.7] - 2026-05-07

### Added

* **Expandable DDL in HTML reports**: PG table pills in the per-collection detail row are now clickable
  * Clicking a table pill toggles a dark-themed `<pre>` block showing the full `CREATE TABLE` DDL
  * An animated `▶` arrow indicates the collapsed/expanded state
  * Works in both single-database and multi-database reports

---

## [0.3.6] - 2026-05-07

### Added

* **Database and server-level `infer` iteration**
  * `--namespace <db>` (no collection) now infers every collection in the database
  * Omitting `--namespace` entirely enumerates all user databases on the server and infers all their collections (`admin`, `local`, and `config` are always skipped)
  * Output files are named `<dbname>_<collname>.json` / `<dbname>_<collname>.stats.txt`
  * Per-database HTML report (`reports/<dbname>.html`) and Mermaid ER diagram (`reports/<dbname>.schema.html`) are generated automatically
* `build_mongo_mermaid` and `render_mongo_schema_html` helpers in `schema_diagram.rs` for ER diagram generation from inferred schemas
* `SYSTEM_DATABASES` constant exported from the library crate to avoid duplication

### Fixed

* `--uri` was erroneously accepted by `to-pg` (which does not connect to MongoDB) – option removed

---

## [0.3.5] - 2026-05-05

### Added

* Score surfaced in the cluster-level report

### Fixed

* Score computation was incorrectly limited to the first 20 sampled values – now uses all sampled values
* System views are excluded from collection inference
* Warning emitted when a referenced database is not found on the server

---

## [0.3.4] - 2026-05-05

### Added

* `cluster-report` subcommand: aggregates migration scores across multiple databases into a single cluster-level HTML report
  * Accepts one or more `--configs` paths (comma-separated or repeated)
  * Cluster label derived from the first config URI when `--cluster` is not provided
* `distinct_fields_over_avg_fields_per_doc` metric added to both text (`.stats.txt`) and YAML (`.stats.yaml`) stats outputs

---

## [0.3.3] - 2026-05-03

### Added

* **Migration complexity score** per collection and database in HTML report
  * Per-collection score: `C = depth_max/2 + array_fields + distinct_fields/avg_fields_per_doc`
  * DB-level score: `C_db = 1.5 × N_collections + Σ C_i`
  * Three summary metrics surfaced in the report header: total score, doc-weighted average, max collection
  * Easy / Medium / Hard badge with colour-coded thresholds (< 30 / 30–80 / > 80)
  * Per-row score column in the collections table, colour-coded green/orange/red
* Score is **JSONB-strategy-aware**: nested Object depth is not counted when `--jsonb` is active (those branches become a single opaque column, not relational depth)
* HTML report title and subtitle now show the **cluster host** and **database name** extracted from the connection URI
* `cluster_from_uri` public helper in `report.rs` to strip credentials from a URI for display

### Changed

* `render_html` signature gains a `cluster: &str` parameter

---

## [0.3.2] - 2026-05-03

### Added

* `CONTRIBUTING.md` with contribution guidelines
* `CHANGELOG.md`
* `--jsonb` flag on `infer` to emit Object fields as JSONB columns instead of child tables
* `sampled` field on `TypeSchema` at all nesting levels
* `Taskfile.yml` for build and test automation (`infer`, `infer-with-jsonb` tasks)
* `--uri` CLI override for commands that connect to MongoDB (`infer`, `export`)
* `--number` / `--percent` fallback from config file

### Fixed

* `--uri` was erroneously required on `to-pg`, `report`, and `schema` subcommands (which don't connect to MongoDB)

---

## [0.3.1] - 2026-05-02

### Added

* HTML report now shows the number of PostgreSQL tables created
* Tutorial documentation for the export workflow
* Default values written as comments in generated `.conf` file (`URI`, `NAMESPACE`, `NUMBER`, `PERCENT`)

### Fixed

* Dead code removed from analyzer

---

## [0.3.0] - 2026-05-01

### Added

* `export` subcommand to stream MongoDB documents to CSV/JSONL
* `report` subcommand to generate an HTML report from inferred schemas
* Layout and reports folder structure in project init

### Fixed

* Sample aggregation stats computation
* Removed unused worker parameters from CI

---

## [0.2.2] - 2026-04-30

### Added

* Distinct value sampling in schema output
* Apple Silicon runner in CI

### Changed

* Removed document count from tree view; replaced with sample count

---

## [0.2.1] - 2026-04-30

### Added

* `to-pg` subcommand to generate PostgreSQL DDL from inferred schemas
* Community MongoDB disclaimer in README

### Changed

* Renamed `prop_in_object` to `probability` throughout
* Improved INSTALL.md to match actual JSON output format

---

## [0.1.0] - 2026-04-29

### Added

* Initial release
* `infer` subcommand to sample MongoDB collections and output a JSON schema
* Schema analysis with type detection, nested objects, arrays, and probability scores
* CI pipeline with GitHub Actions

[Unreleased]: https://github.com/pmpetit/mongo2pg/compare/mongo2pg-v0.8.0...HEAD
[0.6.4]: https://github.com/pmpetit/mongo2pg/compare/v0.6.3...v0.6.4
[0.6.3]: https://github.com/pmpetit/mongo2pg/compare/v0.5.3...v0.6.3
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
