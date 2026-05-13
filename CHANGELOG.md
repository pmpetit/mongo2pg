# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Fixed

- **`$sample` sort-memory failures no longer abort inference**: when `$sample` fails with a sort-memory-limit error (MongoDB error 292, common on Atlas shared/free tiers) or any other aggregation error, the tool now automatically falls back to `find().limit(<sample_size>)`, which has no sort stage and works on all tiers
- **Cursor deserialization errors trigger the same fallback**: if a `$sample` cursor emits a document-level error mid-iteration, the partial result is discarded and `find().limit()` is retried from scratch
- **Per-collection errors no longer abort database-wide or server-wide inference**: conversion errors on individual collections are now caught, a warning is printed to stderr, and the remaining collections continue to be processed
- **Per-database errors no longer abort server-wide inference**: if `list_collection_names()` fails for a database, that database is skipped with a warning and iteration continues
- **`estimated_document_count()` failure is now non-fatal**: falls back to the number of sampled documents instead of propagating the error
- **`SAMPLE_MAX_TIME` cap (120 s) added** to both the `$sample` aggregation and the `find()` fallback to prevent runaway queries
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

[Unreleased]: https://github.com/pmpetit/mongo2pg/compare/v0.3.6...HEAD
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
