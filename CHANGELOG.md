# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

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

[Unreleased]: https://github.com/pmpetit/mongo2pg/compare/v0.3.1...HEAD
[0.3.1]: https://github.com/pmpetit/mongo2pg/compare/v0.3.0...v0.3.1
[0.3.0]: https://github.com/pmpetit/mongo2pg/compare/v0.2.2...v0.3.0
[0.2.2]: https://github.com/pmpetit/mongo2pg/compare/v0.2.1...v0.2.2
[0.2.1]: https://github.com/pmpetit/mongo2pg/compare/v0.1.0...v0.2.1
[0.1.0]: https://github.com/pmpetit/mongo2pg/releases/tag/v0.1.0
