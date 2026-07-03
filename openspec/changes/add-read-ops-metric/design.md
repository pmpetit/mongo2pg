## Context

The infer pipeline already computes collection structure and writes per-collection stats YAML consumed by report rendering. Main report generation reads those YAML files and renders a table with document and complexity metrics. There is no per-collection workload context, which makes migration prioritization harder when collections have similar complexity but different read criticality.

## Goals / Non-Goals

**Goals:**

- Collect per-collection read operation telemetry during infer using MongoDB collection statistics.
- Persist read telemetry in per-collection stats YAML without breaking existing fields.
- Render read telemetry near the Documents value in main report output.
- Fail gracefully when telemetry is unavailable (older MongoDB versions, missing privilege, transient command failure).

**Non-Goals:**

- Realtime monitoring or continuous time-series telemetry.
- Additional report pages or CLI flags for telemetry collection.
- Post-import report telemetry expansion in this change.

## Decisions

1. Collect telemetry in infer phase via `$collStats`.

- Rationale: infer already iterates each collection and has a MongoDB collection handle, so this avoids extra commands in report generation and keeps report as a pure file render step.
- Alternative considered: query telemetry during `report` command. Rejected because report currently supports offline rendering from generated artifacts and should not require live MongoDB access.

1. Extend stats YAML schema with optional `read_ops` object.

- Rationale: preserves backward compatibility and keeps data colocated with existing collection stats artifacts.
- Alternative considered: separate telemetry file per collection. Rejected due to extra file management and parsing complexity.

1. Render read telemetry as secondary text inside Documents cell.

- Rationale: meets requirement to place metric near document count while keeping table layout stable.
- Alternative considered: add dedicated column. Rejected to avoid wider tables and larger layout impact across single-db and multi-db report variants.

1. Best-effort telemetry retrieval.

- Rationale: telemetry availability may vary by deployment and permissions; infer should not fail schema output because of telemetry lookup failure.
- Alternative considered: hard fail when telemetry unavailable. Rejected due to operational fragility.

## Risks / Trade-offs

- [MongoDB command shape/field variation across versions] -> Parse defensively and treat missing fields as absent telemetry.
- [Additional infer latency from one extra command per collection] -> Keep command lightweight and avoid histogram payloads.
- [UI clutter in Documents cell] -> Render telemetry in small muted text as secondary information.
- [Schema drift with existing artifacts] -> Keep `read_ops` optional and default missing in deserializers.

## Migration Plan

1. Update infer collection flow to request read telemetry per collection.
2. Update stats serialization/deserialization model with optional telemetry object.
3. Update report rendering for single-db and multi-db views to display telemetry near Documents.
4. Add focused tests for YAML model compatibility and report rendering.
5. Run infer and report in sample project to verify generated HTML output.

Rollback strategy:

- Revert telemetry fields and rendering logic; existing stats/report behavior remains unchanged because this change is additive.

## Open Questions

- Should telemetry formatting be localized or remain fixed UTC string formatting?
- Should unavailable telemetry display an explicit marker (for example, `reads: n/a`) or stay hidden?
