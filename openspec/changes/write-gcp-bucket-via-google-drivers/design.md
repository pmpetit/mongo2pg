## Context

mongo2pg currently writes export artifacts through filesystem-bound code paths. Users running in managed environments want direct writes to Google Cloud Storage (GCS) to avoid local volume staging and follow cloud-native data movement patterns. The change must preserve existing grouped export semantics and avoid regressions for local filesystem users.

Constraints:

- Existing CLI/export flow should remain compatible for local paths.
- Grouped export behavior must continue producing one artifact per target table.
- Authentication and destination validation need clear user-facing error messages.

## Goals / Non-Goals

**Goals:**

- Support export destinations addressed as GCS bucket URIs.
- Introduce storage backend abstraction so export pipeline can target local filesystem or GCS without changing core export semantics.
- Preserve grouped export artifact resolution and append behavior across destination backends.
- Add deterministic failure attribution for auth, permission, and transient write errors.

**Non-Goals:**

- Implement provider-agnostic support for every object store in this change.
- Redesign import pipeline behavior beyond consuming produced artifacts.
- Replace existing local write implementation.

## Decisions

1. Introduce a writer backend abstraction at export artifact emission boundary.

- Decision: define a small sink trait/interface used by chunk writer and final artifact writer.
- Rationale: isolates destination-specific logic and keeps export data transformation logic unchanged.
- Alternative considered: branching by URI scheme in each write call site. Rejected due to code duplication and higher regression risk.

1. Parse destination using explicit base_dir prefix rule.

- Decision: treat destination as GCS only when `base_dir` starts with `gs://`; otherwise retain current local path behavior.
- Rationale: deterministic trigger, backward compatible default, and easy user guidance.
- Alternative considered: add separate CLI flags for cloud destination. Rejected because it fragments destination configuration and duplicates validation paths.

1. Use official GCS Rust client and ADC/service-account auth chain.

- Decision: rely on Google client/auth stack and support default credential discovery plus explicit credential environment override.
- Rationale: aligns with Google guidance, reduces custom auth code, and simplifies operations on GCP runtimes.
- Alternative considered: custom signed HTTP requests. Rejected due to complexity and security risk.

1. Preserve grouped artifact naming independent of backend.

- Decision: grouped target table naming remains identical; only sink transport changes (file path vs object key).
- Rationale: keeps import compatibility and avoids behavior drift between local and cloud outputs.
- Alternative considered: backend-specific naming templates. Rejected because it complicates import and user mental model.

## Risks / Trade-offs

- SDK dependency size and compile-time increase -> Mitigate by feature-gating cloud backend and keeping local default path minimal.
- Credential misconfiguration can cause confusing failures -> Mitigate by explicit preflight validation and categorized error messages.
- Object storage semantics differ from filesystem append/overwrite patterns -> Mitigate by using staged chunk upload composition strategy with deterministic finalization.
- Network/transient failures can leave partial artifacts -> Mitigate by idempotent temporary object naming and cleanup-on-failure best effort.

## Migration Plan

1. Add backend abstraction and keep local backend as default.
2. Add GCS backend behind feature/config gate; implement URI parsing and validation.
3. Route grouped and non-grouped write paths through backend abstraction.
4. Add integration tests for local and GCS-mocked writes.
5. Roll out with release note guidance on credential setup and fallback behavior.
6. Rollback path: disable cloud destination feature/config and continue local-only writes.

## Open Questions

- Should GCS backend support server-side compression metadata control beyond current file extension conventions?
- Is multipart/chunk composition limit handling needed for very large grouped exports in first iteration?
- Should we accept S3-compatible endpoint overrides in this change or defer to later provider-generalization change?
