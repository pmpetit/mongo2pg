## 1. Preflight Flow Integration

- [x] 1.1 Add a shared preflight entrypoint used by `import` and `kafka-import` before table DDL/apply begins
- [x] 1.2 Wire preflight ordering so database check/create runs before destination DB session bootstrap and schema/table checks
- [x] 1.3 Ensure preflight exits early on hard failures and prevents downstream apply logic from running

## 2. Database and Schema Bootstrap Behavior

- [x] 2.1 Implement missing-database detection with admin fallback connection strategy (`connect_pg_admin_client` path)
- [x] 2.2 Implement missing-schema creation before executing destination table DDL
- [x] 2.3 Preserve explicit target schema handling and search_path behavior in preflight-created sessions

## 3. Guardrails and Error Attribution

- [x] 3.1 Add explicit privilege-denied error mapping for database creation with actionable remediation text
- [x] 3.2 Add explicit privilege-denied error mapping for schema creation with actionable remediation text
- [x] 3.3 Extend connection/error attribution output so preflight authorization failures include backend + operation context while preserving root cause

## 4. Existing Table Stop Conditions

- [x] 4.1 Add destination table existence checks in preflight using mapping-derived destination objects
- [x] 4.2 Abort import when existing tables are found and emit clear remediation to drop/clean before retry
- [x] 4.3 Ensure behavior is consistent across `import` and `kafka-import` code paths

## 5. Verification and Documentation

- [x] 5.1 Add/extend tests for successful missing DB/schema creation flows
- [x] 5.2 Add/extend tests for privilege-denied failures and existing-table early-stop behavior
- [x] 5.3 Update reference docs for import preflight semantics and operator remediation guidance
