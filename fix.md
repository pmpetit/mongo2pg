# Empty Embedded Objects Create Extra Export Rows

## Symptom

After exporting and importing a collection, a PostgreSQL child table can have one extra row compared with MongoDB. This occurs when a source document contains an explicitly empty embedded object, for example `name.native: {}`. The exporter can write a child row containing only its generated primary key and parent foreign key, even though the empty object has no payload.

## Root Cause

`extract_rows_with_mapping` in `src/export.rs` traverses embedded documents recursively. Its row-level empty check does not skip a structural parent table when that table has child tables. As a result, an empty BSON document can materialize as an FK-only row.

## Fix

In `extract_rows_with_mapping`, return for empty non-root BSON documents immediately after matching `val` to `Bson::Document`, before any row is generated:

```rust
let doc = match val {
    Bson::Document(d) => d,
    _ => return,
};
if !is_root && doc.is_empty() {
    return;
}
```

Keep root documents unaffected. Non-empty structural parent documents must still be emitted when they anchor valid nested child rows.

## Regression Test

Add an exporter unit test in `src/export.rs` using a parent-to-child schema such as:

```text
countries_small -> name -> native -> afr
```

Use a BSON document with populated `name.common` and `name.official`, but an empty `name.native` object:

```rust
let doc = doc! {
    "_id": bson::oid::ObjectId::new(),
    "name": {
        "common": "Empty native object",
        "official": "Empty native object",
        "native": {}
    }
};
```

After calling `extract_rows`, assert that the `name` row exists and that neither `native` nor `afr` has rows. This catches FK-only rows emitted for an empty structural object.

Run the focused test and exporter suite:

```sh
cargo test --lib export_skips_empty_structural_object_with_child_tables
cargo test --lib export::tests
```

Do not copy generated files from `results/`; apply the source guard and regression test in the target repository.

---

# Padded ObjectId Text IDs Fail MD5 Checks

## Symptom

The MongoDB `_id` may be a 24-character ObjectId, while the PostgreSQL `id` column is `TEXT` containing a UUID-shaped value such as `00000000-507d-95d5-719d-bef170f15bf9`. The checksum then reports a mismatch because MongoDB hashes the ObjectId as `507d95d5719dbef170f15bf9`.

## Root Cause

Legacy export conversion can store ObjectIds in text columns using a UUID-shaped representation padded with eight leading zeroes. PostgreSQL MD5 normalization previously compared that text literally against the source ObjectId hex string.

## Fix

During checksum calculation only, identify mappings from source `_id` to target `id` whose PostgreSQL target type is text-like. For those columns, convert only values matching the exact padded UUID shape back to the 24-character ObjectId hex form. Apply the same normalization to the PostgreSQL row hash and the values retained for mismatch reporting. Do not update PostgreSQL data or normalize UUID-typed columns or other text fields.

The normalized value is recognizable from the stored value alone. Therefore, a genuine UUID with the same `00000000-` prefix and valid UUID formatting is indistinguishable and will also be normalized for this specific `_id`-to-text-`id` mapping.

## Regression Test

The checksum unit test verifies that a padded ObjectId is trimmed for a text `id` mapping, a UUID-typed `id` mapping is excluded, and a UUID with a nonzero first group is preserved.

Run the focused test and checksum module tests:

```sh
cargo test --lib engine::checksum::tests::pg_checksum_trims_padded_objectid_only_for_text_id_mapping -- --exact
cargo test --lib engine::checksum::tests::
cargo test engine::checksum::tests::test_compute_collection_checksums_via_temp_tables_with_containers -- --nocapture --test-threads=1
```

All three checks passed during implementation. `git diff --check` also passed. `cargo fmt --check` reports pre-existing formatting differences in the module and elsewhere.
