# Installation

## Download a pre-built binary

Go to the [Releases page](https://github.com/pmpetit/mongo2pg/releases) and download the archive that matches your platform:

| Platform | File to download |
|---|---|
| Linux x86_64 | `mongo2pg-linux-x86_64` |
| Linux arm64 | `mongo2pg-linux-aarch64` |
| macOS Intel | `mongo2pg-macos-x86_64` |
| macOS Apple Silicon | `mongo2pg-macos-aarch64` |
| Windows x86_64 | `mongo2pg-windows-x86_64.exe` |
| Windows arm64 | `mongo2pg-windows-aarch64.exe` |

### Linux / macOS

```bash
# Replace <version> and <platform> with your values, e.g. v0.2.0 and linux-x86_64
curl -L https://github.com/pmpetit/mongo2pg/releases/download/<version>/mongo2pg-<platform> \
  -o mongo2pg
chmod +x mongo2pg
sudo mv mongo2pg /usr/local/bin/
```

### Windows

Download the `.exe`, optionally rename it to `mongo2pg.exe`, and place it in a directory that is on your `PATH`.

---

## Build from source

Requires [Rust](https://rustup.rs) (stable).

```bash
git clone https://github.com/pmpetit/mongo2pg
cd mongo2pg
cargo build --release
# Binary at: ./target/release/mongo2pg
```

---

## Usage examples

### Infer the schema of a collection (1 000 documents sampled by default)

```bash
mongo2pg "mongodb://localhost:27017" mydb.mycollection
```

### Sample a fixed number of documents

```bash
mongo2pg "mongodb://localhost:27017" mydb.mycollection -n 5000
```

### Sample 10 % of the collection

```bash
mongo2pg "mongodb://localhost:27017" mydb.mycollection -p 10
```

### Print statistics only (suppress JSON output)

```bash
mongo2pg "mongodb://localhost:27017" mydb.mycollection --no-output
```

### Save the schema to a file

```bash
mongo2pg "mongodb://user:pass@host:27017" mydb.orders > orders-schema.json
```

### Use sequential scan instead of `$sample` (faster on small collections)

```bash
mongo2pg "mongodb://localhost:27017" mydb.mycollection --no-sampling -n 2000
```

### Disable sample-value collection (smaller output)

```bash
mongo2pg "mongodb://localhost:27017" mydb.mycollection --no-values
```

### With authentication and TLS

```bash
mongo2pg "mongodb://user:pass@host:27017/?authSource=admin&tls=true" mydb.mycollection
```

---

## Interpreting the output

`mongo2pg` produces two kinds of output: **stats** printed to stderr and a **JSON schema** on stdout.

### Stats (stderr)

```
Documents in collection : 45 231
Documents sampled       : 1 000
Width (top-level fields): 12
Depth (max nesting)     : 4
Branch (per level)      : L1:12  L2:8  L3:3  L4:1
Top-level types         : _id:ObjectId  name:String  address:Object  …
```

| Stat | What it means |
|---|---|
| **Documents in collection** | Total documents in the collection (from MongoDB metadata, fast estimate). |
| **Documents sampled** | How many documents were actually analysed. A higher number gives more reliable probabilities. |
| **Width** | Number of distinct top-level field names. A large width (> 30–40) often signals a denormalised or poorly modelled collection. |
| **Depth** | Maximum nesting level. Top-level fields are depth 1. An array containing an object with a field counts as depth 2. Depth > 3–4 suggests a heavily nested document model that may be hard to flatten into relations. |
| **Branch (per level)** | Number of distinct fields at each nesting level. `L1:12  L2:8` means 12 fields at the top level and 8 fields inside nested objects one level down. A sharp drop-off (e.g. `L1:5  L2:50`) may indicate polymorphic sub-documents. |
| **Top-level types** | The dominant BSON type for each top-level field, useful for a quick sanity check. |

**What makes a good migration candidate?**

A collection is typically easy to migrate to PostgreSQL when:

- `Depth` is 1 or 2 (flat or only one level of nesting)
- `Width` is stable and small (< 20 fields)
- Per-level branch counts drop quickly (little nesting)
- Field `probability` values are close to 1.0 (fields are present in most documents)

Collections with high depth, high width, or many low-probability fields represent schema flexibility that is harder to map to a fixed relational model.

---

### JSON schema (stdout)

The JSON output follows an extended JSON Schema dialect with three extra keywords:

#### `x-bsonType`

The original BSON type name (e.g. `"ObjectId"`, `"Date"`, `"Decimal128"`). Standard JSON Schema `type` only covers JSON primitives; `x-bsonType` preserves the MongoDB type fidelity.

```json
"_id": {
  "x-bsonType": "ObjectId",
  "x-metadata": { "count": 1000, "prob": 1.0 }
}
```

#### `x-metadata`

Per-field statistics:

| Key | Description |
|---|---|
| `count` | Number of sampled documents in which this field was present. |
| `prob` | `count / total_sampled` — probability the field exists in a document. A value of `1.0` means it was present in every sampled document; `0.5` means it was present in half. Fields with low probability are optional and will need a nullable column in PostgreSQL. |

#### `x-sampleValues`

A reservoir sample of up to 100 values (strings, binary, code) or 10 000 values (all other types) observed during sampling. Useful to spot unexpected values or confirm a field's real content before choosing a PostgreSQL column type.

```json
"status": {
  "x-bsonType": "String",
  "x-metadata": { "count": 998, "prob": 0.998 },
  "x-sampleValues": ["active", "inactive", "pending", "active", "active"]
}
```

#### `anyOf` — mixed-type fields

When a field holds more than one BSON type across documents, the schema uses `anyOf` to list all observed types. Each branch has its own `x-bsonType` and `x-metadata`:

```json
"legacy_id": {
  "anyOf": [
    { "x-bsonType": "String",  "x-metadata": { "count": 750, "prob": 0.75 } },
    { "x-bsonType": "Number",  "x-metadata": { "count": 200, "prob": 0.20 } },
    { "x-bsonType": "Undefined", "x-metadata": { "count": 50, "prob": 0.05 } }
  ]
}
```

`Undefined` is a synthetic type injected for documents where the field was absent. Its `prob` is `1 - (presence_count / total_sampled)`.

Mixed-type fields are the trickiest to migrate: you will need to decide whether to cast to a single type, split into multiple columns, or use a `jsonb` column in PostgreSQL.
