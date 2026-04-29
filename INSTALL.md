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
