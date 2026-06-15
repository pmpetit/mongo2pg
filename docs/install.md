# Installation

`mongo2pg` is a single Rust binary.

---

## Download a pre-built binary

Go to the [Releases page](https://github.com/pmpetit/mongo2pg/releases) and
download the archive that matches your platform:

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
# Replace <version> and <platform> with your values, e.g. v0.5.3 and linux-x86_64
version="v0.5.3"
platform="linux-x86_64"
curl -fL "https://github.com/pmpetit/mongo2pg/releases/download/${version}/mongo2pg-${platform}" \
  -o mongo2pg
chmod +x mongo2pg
sudo mv mongo2pg /usr/local/bin/
```

### Windows

Download the `.exe`, optionally rename it to `mongo2pg.exe`, and place it in a
directory that is on your `PATH`.

---

## Build from source

### Prerequisites

| Tool | Minimum version |
|---|---|
| Rust toolchain | stable |
| Cargo | bundled with Rust |
| MongoDB instance | needed for `infer` and `export` |
| PostgreSQL instance | needed for `report --post-import` |

Install Rust via [rustup](https://rustup.rs/):

```bash
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
```

### Clone and build

```bash
git clone https://github.com/pmpetit/mongo2pg.git
cd mongo2pg
cargo build --release
```

The binary is written to `target/release/mongo2pg`.

### Add to PATH

```bash
# Option 1 – copy to a directory already on your PATH
sudo cp target/release/mongo2pg /usr/local/bin/

# Option 2 – install via Cargo
cargo install --path .
```

Verify the installation:

```bash
mongo2pg --version
```
