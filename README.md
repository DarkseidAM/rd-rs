# rd-rs

[![Rust](https://img.shields.io/badge/rust-1.94%2B-orange.svg)](https://www.rust-lang.org)
[![License: MIT](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)
[![Edition](https://img.shields.io/badge/edition-2024-lightgrey.svg)](https://doc.rust-lang.org/edition-guide/rust-2024/)
[![Platforms](https://img.shields.io/badge/platform-linux%20amd64%20%7C%20arm64-informational.svg)](#building-from-source)
[![GitHub release](https://img.shields.io/github/v/release/DarkseidAM/rd-rs.svg?label=release)](https://github.com/DarkseidAM/rd-rs/releases/latest)

`rd-rs` is a high-performance, asynchronous FUSE filesystem server for **Real-Debrid**, written in Rust. It is a complete rewrite of [zurg](https://github.com/debridmediamanager/zurg-testing), offering improved memory safety, better concurrency, and a lower resource footprint.

Mount your entire Real-Debrid library as a local filesystem and stream media directly into Plex, Jellyfin, or Emby — no manual downloads required.

---

## Table of Contents

- [Features](#features)
- [Prerequisites](#prerequisites)
- [Installation](#installation)
  - [Docker (Recommended)](#docker-recommended)
  - [Building from Source](#building-from-source)
- [Configuration](#configuration)
- [Usage](#usage)
  - [Starting the Server](#starting-the-server)
  - [CLI Commands](#cli-commands)
  - [Log Verbosity](#log-verbosity)
- [Architecture](#architecture)
- [Contributing](#contributing)
- [Acknowledgements](#acknowledgements)
- [License](#license)

---

## Features

- **High-performance FUSE mount** — built on `fuse3` + `tokio`; delivers near-native filesystem throughput with minimal CPU overhead.
- **Smart disk cache** — configurable on-disk chunk cache with read-ahead buffering, parallel chunked downloads, and LRU eviction.
- **Automatic repair engine** — detects broken, missing, or stalled torrents and re-queues them for repair without user intervention; supports passive HEAD checks for playability.
- **CDN optimisation & routing** — probes Real-Debrid CDN endpoints (IPv4 and IPv6) and pins traffic to the fastest host. Supports `auto`, `force_cloudflare`, `force_numbered`, and `force_location` modes.
- **Multi-token bandwidth rotation** — when the primary API token exhausts its daily bandwidth limit, `rd-rs` seamlessly rotates through a pool of backup tokens.
- **Hot-reload credentials** — `config.toml` is watched at runtime; token updates are applied without restarting the mount.
- **SQLite state store** — WAL-mode SQLite tracks torrent states, folder structures, and repair history reliably across restarts.
- **Configurable rate limits** — fine-grained control over API page sizes, refresh intervals, and per-minute request caps.

---

## Prerequisites

| Requirement | Notes |
|---|---|
| Linux kernel ≥ 4.18 | FUSE unprivileged mounts |
| `fuse3` / `libfuse3` | `apt install fuse3` / `dnf install fuse3` |
| Rust ≥ 1.94 | Build from source only |
| Docker + Buildx | Docker install only |
| Real-Debrid API token | [Get one here](https://real-debrid.com/apitoken) |

---

## Installation

### Docker (Recommended)

The Docker image is a multi-stage build that produces a minimal Debian-slim runtime image with a statically-linked SQLite. It supports both `amd64` and `arm64`.

1. **Copy the compose file** already included in the repository and add your token to `config.toml`:

   ```bash
   cp config.example.toml config.toml
   # Set token = "YOUR_REAL_DEBRID_TOKEN" in config.toml
   ```

2. **`docker-compose.yml`** (included in the repo):

   ```yaml
   services:
     rd-rs:
       build: .
       image: rd-rs:local
       cap_add:
         - SYS_ADMIN
       devices:
         - /dev/fuse:/dev/fuse
       security_opt:
         - apparmor:unconfined
       volumes:
         - ./config.toml:/app/config.toml:ro
         - rd-cache:/data/cache
         - ./mnt:/mnt/rd:rshared
       restart: unless-stopped

   volumes:
     rd-cache:
   ```

3. **Start:**

   ```bash
   docker compose up -d
   docker compose logs -f
   ```

> **Note:** If you prefer a simpler setup and your host allows it, you can replace the three FUSE-specific settings above with a single `privileged: true`. This grants broader host access and is not recommended for production.

### Building from Source

```bash
# 1. Install system dependencies (Debian/Ubuntu)
sudo apt-get install -y fuse3 libfuse3-dev pkg-config

# 2. Install Rust (https://rustup.rs)
rustup update stable

# 3. Clone and build
git clone https://github.com/yourusername/rd-rs.git
cd rd-rs
cargo build --release

# 4. Copy config and run
cp config.example.toml config.toml
# Edit config.toml — set your token and mount_path
sudo ./target/release/rd-rs
```

The `vendor/fuse3` directory contains a patched version of the `fuse3` crate (buffer check fix for `readdirplus` on large directories). The build uses it automatically via `[patch.crates-io]` in `Cargo.toml`.

---

## Configuration

`rd-rs` reads `config.toml` from the current working directory on startup. Credential fields (`token`, `download_tokens`) are hot-reloaded on file change (30-second debounce). VFS cache settings require a restart.

### Minimal config

```toml
token = "YOUR_REAL_DEBRID_TOKEN"
mount_path = "/mnt/rd"
cache_dir  = "/data/cache"
```

### Full reference

```toml
# Primary Real-Debrid API token
token = "YOUR_REAL_DEBRID_TOKEN"

# Where to mount the FUSE filesystem
mount_path = "/mnt/rd"

# Directory for the VFS disk cache and SQLite state database
cache_dir = "/data/cache"

# Additional download tokens — rotated when the primary hits its daily bandwidth limit
download_tokens = [
    "BACKUP_TOKEN_1",
    "BACKUP_TOKEN_2",
]

[on_library_update]
# Shell command executed when the library changes. %s is replaced with the changed path.
command = "echo 'Library updated: %s'"

[vfs]
cache_max_size        = "100G"   # Maximum total on-disk cache size
cache_max_age         = "24h"    # Maximum age of a cached chunk before eviction
cache_min_free_space  = "20G"    # Minimum free space to keep on the cache partition
buffer_size           = "256M"   # Per-file RAM read-ahead window
read_ahead            = "128M"   # Sequential read-ahead size
chunk_size            = "4M"     # Size of individual download chunks
max_parallel_streams  = 8        # Max parallel HTTP streams per open file
attr_timeout_secs     = 60       # Kernel attribute cache timeout (seconds)
entry_timeout_secs    = 600      # Kernel directory-entry cache timeout (seconds)

[repair]
enable         = true  # Enable the automatic repair engine
every_mins     = 60    # How often the repair engine runs (minutes)
timeout_mins   = 30    # Time before a torrent is considered timed out
head_check_enabled = true  # Passive HEAD check for playable files

[api]
rate_limit_per_minute           = 250    # Max API requests per minute
timeout_secs                    = 60     # Network timeout for API requests
refresh_interval_secs           = 15     # Background sync loop interval
cdn_mode                        = "auto" # auto | force_cloudflare | force_numbered | force_location
cdn_ipv6_enabled                = true   # Include IPv6 hosts in CDN speed tests
traffic_details_refresh_secs    = 300    # Traffic consumption poll interval
```

---

## Usage

### Starting the Server

Run the binary in the directory that contains `config.toml`. The FUSE filesystem will be mounted at `mount_path`:

```bash
rd-rs
```

Press `Ctrl-C` for a graceful shutdown — the FUSE mount is cleanly unmounted before the process exits.

### CLI Commands

```
rd-rs — Real-Debrid FUSE filesystem

Usage: rd-rs [COMMAND]

Commands:
  repair  Enqueue torrents for repair and run one repair pass (no FUSE mount)
  help    Print this message or the help of the given subcommand(s)

Options:
  -h, --help  Print help
```

#### `rd-rs repair`

Runs a single repair pass without mounting the filesystem. Useful for maintenance scripts or one-off recovery:

```bash
# Repair only torrents already flagged as broken
rd-rs repair

# Repair every torrent in the library, ignoring existing flags
rd-rs repair --all

# Also clear the unrepairable_reason field before enqueueing
rd-rs repair --all --clear-unrepairable

# Include periodic-scan candidates (unassigned links, broken playable files, etc.)
rd-rs repair --periodic-eligible
```

### Log Verbosity

`rd-rs` uses the [`tracing`](https://docs.rs/tracing) crate. Control verbosity with the `RUST_LOG` environment variable:

```bash
# Default level (info)
rd-rs

# Debug output for everything
RUST_LOG=debug rd-rs

# Debug only the repair engine, info everywhere else
RUST_LOG=info,rd_rs::repair=debug rd-rs
```

---

## Architecture

```
rd-rs
├── fuse/          FUSE filesystem implementation (lookup, readdir, readdirplus, read)
│   └── vfs_read_buffer  Async coalesced reader — replicates rclone --buffer-size logic
├── cache/         On-disk chunk cache with bitmap range tracking and eviction
│   ├── worker     Background download and eviction tasks
│   └── range_db   SQLite-backed persistent byte-range map
├── repair/        Automatic torrent repair engine
│   ├── engine     Periodic scan and repair pass orchestration
│   ├── detect     Broken-torrent detection heuristics
│   └── strategies Per-strategy repair actions
├── rd/            Real-Debrid HTTP API client
│   ├── api        Unrestrict links, token pool, traffic details
│   └── cdn        CDN endpoint probing and host ranking
├── torrent/       TorrentManager — in-memory state, refresh loops, hooks
├── db/            SQLite schema migrations and query helpers
└── config/        Config structs, defaults, and file-watcher
```

Key design choices:

- **Userspace FUSE via `fuse3`** — bridges OS filesystem calls directly to Real-Debrid API endpoints without requiring a custom kernel filesystem driver. The standard `fuse` kernel module and `/dev/fuse` device are still required (hence the `devices:` entry in the compose file).
- **WAL-mode SQLite** — all persistent state (torrent records, byte-range maps, repair history) lives in a single portable database file.
- **`arc-swap` + `dashmap`** — hot-reload and concurrent reads of configuration and torrent state without locking the entire map.
- **`tokio`-native I/O** — all HTTP, disk, and FUSE operations run on the same async runtime with no thread-pool bridges.

---

## Contributing

Contributions, bug reports, and feature requests are welcome.

1. **Open an issue** first for non-trivial changes so the approach can be discussed.
2. **Fork** the repository and create a branch from `main`.
3. Follow the project's coding conventions:
   - Run `cargo clippy --all-targets --all-features -- -D warnings` and fix all warnings.
   - Add integration tests in `tests/` (not inline `#[cfg(test)]` modules).
   - Keep source files under 300 lines; split into submodules when needed.
   - Use `tracing` for all runtime output; no `println!` / `eprintln!`.
4. **Open a pull request** with a clear description of what changed and why.

---

## Acknowledgements

- [zurg](https://github.com/debridmediamanager/zurg-testing) — the original project that inspired this rewrite.
- [rclone](https://github.com/rclone/rclone) — the `--buffer-size` read-ahead design that informed the VFS read buffer.
- [fuse3](https://github.com/Sherlock-Holo/fuse3) — the async FUSE library used as the filesystem backend.

---

## License

This project is licensed under the [MIT License](LICENSE).
