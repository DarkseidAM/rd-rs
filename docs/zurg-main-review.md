# Ideas from zurg and Decypharr for rd-rs

**Purpose:** Two reference codebases with patterns worth stealing for rd-rs:

- **zurg** ([`zurg-main`](file:///home/darkseid/zurg-main)) — RD HTTP, multi-token, CDN, downloads.
- **Decypharr** ([`decypharr-temp`](file:///home/darkseid/decypharr-temp)) — includes **DFS** (“Decypharr Filesystem”), an in-tree FUSE stack optional vs rclone (`pkg/mount/dfs/`).

This is a **pattern list**, not a review scorecard. Paths are relative to each repo’s root.

---

# Part A — zurg

---

## 1. Per-token unrestrict cache

zurg keeps a **two-level** structure: token → (link → unrestricted `Download` record), built in `realdebrid.NewRealDebrid` and used in `UnrestrictAndVerify` / downloader error paths (`UnrestrictCache` in `realdebrid/api.go`).

**Why it helps rd-rs:** With multiple accounts, unrestricted links and CDN URLs are **not interchangeable** across tokens. A single global link cache can return another account’s URL or the wrong cache slice. rd-rs could mirror this with something like `DashMap<Arc<str>, DashMap<LinkKey, CachedUnrestrict>>` (or token hash keys) and invalidate using the **token that created** the entry.

---

## 2. “Expired token” state + background recovery

zurg marks a token **expired** on bandwidth exhaustion (`SetTokenAsExpired` in `realdebrid/token_manager.go`; wired from `universal/downloader.go`). `GetCurrentToken` skips expired entries so unrestrict moves to the next account. `MonitorExpiredTokens` (`realdebrid/api.go`) periodically re-unrestricts a sample link and verifies the CDN; on success it clears expired state and flushes that token’s unrestrict cache.

**Why it helps rd-rs:** rd-rs rotates the **download** Bearer on 509-style errors; it does not model **“this RD account is exhausted until rollover.”** A small state machine (per token: active / exhausted / unknown) plus a cheap periodic probe would align behavior with zurg and avoid hammering a dead account.

---

## 3. Midnight CET bandwidth reset job

`StartResetBandwidthCountersJob` in `universal/downloader.go` schedules a timer to **CET midnight** (with fallback zone), then every 24h calls `TokenManager.ResetAllTokens()` and zeros traffic counters.

**Why it helps rd-rs:** If you add token expiry (above), you need a **time-based reset** that matches RD’s daily window so accounts become eligible again without manual restart.

---

## 4. Traffic details cache (all tokens)

`startTrafficDetailsRefresher` + `refreshTrafficDetails` in `realdebrid/api.go` polls `/traffic/details` **per configured token** on an interval and stores results in an atomic snapshot.

**Why it helps rd-rs:** Useful for UI/logging (“which host burned quota?”) and for smarter decisions (e.g. preferring tokens with headroom). rd-rs already has premium-oriented loops; this is a **richer, per-token** view.

---

## 5. CDN request shaping beyond “pick fastest host”

`rdclient/ensure.go` (`ensureReachableDownloadServer`) encodes several policies:

- Preserve **letter-prefixed** geo hostnames when RD assigned them.
- **`force_cloudflare`** (`.cloud` TLD), **`force_numbered`** (`.com` + DNS bypass), **`force_location_*`** (rewrite to a random reachable host in that geo from IP maps).
- If the numbered subdomain from the URL is **not** in the reachable set, fall back to **another reachable** numbered host (still random among allowed).

**Why it helps rd-rs:** rd-rs has host pinning from probe results; zurg adds **explicit user modes** (geo lock, Cloudflare-only, numbered + direct IP) that are easy to expose as config enums and map to URL/dial behavior.

---

## 6. Geo-aware unrestrict IP

zurg’s download client holds a **location → verified IP** map (`geoUnrestrictIPs`), populated from network testing, and `UnrestrictLinkWithToken` passes the right IP in the unrestrict form when `cdn_host_preference` is location-based (`realdebrid/api.go` + `rdclient`).

**Why it helps rd-rs:** When forcing a region, unrestraining and downloading should agree on **where** RD thinks you are; carrying a tested IP through unrestrict reduces mismatched CDN vs unrestrict geo.

---

## 7. Hot-apply network test results to the download stack

`RealDebrid.UpdateDownloadHosts` / `UpdateIPAddresses` / `UpdateGeoUnrestrictIPs` (`realdebrid/api.go`) push into the download `HTTPClient` without process restart.

**Why it helps rd-rs:** You already load pinned hosts at startup; zurg’s pattern is **“re-run probe, atomically swap host/IP maps”**—good fit for `ArcSwap` or similar on the CDN side.

---

## 8. Hourly unrestrict cache sweeper

`startUnrestrictCacheCleaner` removes entries older than `UnrestrictCacheMaxAge` (4h) across **all** token buckets (`realdebrid/api.go`).

**Why it helps rd-rs:** Prevents stale unrestricted rows from living forever in memory; complements TTL on disk cache if you add in-memory unrestrict dedup.

---

## 9. Parallel pagination for “downloads” list

`GetDownloads` in `realdebrid/downloads.go` fetches pages with a **small worker pool** (up to 4 concurrent page requests) instead of strictly serial pagination.

**Why it helps rd-rs:** Any similar “fetch many pages” API (torrents, downloads) can shave latency the same way, within rate-limit bounds.

---

## 10. Streaming HTTP client without whole-request timeout

`HTTPClient.DoStreaming` in `rdclient/client.go` shares the transport but uses an `http.Client` **without** `Timeout`, so long body reads are not cut off by the same deadline as short API calls.

**Why it helps rd-rs:** Ensure range/stream paths use a **long or no** total timeout while keeping connect/TLS sane—mirrors zurg’s split between normal and streaming clients.

---

## 11. Operational polish worth copying

- **Per-token traffic on startup** (`Downloader` init): baseline counters per account for logging or limits (`universal/downloader.go`).
- **Broken download → invalidate the right cache entry** using `unrestrict.Token` as key (`downloader.go`) so multi-token caches stay consistent.
- **VFS invalidation before repair enqueue** (`markFileAsBrokenAndRepair`): ordering that avoids stale cache serving after a link is declared broken.

---

# Part B — Decypharr DFS and related mount stack

DFS is Decypharr’s **built-in** streaming filesystem (`pkg/mount/dfs/`): users can choose **DFS** or **rclone** in setup (`pkg/server/setup.go`, UI copy in `config.html`). rd-rs is already “in-process FUSE,” but DFS is still a useful **design reference** for layering, timeouts, and download coordination.

---

## B1. Split: FUSE backend vs VFS vs cache vs download workers

Rough pipeline:

1. **`dfs.Manager`** (`pkg/mount/dfs/manager.go`) — wires `vfs.Manager`, picks a **pluggable** FUSE backend, mount/unmount, `Refresh` for directory invalidation.
2. **`vfs.Manager`** (`pkg/mount/dfs/vfs/manager.go`) — maps logical files to `CacheItem`s; owns open/close lifecycle.
3. **`vfs.Cache`** (`vfs/cache.go`) — sparse on-disk files, range tracking (`vfs/ranges/`), eviction near **90%** of disk budget, `singleflight` to dedup concurrent item creation, JSON metadata sidecars with periodic flush.
4. **`vfs.Downloaders`** (`vfs/downloaders.go`) — multiple chunk downloaders per item, waiters, kickers, circuit breaker, adaptive chunk cap (**16×** base), no-progress watchdog (**45s** in this tree).

**Why it helps rd-rs:** Clear boundaries match rd-rs (`fuse` ↔ `cache` ↔ `worker`) but Decypharr documents an alternative decomposition (explicit `StreamingFile` + `ReadAtContext`) you can compare to your read path.

---

## B2. Pluggable FUSE backends (Linux vs cross-platform)

`pkg/mount/dfs/backend/interface.go` registers constructors:

- **`hanwen`** — pure Go [`go-fuse`](https://github.com/hanwen/go-fuse) on Linux (default); build tags also allow darwin/amd64 in `backend/hanwen/backend.go`.
- **`cgo`** — `cgofuse` for macOS/Windows / Fuse-T / WinFsp (`backend/cgofuse/`).

`GetDefaultBackendType` picks **hanwen on Linux**, else **cgo**, with env override `DFS_FUSE_BACKEND`.

**Why it helps rd-rs:** If you ever target non-Linux FUSE, a **backend trait + registry** (Rust equivalent) avoids forking the whole VFS; Linux keeps the fast path.

---

## B3. Open handle lifecycle without a global file lock

`vfs.Manager.GetFile` / `ReleaseFile` use **refcount first**, then an atomic **`deleted`** flag on `fileEntry` so a concurrent `ReleaseFile` cannot hand out a stale entry after map removal (`vfs/manager.go` comments). `LoadOrStore` handles creation races.

**Why it helps rd-rs:** Same class of bug as concurrent `open`/`release` on `ManagedTorrent` / cache items; the refcount+deleted pattern is a compact recipe.

---

## B4. Context-aware reads through the stack

`StreamingFile.ReadAtContext` (`vfs/file.go`) passes `ctx` into `CacheItem.ReadAtContext` so **client disconnect or FUSE read timeout** can cancel work in the downloader layer.

**Why it helps rd-rs:** Ensures range workers honor `CancellationToken`/`JoinHandle` abort consistently from `read()` down to HTTP—worth auditing if any path still uses “fire and forget” IO.

---

## B5. Downloader coordinator details (tunable behavior)

From `vfs/downloaders.go` (names and values are implementation choices, not gospel):

- **Dual kicker cadence:** `kickerInterval` (5s) vs **`activeWaiterKickerInterval` (1s)** when readers are blocked—similar spirit to tuning “read wait” / wake latency under backpressure.
- **Circuit breaker** after repeated errors, with a long **cooldown** (20 minutes) before retrying.
- **Downloader locality window** (`downloaderWindow` 4MiB) to reuse a worker when sequential reads stay nearby.
- **Stream registration** (`TrackStream` / `UntrackStream`) for stats/UI.

**Why it helps rd-rs:** rd-rs already has retries, adaptive chunks, and no-progress timeouts; Decypharr makes the **waiter/kicker** and **circuit** policies explicit—good for parity checks against `cache/worker.rs` and fuse read retry loops.

---

## B6. FUSE kernel attribute caching (hanwen)

`backend/hanwen/backend.go` sets **`AttrTimeout`**, **`EntryTimeout`**, and a **per-read `ReadTimeout`** (120s in this tree)—tighter than “always kernel default.”

**Why it helps rd-rs:** Older audits called out missing `attr_timeout` / `entry_timeout`; Decypharr shows concrete values you can compare to `fuser` mount options.

---

## B7. DFS-focused config surface

`pkg/mount/dfs/config/config.go` (`FuseConfig`) exposes cache dir, max disk size, cleanup interval, **chunk size**, **read-ahead**, daemon timeout, retries, UID/GID, `allow_other`, etc.—aligned with rclone-style knobs but native to the app.

**Why it helps rd-rs:** Checklist for documenting or exposing the same tunables in `config.toml` with the same semantics (especially read-ahead vs chunk size vs idle eviction).

---

## Implementation note

zurg ideas lean on **thread-safe maps and invalidation rules**; Decypharr DFS leans on **layered concurrency** (atomics + fine locks + `singleflight`). Use these trees as **behavioral specs**, not line-by-line ports.

---

*Sources: static read of zurg-main and decypharr-temp; both upstreams may drift.*
