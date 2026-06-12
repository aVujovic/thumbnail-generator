# Thumbnail Generation Service

Scans a file system for video files and generates thumbnail preview images for them.

The work is split into **four decoupled services** connected by **Redpanda** (Kafka-wire-compatible
broker): an **orchestrator** that issues commands, a **scanner** that discovers videos on demand, a
**generator** that produces thumbnails, and a **sync-service** that replicates finished thumbnails to
their destination. This models the real problem shape — a fast, I/O-bound scan feeding a slow,
CPU-bound generation step, followed by a delivery step — and lets the parts scale, fail, and be
controlled independently. Nothing scans automatically: the scanner, generator, and sync-service are
long-lived daemons that sit idle until commanded / fed work.

See [PLAN.md](./PLAN.md) for the full architecture rationale and design decisions.

---

## How it works

All messages flow through **Redpanda** — every service only produces to or consumes from a topic;
no service talks to another directly. The broker is the hub in the middle, holding four topics:

```
  orchestrator        scanner-service          generator-service          sync-service
  (CLI, one-shot)     (daemon)                 (daemon)                   (daemon)
        │             ▲       │                ▲    │      │              ▲
        │ produce     │ scan- │ produce        │    │ scan │ produce      │ consume
        │ scan-cmds   │ cmds  │ video-jobs     │    │ cmds │ thumbnail-   │ thumbnail-
        │             │       │ + scan-events  │    │      │ ready        │ ready
        ▼             │       ▼                │    ▼      ▼              │
  ╔═════════════════════════════════════════════════════════════════════╪═══════╗
  ║                            R E D P A N D A                            │       ║
  ║  [ scan-commands ]   [ video-jobs ]   [ scan-events ]   [ thumbnail-ready ]   ║
  ╚══════════════════════════════════════════════════════════════════════════════╝
                                              │                            │
                                      ffmpeg → thumbnail            "video X synced"
                                              ▼                            ▼
                            thumbnails (mirror-tree) + videoDb.jsonl   videoDb row: synced=true
```

Who produces and consumes each topic:

| Topic | Produced by | Consumed by |
|---|---|---|
| `scan-commands` | orchestrator | scanner (`StartScan`/`StopScan`), generator (`Pause`/`Resume`) |
| `video-jobs` | scanner | generator |
| `scan-events` | scanner | orchestrator / observers |
| `thumbnail-ready` | generator | sync-service |

So a typical scan is: orchestrator **produces** `StartScan` → scanner **consumes** it, walks the
directory, and **produces** `VideoJob`s (and `ScanStarted`/`ScanCompleted` events) → generator
**consumes** the jobs, makes thumbnails, writes a `videoDb.jsonl` row (`synced: false`), and
**produces** a `ThumbnailReady` event → sync-service **consumes** it, replicates the thumbnail, and
flips that row to `synced: true`. No service ever calls another — Redpanda sits between every pair.

1. **orchestrator-service** is a thin CLI and the control plane. It resolves a `--tenant <id>` to a
   scan root via the tenant catalog, then publishes one command onto the `scan-commands` topic and
   exits: `StartScan` (with the resolved path, `scanId`, and `tenantId`), `StopScan`,
   `PauseGenerator`, `ResumeGenerator`. This is where scheduling/automation would live in production.
2. **scanner-service** is a long-lived daemon consuming `scan-commands`. On `StartScan` it walks the
   given directory (iteratively, depth-guarded), detects videos by extension, and produces one
   `VideoJob` per video onto `video-jobs` — tagged with the `scanId`. It runs one scan at a time
   (FIFO queue), can be cancelled mid-scan (`StopScan`), and emits `ScanStarted`/`ScanCompleted`/
   `ScanCancelled` on `scan-events` for the orchestrator to observe.
3. **Redpanda** holds the queues. The broker provides backpressure (the consumer pulls at its own
   pace), durability, and at-least-once delivery — the scanner never holds the whole work-set in memory.
4. **generator-service** is a long-lived daemon consuming `video-jobs`. For each job it runs ffmpeg to
   extract a thumbnail, with a per-job timeout (kill on hang), bounded retries with backoff, and
   idempotent skipping if the thumbnail already exists. It records the outcome (carrying the `scanId`)
   as a `videoDb.jsonl` row with `synced: false`, and — when a thumbnail actually exists (generated or
   skipped) — produces a `ThumbnailReady` event for the sync-service. It also consumes `scan-commands`
   to honour `Pause`/`Resume`. (A `failed` job produces nothing, so it emits no ready event.)
5. **sync-service** is a long-lived daemon consuming `thumbnail-ready`. For each event it "syncs" the
   thumbnail to its destination (a stand-in that logs `video X synced` — in production an rsync / scp /
   S3 upload to the server that serves the previews), then flips that video's `videoDb.jsonl` row to
   `synced: true`. Same write-ahead commit as the generator: the offset commits only after the sync +
   DB update succeed, and re-syncing is idempotent, so a crash mid-sync just redelivers.

### Key principles

- **Command-driven, nothing automatic** — daemons start idle and act only on orchestrator commands.
  You can scan a tenant at runtime, scan a different one, queue scans, stop, or pause/resume —
  without restarting a process.
- **Multi-tenant** — every scan is tenant-scoped; the orchestrator resolves `--tenant <id>` to a path
  from its catalog, and the `tenantId` flows through jobs/events/results for per-tenant output and
  tracking. Operators work with logical tenants, never raw paths.
- **Command vs. event separation** — `scan-commands` carries imperatives (orchestrator → services);
  `scan-events` carries facts (scanner → orchestrator). Commands and events never share a topic.
- **Decoupled producer/consumer** — scan and generation run in parallel; the first video is being
  processed while the scan is still walking. The slow generator scales horizontally — run more
  instances and the broker splits the work across them, no code change (see [Scaling](#scaling-the-generator)).
- **Write-ahead commit** — a video-job offset is committed only *after* the job reaches a terminal
  state (`autoCommit: false`). A crash mid-work redelivers the message, so no thumbnail is silently lost.
- **Resilient by default** — permission errors, broken symlinks, TOCTOU races, ffmpeg hangs, corrupt
  videos, and poison messages are all handled and logged; one bad entry never aborts the run.
- **Everything injected (DI)** — the file system, the ffmpeg launcher, the logger, and the result
  sink are all interfaces wired through an awilix container, so each piece is swappable and unit-
  testable with fakes (no real I/O in tests).
- **Idempotent / resumable** — re-running skips videos whose thumbnail already exists (`--force` /
  `GENERATE_FORCE=true` to override).

---

## Project structure (pnpm monorepo)

```
packages/
├── domain/        # @thumbnailer/domain   — pure interfaces (IFileSystem, IProcess, ILogger) + test fakes
├── contracts/     # @thumbnailer/contracts — shared schemas/builders (VideoJob, ThumbnailResult,
│                  #                            ScanCommand, ScanEvent, topic names) via zod
└── core/          # @thumbnailer/core      — createService() + infra providers (config, logger, kafka,
                   #                            kafkaUtils, signals, commands) wired into one DI container
apps/
├── orchestrator-service/ # CLI: TenantResolver + parseArgs → CommandPublisher → publishes a ScanCommand
├── scanner-service/      # daemon: CommandConsumer → ScanManager → FileScanner / JobProducer / EventProducer
├── generator-service/    # daemon: VideoJobConsumer → ThumbnailGenerator → ThumbnailStore + ResultRepository (videoDb)
└── sync-service/         # daemon: ThumbnailReadyConsumer → SyncTarget (replicate) → VideoDb (mark synced)
```

- **domain** — the two abstract dependencies from the task (`IFileSystem`,
  `startThumbnailProcess`/`IProcess`) plus a structured `ILogger`, as interfaces only. Fakes live
  here too and are reused across the services' tests.
- **contracts** — the data shapes shared between services: `VideoJob` (the work message),
  `ThumbnailResult` (the per-video outcome record), `ScanCommand` / `ScanEvent` (control plane), and
  the topic-name constants. Command names live in one place (`SCAN_COMMAND` constants + `scanCommands`
  builders) so no service hand-writes a magic string. Validated with zod, so a malformed message is
  rejected, not silently mis-processed.
- **core** — cross-cutting infrastructure as DI providers, registered in one `createService()` call.
  Each service then registers only its own domain classes; everything (logger, config, kafka client,
  lifecycle/shutdown) arrives via constructor injection. The interesting domain logic — commit
  semantics, timeout/kill/retry, the scan queue — stays visible in `apps/*`, not hidden in `core`.
- **orchestrator-service** — a one-shot CLI and control plane. `TenantResolver` maps `--tenant <id>`
  to a scan root (from `tenants.json`), `parseArgs` turns the sub-command into a typed `ScanCommand`,
  and `CommandPublisher` sends it. No daemon, no business logic — just the control surface.
- **scanner-service** — a daemon. `CommandConsumer` routes commands to `ScanManager`, which runs scans
  one at a time (FIFO queue) and is cancellable; `FileScanner` does the iterative walk, `JobProducer`
  emits `VideoJob`s, `EventProducer` emits lifecycle events.
- **generator-service** — a daemon. `VideoJobConsumer` consumes work + control commands;
  `ThumbnailGenerator` owns the resilient ffmpeg run; `ThumbnailStore` writes the image and
  `ResultRepository` writes the `videoDb.jsonl` row; on success it emits a `ThumbnailReady` event.
- **sync-service** — a daemon. `ThumbnailReadyConsumer` consumes `thumbnail-ready`; `SyncTarget`
  replicates the thumbnail (log-only here, rsync/S3 in production); `VideoDb` flips the row's `synced`
  flag. Both `SyncTarget` and `VideoDb` are interfaces, so the destination and the store are swappable.

---

## The video DB (`videoDb.jsonl` — the production DB equivalent)

There's one shared store, the **video DB**, holding one row per video — its metadata, the terminal
generation `status`, and whether its thumbnail has been **synced**. It lives at the repo root
(`videoDb.jsonl`, beside `tenants.json`) so two services share it: the **generator** INSERTs rows, the
**sync-service** UPDATEs the `synced` flag.

```json
{"scanId":"s-1","tenantId":"tenant-1","videoPath":"/data/clip.mov","outputPath":"/out/tenant-1/data/clip.mov.jpg","size":3422636,"discoveredAt":1781208138506,"format":"jpg","status":"generated","attempts":1,"processedAt":1781208139348,"synced":true,"syncedAt":1781208140002}
{"scanId":"s-1","tenantId":"tenant-1","videoPath":"/data/bad.mp4","outputPath":"/out/tenant-1/data/bad.mp4.jpg","size":262,"discoveredAt":1781208138485,"format":"jpg","status":"failed","attempts":3,"error":"exited with code 234","processedAt":1781208139037,"synced":false}
```

Each row carries the video's metadata, the `scanId` + `tenantId` (so rows group back to their scan and
tenant), the terminal `status` (`generated` | `skipped` | `failed`), and the **sync lifecycle**:

- The generator writes every row with `synced: false`.
- For a row whose thumbnail exists (`generated` / `skipped`) it emits a `ThumbnailReady` event.
- The sync-service consumes that event, replicates the thumbnail, and flips the row to
  `synced: true` (stamping `syncedAt`). A `failed` row produced no thumbnail, so it's never synced.

This sits behind two interfaces — **`IResultRepository`** (generator-side INSERT, `save()`) and
**`IVideoDb`** (sync-side UPDATE, `markSynced()`) — over a JSONL implementation. **In production this
is one Postgres/MySQL table**: `save()` is a row `INSERT`, `markSynced()` is
`UPDATE videos SET synced=true WHERE path=?`. Swapping the JSONL classes for DB ones touches neither
consumer nor their tests. JSONL is used here because it gives the same per-row durability without
standing up a database for the assessment.

Thumbnails themselves mirror the source tree under `OUTPUT_ROOT/<tenantId>/`
(e.g. `/data/movies/clip.mov` for `tenant-1` → `OUTPUT_ROOT/tenant-1/data/movies/clip.mov.jpg`) so
neither two videos nor two tenants can ever collide on the same output path.

### Why a separate sync-service?

Replication is its own concern with its own failure modes (network, destination down, slow transfers)
and its own scaling needs (more sync workers when the destination is the bottleneck) — independent of
how fast thumbnails are generated. Putting it behind its own topic means the generator never blocks on
a slow upload, the two scale separately, and other consumers (a search indexer, a CDN warmer) can
react to the same `ThumbnailReady` event without touching the generator. That's the whole point of the
decoupled, event-driven shape: the producer announces a fact and is done.

---

## Requirements

- Node.js >= 20
- pnpm (`npm i -g pnpm`)
- Docker (for Redpanda)
- ffmpeg on `PATH` (`brew install ffmpeg`). Without it, a placeholder file is written instead of a
  real image so the pipeline still runs end-to-end.

## Setup

```bash
pnpm install
cp .env.example .env
```

## Run

There are two steps: **bring the daemons up** (they start idle), then **drive them with the
orchestrator**.

**1. Start infra + both daemons:**

```bash
pnpm up        # infra:up + create topics + run scanner, generator & sync daemons
```

Or step by step:

```bash
pnpm infra:up        # start Redpanda + Console, waits until healthy
pnpm topic:create    # create video-jobs / scan-commands / scan-events / thumbnail-ready topics (idempotent)
pnpm services        # run scanner + generator + sync daemons (no watch)
```

At this point all three daemons are connected and **idle** — the scanner logs `waiting for commands`,
the generator `waiting for jobs`, the sync-service `waiting for ready thumbnails`. Nothing happens
until you tell it to.

**2. Drive the pipeline with the orchestrator** (in another terminal):

```bash
pnpm scan:start --tenant tenant-1          # scan tenant-1's directory
pnpm scan:start --tenant tenant-1 --id s-1 # explicit scanId for correlation
pnpm scan:stop                             # cancel the running scan + clear the queue
pnpm scan:stop --id s-1                    # cancel/drop a specific scan
pnpm gen:pause                             # pause thumbnail generation (jobs queue up)
pnpm gen:resume                            # resume generation
```

Every scan is **tenant-scoped**: you pass a `--tenant <id>`, and the orchestrator resolves the
directory from the tenant catalog (see [Tenants](#tenants) below) — raw paths are not accepted, since
the scan root is a tenant configuration detail, not a CLI argument. Issue `scan:start` again for a
different tenant at any time; a scan already running is queued behind the current one (FIFO).

Watch progress in the **Redpanda Console** (http://localhost:8086) or in the daemon logs. Thumbnails
land under `OUTPUT_ROOT` (mirror-tree), every outcome is written as a `videoDb.jsonl` row, and the
sync-service logs `🔄 video synced — …` then flips that row's `synced` flag to `true`.

> **Use `pnpm services` to run the daemons**, not `pnpm dev`. `pnpm dev` runs them under
> `node --watch` for live code reload while developing; for actually running the pipeline use
> `pnpm services` (no watch).

## Tenants

The system is **multi-tenant**: every scan runs for a tenant, and a `tenantId` flows through the whole
pipeline (jobs, events, results) for isolation and tracking. The orchestrator owns the tenant catalog
([tenants.json](./tenants.json)) — it maps a logical tenant id to a scan root:

```json
{
  "tenant-1": { "scanRoot": "~/Downloads" },
  "tenant-2": { "scanRoot": "~/Desktop" }
}
```

`scanRoot` accepts absolute, relative, or `~`-prefixed paths. The catalog is the orchestrator's
control-plane knowledge — the scanner and generator never see the tenant→path mapping, only the
resolved path plus the `tenantId`.

What the `tenantId` buys you:

- **Resolution** — `pnpm scan:start --tenant tenant-1` → orchestrator looks up `~/Downloads` and
  publishes `StartScan { tenantId, scanId, scanRoot }`.
- **Per-tenant output** — thumbnails land under `OUTPUT_ROOT/<tenantId>/…` (mirror-tree inside), so
  tenants never collide on disk.
- **Per-tenant tracking** — every `videoDb.jsonl` row carries the `tenantId`, so rows group back
  to their tenant.

In production the catalog would be a database table; the path it points at would be the real
production `IFileSystem` root, not a local directory. The configurable `TENANTS_FILE` env var points
at the catalog (default: repo-root `tenants.json`).

## Scaling the generator

The scanner is fast (it only reads file-system metadata); the generator is slow (one ffmpeg run per
video, seconds each). So the generator is the bottleneck, and it's where you scale out. Because all
generators join the same Kafka consumer group (`thumbnail-generators`), the broker **partitions the
work across them automatically** — no code change, no internal thread pool. This is why the generator
is intentionally a simple one-job-at-a-time consumer: parallelism is a deployment concern, not a code
concern.

Two things matter:

1. **The `video-jobs` topic needs N partitions.** A partition is the unit of parallelism: each is
   consumed by exactly one generator in the group. With the default 1 partition, only one generator
   ever gets messages (the rest sit idle). For N parallel generators, create the topic with N
   partitions:
   ```bash
   docker exec thumbnailer-redpanda rpk topic create video-jobs -p 4 -X brokers=localhost:9092
   ```
2. **Run the generator process N times.** Same command, more instances — the broker assigns each a
   share of the partitions:
   ```bash
   # in N separate terminals (or backgrounded), from the repo root:
   pnpm generate
   ```

Measured locally: scanning `~/Desktop` produced 175 video jobs. With **one** generator the queue
backed up (lag ~90, generator trailing the scan); with **three** generators sharing 4 partitions the
175 jobs were split (40 / 51 / 84) and the lag stayed at ~0 — the generators kept up with the scan.
Scaling is horizontal and requires no code change.

### How the work is sharded

A `VideoJob` is produced with the **video path as the message key** (`JobProducer` sends
`{ key: job.path, … }`). Kafka picks the partition from the key:

```
partition = murmur2(key) & 0x7fffffff % partitionCount
```

i.e. a hash of the path, modulo the number of partitions. This is consistent hashing / sharding, and
the key choice is deliberate:

- **Even distribution** — paths are diverse, so the hash spreads jobs roughly evenly across
  partitions (and therefore across generators).
- **Idempotency** — the same path always hashes to the same partition → the same generator, so a
  re-scan or redelivery of a video is handled by the consumer that already wrote (or skipped) it.
- **Per-key ordering** — Kafka preserves order within a partition, so all jobs for one path stay ordered.

A poor key would break this — e.g. keying by `tenantId` would send a whole tenant's videos to one
partition (one generator), defeating the balance. The path is the right shard key.

Gotchas:

- **Partition count is fixed up front.** Changing it re-shuffles the key→partition mapping
  (`hash % 4` ≠ `hash % 8`), so pick it for peak generator count and leave it.
- **`rpk topic delete` is async.** To re-create with a different partition count, wait for the topic
  to actually disappear first, or `create` silently no-ops with `TOPIC_ALREADY_EXISTS` and you keep
  the old partition count.

> **In production this is Kubernetes.** The generator becomes a `Deployment` with `replicas: N`
> (or an HPA that auto-scales on Kafka consumer lag), and the topic is provisioned with enough
> partitions to match peak replica count. The local "run it N times" above is just the dev-time
> equivalent of bumping replicas.

## Endpoints

- **Redpanda Console** (topics, messages, consumer-group lag): http://localhost:8086
- **Kafka API**: `localhost:29092`

## Commands

```bash
# infra + daemons
pnpm up              # infra + topics + all daemons
pnpm services        # scanner + generator + sync daemons (no watch — use this to run)
pnpm scan            # scanner daemon only
pnpm generate        # generator daemon only
pnpm sync            # sync daemon only
pnpm dev             # all daemons in watch mode (development only)

# orchestration (control the running daemons)
pnpm scan:start --tenant <id> [--id <id>]   # StartScan (path from tenants.json)
pnpm scan:stop [--id <id>]                  # StopScan
pnpm gen:pause                              # PauseGenerator
pnpm gen:resume                             # ResumeGenerator

# tooling
pnpm test            # run all tests
pnpm type-check      # type-check all packages
pnpm infra:up        # start Redpanda + Console
pnpm infra:down      # stop Redpanda
pnpm infra:logs      # follow Redpanda logs
pnpm topic:create    # create the topics
```

If a consumer group ever gets wedged (e.g. after killing daemons hard), reset it:

```bash
docker exec thumbnailer-redpanda rpk group delete scanners thumbnail-generators thumbnail-syncers -X brokers=localhost:9092
```

## Configuration

All config is environment-driven and validated against a zod schema at startup (fails fast with a
readable error). See [.env.example](./.env.example) for the full list. Most-used settings:

| Variable | Service | Description |
|---|---|---|
| `KAFKA_BROKERS` | all | Redpanda broker address (default `localhost:29092`) |
| `TENANTS_FILE` | orchestrator | Path to the tenant catalog (default repo-root `tenants.json`) |
| `SCANNER_GROUP_ID` | scanner | Consumer group for the scanner (must differ from the others') |
| `GENERATOR_GROUP_ID` | generator | Consumer group for the generator |
| `SYNC_GROUP_ID` | sync | Consumer group for the sync-service |
| `VIDEO_EXTENSIONS` | scanner | Comma-separated extensions to treat as video |
| `SCAN_MAX_DEPTH` | scanner | Max directory depth (cheap cycle guard) |
| `OUTPUT_ROOT` | generator | Where thumbnails are written (`OUTPUT_ROOT/<tenantId>/`, mirror-tree) |
| `VIDEO_DB_PATH` | generator, sync | Shared video DB (JSONL) path (default repo-root `videoDb.jsonl`) |
| `THUMBNAIL_FORMAT` | generator | Output image format (`jpg`, `png`, …) |
| `GENERATE_CONCURRENCY` | generator | Max concurrent ffmpeg processes |
| `GENERATE_TIMEOUT_MS` | generator | Per-job timeout before the process is killed |
| `GENERATE_MAX_RETRIES` | generator | Retries on failure (total attempts = retries + 1) |
| `GENERATE_FORCE` | generator | Regenerate even if a thumbnail already exists |
| `SYNC_DELAY_MS` | sync | Simulated per-thumbnail transfer latency (ms, default 0) |

## Tests

```bash
pnpm test
```

Unit tests cover:

- **file-system walk** — deep trees, symlink cycles, permission/TOCTOU errors
- **video detection** — extension allowlist, empty/unreadable skips
- **scan orchestration** — `ScanManager` FIFO queue, run-one-at-a-time, cancel mid-scan,
  `scanId`/`tenantId` propagation; command parsing (`parseArgs`); command/event schema validation
- **tenants** — `TenantResolver` (resolve, `~` expansion, unknown-tenant error, catalog load/validate)
- **thumbnail generation** — timeout→kill (no zombie), retry, launch failure, idempotent skip,
  per-tenant output path
- **Kafka semantics** — write-ahead commit / redeliver-on-crash, poison-message handling,
  pause/resume control
- **result recording** — every status written, `scanId` + `tenantId` carried through

Everything runs with in-memory fakes — no real Kafka, ffmpeg, or disk.
