# Thumbnail Generation Service

Scans a file system for video files and generates thumbnail preview images for them.

The work is split into **three decoupled services** connected by **Redpanda** (Kafka-wire-compatible
broker): an **orchestrator** that issues commands, a **scanner** that discovers videos on demand, and
a **generator** that produces thumbnails. This models the real problem shape — a fast, I/O-bound scan
feeding a slow, CPU-bound generation step — and lets the parts scale, fail, and be controlled
independently. Nothing scans automatically: the scanner and generator are long-lived daemons that sit
idle until the orchestrator tells them what to do.

See [PLAN.md](./PLAN.md) for the full architecture rationale and design decisions.

---

## How it works

```
                       pnpm scan:start --root ~/Movies
                                  │
                        ┌─────────▼──────────┐
                        │ orchestrator-service│   (CLI: publishes one command, exits)
                        └─────────┬──────────┘
                                  │  topic: scan-commands
              ┌───────────────────┼───────────────────┐
              ▼                                        ▼
   ┌─────────────────────┐                  ┌──────────────────────┐
   │  scanner-service     │  topic:          │  generator-service    │
   │  (daemon)            │  video-jobs      │  (daemon)             │
   │                      │ ───────────────► │                      │
   │  StartScan → walk    │                  │  pull job             │
   │  StopScan  → cancel  │                  │  Pause/Resume         │
   │  produce VideoJob ───┼──► scan-events   │  ffmpeg → thumbnail   │
   │  emit ScanStarted/   │  (orchestrator   │  timeout/kill/retry   │
   │       Completed      │   observes)      │  write result + commit│
   └─────────────────────┘                  └──────────┬───────────┘
                                                        │
                                      thumbnails (mirror-tree)  +  results.jsonl
```

1. **orchestrator-service** is a thin CLI. Each invocation publishes one command onto the
   `scan-commands` topic and exits: `StartScan` (with a path and a `scanId`), `StopScan`,
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
   and commits the Kafka offset. It also consumes `scan-commands` to honour `Pause`/`Resume`.

### Key principles

- **Command-driven, nothing automatic** — daemons start idle and act only on orchestrator commands.
  You can scan any directory at runtime, scan a different one, queue scans, stop, or pause/resume —
  without restarting a process.
- **Command vs. event separation** — `scan-commands` carries imperatives (orchestrator → services);
  `scan-events` carries facts (scanner → orchestrator). Commands and events never share a topic.
- **Decoupled producer/consumer** — scan and generation run in parallel; the first video is being
  processed while the scan is still walking.
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
├── orchestrator-service/ # CLI: parseArgs → CommandPublisher → publishes a ScanCommand
├── scanner-service/      # daemon: CommandConsumer → ScanManager → FileScanner / JobProducer / EventProducer
└── generator-service/    # daemon: VideoJobConsumer → ThumbnailGenerator → ThumbnailStore + ResultRepository
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
- **orchestrator-service** — a one-shot CLI. `parseArgs` turns the sub-command into a typed
  `ScanCommand`; `CommandPublisher` sends it. No daemon, no business logic — it's the control surface.
- **scanner-service** — a daemon. `CommandConsumer` routes commands to `ScanManager`, which runs scans
  one at a time (FIFO queue) and is cancellable; `FileScanner` does the iterative walk, `JobProducer`
  emits `VideoJob`s, `EventProducer` emits lifecycle events.
- **generator-service** — a daemon. `VideoJobConsumer` consumes work + control commands;
  `ThumbnailGenerator` owns the resilient ffmpeg run; `ThumbnailStore` and `ResultRepository` own the
  outputs.

---

## Results output (JSONL — the production DB equivalent)

For every processed video the generator appends one record to a results log
(`OUTPUT_ROOT/results.jsonl` by default), in addition to writing the thumbnail image:

```json
{"scanId":"dl-1","videoPath":"/data/movies/clip.mov","outputPath":"/out/.../clip.mov.jpg","size":3422636,"discoveredAt":1781208138506,"format":"jpg","status":"generated","attempts":1,"processedAt":1781208139348}
{"scanId":"dl-1","videoPath":"/data/bad.mp4","outputPath":"/out/.../bad.mp4.jpg","size":262,"discoveredAt":1781208138485,"format":"jpg","status":"failed","attempts":3,"error":"exited with code 234","processedAt":1781208139037}
```

Each line is one record with the video's metadata, the `scanId` (so results group back to the scan
that produced them), and the terminal `status` (`generated` | `skipped` | `failed`, with the error
reason on failure).

This is deliberately behind an **`IResultRepository`** interface, with a JSONL (append-only)
implementation. **In production this would be a Postgres/MySQL table** — `save()` models a single
row `INSERT`, and swapping `JsonlResultRepository` for a `PostgresResultRepository` requires no
change to the consumer, the generator, or their tests. JSONL is used here because append is atomic
under concurrency (multiple consumers never clobber each other) and survives crashes — the same
guarantees a row-insert gives, without standing up a database for the assessment.

Thumbnails themselves mirror the source tree under `OUTPUT_ROOT`
(e.g. `/data/movies/clip.mov` → `OUTPUT_ROOT/data/movies/clip.mov.jpg`) so two videos can never
collide on the same output path.

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
pnpm up        # infra:up + create topics + run scanner & generator daemons
```

Or step by step:

```bash
pnpm infra:up        # start Redpanda + Console, waits until healthy
pnpm topic:create    # create video-jobs / scan-commands / scan-events topics (idempotent)
pnpm services        # run scanner + generator daemons (no watch)
```

At this point both daemons are connected and **idle** — the scanner logs `waiting for commands`, the
generator `waiting for jobs`. Nothing is scanned until you tell it to.

**2. Drive the pipeline with the orchestrator** (in another terminal):

```bash
pnpm scan:start --root ~/Movies          # scan a directory (auto-generated scanId)
pnpm scan:start --root ~/Downloads --id dl-1   # explicit scanId for correlation
pnpm scan:stop                            # cancel the running scan + clear the queue
pnpm scan:stop --id dl-1                  # cancel/drop a specific scan
pnpm gen:pause                            # pause thumbnail generation (jobs queue up)
pnpm gen:resume                           # resume generation
```

`--root` accepts absolute, relative, or `~`-prefixed paths (expanded the same way everywhere). Issue
`scan:start` again to scan a **different** directory at any time; a scan already running is queued
behind the current one (FIFO).

Watch progress in the **Redpanda Console** (http://localhost:8086) or in the daemon logs. Thumbnails
land under `OUTPUT_ROOT` (mirror-tree) and every outcome is appended to `results.jsonl`.

> **Use `pnpm services` to run the daemons**, not `pnpm dev`. `pnpm dev` runs them under
> `node --watch` for live code reload while developing; for actually running the pipeline use
> `pnpm services` (no watch).

## Endpoints

- **Redpanda Console** (topics, messages, consumer-group lag): http://localhost:8086
- **Kafka API**: `localhost:29092`

## Commands

```bash
# infra + daemons
pnpm up              # infra + topics + both daemons
pnpm services        # scanner + generator daemons (no watch — use this to run)
pnpm scan            # scanner daemon only
pnpm generate        # generator daemon only
pnpm dev             # both daemons in watch mode (development only)

# orchestration (control the running daemons)
pnpm scan:start --root <path> [--id <id>]   # StartScan
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
docker exec thumbnailer-redpanda rpk group delete scanners thumbnail-generators -X brokers=localhost:9092
```

## Configuration

All config is environment-driven and validated against a zod schema at startup (fails fast with a
readable error). See [.env.example](./.env.example) for the full list. Most-used settings:

| Variable | Service | Description |
|---|---|---|
| `KAFKA_BROKERS` | all | Redpanda broker address (default `localhost:29092`) |
| `SCANNER_GROUP_ID` | scanner | Consumer group for the scanner (must differ from the generator's) |
| `GENERATOR_GROUP_ID` | generator | Consumer group for the generator |
| `VIDEO_EXTENSIONS` | scanner | Comma-separated extensions to treat as video |
| `SCAN_MAX_DEPTH` | scanner | Max directory depth (cheap cycle guard) |
| `SCAN_ROOT` | scanner | Fallback scan root only — the orchestrator supplies the path per scan |
| `OUTPUT_ROOT` | generator | Where thumbnails are written (mirror-tree) |
| `RESULTS_PATH` | generator | Results JSONL path (default `OUTPUT_ROOT/results.jsonl`) |
| `THUMBNAIL_FORMAT` | generator | Output image format (`jpg`, `png`, …) |
| `GENERATE_CONCURRENCY` | generator | Max concurrent ffmpeg processes |
| `GENERATE_TIMEOUT_MS` | generator | Per-job timeout before the process is killed |
| `GENERATE_MAX_RETRIES` | generator | Retries on failure (total attempts = retries + 1) |
| `GENERATE_FORCE` | generator | Regenerate even if a thumbnail already exists |

## Tests

```bash
pnpm test
```

Unit tests cover:

- **file-system walk** — deep trees, symlink cycles, permission/TOCTOU errors
- **video detection** — extension allowlist, empty/unreadable skips
- **scan orchestration** — `ScanManager` FIFO queue, run-one-at-a-time, cancel mid-scan, `scanId`
  propagation; command parsing (`parseArgs`); command/event schema validation
- **thumbnail generation** — timeout→kill (no zombie), retry, launch failure, idempotent skip
- **Kafka semantics** — write-ahead commit / redeliver-on-crash, poison-message handling,
  pause/resume control
- **result recording** — every status written, `scanId` carried through

Everything runs with in-memory fakes — no real Kafka, ffmpeg, or disk.
