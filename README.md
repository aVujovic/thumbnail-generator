# Thumbnail Generation Service

Scans a file system for video files and generates thumbnail preview images for them.

The work is split into **two decoupled services** connected by **Redpanda** (Kafka-wire-compatible
broker): a producer that discovers videos and a consumer that generates thumbnails. This models the
real problem shape — a fast, I/O-bound scan feeding a slow, CPU-bound generation step — and lets the
two scale and fail independently.

See [PLAN.md](./PLAN.md) for the full architecture rationale and design decisions.

---

## How it works

```
┌────────────────────┐                          ┌──────────────────────┐
│  scanner-service    │      topic: video-jobs   │  generator-service    │
│  (producer)         │  ──────────────────────► │  (consumer group)     │
│                     │   ┌──────────────┐       │                      │
│  walk IFileSystem   │   │   Redpanda   │       │  pull job             │
│  detect video       │   │  (Kafka API) │       │  ffmpeg → thumbnail   │
│  produce VideoJob   │   └──────────────┘       │  timeout/kill/retry   │
└────────────────────┘                          │  write result + commit│
                                                 └──────────┬───────────┘
                                                            │
                                          thumbnails (mirror-tree)  +  results.jsonl
```

1. **scanner-service** walks the file system (iteratively, depth-guarded), detects video files by
   extension, and produces one `VideoJob` message per video onto the `video-jobs` topic. It is a
   one-shot job: it scans, produces, and exits.
2. **Redpanda** holds the queue. The broker provides backpressure (the consumer pulls at its own
   pace), durability, and at-least-once delivery — the scanner never has to hold the whole work-set
   in memory.
3. **generator-service** is a long-lived consumer daemon. For each job it runs ffmpeg to extract a
   thumbnail, with a per-job timeout (kill on hang), bounded retries with backoff, and idempotent
   skipping if the thumbnail already exists. It records the outcome and commits the Kafka offset.

### Key principles

- **Decoupled producer/consumer** — scan and generation run in parallel; the first video is being
  processed while the scan is still walking.
- **Write-ahead commit** — the Kafka offset is committed only *after* a job reaches a terminal state
  (`autoCommit: false`). A crash mid-work redelivers the message, so no thumbnail is silently lost.
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
├── contracts/     # @thumbnailer/contracts — shared event/record schemas (VideoJob, ThumbnailResult) via zod
└── core/          # @thumbnailer/core      — createService() + infra providers (config, logger, kafka,
                   #                            kafkaUtils, signals, commands) wired into one DI container
apps/
├── scanner-service/    # producer: FileScanner (walk) → VideoDetector → JobProducer
└── generator-service/  # consumer: VideoJobConsumer → ThumbnailGenerator → ThumbnailStore + ResultRepository
```

- **domain** — the two abstract dependencies from the task (`IFileSystem`,
  `startThumbnailProcess`/`IProcess`) plus a structured `ILogger`, as interfaces only. Fakes live
  here too and are reused across both services' tests.
- **contracts** — the data shapes shared between the services: `VideoJob` (the queue message) and
  `ThumbnailResult` (the per-video outcome record). Validated with zod, so a malformed message is
  rejected, not silently mis-processed.
- **core** — cross-cutting infrastructure as DI providers, registered in one `createService()` call.
  Each service then registers only its own domain classes; everything (logger, config, kafka client,
  lifecycle/shutdown) arrives via constructor injection. The interesting domain logic — commit
  semantics, timeout/kill/retry — stays visible in `apps/*`, not hidden in `core`.

---

## Results output (JSONL — the production DB equivalent)

For every processed video the generator appends one record to a results log
(`OUTPUT_ROOT/results.jsonl` by default), in addition to writing the thumbnail image:

```json
{"videoPath":"/data/movies/clip.mov","outputPath":"/out/.../clip.mov.jpg","size":3422636,"discoveredAt":1781208138506,"format":"jpg","status":"generated","attempts":1,"processedAt":1781208139348}
{"videoPath":"/data/bad.mp4","outputPath":"/out/.../bad.mp4.jpg","size":262,"discoveredAt":1781208138485,"format":"jpg","status":"failed","attempts":3,"error":"exited with code 234","processedAt":1781208139037}
```

Each line is one record with the video's metadata and the terminal `status`
(`generated` | `skipped` | `failed`, with the error reason on failure).

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

Bring up everything (Redpanda + topic + both services) with one command:

```bash
pnpm up
```

Or step by step:

```bash
pnpm infra:up        # start Redpanda + Console, waits until healthy
pnpm topic:create    # create the video-jobs topic (idempotent)
pnpm services        # run both services (generator daemon + scanner one-shot)
```

The generator runs as a long-lived daemon; the scanner produces its jobs and exits. Point the scan
at a real directory by setting `SCAN_ROOT` in `.env` (e.g. `SCAN_ROOT=~/Movies`).

> **Use `pnpm services` to run the pipeline**, not `pnpm dev`. `pnpm dev` runs the services under
> `node --watch` for live code reload while developing — with a one-shot producer plus a daemon
> consumer, the watcher restarts the processes (and churns the Kafka consumer group), which you
> don't want for an actual scan. `pnpm services` runs them without watch.

## Endpoints

- **Redpanda Console** (topics, messages, consumer-group lag): http://localhost:8086
- **Kafka API**: `localhost:29092`

## Commands

```bash
pnpm up              # infra + topic + both services
pnpm services        # both services, no watch (use this to run the pipeline)
pnpm scan            # scanner only (one-shot)
pnpm generate        # generator only (daemon)
pnpm dev             # both services in watch mode (development only)
pnpm test            # run all tests
pnpm type-check      # type-check all packages
pnpm infra:up        # start Redpanda + Console
pnpm infra:down      # stop Redpanda
pnpm infra:logs      # follow Redpanda logs
pnpm topic:create    # create the video-jobs topic
```

If the consumer group ever gets wedged (e.g. after killing dev processes hard), reset it:

```bash
docker exec thumbnailer-redpanda rpk group delete thumbnail-generators -X brokers=localhost:9092
```

## Configuration

All config is environment-driven and validated against a zod schema at startup (fails fast with a
readable error). See [.env.example](./.env.example) for the full list. Most-used settings:

| Variable | Service | Description |
|---|---|---|
| `KAFKA_BROKERS` | both | Redpanda broker address (default `localhost:29092`) |
| `SCAN_ROOT` | scanner | Directory to scan (recursively). `~` is expanded. |
| `VIDEO_EXTENSIONS` | scanner | Comma-separated extensions to treat as video |
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

Unit tests cover the file-system walk (deep trees, cycles, permission/TOCTOU errors), video
detection, the thumbnail generator's resilience (timeout→kill, retry, launch failure, idempotent
skip), Kafka commit semantics (write-ahead / redeliver-on-crash), and result recording — all with
in-memory fakes, no real Kafka, ffmpeg, or disk.
