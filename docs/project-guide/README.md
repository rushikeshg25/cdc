# CDC project guide

> Generated: 2026-09-20 from commit `9d41af0`. Scope: the 35 tracked files in this single Go module; existing runtime output was not used as evidence.

Validation: all 393 relative links and their source-line/heading anchors checked; all nine Mermaid diagrams parsed. Existing `go test ./...` passed with a temporary build cache and loopback access for HTTP tests. No live PostgreSQL/Kafka capture or crash-recovery experiment was run.

## The five-file tour

Read these in order to follow one change from process startup to durable file output.

| # | File | Why this one | Then look at |
| --- | --- | --- | --- |
| 1 | [`cmd/cdc/main.go:40`](../../cmd/cdc/main.go#L40) | Wires configuration, connections, snapshot/resume, sink, and checkpoint. | [Startup](02-flow.md#startup) |
| 2 | [`internal/snapshot/snapshot.go:58`](../../internal/snapshot/snapshot.go#L58) | Explains where the initial rows come from before live streaming. | [Initial snapshot](02-flow.md#initial-snapshot) |
| 3 | [`internal/replication/replication.go:111`](../../internal/replication/replication.go#L111) | Owns the receive loop and the order of flush, checkpoint, and feedback. | [Live capture](02-flow.md#live-capture) |
| 4 | [`internal/decode/decode.go:42`](../../internal/decode/decode.go#L42) | Turns PostgreSQL protocol messages into named row changes. | [Event contract](01-architecture.md#boundaries-and-contracts) |
| 5 | [`internal/sink/file.go:20`](../../internal/sink/file.go#L20) | Finishes the default path with append-only JSONL and an explicit disk sync. | [Delivery decisions](05-decisions.md#flush-before-checkpoint-before-feedback) |

## What this is

CDC reads PostgreSQL logical replication messages and emits row changes to a selected output, with an initial snapshot on a fresh slot ([startup](../../cmd/cdc/main.go#L105), [stream](../../internal/replication/replication.go#L176)). It is presented as a learning project for developers exploring the replication protocol ([root README](../../README.md#L3)). It runs as one Go command with file, stdout, HTTP, or Kafka output and optional Prometheus metrics ([sink factory](../../internal/sink/factory.go#L17), [metrics](../../internal/metrics/metrics.go#L34)).

## Run it

From the repository root, with Go 1.25.0 or a compatible newer toolchain, Docker with Compose, and Make available:

```bash
go mod download
docker compose up -d --wait postgres
go run ./cmd/cdc --output guide-events.jsonl --slot guide_slot
```

The dedicated output/slot names avoid consuming the repository's existing `events.jsonl.offset`; use unused names for a fresh snapshot. The default connection flags target the Compose PostgreSQL instance on host port 5433; credentials remain in the existing configuration, not in this guide ([flags](../../internal/config/config.go#L45), [Compose](../../docker-compose.yml#L19)). The container initializes an all-table publication only on its first data-directory initialization ([mount](../../docker-compose.yml#L22), [SQL](../../scripts/init_pg.sql#L5)). No application-specific environment variables are required: configuration is through flags ([parser](../../internal/config/config.go#L41)).

In a second terminal, on the disposable demo database:

```bash
make demo
tail -f guide-events.jsonl
```

`make demo` creates and **truncates** `products`, then inserts, updates, deletes, and truncates again ([workload](../../scripts/demo.sh#L14)). It demonstrates live events; to see `read` events, existing rows must be present when a new slot is first created ([snapshot branch](../../cmd/cdc/main.go#L115)). Allow roughly ten seconds for ordinary streaming flushes; Ctrl-C requests shutdown ([timer](../../internal/replication/replication.go#L24), [signal handling](../../cmd/cdc/main.go#L42)).

```bash
make build
make test
make vet
```

These invoke the Go toolchain directly ([Makefile](../../Makefile#L12)). Unit tests cover decoding, helper logic, local files, and a local HTTP server; they do not exercise a live replication stream or Kafka broker ([test map](03-structure.md#test-coverage)). For Homebrew PostgreSQL, inspect the setup script first: it changes server settings and restarts the service, and the command needs an explicit `--dsn` matching that installation ([script](../../scripts/setup_pg.sh#L27), [default flags](../../internal/config/config.go#L45)).

## Reading order for this guide

1. [Architecture](01-architecture.md) — components, contracts, state, and deployment.
2. [Flow](02-flow.md) — startup, snapshot, capture, acknowledgements, and shutdown.
3. [Structure](03-structure.md) — every significant source file and its callers.
4. [Tech stack](04-tech-stack.md) — versions, dependencies, and local tooling.
5. [Decisions](05-decisions.md) — evidenced choices and newcomer gotchas.

## Open questions

- **Restart correctness:** the stream checkpoints `WALStart + len(WALData)` after each received payload, and the decoder does not expose a commit boundary. Is this position calculation and restart behavior valid across multi-message transactions? There is no replication test in this checkout; this guide does not establish an end-to-end delivery guarantee ([position](../../internal/replication/replication.go#L192), [commit handling](../../internal/decode/decode.go#L64)).
- **Interrupted initial copy:** how should a failed snapshot recover? An existing slot without a checkpoint skips the snapshot; there is no durable snapshot-in-progress/completed state ([startup branches](../../cmd/cdc/main.go#L105), [snapshot writes](../../internal/snapshot/snapshot.go#L88)).
- **Checkpoint identity and durability:** should checkpoints record server/slot/publication/sink identity, and should writes sync the file and directory? Today they contain only an LSN written with temporary-file rename ([store](../../internal/checkpoint/checkpoint.go#L23)); the path derives solely from `--output` ([wiring](../../cmd/cdc/main.go#L99)).
- **Destination guarantees:** what durable-acceptance contract must an HTTP endpoint provide, and which Kafka acknowledgement policy is required? HTTP trusts successful status responses; Kafka leaves acknowledgement settings to its library defaults; stdout only flushes a buffer ([HTTP](../../internal/sink/http.go#L43), [Kafka](../../internal/sink/kafka.go#L28), [stdout](../../internal/sink/stdout.go#L33)).
- **Documentation drift:** the root README's opening scope excludes features now implemented and lists Go 1.24+, while the module specifies Go 1.25.0 ([scope](../../README.md#L8), [requirements](../../README.md#L49), [module](../../go.mod#L3)). The current implementation is described here; the root README remains unchanged.
