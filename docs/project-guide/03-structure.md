# Structure

## What lives where

The command is a composition root; `internal/replication` drives execution, `internal/decode` supplies protocol interpretation, and `internal/sink` owns destination behavior. Snapshot and publication use separate SQL connections; checkpoint and metrics support the lifecycle. The file map below covers every tracked Go source/test, script, and meaningful build/deployment file at the documented commit ([command wiring](../../cmd/cdc/main.go#L40)).

```text
cmd/cdc/                 CLI composition root
internal/
  config/                flags
  publication/           table-scoped publication DDL
  replication/           replication protocol and feedback loop
  snapshot/              initial SQL copy
  decode/                relation cache and tuple decoding
  event/                 shared event envelope
  sink/                  file, stdout, HTTP, Kafka destinations
  checkpoint/            local LSN file
  metrics/               Prometheus counters and endpoint
scripts/                 local services and demo workload
docs/project-guide/      this six-file guide
```

## Root

| File | Responsibility | Key exports or commands | Called by |
| --- | --- | --- | --- |
| [`go.mod:1`](../../go.mod#L1) | Module identity, Go version, dependencies. | Module `github.com/rushikeshg25/cdc`. | Go toolchain. |
| [`Makefile:8`](../../Makefile#L8) | Local build/test/vet, service setup, demo, cleanup, Kafka consumption. | `build`, `run`, `test`, `vet`, `setup-docker`, `demo`, `kafka-up`, `kafka-consume`. | Developer shell. |
| [`docker-compose.yml:1`](../../docker-compose.yml#L1) | PostgreSQL logical replication and single-node Kafka topology. | Services `postgres`, `kafka`; volume `cdc_pgdata`. | Docker Compose and setup script. |
| [`README.md:1`](../../README.md#L1) | Original conceptual introduction and usage; contains [known drift](README.md#open-questions). | Human-facing instructions. | Maintainers. |

## Command

Folder: [`cmd/cdc/`](../../cmd/cdc/).

| File | Responsibility | Key exports or functions | Called by |
| --- | --- | --- | --- |
| [`main.go:21`](../../cmd/cdc/main.go#L21) | Flags/logging, signals, connection, publication/slot/sink, resume choice, cleanup. | `main`, `run` (unexported). | Go executable entrypoint. |

## Configuration

Folder: [`internal/config/`](../../internal/config/).

| File | Responsibility | Key exports or functions | Called by |
| --- | --- | --- | --- |
| [`config.go:11`](../../internal/config/config.go#L11) | Parse CLI options and comma-separated lists. | `Config`, `Parse`; internal `splitCSV`. | `main`. |
| [`config_test.go:8`](../../internal/config/config_test.go#L8) | Defaults, overrides, table list trimming, unknown flags. | `TestParse*`. | `go test`. |

## Publication

Folder: [`internal/publication/`](../../internal/publication/).

| File | Responsibility | Key exports or functions | Called by |
| --- | --- | --- | --- |
| [`publication.go:18`](../../internal/publication/publication.go#L18) | Optional create/alter publication; quote schema/table names. | `Ensure`; internal `quoteTables`. | `run`. |
| [`publication_test.go:5`](../../internal/publication/publication_test.go#L5) | Identifier quoting and invalid table names. | `TestQuoteTables*`. | `go test`. |

## Replication

Folder: [`internal/replication/`](../../internal/replication/).

| File | Responsibility | Key exports or functions | Called by |
| --- | --- | --- | --- |
| [`replication.go:36`](../../internal/replication/replication.go#L36) | Replication connection, server identification, exported slot snapshot, CopyData receive loop and feedback. | `Connect`, `IdentifySystem`, `SlotInfo`, `EnsureSlot`, `Stream`; internal `flushAndReport`. | `run`; stream invokes decoder/sink/metrics and checkpoint callback. |

## Snapshot

Folder: [`internal/snapshot/`](../../internal/snapshot/).

| File | Responsibility | Key exports or functions | Called by |
| --- | --- | --- | --- |
| [`snapshot.go:21`](../../internal/snapshot/snapshot.go#L21) | Discover publication tables, import snapshot, select rows and emit `read`. | `Table`, `ListPublicationTables`, `Run`; internal `copyTable`, `fieldNames`, `rowToMap`, `quoteLiteral`. | `run` for a fresh slot without checkpoint. |
| [`snapshot_test.go:10`](../../internal/snapshot/snapshot_test.go#L10) | Row/name mapping, short-value handling, SQL literal escaping. | `TestRowToMap*`, `TestFieldNames`, `TestQuoteLiteral`. | `go test`; no database. |

## Decoder

Folder: [`internal/decode/`](../../internal/decode/).

| File | Responsibility | Key exports or functions | Called by |
| --- | --- | --- | --- |
| [`decode.go:26`](../../internal/decode/decode.go#L26) | Parse pgoutput, track relations/transaction context, convert tuples to events. | `Decoder`, `New`, `Process`; internal `handle`, `newEvent`, `tupleToMap`, `convert`. | Replication `Stream`. |
| [`decode_test.go:14`](../../internal/decode/decode_test.go#L14) | Typed values, NULL/TOAST, constructed insert/update/delete/truncate messages, unknown relations. | `TestConvert`, `TestTupleToMap*`, `TestHandle*`; message helpers. | `go test`; calls internal dispatcher directly. |

## Event model

Folder: [`internal/event/`](../../internal/event/).

| File | Responsibility | Key exports or functions | Called by |
| --- | --- | --- | --- |
| [`event.go:7`](../../internal/event/event.go#L7) | Shared operation enum and JSON-tagged change envelope. | `Op`, `OpInsert`, `OpUpdate`, `OpDelete`, `OpTruncate`, `OpRead`, `ChangeEvent`. | Decoder, snapshot, sinks and their tests. |

## Sinks

Folder: [`internal/sink/`](../../internal/sink/).

| File | Responsibility | Key exports or functions | Called by |
| --- | --- | --- | --- |
| [`sink.go:12`](../../internal/sink/sink.go#L12) | Write/flush/close contract. | `Sink`. | Replication and snapshot. |
| [`factory.go:6`](../../internal/sink/factory.go#L6) | Select implementation and reject unknown kind. | `Options`, `New`. | `run`. |
| [`file.go:14`](../../internal/sink/file.go#L14) | Buffered append-only JSONL, flush plus fsync. | `File`, `NewFile`, `Write`, `Flush`, `Close`. | Factory, snapshot, stream, cleanup. |
| [`file_test.go:13`](../../internal/sink/file_test.go#L13) | JSONL output and append across opens. | `TestFileSinkWritesJSONL`, `TestFileSinkAppends`. | `go test` with temporary files. |
| [`stdout.go:13`](../../internal/sink/stdout.go#L13) | Buffered stdout JSONL. | `Stdout`, `NewStdout`, `Write`, `Flush`, `Close`. | Factory and `Sink` callers. |
| [`http.go:16`](../../internal/sink/http.go#L16) | Buffer event objects; POST array on flush. | `HTTP`, `NewHTTP`, `Write`, `Flush`, `Close`. | Factory and `Sink` callers. |
| [`http_test.go:14`](../../internal/sink/http_test.go#L14) | Batch delivery, retained buffer after HTTP 500, required URL. | `TestHTTPSink*`, `TestNewHTTPRequiresURL`. | `go test` with `httptest` server. |
| [`kafka.go:17`](../../internal/sink/kafka.go#L17) | Buffer keyed JSON messages, route topics, publish with limited metadata retry. | `Kafka`, `NewKafka`, `Write`, `Flush`, `Close`; internal `topicFor`. | Factory and `Sink` callers. |
| [`kafka_test.go:9`](../../internal/sink/kafka_test.go#L9) | Topic routing and missing broker validation. | `TestTopicFor`, `TestNewKafkaRequiresBrokers`. | `go test`; no broker. |

## Checkpoint

Folder: [`internal/checkpoint/`](../../internal/checkpoint/).

| File | Responsibility | Key exports or functions | Called by |
| --- | --- | --- | --- |
| [`checkpoint.go:16`](../../internal/checkpoint/checkpoint.go#L16) | Read one LSN; replace checkpoint via temporary file/rename. | `Store`, `New`, `Load`, `Save`. | `run`; `Save` passed into stream. |
| [`checkpoint_test.go:11`](../../internal/checkpoint/checkpoint_test.go#L11) | Missing file, save/load, overwrite and temporary-file removal. | `TestLoadMissing`, `TestSaveLoadRoundTrip`, `TestSaveOverwrites`. | `go test`. |

## Metrics

Folder: [`internal/metrics/`](../../internal/metrics/).

| File | Responsibility | Key exports or functions | Called by |
| --- | --- | --- | --- |
| [`metrics.go:13`](../../internal/metrics/metrics.go#L13) | Auto-register collectors; optionally start `/metrics`. | `EventsTotal`, `SinkErrorsTotal`, `ReplicationLagBytes`, `Serve`. | Command, replication, snapshot. |
| [`metrics_test.go:9`](../../internal/metrics/metrics_test.go#L9) | Counter increments and lag gauge assignment. | `TestCountersIncrement`, `TestLagGauge`. | `go test`. |

## Scripts

Folder: [`scripts/`](../../scripts/).

| File | Responsibility | Key exports or commands | Called by |
| --- | --- | --- | --- |
| [`setup_pg.sh:17`](../../scripts/setup_pg.sh#L17) | Homebrew PostgreSQL settings/restart, database and publication creation. | Environment overrides `PG_SERVICE`, `PGBIN`, `DB_NAME`, `PUBLICATION`, `ADMIN_DB`. | `make setup-pg` or shell. |
| [`setup_pg_docker.sh:11`](../../scripts/setup_pg_docker.sh#L11) | Start Compose services and poll PostgreSQL health. | `docker compose up -d`, `docker inspect`. | `make setup-docker` or shell. |
| [`init_pg.sql:5`](../../scripts/init_pg.sql#L5) | Create default all-table publication on first database initialization. | `CREATE PUBLICATION`. | PostgreSQL container entrypoint. |
| [`demo.sh:10`](../../scripts/demo.sh#L10) | Create/reset products and drive row changes. | `PSQL` override, internal shell `run`. | `make demo` or shell. |

## Test coverage

All tests sit beside their implementation and use Go's `testing` package. Decoder tests deliberately bypass wire parsing through `handle` ([seam](../../internal/decode/decode.go#L50)); snapshot/publication tests cover helpers rather than SQL execution ([snapshot tests](../../internal/snapshot/snapshot_test.go#L10), [publication tests](../../internal/publication/publication_test.go#L5)). HTTP tests use a real loopback test server, file/checkpoint tests use temporary files, and Kafka tests cover routing only ([HTTP](../../internal/sink/http_test.go#L19), [file](../../internal/sink/file_test.go#L14), [checkpoint](../../internal/checkpoint/checkpoint_test.go#L12), [Kafka](../../internal/sink/kafka_test.go#L9)).

The tracked test inventory has no command, replication, or stdout-specific tests and no end-to-end snapshot/restart suite. Passing these unit tests does not establish recovery correctness; see [open questions](README.md#open-questions).

## Excluded

`go.sum` and `.gitignore` are dependency bookkeeping and boilerplate. Runtime `events.jsonl` and the tracked `events.jsonl.offset` are not source or fixtures; their values were not read for this guide. The six files in this directory describe the implementation rather than adding another runtime component.
