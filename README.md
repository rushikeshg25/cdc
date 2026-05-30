# cdc — a Postgres Change Data Capture engine from scratch

A learning project: stream row-level changes (INSERT/UPDATE/DELETE) out of Postgres using
**logical replication** and write them to a JSONL file. This is the same mechanism Debezium
and `pg_recvlogical` use — we register ourselves with Postgres as a logical replica and let
it stream WAL changes to us.

**Scope:** capture engine only. No Kafka, no initial snapshot of existing rows, no durable
cross-restart checkpointing. We *do* implement the mandatory in-session LSN feedback that
keeps a replication slot healthy.

## Concepts

- **Replication connection** — a special Postgres connection mode (`replication=database`),
  *not* SQL. After the handshake the socket becomes a bidirectional `CopyBoth` stream.
  Commands: `IDENTIFY_SYSTEM`, `CREATE_REPLICATION_SLOT`, `START_REPLICATION`.
- **WAL / LSN** — the Write-Ahead Log records every change. An LSN (`X/Y` hex) is a byte
  position in the WAL. We track the LSN we've processed and report it back.
- **`pgoutput` plugin** — the built-in server-side output plugin that decodes raw WAL into
  structured messages: `Begin / Relation / Insert / Update / Delete / Truncate / Commit`.
  It only streams tables listed in a **publication**.
- **Replication slot** — a named, persistent server-side bookmark. Postgres won't recycle
  WAL past the slot's confirmed LSN, so a consumer can resume after a disconnect. Created
  once and reused.
- **Standby Status Update (LSN feedback)** — we must periodically tell Postgres the LSN
  we've flushed, or its WAL grows unbounded on disk. This is mandatory, not optional.
- **Relation message + cache** — tuple messages (`Insert`/`Update`/`Delete`) reference a
  table by a numeric relation ID and carry only column *values*. A preceding `Relation`
  message describes the schema (column names, type OIDs). We cache relations by ID to
  resolve names/types when an event arrives.
- **Replica identity** — controls what *old* row data appears in UPDATE/DELETE events.
  `DEFAULT` = primary key only; `FULL` = all old columns. Set demo tables to `FULL` to see
  full before-images: `ALTER TABLE t REPLICA IDENTITY FULL;`

## Data flow

```
Postgres WAL ──(pgoutput)──▶ replication (CopyBoth stream)
                                   │  XLogData
                                   ▼
                               decode (relation cache, pgoutput → ChangeEvent)
                                   │
                                   ▼
                               sink (JSONL file)
```

`replication` also answers keepalives and reports the flushed LSN back to Postgres.

## Requirements

- Go 1.24+
- PostgreSQL 17 (local, via Homebrew) with `wal_level=logical`

## Setup

Enable logical replication and create the dev database, publication, and slot:

```sh
./scripts/setup_pg.sh
```

This sets `wal_level=logical` (requires a Postgres restart) and creates a publication for
the tables we want to capture.

## Run

```sh
go run ./cmd/cdc --output events.jsonl
```

Then, in another terminal, run some changes against the database and watch `events.jsonl`.

On a fresh slot the engine first **snapshots** existing rows (as `read` events) consistently,
then streams live changes from the exact point the snapshot ended. Progress is checkpointed
to `<output>.offset`, so a restart **resumes** from there and skips the snapshot.

### Capture only some tables

Scope capture to a subset of tables with `--tables` (a publication `FOR TABLE` is managed
for you). Use a dedicated publication/slot name, since the default `cdc_pub` is `FOR ALL
TABLES` and can't be narrowed:

```sh
go run ./cmd/cdc --publication cdc_scoped --slot cdc_scoped_slot \
  --tables public.users,public.orders --output events.jsonl
```

## Sinks

Select the output with `--sink` (default `file`):

| `--sink`  | Flags | Behavior |
|-----------|-------|----------|
| `file`    | `--output events.jsonl` | Append JSONL, fsync on flush |
| `stdout`  | — | JSONL to stdout |
| `http`    | `--http-url URL` | POST a JSON array batch per flush |
| `kafka`   | `--kafka-brokers`, `--kafka-topic` | Publish per event |

All sinks honor the **at-least-once** invariant: the engine only advances the replication
LSN (and checkpoint) after the sink's `Flush()` durably persists the batch. A failing sink
keeps its buffer and stalls progress rather than dropping events (duplicates are possible
after a crash).

**Kafka routing:** messages are keyed by `schema.table` (hash-partitioned, so per-table
order is preserved). With no `--kafka-topic`, events route to `cdc.<schema>.<table>`; set
`--kafka-topic` to send everything to one topic.

```sh
# Kafka (start the broker first: make kafka-up)
go run ./cmd/cdc --sink kafka --kafka-brokers localhost:9092
make kafka-consume TOPIC=cdc.public.users

# HTTP
go run ./cmd/cdc --sink http --http-url http://localhost:8080/events
```

## Observability

Logs are structured (`log/slog`); `--verbose` switches to debug level.

Enable Prometheus metrics with `--metrics-addr`:

```sh
go run ./cmd/cdc --metrics-addr :9100
curl -s localhost:9100/metrics | grep '^cdc_'
```

| Metric | Type | Meaning |
|--------|------|---------|
| `cdc_events_total{op}` | counter | events emitted, by op (insert/update/delete/truncate/read) |
| `cdc_sink_errors_total` | counter | sink write/flush failures |
| `cdc_replication_lag_bytes` | gauge | server WAL end minus our processed position |

## Layout

```
cmd/cdc/             entrypoint: flags, signal handling, wiring
internal/config      flags: DSN, slot, publication, output, tables, sink, metrics
internal/replication connection, slot+snapshot export, CopyBoth loop, LSN feedback
internal/decode      pgoutput messages → ChangeEvent, relation cache
internal/event       ChangeEvent type + JSON schema
internal/snapshot    consistent initial copy (SET TRANSACTION SNAPSHOT)
internal/checkpoint  durable LSN store for resume across restarts
internal/publication manage a FOR TABLE publication (table filtering)
internal/sink        Sink interface + file/stdout/http/kafka implementations
internal/metrics     Prometheus metrics + /metrics server
scripts/             pg setup + demo workload
```
