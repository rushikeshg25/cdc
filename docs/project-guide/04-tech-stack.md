# Tech stack

## Languages and runtimes

| Language or runtime | Version | Pinned at or evidence |
| --- | --- | --- |
| Go | Module language/toolchain baseline `1.25.0`; no separate `toolchain` directive. | [`go.mod:3`](../../go.mod#L3). The root README's 1.24+ requirement is stale. |
| Bash | No version pin. | Script shebangs, e.g. [`setup_pg.sh:1`](../../scripts/setup_pg.sh#L1); scripts use `set -euo pipefail`. |
| PostgreSQL SQL / logical replication | Development server major version 17. | [`docker-compose.yml:3`](../../docker-compose.yml#L3), [`setup_pg.sh:17`](../../scripts/setup_pg.sh#L17). |

## Frameworks and major libraries

There is no application framework. The command uses standard Go `flag`, `context`, `os/signal`, `log/slog`, `encoding/json`, and `net/http` directly ([flags](../../internal/config/config.go#L5), [entrypoint](../../cmd/cdc/main.go#L5), [HTTP sink](../../internal/sink/http.go#L3)).

| Library | Version and pin | Used for | Used in |
| --- | --- | --- | --- |
| `jackc/pglogrepl` | `v0.0.0-20260401131349-e37c41485510` — [`go.mod:6`](../../go.mod#L6) | Replication commands, LSN parsing/formatting, CopyData payload parsing, pgoutput decoding. | [`replication.go:83`](../../internal/replication/replication.go#L83), [`decode.go:42`](../../internal/decode/decode.go#L42), [`checkpoint.go:32`](../../internal/checkpoint/checkpoint.go#L32). |
| `jackc/pgx/v5` | `v5.9.2` — [`go.mod:7`](../../go.mod#L7) | Normal SQL connections/transactions; low-level pgconn replication socket; pgproto3 message types and pgtype OIDs. | [`snapshot.go:59`](../../internal/snapshot/snapshot.go#L59), [`publication.go:23`](../../internal/publication/publication.go#L23), [`replication.go:36`](../../internal/replication/replication.go#L36), [`decode.go:163`](../../internal/decode/decode.go#L163). |
| `prometheus/client_golang` | `v1.23.2` — [`go.mod:8`](../../go.mod#L8) | Auto-registered collectors, HTTP exposition, test helpers. | [`metrics.go:13`](../../internal/metrics/metrics.go#L13), [`metrics_test.go:6`](../../internal/metrics/metrics_test.go#L6). |
| `segmentio/kafka-go` | `v0.4.51` — [`go.mod:9`](../../go.mod#L9) | Synchronous message writes with hash balancing and auto-topic creation enabled. | [`kafka.go:28`](../../internal/sink/kafka.go#L28), [`kafka.go:60`](../../internal/sink/kafka.go#L60). |

Indirect modules are listed separately in [`go.mod:12`](../../go.mod#L12); they are not additional application-owned services. The replication library uses a pseudo-version pin, so its exact revision matters when checking protocol behavior.

## Data and infrastructure

| Service or storage | Role | Version/configuration evidence |
| --- | --- | --- |
| PostgreSQL | Source rows, publication, persistent replication slot and WAL. | Image `postgres:17`, logical WAL, sender and slot limits of 10 — [`docker-compose.yml:3`](../../docker-compose.yml#L3). Minor image version/digest is not pinned. |
| PostgreSQL named volume | Persist database state between ordinary container restarts. | `cdc_pgdata` mounted at the PostgreSQL data directory — [`docker-compose.yml:22`](../../docker-compose.yml#L22). |
| Kafka | Optional output destination; one broker/controller in KRaft mode. | Image `apache/kafka:3.8.0`, host port 9092, plaintext listeners and single-replica internal topics — [`docker-compose.yml:32`](../../docker-compose.yml#L32). No image digest or explicit volume. |
| Local filesystem | Append JSONL plus separately replaced LSN checkpoint. | [`file.go:20`](../../internal/sink/file.go#L20), [`checkpoint.go:41`](../../internal/checkpoint/checkpoint.go#L41). |
| HTTP endpoint | Optional external JSON-array receiver, not supplied by this repo. | `--http-url` — [`config.go:52`](../../internal/config/config.go#L52); 30-second client timeout — [`http.go:27`](../../internal/sink/http.go#L27). |
| Metrics endpoint | Optional Prometheus exposition; no scraper deployed here. | `--metrics-addr` — [`config.go:56`](../../internal/config/config.go#L56); `/metrics` — [`metrics.go:39`](../../internal/metrics/metrics.go#L39). |

## Tooling

| Tool | Role | Configured at |
| --- | --- | --- |
| Go build/run | Compile all packages or launch the command. | [`Makefile:12`](../../Makefile#L12). |
| Go tests | Standard `testing`; temporary files, constructed protocol messages, and HTTP test server. | [`Makefile:18`](../../Makefile#L18); [coverage map](03-structure.md#test-coverage). |
| `go vet` | Static checks across all packages. | [`Makefile:21`](../../Makefile#L21). |
| `go mod tidy` | Maintain dependency declarations. | [`Makefile:24`](../../Makefile#L24). |
| Make | Developer command aliases; no version pin. | [`Makefile:8`](../../Makefile#L8). |
| Docker Compose | Local database/broker services; no CLI version pin. | [`Makefile:34`](../../Makefile#L34), [`docker-compose.yml:1`](../../docker-compose.yml#L1). |
| Homebrew, `psql`, `pg_isready` | Alternate local PostgreSQL setup; Homebrew service and executable directory default to PostgreSQL 17. | [`setup_pg.sh:17`](../../scripts/setup_pg.sh#L17). |
| Kafka console consumer | Inspect emitted topics from inside the broker container. | [`Makefile:52`](../../Makefile#L52). |

## Notes

- The tracked repository has no CI workflow, Dockerfile for the Go command, release pipeline, or dedicated formatter/linter configuration. The checked-in automation is the [Makefile](../../Makefile#L1) and [scripts](03-structure.md#scripts).
- Compose and local defaults are a development topology, including a localhost Kafka advertisement; an application running in another container would require connection/listener changes ([Kafka environment](../../docker-compose.yml#L38), [CLI defaults](../../internal/config/config.go#L45)).
- PostgreSQL's built-in `pgoutput` uses protocol version 1 here; the application has no separately selected decoding plugin or protocol version flag ([constants](../../internal/replication/replication.go#L26), [stream arguments](../../internal/replication/replication.go#L113)).
