# Decisions

These explain implemented choices, with rationale labeled as documented or inferred. They are not evidence that every intended guarantee has been verified; unresolved questions remain in the [index](README.md#open-questions).

## Direct logical replication in a small Go command

- **What:** connect through pgconn in replication mode and explicitly handle PostgreSQL messages.
- **Evidence:** [`Connect`](../../internal/replication/replication.go#L36), [`Stream`](../../internal/replication/replication.go#L111), and the [learning-project introduction](../../README.md#L3).
- **Why, apparently:** make the replication mechanism visible and learnable. This rationale is documented in the README.
- **Tradeoff:** small, inspectable modules expose the protocol clearly, but reconnect policy, resume correctness, transaction handling, and compatibility remain this implementation's responsibility.
- **Confidence:** learning purpose confirmed by documentation; maintenance implications inferred from the explicit loop.

## One event and sink contract for snapshot and stream

- **What:** both data paths emit `ChangeEvent` through `Sink.Write`; the command chooses exactly one sink.
- **Evidence:** [snapshot construction](../../internal/snapshot/snapshot.go#L115), [live construction](../../internal/decode/decode.go#L121), [sink interface](../../internal/sink/sink.go#L12), [factory](../../internal/sink/factory.go#L17).
- **Why, apparently:** inferred separation of row acquisition from destination delivery lets the same capture path target multiple outputs.
- **Tradeoff:** the interface is compact, but has no cancellation context, transaction envelope, or acknowledgement detail. Snapshot and live values use different conversion paths ([snapshot values](../../internal/snapshot/snapshot.go#L111), [live conversion](../../internal/decode/decode.go#L163)).
- **Confidence:** shared abstraction confirmed by code; rationale inferred.

## Flush before checkpoint before feedback

- **What:** `flushAndReport` flushes the sink, saves local progress, then reports progress to PostgreSQL.
- **Evidence:** [ordered helper](../../internal/replication/replication.go#L205), [file fsync](../../internal/sink/file.go#L40), [checkpoint replacement](../../internal/checkpoint/checkpoint.go#L41).
- **Why, apparently:** comments explicitly intend at-least-once delivery by avoiding acknowledgement of unflushed events.
- **Tradeoff:** destination acceptance followed by checkpoint failure can lead to replay/duplicates. Atomic file replacement is not a power-loss durability proof because neither checkpoint file nor directory is explicitly synced. Stdout and HTTP also require assumptions outside the interface to establish durable consumption ([stdout](../../internal/sink/stdout.go#L33), [HTTP acceptance](../../internal/sink/http.go#L43)).
- **Confidence:** ordering and intent confirmed; end-to-end delivery guarantee remains unverified, particularly the [position calculation](../../internal/replication/replication.go#L192).

## Export the initial snapshot from slot creation

- **What:** a fresh slot exports the view copied through a separate repeatable-read transaction before streaming starts.
- **Evidence:** [`EnsureSlot`](../../internal/replication/replication.go#L79), [snapshot import](../../internal/snapshot/snapshot.go#L72), [startup branch](../../cmd/cdc/main.go#L105).
- **Why, apparently:** comments explicitly connect the exported view to the slot's consistent point to align baseline and live capture.
- **Tradeoff:** the replication connection must remain idle until the snapshot has been imported/copied. Initial copying is serial, with no completion checkpoint or intra-copy flush. A failed run can leave an existing slot which the next run reuses without re-copying.
- **Confidence:** mechanism and intended boundary confirmed; crash-recovery semantics unresolved.

## Filter through PostgreSQL publications

- **What:** `--tables` modifies the publication, and the snapshot discovers tables from that publication.
- **Evidence:** [publication DDL](../../internal/publication/publication.go#L18), [table lookup](../../internal/snapshot/snapshot.go#L28), [startup ordering](../../cmd/cdc/main.go#L63).
- **Why, apparently:** inferred use of one server-side scope keeps baseline and stream selection aligned without a second client-side filter.
- **Tradeoff:** changing table selection mutates the publication. Existing `FOR ALL TABLES` publications cannot be narrowed with this `SET TABLE` path; use a dedicated publication, as the [root usage notes](../../README.md#L77) explain. Adding tables to an existing stream configuration does not trigger a new snapshot ([startup branches](../../cmd/cdc/main.go#L111)).
- **Confidence:** mechanism confirmed; rationale inferred.

## Keep decoder state small and testable

- **What:** relation messages populate a per-session cache; Begin carries metadata; Commit clears it; row messages emit immediately. A separate dispatcher can be tested with constructed messages.
- **Evidence:** [state and dispatcher](../../internal/decode/decode.go#L24), [tests](../../internal/decode/decode_test.go#L86).
- **Why, apparently:** the comment explicitly says the `handle` seam allows tests without a live server. Avoiding transaction buffering is an inference from the implementation, not a recorded design rationale.
- **Tradeoff:** unknown relations prevent normal row decoding, while truncate silently skips unknown tables. Consumers do not receive commit markers or atomic transaction batches ([dispatch](../../internal/decode/decode.go#L64)).
- **Confidence:** test seam confirmed by comment; broader design rationale inferred.

## Batch delivery and Kafka table keys

- **What:** HTTP batches event objects into one POST; Kafka batches individual JSON messages and hashes `schema.table`, optionally routing to `cdc.<schema>.<table>`.
- **Evidence:** [HTTP flush](../../internal/sink/http.go#L35), [Kafka writer and message](../../internal/sink/kafka.go#L28), [topic routing](../../internal/sink/kafka.go#L79).
- **Why, apparently:** Kafka comments explicitly aim to preserve per-table ordering; batching until flush is visible in code and consistent with the sink contract.
- **Tradeoff:** a hot table shares one key, limiting distribution across partitions. Application buffers have no size cap, and HTTP offers no per-event acknowledgement. The explicit Kafka retry loop handles only `UnknownTopicOrPartition`, with any additional retries delegated to the library ([flush](../../internal/sink/kafka.go#L49)).
- **Confidence:** key choice and buffering confirmed; throughput implications inferred.

## Gotchas

- **Output name is checkpoint identity even for remote sinks.** Changing `--sink`, broker, URL, DSN, or slot does not change `<output>.offset`; an unrelated checkpoint can silently select the resume branch. Use a distinct output/checkpoint path for each independent capture configuration ([wiring](../../cmd/cdc/main.go#L99)).
- **Default output has a tracked checkpoint.** The repository includes `events.jsonl.offset`, so the default run need not snapshot even with a newly created slot. Checkpoint presence wins over slot creation ([branch order](../../cmd/cdc/main.go#L111)); the [run instructions](README.md#run-it) use separate names.
- **Clean does not reset capture.** `make clean` removes only the selected output file after `go clean`; it does not remove its offset or drop the slot. Do not treat it as a fresh-start command ([target](../../Makefile#L27)).
- **Received position differs from event LSN.** Events carry `WALStart`; the saved position adds logical payload length. Commit messages are not used as checkpoint boundaries ([event stamping](../../internal/decode/decode.go#L121), [position](../../internal/replication/replication.go#L192)).
- **Counters do not mean durable delivery.** Event counts rise after `Write`, before flush; lag is based on the processed position. A sink may still hold those events in memory ([stream metrics](../../internal/replication/replication.go#L185)).
- **Partial row images are intentional.** NULL is present as JSON null, unchanged TOAST is absent, and replica identity determines available old values. Numeric, timestamp, UUID, and other types outside the explicit conversion cases remain strings in live events ([tuple mapping](../../internal/decode/decode.go#L140), [conversion](../../internal/decode/decode.go#L163)).
- **Stale comments describe earlier commits.** The `Stream` comment says it only logs messages, but the function now decodes and writes them. The tuple-mapping comment says conversion comes later, but `convert` already handles several types ([stream comment](../../internal/replication/replication.go#L102), [tuple comment](../../internal/decode/decode.go#L136)).
- **Docker setup starts Kafka too.** Both the setup script and `make docker-up` call unscoped `docker compose up -d`, despite PostgreSQL-focused names ([script](../../scripts/setup_pg_docker.sh#L12), [target](../../Makefile#L40)). The setup script prints the final health state but does not explicitly reject a nonhealthy result ([poll](../../scripts/setup_pg_docker.sh#L15)).
- **Demo name in the message is misleading.** The script prints `demo.products` but creates unqualified `products` without creating/selecting a `demo` schema; its actual schema follows the connection's search path ([demo](../../scripts/demo.sh#L14)).
- **Shutdown success is narrower than complete delivery.** The receive cancellation branch logs final feedback failure and returns nil; deferred sink close also only logs errors. Error classification checks timeout before cancellation ([receive error](../../internal/replication/replication.go#L143), [cleanup](../../cmd/cdc/main.go#L92)).

## Conventions

- Keep wiring in the command, protocol behavior in replication/decoder, and destination code behind `Sink`; this is the existing dependency boundary ([composition](../../cmd/cdc/main.go#L82), [interface](../../internal/sink/sink.go#L12)).
- Add context to returned errors with `%w`; let the command log terminal errors. Existing lifecycle/metrics failures that cannot be returned are logged locally ([connection errors](../../internal/replication/replication.go#L36), [main error](../../cmd/cdc/main.go#L34), [metrics failure](../../internal/metrics/metrics.go#L40)).
- Keep unit tests beside implementation, using temporary files, table cases, and constructed protocol messages where practical ([checkpoint tests](../../internal/checkpoint/checkpoint_test.go#L11), [conversion tests](../../internal/decode/decode_test.go#L14)). The current conventions do not replace the missing end-to-end recovery tests.
