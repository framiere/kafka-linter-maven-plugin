# Kafka Streams Anti-Pattern Catalog

This directory catalogs build-time anti-patterns that a Maven linter should flag in code using `org.apache.kafka:kafka-streams`. Each rule has its own `STREAMS_*.md` file with TL;DR, mechanism, operational impact, fix, false positives, and detection strategy.

## What this catalog covers

Kafka Streams has a much larger surface area than `kafka-clients`: a stateful DSL, embedded RocksDB, internal topic management, two notions of time, and a state machine with rebalance interactions. The rules below cluster into six themes:

1. **Topology DSL pitfalls** — operators that silently change semantics (`groupByKey` on null keys), spawn extra repartitions, or use deprecated APIs.
2. **Named resources** — auto-generated state-store / changelog names that break across deploys.
3. **Configuration anti-patterns** — defaults that are dangerous in production (`replication.factor=1`, `state.dir` on /tmp, missing exception handlers).
4. **State store sizing & tuning** — in-memory stores for unbounded data, missing RocksDB config setter.
5. **Time semantics** — wall-clock vs stream-time confusion, non-deterministic processors.
6. **Lifecycle & threading** — single-thread topologies, missing shutdown hooks, dangerous `cleanUp()` calls.

## Severity legend

- **ERROR**: should fail the build by default. The defect is almost certainly a bug.
- **WARNING**: should report but not fail by default. Sometimes legitimate; usually a bug.

## Confidence legend

- **HIGH**: detection is mechanical; false positives are rare.
- **MEDIUM**: detection involves heuristics or context the linter can't always see.
- **CONTEXT**: the rule is only meaningful with extra context (e.g. topic partition count, environment).

## Summary table

| Rule ID | Severity | Confidence | Detection | One-liner |
| --- | --- | --- | --- | --- |
| [STREAMS_GROUPBY_NULL_KEY](STREAMS_GROUPBY_NULL_KEY.md) | WARNING | MEDIUM | dsl-chain | `groupByKey()` on a stream that can produce null keys silently drops records. |
| [STREAMS_UNNAMED_MATERIALIZED](STREAMS_UNNAMED_MATERIALIZED.md) | ERROR | HIGH | dsl-chain | Stateful operator without `Materialized.as("explicit-name")` → state lost on topology edit. |
| [STREAMS_DOUBLE_REPARTITION](STREAMS_DOUBLE_REPARTITION.md) | WARNING | MEDIUM | dsl-chain | Chained key-changers cause double repartitioning. |
| [STREAMS_THROUGH_DEPRECATED](STREAMS_THROUGH_DEPRECATED.md) | WARNING | HIGH | bytecode | `KStream.through()` deprecated (KIP-221) — use `repartition()`. |
| [STREAMS_TRANSFORM_DEPRECATED](STREAMS_TRANSFORM_DEPRECATED.md) | WARNING | HIGH | bytecode | `transform*` / `Transformer` deprecated (KIP-820) — use `process` / `FixedKeyProcessor`. |
| [STREAMS_EOS_V1_DEPRECATED](STREAMS_EOS_V1_DEPRECATED.md) | ERROR | HIGH | config-file | `processing.guarantee=exactly_once` / `exactly_once_beta` deprecated (KIP-732) — use `exactly_once_v2`. |
| [STREAMS_NUM_THREADS_ONE](STREAMS_NUM_THREADS_ONE.md) | WARNING | MEDIUM | config-file | `num.stream.threads=1` is a single point of failure for production. |
| [STREAMS_NUM_THREADS_OVER_PARTITIONS](STREAMS_NUM_THREADS_OVER_PARTITIONS.md) | WARNING | MEDIUM | combination | `num.stream.threads` > input partition count → idle threads. |
| [STREAMS_REPLICATION_FACTOR_ONE](STREAMS_REPLICATION_FACTOR_ONE.md) | ERROR | HIGH | config-file | `replication.factor=1` on internal topics → state loss on any broker failure. |
| [STREAMS_STATE_DIR_TMP](STREAMS_STATE_DIR_TMP.md) | ERROR | HIGH | config-file | `state.dir` under `/tmp` (also the default!) → state wiped on reboot. |
| [STREAMS_APP_ID_UNSTABLE](STREAMS_APP_ID_UNSTABLE.md) | ERROR | MEDIUM | config-file | `application.id` derived from hostname / UUID / version → state lost on every deploy. |
| [STREAMS_CACHE_DISABLED](STREAMS_CACHE_DISABLED.md) | WARNING | MEDIUM | config-file | `statestore.cache.max.bytes=0` floods brokers with intermediate updates. |
| [STREAMS_COMMIT_INTERVAL_TOO_LOW](STREAMS_COMMIT_INTERVAL_TOO_LOW.md) | WARNING | MEDIUM | config-file | `commit.interval.ms < 100` saturates `__consumer_offsets` and (for EOS) `__transaction_state`. |
| [STREAMS_DEFAULT_DESERIALIZATION_HANDLER](STREAMS_DEFAULT_DESERIALIZATION_HANDLER.md) | WARNING | HIGH | config-file | No deserialization exception handler set → poison pill takes down the topology. |
| [STREAMS_DEFAULT_PRODUCTION_HANDLER](STREAMS_DEFAULT_PRODUCTION_HANDLER.md) | WARNING | MEDIUM | config-file | No production exception handler → `RecordTooLargeException` kills stream thread. |
| [STREAMS_UNCAUGHT_HANDLER_MISSING](STREAMS_UNCAUGHT_HANDLER_MISSING.md) | WARNING | MEDIUM | bytecode | No `StreamsUncaughtExceptionHandler` → default `SHUTDOWN_CLIENT` on any error. |
| [STREAMS_TOPOLOGY_OPTIMIZATION_DISABLED](STREAMS_TOPOLOGY_OPTIMIZATION_DISABLED.md) | WARNING | MEDIUM | config-file | `topology.optimization=none` (default) leaves source-KTable and repartition merges on the table. |
| [STREAMS_JOIN_WINDOW_LEGACY_API](STREAMS_JOIN_WINDOW_LEGACY_API.md) | WARNING | HIGH | bytecode | `JoinWindows.of` / `TimeWindows.of` have a hidden 24h grace (deprecated, KIP-633). |
| [STREAMS_SUPPRESS_NO_GRACE](STREAMS_SUPPRESS_NO_GRACE.md) | WARNING | HIGH | dsl-chain | `Suppressed.untilWindowCloses` on legacy windows = 24h emit delay. |
| [STREAMS_JOIN_DEFAULT_SERDES](STREAMS_JOIN_DEFAULT_SERDES.md) | WARNING | MEDIUM | dsl-chain | `KStream#join` / KTable foreign-key join without explicit Serdes. |
| [STREAMS_INMEMORY_STORE_UNBOUNDED](STREAMS_INMEMORY_STORE_UNBOUNDED.md) | WARNING | MEDIUM | bytecode | `Stores.inMemoryKeyValueStore` for large datasets → OOM. |
| [STREAMS_ROCKSDB_NO_CONFIG_SETTER](STREAMS_ROCKSDB_NO_CONFIG_SETTER.md) | WARNING | MEDIUM | config-file | No `RocksDBConfigSetter` → unbounded block cache contention. |
| [STREAMS_PUNCTUATOR_WRONG_TIME](STREAMS_PUNCTUATOR_WRONG_TIME.md) | WARNING | MEDIUM | bytecode | Punctuator `PunctuationType` mismatched to intent; or `System.currentTimeMillis()` in a processor. |
| [STREAMS_WALLCLOCK_TIMESTAMP_EXTRACTOR](STREAMS_WALLCLOCK_TIMESTAMP_EXTRACTOR.md) | WARNING | HIGH | config-file | `WallclockTimestampExtractor` breaks event-time correctness and replay determinism. |
| [STREAMS_NON_DETERMINISTIC_AGGREGATOR](STREAMS_NON_DETERMINISTIC_AGGREGATOR.md) | WARNING | MEDIUM | bytecode | Aggregator/Reducer/Predicate uses `Math.random()` / `System.currentTimeMillis()` → non-deterministic state. |
| [STREAMS_INTERACTIVE_QUERY_BEFORE_RUNNING](STREAMS_INTERACTIVE_QUERY_BEFORE_RUNNING.md) | WARNING | MEDIUM | bytecode | IQ endpoint calls `KafkaStreams.store(...)` without checking `State == RUNNING`. |
| [STREAMS_KSTREAMS_MULTIPLE_START](STREAMS_KSTREAMS_MULTIPLE_START.md) | ERROR | HIGH | bytecode | `KafkaStreams.start()` called twice; or builder mutated after `build()`. |
| [STREAMS_NO_SHUTDOWN_HOOK](STREAMS_NO_SHUTDOWN_HOOK.md) | WARNING | MEDIUM | bytecode | `start()` without a corresponding `close()` in a shutdown hook. |
| [STREAMS_CLEANUP_IN_PROD](STREAMS_CLEANUP_IN_PROD.md) | ERROR | HIGH | bytecode | `KafkaStreams.cleanUp()` called outside test code. |
| [STREAMS_KTABLE_FILTER_SIDE_EFFECT](STREAMS_KTABLE_FILTER_SIDE_EFFECT.md) | WARNING | MEDIUM | bytecode | `filter`/`mapValues`/`peek` lambda performs external I/O — replays duplicate the side effect. |
| [STREAMS_FOREACH_FOR_STATE](STREAMS_FOREACH_FOR_STATE.md) | WARNING | MEDIUM | bytecode | `foreach` lambda owns mutable state — use Processor API + state store. |
| [STREAMS_DESERIALIZER_OBJECT_KEY](STREAMS_DESERIALIZER_OBJECT_KEY.md) | WARNING | MEDIUM | bytecode | Non-deterministic key encoding (JSON object as key) breaks co-partitioning. |
| [STREAMS_KTABLE_REGROUP_BY_SAME_KEY](STREAMS_KTABLE_REGROUP_BY_SAME_KEY.md) | WARNING | HIGH | dsl-chain | `KTable.toStream().groupByKey()` triggers a useless repartition. |
| [STREAMS_MATERIALIZED_NULL_SERDES](STREAMS_MATERIALIZED_NULL_SERDES.md) | WARNING | MEDIUM | bytecode | `Materialized.with(null, null)` (or missing explicit serdes) relies on global defaults. |
| [STREAMS_GROUP_PROTOCOL_NOT_STREAMS](STREAMS_GROUP_PROTOCOL_NOT_STREAMS.md) | WARNING | CONTEXT | config-file | Not opting in to `group.protocol=streams` on AK 4.2+ / CP 8.2+ (KIP-1071) — slower rebalances than necessary. |
| [STREAMS_GROUP_PROTOCOL_STREAMS_INCOMPAT](STREAMS_GROUP_PROTOCOL_STREAMS_INCOMPAT.md) | ERROR | HIGH | config-file | `group.protocol=streams` combined with `group.instance.id` / regex subscriptions / standby replicas — runtime crash. |
| [STREAMS_EXPLICIT_INTERNAL_NAMING_DISABLED](STREAMS_EXPLICIT_INTERNAL_NAMING_DISABLED.md) | WARNING | HIGH | config-file | `ensure.explicit.internal.resource.naming=true` missing — auto-generated changelog names will drift on topology edits. |
| [STREAMS_PROCESSING_EXCEPTION_HANDLER_MISSING](STREAMS_PROCESSING_EXCEPTION_HANDLER_MISSING.md) | WARNING | HIGH | config-file | KIP-1034 processing-exception handler not set — any uncaught exception in `process`/`mapValues` kills the thread. |
| [STREAMS_EOS_COMMIT_INTERVAL_SET](STREAMS_EOS_COMMIT_INTERVAL_SET.md) | WARNING | HIGH | config-file | EOS v2 with an explicit `commit.interval.ms` — overrides the EOS-safe 100ms default and inflates txn overhead. |
| [STREAMS_EOS_TRANSACTION_TIMEOUT_DEFAULT](STREAMS_EOS_TRANSACTION_TIMEOUT_DEFAULT.md) | WARNING | MEDIUM | config-file | EOS v2 with default 10s `transaction.timeout.ms` — long pauses (GC, rebalance) abort transactions. |
| [STREAMS_MAX_POLL_LT_TRANSACTION_TIMEOUT](STREAMS_MAX_POLL_LT_TRANSACTION_TIMEOUT.md) | ERROR | HIGH | config-file | `max.poll.interval.ms < transaction.timeout.ms` under EOS — guaranteed abort if a single record is slow. |
| [STREAMS_REPARTITION_RETENTION_SET](STREAMS_REPARTITION_RETENTION_SET.md) | ERROR | HIGH | bytecode | Setting `retention.ms` on a `*-repartition` topic violates Streams' "ephemeral, purged" contract — silent data loss. |
| [STREAMS_PROBING_REBALANCE_DEFAULT_LOOP](STREAMS_PROBING_REBALANCE_DEFAULT_LOOP.md) | WARNING | CONTEXT | config-file | Default `probing.rebalance.interval.ms=600000` causes 10-min loops while standby tasks warm — tune for the workload. |
| [STREAMS_SUPPRESS_UNBOUNDED_BUFFER](STREAMS_SUPPRESS_UNBOUNDED_BUFFER.md) | ERROR | HIGH | bytecode | `Suppressed.unboundedSuppression()` / `BufferConfig.unbounded()` retains every window in heap → OOM. |

## Cross-cutting themes

- **State identity is fragile**: at least four rules (`STREAMS_UNNAMED_MATERIALIZED`, `STREAMS_APP_ID_UNSTABLE`, `STREAMS_TOPOLOGY_OPTIMIZATION_DISABLED` enabling mid-life, `STREAMS_CLEANUP_IN_PROD`) ultimately cause the same operational disaster — silent state loss → full restore → empty aggregations during the restore window.
- **Deprecations carry context**: `STREAMS_THROUGH_DEPRECATED`, `STREAMS_TRANSFORM_DEPRECATED`, `STREAMS_JOIN_WINDOW_LEGACY_API`, `STREAMS_EOS_V1_DEPRECATED` aren't just lint nits — the deprecated APIs encode pre-2026 semantics (24h grace, per-task producer, weak typing) that are wrong by today's expectations.
- **Default config is calibrated for "works on my machine"**: replication=1, single thread, /tmp, no exception handlers, no shutdown hook, optimization off. Every one of those defaults is a production-grade footgun.
- **Side effects vs replay**: at-least-once or exactly-once doesn't change Streams' replay-during-restore behavior. The same record is processed many times in a Streams instance's lifetime; non-pure operators are a category of bug, not an edge case.

## Detection complexity ranking

From easiest to hardest:

1. **Config-file rules** — direct property lookups. Trivial.
2. **Deprecated-API rules** — single `INVOKEINTERFACE/STATIC` match. Trivial.
3. **DSL-chain rules** — pattern-match adjacent method invocations on the same `KStream`/`KTable` instance through ASM frame analysis. Moderate.
4. **Lambda-body analysis** — `STREAMS_NON_DETERMINISTIC_AGGREGATOR`, `STREAMS_KTABLE_FILTER_SIDE_EFFECT`, `STREAMS_DESERIALIZER_OBJECT_KEY` require decompiling lambda `LambdaMetafactory` invokes and walking the synthetic method's bytecode. Hard.
5. **Combination rules** — `STREAMS_NUM_THREADS_OVER_PARTITIONS` needs topology metadata the linter doesn't have. Hardest; degrade to a coarse threshold.

## Related catalogs

- [`../kafka-clients/`](../kafka-clients/) — producer & consumer anti-patterns (independent of Streams).
- [`../spring-kafka/`](../spring-kafka/) — Spring for Apache Kafka anti-patterns.
- [`../quarkus-kafka/`](../quarkus-kafka/) — Quarkus / SmallRye Reactive Messaging anti-patterns.
