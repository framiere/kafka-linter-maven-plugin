# Kafka Streams Anti-Pattern Catalog

This directory catalogs build-time anti-patterns that the Maven linter flags in code using `org.apache.kafka:kafka-streams`. Each rule has its own `STREAMS_*.md` file with TL;DR, mechanism, operational impact, fix, false positives, and detection strategy.

## What this catalog covers

Kafka Streams has a much larger surface area than `kafka-clients`: a stateful DSL, embedded RocksDB, internal topic management, two notions of time, and a state machine with rebalance interactions. The rules below cluster into seven themes:

1. **Topology DSL pitfalls** — operators that silently change semantics (`groupByKey` on null keys), spawn extra repartitions, or use deprecated APIs.
2. **Named resources** — auto-generated state-store / changelog names that break across deploys.
3. **Configuration anti-patterns** — defaults that are dangerous in production (`replication.factor=1`, `state.dir` on /tmp, missing exception handlers).
4. **State store sizing and tuning** — in-memory stores for unbounded data, missing RocksDB config setter, unbounded `Suppressed` buffers.
5. **Time semantics** — wall-clock vs stream-time confusion, non-deterministic processors.
6. **Lifecycle and threading** — single-thread topologies, missing shutdown hooks, dangerous `cleanUp()` calls.
7. **EOS v2 and the new (KIP-1071) group protocol** — interactions between `processing.guarantee`, `commit.interval.ms`, `transaction.timeout.ms`, `max.poll.interval.ms`, and the `streams` group protocol.

## Legend

**Severity**
- `ERROR` — should fail the build by default. The defect is almost certainly a bug.
- `WARNING` — should report but not fail by default. Sometimes legitimate; usually a bug.

**Confidence**
- `HIGH` — detection is mechanical; false positives are rare.
- `MEDIUM` — detection involves heuristics or context the linter can't always see.
- `CONTEXT` — only meaningful with extra context (topic partition count, environment, broker version).

**Detection** — `bytecode`, `config-file`, `dsl-chain`, or a combination (` + `, alphabetical).

A trailing 🤝 in the tagline means the rule's doc carries a `## Consult a friend?` block — read it before changing prod config.

## Catalog (47)

| Rule | Severity | Confidence | Detection | Tagline |
|---|---|---|---|---|
| [STREAMS_APP_ID_UNSTABLE](./STREAMS_APP_ID_UNSTABLE.md) | ERROR | MEDIUM | config-file | If your `application.id` shifts, your state is gone. |
| [STREAMS_CACHE_DISABLED](./STREAMS_CACHE_DISABLED.md) | WARNING | MEDIUM | config-file | Cache=0 makes brokers cry. Update rates explode without it. |
| [STREAMS_CLEANUP_IN_PROD](./STREAMS_CLEANUP_IN_PROD.md) | ERROR | HIGH | bytecode | `cleanUp()` is for tests. In prod it's `rm -rf` on your state. |
| [STREAMS_COMMIT_INTERVAL_TOO_LOW](./STREAMS_COMMIT_INTERVAL_TOO_LOW.md) | WARNING | MEDIUM | config-file | Committing every millisecond means the broker commits a million times a millisecond. 🤝 |
| [STREAMS_DEFAULT_DESERIALIZATION_HANDLER](./STREAMS_DEFAULT_DESERIALIZATION_HANDLER.md) | WARNING | HIGH | config-file | One poison pill kills the whole topology by default. |
| [STREAMS_DESERIALIZER_OBJECT_KEY](./STREAMS_DESERIALIZER_OBJECT_KEY.md) | WARNING | MEDIUM | bytecode | JSON keys don't hash the same on every JVM. Pick a stable representation. |
| [STREAMS_DESER_HANDLER_BLANKET_CONTINUE](./STREAMS_DESER_HANDLER_BLANKET_CONTINUE.md) | ERROR | HIGH | bytecode | A custom `DeserializationExceptionHandler` that always returns `CONTINUE` is data loss with a stack trace. 🤝 |
| [STREAMS_DOUBLE_REPARTITION](./STREAMS_DOUBLE_REPARTITION.md) | WARNING | MEDIUM | dsl-chain | Every `groupBy()` is a network round-trip. Two in a row is two too many. |
| [STREAMS_EOS_COMMIT_INTERVAL_SET](./STREAMS_EOS_COMMIT_INTERVAL_SET.md) | WARNING | HIGH | config-file | Setting `commit.interval.ms` under EOS overrides a correctness-critical default. 🤝 |
| [STREAMS_EOS_TRANSACTION_TIMEOUT_DEFAULT](./STREAMS_EOS_TRANSACTION_TIMEOUT_DEFAULT.md) | WARNING | MEDIUM | config-file | 10s `transaction.timeout.ms` under EOS is a state-wipe cascade waiting for the first slow record. 🤝 |
| [STREAMS_EOS_V1_DEPRECATED](./STREAMS_EOS_V1_DEPRECATED.md) | ERROR on Streams 4.x (won't start), WARNING on Streams 3.x (deprecated) | HIGH | config-file + pom-dependency | `exactly_once` (v1) is a per-task producer. `exactly_once_v2` is the only one you should pick. 🤝 |
| [STREAMS_EXPLICIT_INTERNAL_NAMING_DISABLED](./STREAMS_EXPLICIT_INTERNAL_NAMING_DISABLED.md) | WARNING | HIGH | config-file | `ensure.explicit.internal.resource.naming=true` is the one-line safety net for state identity. |
| [STREAMS_FOREACH_FOR_STATE](./STREAMS_FOREACH_FOR_STATE.md) | WARNING | MEDIUM | bytecode | `foreach` is a terminal sink. Don't put your state machine in there. |
| [STREAMS_GROUPBY_NULL_KEY](./STREAMS_GROUPBY_NULL_KEY.md) | WARNING | MEDIUM | dsl-chain | `groupByKey()` on null keys silently eats your records. |
| [STREAMS_GROUP_PROTOCOL_NOT_STREAMS](./STREAMS_GROUP_PROTOCOL_NOT_STREAMS.md) | WARNING | CONTEXT | config-file + pom-dependency | On AK 4.2+ / CP 8.2+, classic rebalancing is 50-80% slower for no reason. |
| [STREAMS_GROUP_PROTOCOL_STREAMS_INCOMPAT](./STREAMS_GROUP_PROTOCOL_STREAMS_INCOMPAT.md) | ERROR | HIGH | config-file | `group.protocol=streams` plus static membership is a `ConfigException` you can ship. 🤝 |
| [STREAMS_INMEMORY_STORE_UNBOUNDED](./STREAMS_INMEMORY_STORE_UNBOUNDED.md) | WARNING | MEDIUM | bytecode | In-memory stores have one knob: heap. |
| [STREAMS_INTERACTIVE_QUERY_BEFORE_RUNNING](./STREAMS_INTERACTIVE_QUERY_BEFORE_RUNNING.md) | WARNING | MEDIUM | bytecode | Querying a store before `RUNNING` is querying a closet that hasn't been built. |
| [STREAMS_JOIN_DEFAULT_SERDES](./STREAMS_JOIN_DEFAULT_SERDES.md) | WARNING | MEDIUM | dsl-chain | A join without explicit Serdes is a join with implicit assumptions. |
| [STREAMS_JOIN_WINDOW_LEGACY_API](./STREAMS_JOIN_WINDOW_LEGACY_API.md) | WARNING | HIGH | bytecode | `JoinWindows.of(...)` gives you a 24-hour grace period for free, whether you wanted it or not. |
| [STREAMS_KSTREAMS_MULTIPLE_START](./STREAMS_KSTREAMS_MULTIPLE_START.md) | ERROR | HIGH | bytecode | `KafkaStreams#start()` is one-shot. Calling it twice throws `IllegalStateException`. |
| [STREAMS_KTABLE_FILTER_SIDE_EFFECT](./STREAMS_KTABLE_FILTER_SIDE_EFFECT.md) | WARNING | MEDIUM | bytecode | `filter`/`mapValues` with side effects: rerun the changelog, rerun the side effect. |
| [STREAMS_KTABLE_REGROUP_BY_SAME_KEY](./STREAMS_KTABLE_REGROUP_BY_SAME_KEY.md) | WARNING | MEDIUM | dsl-chain | `toStream().groupByKey()` is a repartition you didn't need to pay for. |
| [STREAMS_MATERIALIZED_NULL_SERDES](./STREAMS_MATERIALIZED_NULL_SERDES.md) | WARNING | MEDIUM | bytecode | `Materialized.with(null, null)` says "trust me, the default serdes are right." They aren't. |
| [STREAMS_MAX_POLL_LT_TRANSACTION_TIMEOUT](./STREAMS_MAX_POLL_LT_TRANSACTION_TIMEOUT.md) | ERROR | HIGH | config-file | Under EOS, `max.poll.interval.ms` smaller than `transaction.timeout.ms` is a guaranteed cascade. |
| [STREAMS_NON_DETERMINISTIC_AGGREGATOR](./STREAMS_NON_DETERMINISTIC_AGGREGATOR.md) | WARNING | MEDIUM | bytecode | Aggregators with `Math.random()` give different answers on replay. That's a bug, not a feature. |
| [STREAMS_NO_SHUTDOWN_HOOK](./STREAMS_NO_SHUTDOWN_HOOK.md) | WARNING | MEDIUM | bytecode | Without a shutdown hook, you commit offsets via SIGKILL. 🤝 |
| [STREAMS_NUM_THREADS_ONE](./STREAMS_NUM_THREADS_ONE.md) | WARNING | MEDIUM | config-file | One stream thread is one heart attack from offline. |
| [STREAMS_NUM_THREADS_OVER_PARTITIONS](./STREAMS_NUM_THREADS_OVER_PARTITIONS.md) | WARNING | CONTEXT | annotation + config-file | Threads with nothing to do still cost you a heap. |
| [STREAMS_PROBING_REBALANCE_DEFAULT_LOOP](./STREAMS_PROBING_REBALANCE_DEFAULT_LOOP.md) | WARNING | CONTEXT | config-file + dsl-chain | Stateful app with standbys that never catch up → 10-minute rebalance loop forever. |
| [STREAMS_PROCESSING_EXCEPTION_HANDLER_MISSING](./STREAMS_PROCESSING_EXCEPTION_HANDLER_MISSING.md) | WARNING | HIGH | config-file | A KIP-1034 handler is the only thing between a lambda NPE and a fleet-wide restart loop. 🤝 |
| [STREAMS_PRODUCTION_HANDLER_MISSING](./STREAMS_PRODUCTION_HANDLER_MISSING.md) | WARNING | MEDIUM | bytecode + config-file | `default.production.exception.handler` unset means the first `RecordTooLargeException` kills the topology. 🤝 |
| [STREAMS_PROD_HANDLER_BLANKET_CONTINUE](./STREAMS_PROD_HANDLER_BLANKET_CONTINUE.md) | ERROR | HIGH | bytecode | A `ProductionExceptionHandler` that always returns `CONTINUE` swallows every produce failure — including the ones that mean the broker is gone. 🤝 |
| [STREAMS_PUNCTUATOR_WRONG_TIME](./STREAMS_PUNCTUATOR_WRONG_TIME.md) | WARNING | MEDIUM | bytecode | Wall-clock punctuators fire even when no data arrives. Stream-time punctuators don't. |
| [STREAMS_REPARTITION_RETENTION_SET](./STREAMS_REPARTITION_RETENTION_SET.md) | ERROR | HIGH | bytecode + config-file | Set retention on a repartition topic and the records you haven't processed yet vanish. |
| [STREAMS_REPLICATION_FACTOR_ONE](./STREAMS_REPLICATION_FACTOR_ONE.md) | ERROR | HIGH | config-file | `replication.factor=1` is a state store with an expiration date. 🤝 |
| [STREAMS_ROCKSDB_NO_CONFIG_SETTER](./STREAMS_ROCKSDB_NO_CONFIG_SETTER.md) | WARNING | MEDIUM | config-file | A `RocksDBConfigSetter` is the difference between tuned RocksDB and 64 MB of "default". |
| [STREAMS_STATE_DIR_TMP](./STREAMS_STATE_DIR_TMP.md) | ERROR | HIGH | config-file | RocksDB on /tmp is state-by-coincidence. |
| [STREAMS_SUPPRESS_NO_GRACE](./STREAMS_SUPPRESS_NO_GRACE.md) | WARNING | MEDIUM | dsl-chain | `suppress(untilWindowCloses)` without explicit grace is a 24-hour pause. |
| [STREAMS_SUPPRESS_UNBOUNDED_BUFFER](./STREAMS_SUPPRESS_UNBOUNDED_BUFFER.md) | ERROR | HIGH | bytecode | `Suppressed.BufferConfig.unbounded()` in production is a heap-OOM with a paper trail. |
| [STREAMS_THROUGH_DEPRECATED](./STREAMS_THROUGH_DEPRECATED.md) | WARNING | HIGH | bytecode | `through()` is dead. Long live `repartition()`. |
| [STREAMS_TOPOLOGY_OPTIMIZATION_DISABLED](./STREAMS_TOPOLOGY_OPTIMIZATION_DISABLED.md) | WARNING | MEDIUM | config-file | Default is `none`. You're leaving free CPU and bandwidth on the table. |
| [STREAMS_TRANSFORM_DEPRECATED](./STREAMS_TRANSFORM_DEPRECATED.md) | WARNING | HIGH | bytecode | Transformers are last decade's Processor. Use `process` / `processValues`. |
| [STREAMS_UNCAUGHT_HANDLER_DEPRECATED_API](./STREAMS_UNCAUGHT_HANDLER_DEPRECATED_API.md) | WARNING | HIGH | bytecode | `setUncaughtExceptionHandler(Thread.UncaughtExceptionHandler)` is a JDK API. Streams gave you a real one in KIP-671 — use it. 🤝 |
| [STREAMS_UNCAUGHT_HANDLER_MISSING](./STREAMS_UNCAUGHT_HANDLER_MISSING.md) | WARNING | MEDIUM | bytecode | No uncaught handler? Then your topology has one shape — `DEAD`. |
| [STREAMS_UNNAMED_MATERIALIZED](./STREAMS_UNNAMED_MATERIALIZED.md) | ERROR | HIGH | dsl-chain | An unnamed store is a store you'll lose on the next deploy. |
| [STREAMS_WALLCLOCK_TIMESTAMP_EXTRACTOR](./STREAMS_WALLCLOCK_TIMESTAMP_EXTRACTOR.md) | WARNING | HIGH | config-file | `WallclockTimestampExtractor` says "ignore the data's timestamp, use mine." |

## Cross-cutting themes

- **State identity is fragile**: at least four rules (`STREAMS_UNNAMED_MATERIALIZED`, `STREAMS_APP_ID_UNSTABLE`, `STREAMS_TOPOLOGY_OPTIMIZATION_DISABLED` enabling mid-life, `STREAMS_CLEANUP_IN_PROD`) ultimately cause the same operational disaster — silent state loss → full restore → empty aggregations during the restore window.
- **Deprecations carry context**: `STREAMS_THROUGH_DEPRECATED`, `STREAMS_TRANSFORM_DEPRECATED`, `STREAMS_JOIN_WINDOW_LEGACY_API`, `STREAMS_EOS_V1_DEPRECATED`, `STREAMS_UNCAUGHT_HANDLER_DEPRECATED_API` aren't just lint nits — the deprecated APIs encode pre-2026 semantics (24h grace, per-task producer, weak typing, `Thread.UncaughtExceptionHandler`) that are wrong by today's expectations.
- **Default config is calibrated for "works on my machine"**: replication=1, single thread, `/tmp`, no exception handlers, no shutdown hook, optimization off. Every one of those defaults is a production-grade footgun.
- **Error handlers should not silently swallow**: `STREAMS_DESER_HANDLER_BLANKET_CONTINUE` and `STREAMS_PROD_HANDLER_BLANKET_CONTINUE` are the same shape — a custom handler that returns `CONTINUE` unconditionally is silent data loss with no DLT trail.
- **Side effects vs replay**: at-least-once or exactly-once doesn't change Streams' replay-during-restore behavior. The same record is processed many times in a Streams instance's lifetime; non-pure operators are a category of bug, not an edge case.

## Detection complexity ranking

From easiest to hardest:

1. **Config-file rules** — direct property lookups. Trivial.
2. **Deprecated-API rules** — single `INVOKEINTERFACE`/`INVOKESTATIC` match. Trivial.
3. **DSL-chain rules** — pattern-match adjacent method invocations on the same `KStream`/`KTable` instance through ASM frame analysis. Moderate.
4. **Lambda-body analysis** — `STREAMS_NON_DETERMINISTIC_AGGREGATOR`, `STREAMS_KTABLE_FILTER_SIDE_EFFECT`, `STREAMS_DESERIALIZER_OBJECT_KEY`, the blanket-`CONTINUE` handler rules — require decompiling lambda `LambdaMetafactory` invokes and walking the synthetic method's bytecode. Hard.
5. **Combination rules** — `STREAMS_NUM_THREADS_OVER_PARTITIONS` needs topology metadata the linter doesn't have. Hardest; degrade to a coarse threshold.

## Related catalogs

- [`../kafka-clients/`](../kafka-clients/) — producer and consumer anti-patterns (independent of Streams).
- [`../spring-kafka/`](../spring-kafka/) — Spring for Apache Kafka anti-patterns.
- [`../quarkus-kafka/`](../quarkus-kafka/) — Quarkus / SmallRye Reactive Messaging anti-patterns.
- [`../good-practices/`](../good-practices/) — positive checks (`STREAMS_EOS_V2_ENABLED`, `STREAMS_NAMED_OPERATORS`, …).
