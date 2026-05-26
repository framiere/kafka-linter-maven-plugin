# Catalog — kafka-linter-maven-plugin

This file is a triage table for the linter's rule catalog: each row carries a rule's severity, confidence, detection strategy, and the one-line tagline from its doc header. The plugin currently registers **746 rules across 10 categories** (`kafka-clients`, `spring-kafka`, `kafka-streams`, `kafka-connect`, `quarkus-kafka`, `security`, `versions`, `observability`, `schema-registry`, `quarkus`). The authoritative source of registered rule IDs is `src/main/java/io/conductor/kafkalinter/RuleId.java` — the rows below cover the original curated subset and trail the registry; treat the registry as truth and this table as a guided tour. The editorial intent is strict: every rule traces to a real production failure mode (data loss, silent footgun, EOL/CVE exposure, observability gap, or the absence of a well-known production default), the doc explains the mechanism without hand-waving, and the linter's behaviour is what an operator-of-Kafka with five years of pager experience would expect. Rules where the obvious fix has a non-obvious correctness cost carry a 🤝 glyph — those are the ones to read carefully before changing prod config.

## Conventions

### Severity

- `ERROR` — production data loss, runtime defect that will fire, or an unmistakable broken invariant. Fix before shipping.
- `WARNING` — silent footgun, observability gap, or correctness risk under load.
- `INFO` — best-practice nudge; not a defect.

For some rules severity is **conditional** (e.g. `ERROR for acks=0, WARNING for acks=1`). The body of the rule explains the condition; the table cell shows the strongest applicable level.

### Confidence

- `HIGH` — descriptor-level match. Very few false positives.
- `MEDIUM` — heuristic detection or cross-method bytecode walk. Requires human review.
- `CONTEXT` — depends on environment / runtime signals the linter cannot see (e.g. broker version, scrape configuration, deployment shape). False-positive rate is intent-dependent.

### Detection strategies (atomic)

- `bytecode` — ASM-level inspection of compiled `.class` files.
- `annotation` — annotation presence (`@KafkaListener`, `@Incoming`, `@AvroGenerated`, …).
- `config-file` — scan `application.yml`/`application.properties`, `bootstrap.yml`, etc.
- `pom-dependency` — `pom.xml` dependency presence / absence / version range.
- `pom-property` — `pom.xml` `<properties>` value inspection.
- `dsl-chain` — Kafka Streams DSL builder walk (semantically distinct from generic bytecode).

Multi-strategy detection joins strategies with ` + ` in alphabetical order (e.g. `bytecode + config-file`).

### 🤝 Consult a friend?

Rules where the "obvious fix" has a correctness cost that's easy to miss carry a `## Consult a friend?` block in the doc. These are EOS / transaction / atomicity / isolation territory. The block lists the questions a reviewer should ask before the fix, not a checklist.

## How to read this table

Each row is `(ID, Severity, Confidence, Detection, Tagline)`. The ID links to the rule doc. The tagline is copied verbatim from the doc's `**Tagline**:` line. A trailing 🤝 means the doc carries a `## Consult a friend?` block.

## Categories

| Category | Count | Scope |
|---|---|---|
| [kafka-clients](kafka-clients/_INDEX.md) | 40 | Plain Apache Kafka producers and consumers — the lowest-level surface. |
| [kafka-streams](kafka-streams/_INDEX.md) | 47 | Kafka Streams DSL, processor API, state stores, EOS v2. |
| [spring-kafka](spring-kafka/_INDEX.md) | 32 | Spring for Apache Kafka — `@KafkaListener`, `KafkaTemplate`, error handlers, `ErrorHandlingDeserializer`. |
| [quarkus-kafka](quarkus-kafka/_INDEX.md) | 36 | Quarkus + SmallRye Reactive Messaging — `@Incoming`/`@Outgoing`, `Emitter`, channel config. |
| [observability](observability/_INDEX.md) | 14 | Cross-cutting concerns: interceptors, metric reporters, OTel, deserialization safety (CVE-2023-34040), lambda hygiene, async/reactive bridge. |
| [schema-registry](schema-registry/_INDEX.md) | 7 | Schema Registry serde configuration, Avro 1.12+ logical-type Java mappings. |
| [warpstream](warpstream/_INDEX.md) | 4 | WarpStream-specific tuning — agent-aware client config overrides. |
| [versions](versions/_INDEX.md) | 28 | Library-version, EOL, BOM-drift, and CVE rules — pom-level only. |
| [good-practices](good-practices/_INDEX.md) | 24 | Positive checks — patterns the linter rewards rather than flags. |

### Known drift (doc-tree vs. registry)

The table above counts curated docs. The compiled registry in `RuleId.java` has expanded faster than the doc-tree:

- **Registry-only categories** — `kafka-connect` (~92 rules) and `quarkus` (~6 rules, distinct from `quarkus-kafka`) are registered with `docPath("kafka-connect/...")` / `docPath("quarkus/...")`, but no `docs/rules/kafka-connect/` or `docs/rules/quarkus/` directory exists yet. The full didactic prose lives inline in each `RuleId.message(...)`; the standalone doc files are a backlog item.
- **Doc-only categories** — `docs/rules/good-practices/` (24 files) and `docs/rules/warpstream/` (4 files) ship as curated guidance documents that pre-date the registry expansion. No `RuleId` is wired to them today; they read as project-curated reference material, not enforced rules.
- **Curated vs. registered totals** — the per-category counts in the table reflect the curated subset shown below; the registered totals are larger (e.g. `kafka-clients` shows 40 here, the registry has ~259). Treat the table as a guided tour, the registry as truth.

## Master rule table

### kafka-clients (40)

| Rule | Severity | Confidence | Detection | Tagline |
|---|---|---|---|---|
| [ALLOW_AUTO_CREATE_TOPICS_TRUE](kafka-clients/ALLOW_AUTO_CREATE_TOPICS_TRUE.md) | WARNING | HIGH | config-file | Auto-creating topics from the consumer makes typos into infrastructure. |
| [AVRO_SPECIFIC_READER_MISSING](kafka-clients/AVRO_SPECIFIC_READER_MISSING.md) | WARNING | HIGH | bytecode + config-file | Generic Avro on a specific topic gives you back GenericRecord — and a ClassCastException later. |
| [BOOTSTRAP_SERVERS_SINGLE_BROKER](kafka-clients/BOOTSTRAP_SERVERS_SINGLE_BROKER.md) | WARNING | HIGH | config-file | Bootstrap with one host and you're one DNS blip from no Kafka at all. |
| [CLIENT_ID_MISSING](kafka-clients/CLIENT_ID_MISSING.md) | WARNING | MEDIUM | bytecode + config-file | No client.id is a faceless caller — broker logs see "producer-1" and so do you. |
| [COMMIT_ASYNC_NO_FINAL_SYNC](kafka-clients/COMMIT_ASYNC_NO_FINAL_SYNC.md) | WARNING | MEDIUM | bytecode | commitAsync is fast and forgetful — pair it with one commitSync on the way out. |
| [CONSUMER_ASSIGN_AND_SUBSCRIBE](kafka-clients/CONSUMER_ASSIGN_AND_SUBSCRIBE.md) | ERROR | HIGH | bytecode | Pick one. assign() and subscribe() are mutually exclusive — Kafka will tell you. |
| [CONSUMER_AUTO_OFFSET_RESET_LATEST](kafka-clients/CONSUMER_AUTO_OFFSET_RESET_LATEST.md) | WARNING | MEDIUM | bytecode + config-file | auto.offset.reset=latest is "skip the backlog you didn't know you had." |
| [CONSUMER_AUTO_OFFSET_RESET_NONE_UNHANDLED](kafka-clients/CONSUMER_AUTO_OFFSET_RESET_NONE_UNHANDLED.md) | WARNING | MEDIUM | bytecode | auto.offset.reset=none is great — if you handle the exception. |
| [CONSUMER_GROUP_INSTANCE_ID_MISSING](kafka-clients/CONSUMER_GROUP_INSTANCE_ID_MISSING.md) | WARNING | CONTEXT | bytecode | Without group.instance.id, every rolling restart is a full rebalance you didn't need. 🤝 |
| [CONSUMER_HEARTBEAT_SESSION_RATIO](kafka-clients/CONSUMER_HEARTBEAT_SESSION_RATIO.md) | WARNING | HIGH | config-file | Heartbeats should be 1/3 of the session — two misses, you're still alive. |
| [CONSUMER_ISOLATION_READ_UNCOMMITTED_WITH_TXN](kafka-clients/CONSUMER_ISOLATION_READ_UNCOMMITTED_WITH_TXN.md) | WARNING | MEDIUM | config-file | read_uncommitted from a transactional topic is reading the draft, not the final. 🤝 |
| [CONSUMER_MAX_POLL_INTERVAL_TOO_LOW](kafka-clients/CONSUMER_MAX_POLL_INTERVAL_TOO_LOW.md) | WARNING | MEDIUM | config-file | Lower max.poll.interval.ms than your slowest record and you've built a rebalance perpetual motion machine. |
| [CONSUMER_MAX_POLL_RECORDS_TOO_HIGH](kafka-clients/CONSUMER_MAX_POLL_RECORDS_TOO_HIGH.md) | WARNING | MEDIUM | config-file | A batch you can't process in max.poll.interval.ms is a batch you'll process twice. |
| [CONSUMER_NOT_CLOSED](kafka-clients/CONSUMER_NOT_CLOSED.md) | WARNING | HIGH | bytecode | If you don't close the consumer, the group waits 30s+ to forget you. |
| [CONSUMER_NOT_THREAD_SAFE](kafka-clients/CONSUMER_NOT_THREAD_SAFE.md) | ERROR | MEDIUM | bytecode | Kafka consumers are single-threaded. Share one and the JVM tells you so. |
| [CONSUMER_NO_WAKEUP_SHUTDOWN](kafka-clients/CONSUMER_NO_WAKEUP_SHUTDOWN.md) | WARNING | MEDIUM | bytecode | Without wakeup(), poll() never gets the memo to stop. |
| [CONSUMER_SEEK_BEFORE_POLL](kafka-clients/CONSUMER_SEEK_BEFORE_POLL.md) | ERROR | MEDIUM | bytecode | seek() before poll() is asking to skip to a chapter of a book you haven't opened. |
| [HEADERS_SENSITIVE_KEYS](kafka-clients/HEADERS_SENSITIVE_KEYS.md) | WARNING | MEDIUM | bytecode | Kafka headers are plaintext — don't put your password in them. |
| [POLL_IN_REBALANCE_CALLBACK](kafka-clients/POLL_IN_REBALANCE_CALLBACK.md) | ERROR | HIGH | bytecode | Calling poll() inside onPartitionsRevoked is asking the consumer to interrupt itself. |
| [PRODUCER_ACKS_ONE](kafka-clients/PRODUCER_ACKS_ONE.md) | WARNING | HIGH | bytecode + config-file | acks=1 means "the leader saw it" — and then the leader died. 🤝 |
| [PRODUCER_ACKS_ZERO](kafka-clients/PRODUCER_ACKS_ZERO.md) | ERROR | HIGH | bytecode + config-file | acks=0 is fire-and-pray. 🤝 |
| [PRODUCER_BUFFER_MEMORY_MISCONFIG](kafka-clients/PRODUCER_BUFFER_MEMORY_MISCONFIG.md) | WARNING | MEDIUM | config-file | buffer.memory below batch.size is "I asked for back-pressure on every send." |
| [PRODUCER_COMPRESSION_NONE_EXPLICIT](kafka-clients/PRODUCER_COMPRESSION_NONE_EXPLICIT.md) | WARNING | MEDIUM | bytecode + config-file | compression.type=none isn't a default — it's a choice. Make sure you meant it. |
| [PRODUCER_DELIVERY_TIMEOUT_TOO_SMALL](kafka-clients/PRODUCER_DELIVERY_TIMEOUT_TOO_SMALL.md) | ERROR | HIGH | config-file | delivery.timeout.ms < request.timeout.ms + linger.ms doesn't even start. |
| [PRODUCER_DEPRECATED_PARTITIONER](kafka-clients/PRODUCER_DEPRECATED_PARTITIONER.md) | WARNING | HIGH | bytecode + config-file | DefaultPartitioner is older than your phone. Stop naming it. |
| [PRODUCER_IDEMPOTENCE_DISABLED](kafka-clients/PRODUCER_IDEMPOTENCE_DISABLED.md) | WARNING | HIGH | bytecode + config-file | Turning idempotence off in 2026 is undoing five years of work the client did for you. 🤝 |
| [PRODUCER_LINGER_ZERO_NO_BATCH](kafka-clients/PRODUCER_LINGER_ZERO_NO_BATCH.md) | WARNING | MEDIUM | config-file | linger.ms=0 with default batch.size means every record is a network round trip. |
| [PRODUCER_MAX_BLOCK_MS_ZERO](kafka-clients/PRODUCER_MAX_BLOCK_MS_ZERO.md) | ERROR | HIGH | bytecode + config-file | max.block.ms=0 means "fail before you even ask the broker." |
| [PRODUCER_MAX_IN_FLIGHT_TOO_HIGH](kafka-clients/PRODUCER_MAX_IN_FLIGHT_TOO_HIGH.md) | ERROR | HIGH | bytecode + config-file | The broker only remembers your last 5 sequences. Sixth one rewrites history. 🤝 |
| [PRODUCER_NOT_CLOSED](kafka-clients/PRODUCER_NOT_CLOSED.md) | ERROR | HIGH | bytecode | An unclosed producer is unflushed buffers waiting for JVM shutdown to lose them. |
| [PRODUCER_PER_RECORD_ALLOCATION](kafka-clients/PRODUCER_PER_RECORD_ALLOCATION.md) | ERROR | HIGH | bytecode | Producers are oxygen — share one, breathe easy. |
| [PRODUCER_RECORD_PARTITION_AND_KEY](kafka-clients/PRODUCER_RECORD_PARTITION_AND_KEY.md) | WARNING | MEDIUM | bytecode | Specifying both partition and key means you don't trust the key. Pick one. |
| [PRODUCER_RETRIES_ZERO](kafka-clients/PRODUCER_RETRIES_ZERO.md) | WARNING | MEDIUM | bytecode + config-file | retries=0 means every transient network blip is a permanent failure. 🤝 |
| [PRODUCER_TXN_ID_WITHOUT_IDEMPOTENCE](kafka-clients/PRODUCER_TXN_ID_WITHOUT_IDEMPOTENCE.md) | ERROR | HIGH | bytecode + config-file | A transactional.id without idempotence is a transaction that isn't. 🤝 |
| [PRODUCER_USED_AFTER_CLOSE](kafka-clients/PRODUCER_USED_AFTER_CLOSE.md) | ERROR | HIGH | bytecode | A closed producer answers "no" to every send. Forever. |
| [PROPERTIES_MUTATED_AFTER_CTOR](kafka-clients/PROPERTIES_MUTATED_AFTER_CTOR.md) | WARNING | HIGH | bytecode | KafkaProducer reads your Properties once. After that you're talking to yourself. |
| [REQUEST_TIMEOUT_LT_REPLICA_LAG](kafka-clients/REQUEST_TIMEOUT_LT_REPLICA_LAG.md) | WARNING | MEDIUM | config-file | A request timeout shorter than replica lag is an SLA against physics. |
| [SCHEMA_REGISTRY_URL_MISSING](kafka-clients/SCHEMA_REGISTRY_URL_MISSING.md) | ERROR | HIGH | bytecode + config-file | A Schema Registry serializer without a URL is just an exception generator. |
| [SECURITY_PROTOCOL_PLAINTEXT_REMOTE](kafka-clients/SECURITY_PROTOCOL_PLAINTEXT_REMOTE.md) | WARNING | CONTEXT | config-file | Plaintext to a hostname you don't own is a credential leak in a green box. |
| [STRING_SERIALIZER_NON_STRING](kafka-clients/STRING_SERIALIZER_NON_STRING.md) | WARNING | MEDIUM | bytecode | StringSerializer with a non-String value is a UTF-8 encoder pretending to be a serializer. |

### kafka-streams (47)

| Rule | Severity | Confidence | Detection | Tagline |
|---|---|---|---|---|
| [STREAMS_APP_ID_UNSTABLE](kafka-streams/STREAMS_APP_ID_UNSTABLE.md) | ERROR | MEDIUM | config-file | If your `application.id` shifts, your state is gone. |
| [STREAMS_CACHE_DISABLED](kafka-streams/STREAMS_CACHE_DISABLED.md) | WARNING | MEDIUM | config-file | Cache=0 makes brokers cry. Update rates explode without it. |
| [STREAMS_CLEANUP_IN_PROD](kafka-streams/STREAMS_CLEANUP_IN_PROD.md) | ERROR | HIGH | bytecode | `cleanUp()` is for tests. In prod it's `rm -rf` on your state. |
| [STREAMS_COMMIT_INTERVAL_TOO_LOW](kafka-streams/STREAMS_COMMIT_INTERVAL_TOO_LOW.md) | WARNING | MEDIUM | config-file | Committing every millisecond means the broker commits a million times a millisecond. 🤝 |
| [STREAMS_DEFAULT_DESERIALIZATION_HANDLER](kafka-streams/STREAMS_DEFAULT_DESERIALIZATION_HANDLER.md) | WARNING | HIGH | config-file | One poison pill kills the whole topology by default. |
| [STREAMS_DESERIALIZER_OBJECT_KEY](kafka-streams/STREAMS_DESERIALIZER_OBJECT_KEY.md) | WARNING | MEDIUM | bytecode | JSON keys don't hash the same on every JVM. Pick a stable representation. |
| [STREAMS_DESER_HANDLER_BLANKET_CONTINUE](kafka-streams/STREAMS_DESER_HANDLER_BLANKET_CONTINUE.md) | ERROR | HIGH | bytecode | A custom `DeserializationExceptionHandler` that always returns `CONTINUE` is data loss with a stack trace. 🤝 |
| [STREAMS_DOUBLE_REPARTITION](kafka-streams/STREAMS_DOUBLE_REPARTITION.md) | WARNING | MEDIUM | dsl-chain | Every `groupBy()` is a network round-trip. Two in a row is two too many. |
| [STREAMS_EOS_COMMIT_INTERVAL_SET](kafka-streams/STREAMS_EOS_COMMIT_INTERVAL_SET.md) | WARNING | HIGH | config-file | Setting `commit.interval.ms` under EOS overrides a correctness-critical default. 🤝 |
| [STREAMS_EOS_TRANSACTION_TIMEOUT_DEFAULT](kafka-streams/STREAMS_EOS_TRANSACTION_TIMEOUT_DEFAULT.md) | WARNING | MEDIUM | config-file | 10s `transaction.timeout.ms` under EOS is a state-wipe cascade waiting for the first slow record. 🤝 |
| [STREAMS_EOS_V1_DEPRECATED](kafka-streams/STREAMS_EOS_V1_DEPRECATED.md) | ERROR on Streams 4.x (won't start), WARNING on Streams 3.x (deprecated) | HIGH | config-file + pom-dependency | `exactly_once` (v1) is a per-task producer. `exactly_once_v2` is the only one you should pick. 🤝 |
| [STREAMS_EXPLICIT_INTERNAL_NAMING_DISABLED](kafka-streams/STREAMS_EXPLICIT_INTERNAL_NAMING_DISABLED.md) | WARNING | HIGH | config-file | `ensure.explicit.internal.resource.naming=true` is the one-line safety net for state identity. |
| [STREAMS_FOREACH_FOR_STATE](kafka-streams/STREAMS_FOREACH_FOR_STATE.md) | WARNING | MEDIUM | bytecode | `foreach` is a terminal sink. Don't put your state machine in there. |
| [STREAMS_GROUPBY_NULL_KEY](kafka-streams/STREAMS_GROUPBY_NULL_KEY.md) | WARNING | MEDIUM | dsl-chain | `groupByKey()` on null keys silently eats your records. |
| [STREAMS_GROUP_PROTOCOL_NOT_STREAMS](kafka-streams/STREAMS_GROUP_PROTOCOL_NOT_STREAMS.md) | WARNING | CONTEXT | config-file + pom-dependency | On AK 4.2+ / CP 8.2+, classic rebalancing is 50-80% slower for no reason. |
| [STREAMS_GROUP_PROTOCOL_STREAMS_INCOMPAT](kafka-streams/STREAMS_GROUP_PROTOCOL_STREAMS_INCOMPAT.md) | ERROR | HIGH | config-file | `group.protocol=streams` plus static membership is a `ConfigException` you can ship. 🤝 |
| [STREAMS_INMEMORY_STORE_UNBOUNDED](kafka-streams/STREAMS_INMEMORY_STORE_UNBOUNDED.md) | WARNING | MEDIUM | bytecode | In-memory stores have one knob: heap. |
| [STREAMS_INTERACTIVE_QUERY_BEFORE_RUNNING](kafka-streams/STREAMS_INTERACTIVE_QUERY_BEFORE_RUNNING.md) | WARNING | MEDIUM | bytecode | Querying a store before `RUNNING` is querying a closet that hasn't been built. |
| [STREAMS_JOIN_DEFAULT_SERDES](kafka-streams/STREAMS_JOIN_DEFAULT_SERDES.md) | WARNING | MEDIUM | dsl-chain | A join without explicit Serdes is a join with implicit assumptions. |
| [STREAMS_JOIN_WINDOW_LEGACY_API](kafka-streams/STREAMS_JOIN_WINDOW_LEGACY_API.md) | WARNING | HIGH | bytecode | `JoinWindows.of(...)` gives you a 24-hour grace period for free, whether you wanted it or not. |
| [STREAMS_KSTREAMS_MULTIPLE_START](kafka-streams/STREAMS_KSTREAMS_MULTIPLE_START.md) | ERROR | HIGH | bytecode | `KafkaStreams#start()` is one-shot. Calling it twice throws `IllegalStateException`. |
| [STREAMS_KTABLE_FILTER_SIDE_EFFECT](kafka-streams/STREAMS_KTABLE_FILTER_SIDE_EFFECT.md) | WARNING | MEDIUM | bytecode | `filter`/`mapValues` with side effects: rerun the changelog, rerun the side effect. |
| [STREAMS_KTABLE_REGROUP_BY_SAME_KEY](kafka-streams/STREAMS_KTABLE_REGROUP_BY_SAME_KEY.md) | WARNING | MEDIUM | dsl-chain | `toStream().groupByKey()` is a repartition you didn't need to pay for. |
| [STREAMS_MATERIALIZED_NULL_SERDES](kafka-streams/STREAMS_MATERIALIZED_NULL_SERDES.md) | WARNING | MEDIUM | bytecode | `Materialized.with(null, null)` says "trust me, the default serdes are right." They aren't. |
| [STREAMS_MAX_POLL_LT_TRANSACTION_TIMEOUT](kafka-streams/STREAMS_MAX_POLL_LT_TRANSACTION_TIMEOUT.md) | ERROR | HIGH | config-file | Under EOS, `max.poll.interval.ms` smaller than `transaction.timeout.ms` is a guaranteed cascade. |
| [STREAMS_NON_DETERMINISTIC_AGGREGATOR](kafka-streams/STREAMS_NON_DETERMINISTIC_AGGREGATOR.md) | WARNING | MEDIUM | bytecode | Aggregators with `Math.random()` give different answers on replay. That's a bug, not a feature. |
| [STREAMS_NO_SHUTDOWN_HOOK](kafka-streams/STREAMS_NO_SHUTDOWN_HOOK.md) | WARNING | MEDIUM | bytecode | Without a shutdown hook, you commit offsets via SIGKILL. 🤝 |
| [STREAMS_NUM_THREADS_ONE](kafka-streams/STREAMS_NUM_THREADS_ONE.md) | WARNING | MEDIUM | config-file | One stream thread is one heart attack from offline. |
| [STREAMS_NUM_THREADS_OVER_PARTITIONS](kafka-streams/STREAMS_NUM_THREADS_OVER_PARTITIONS.md) | WARNING | CONTEXT | annotation + config-file | Threads with nothing to do still cost you a heap. |
| [STREAMS_PROBING_REBALANCE_DEFAULT_LOOP](kafka-streams/STREAMS_PROBING_REBALANCE_DEFAULT_LOOP.md) | WARNING | CONTEXT | config-file + dsl-chain | Stateful app with standbys that never catch up → 10-minute rebalance loop forever. |
| [STREAMS_PROCESSING_EXCEPTION_HANDLER_MISSING](kafka-streams/STREAMS_PROCESSING_EXCEPTION_HANDLER_MISSING.md) | WARNING | HIGH | config-file | A KIP-1034 handler is the only thing between a lambda NPE and a fleet-wide restart loop. 🤝 |
| [STREAMS_PRODUCTION_HANDLER_MISSING](kafka-streams/STREAMS_PRODUCTION_HANDLER_MISSING.md) | WARNING | MEDIUM | bytecode + config-file | `default.production.exception.handler` unset means the first `RecordTooLargeException` kills the topology. 🤝 |
| [STREAMS_PROD_HANDLER_BLANKET_CONTINUE](kafka-streams/STREAMS_PROD_HANDLER_BLANKET_CONTINUE.md) | ERROR | HIGH | bytecode | A `ProductionExceptionHandler` that always returns `CONTINUE` swallows every produce failure — including the ones that mean the broker is gone. 🤝 |
| [STREAMS_PUNCTUATOR_WRONG_TIME](kafka-streams/STREAMS_PUNCTUATOR_WRONG_TIME.md) | WARNING | MEDIUM | bytecode | Wall-clock punctuators fire even when no data arrives. Stream-time punctuators don't. |
| [STREAMS_REPARTITION_RETENTION_SET](kafka-streams/STREAMS_REPARTITION_RETENTION_SET.md) | ERROR | HIGH | bytecode + config-file | Set retention on a repartition topic and the records you haven't processed yet vanish. |
| [STREAMS_REPLICATION_FACTOR_ONE](kafka-streams/STREAMS_REPLICATION_FACTOR_ONE.md) | ERROR | HIGH | config-file | `replication.factor=1` is a state store with an expiration date. 🤝 |
| [STREAMS_ROCKSDB_NO_CONFIG_SETTER](kafka-streams/STREAMS_ROCKSDB_NO_CONFIG_SETTER.md) | WARNING | MEDIUM | config-file | A `RocksDBConfigSetter` is the difference between tuned RocksDB and 64 MB of "default". |
| [STREAMS_STATE_DIR_TMP](kafka-streams/STREAMS_STATE_DIR_TMP.md) | ERROR | HIGH | config-file | RocksDB on /tmp is state-by-coincidence. |
| [STREAMS_SUPPRESS_NO_GRACE](kafka-streams/STREAMS_SUPPRESS_NO_GRACE.md) | WARNING | MEDIUM | dsl-chain | `suppress(untilWindowCloses)` without explicit grace is a 24-hour pause. |
| [STREAMS_SUPPRESS_UNBOUNDED_BUFFER](kafka-streams/STREAMS_SUPPRESS_UNBOUNDED_BUFFER.md) | ERROR | HIGH | bytecode | `Suppressed.BufferConfig.unbounded()` in production is a heap-OOM with a paper trail. |
| [STREAMS_THROUGH_DEPRECATED](kafka-streams/STREAMS_THROUGH_DEPRECATED.md) | WARNING | HIGH | bytecode | `through()` is dead. Long live `repartition()`. |
| [STREAMS_TOPOLOGY_OPTIMIZATION_DISABLED](kafka-streams/STREAMS_TOPOLOGY_OPTIMIZATION_DISABLED.md) | WARNING | MEDIUM | config-file | Default is `none`. You're leaving free CPU and bandwidth on the table. |
| [STREAMS_TRANSFORM_DEPRECATED](kafka-streams/STREAMS_TRANSFORM_DEPRECATED.md) | WARNING | HIGH | bytecode | Transformers are last decade's Processor. Use `process` / `processValues`. |
| [STREAMS_UNCAUGHT_HANDLER_DEPRECATED_API](kafka-streams/STREAMS_UNCAUGHT_HANDLER_DEPRECATED_API.md) | WARNING | HIGH | bytecode | `setUncaughtExceptionHandler(Thread.UncaughtExceptionHandler)` is a JDK API. Streams gave you a real one in KIP-671 — use it. 🤝 |
| [STREAMS_UNCAUGHT_HANDLER_MISSING](kafka-streams/STREAMS_UNCAUGHT_HANDLER_MISSING.md) | WARNING | MEDIUM | bytecode | No uncaught handler? Then your topology has one shape — `DEAD`. |
| [STREAMS_UNNAMED_MATERIALIZED](kafka-streams/STREAMS_UNNAMED_MATERIALIZED.md) | ERROR | HIGH | dsl-chain | An unnamed store is a store you'll lose on the next deploy. |
| [STREAMS_WALLCLOCK_TIMESTAMP_EXTRACTOR](kafka-streams/STREAMS_WALLCLOCK_TIMESTAMP_EXTRACTOR.md) | WARNING | HIGH | config-file | `WallclockTimestampExtractor` says "ignore the data's timestamp, use mine." |

### spring-kafka (32)

| Rule | Severity | Confidence | Detection | Tagline |
|---|---|---|---|---|
| [SPRING_ACK_MODE_RECORD_HIGH_THROUGHPUT](spring-kafka/SPRING_ACK_MODE_RECORD_HIGH_THROUGHPUT.md) | WARNING | CONTEXT | config-file | AckMode.RECORD on a hot topic is one offset commit per record — say hi to the brokers. |
| [SPRING_AFTER_ROLLBACK_PROCESSOR_DEFAULT](spring-kafka/SPRING_AFTER_ROLLBACK_PROCESSOR_DEFAULT.md) | WARNING | MEDIUM | bytecode | Transactional listeners don't use `DefaultErrorHandler` — they use `DefaultAfterRollbackProcessor`, with its own equally-bad default backoff. 🤝 |
| [SPRING_BOOT_ACK_MODE_MANUAL_WITHOUT_CODE_ACK](spring-kafka/SPRING_BOOT_ACK_MODE_MANUAL_WITHOUT_CODE_ACK.md) | ERROR | MEDIUM | annotation + bytecode + config-file | ack-mode=MANUAL plus no acknowledge() means no commits, ever. |
| [SPRING_BOOT_AUTO_OFFSET_RESET_LATEST](spring-kafka/SPRING_BOOT_AUTO_OFFSET_RESET_LATEST.md) | WARNING | MEDIUM | config-file | `auto-offset-reset: latest` means "lose every record produced before the first deploy". |
| [SPRING_BOOT_BOOTSTRAP_SERVERS_LOCALHOST](spring-kafka/SPRING_BOOT_BOOTSTRAP_SERVERS_LOCALHOST.md) | ERROR (suppress under dev/local/test profile) | HIGH | config-file | `bootstrap-servers: localhost:9092` shipped to prod is a Friday-evening pager. |
| [SPRING_BOOT_ENABLE_AUTO_COMMIT_VS_MANUAL_ACK](spring-kafka/SPRING_BOOT_ENABLE_AUTO_COMMIT_VS_MANUAL_ACK.md) | ERROR | HIGH | annotation + bytecode + config-file | `enable-auto-commit=true` + Acknowledgment in code = two cooks, no kitchen. |
| [SPRING_BOOT_PRODUCER_ACKS_NOT_ALL](spring-kafka/SPRING_BOOT_PRODUCER_ACKS_NOT_ALL.md) | ERROR for acks=0, WARNING for acks=1 | HIGH | config-file | `spring.kafka.producer.acks=1` ships durability away one record at a time. 🤝 |
| [SPRING_CUSTOM_HANDLER_MISSING_OTHER_EXCEPTION](spring-kafka/SPRING_CUSTOM_HANDLER_MISSING_OTHER_EXCEPTION.md) | WARNING | HIGH | bytecode | A custom `CommonErrorHandler` that skips `handleOtherException` is half an error handler. |
| [SPRING_DEH_DEFAULT_BACKOFF](spring-kafka/SPRING_DEH_DEFAULT_BACKOFF.md) | WARNING | HIGH | bytecode | `new DefaultErrorHandler()` retries ten times with zero delay — that's not a backoff, that's a tantrum. 🤝 |
| [SPRING_DEH_NO_DLT_RECOVERER](spring-kafka/SPRING_DEH_NO_DLT_RECOVERER.md) | WARNING | HIGH | bytecode | `DefaultErrorHandler` with no recoverer logs your data loss and calls it a feature. 🤝 |
| [SPRING_DEPRECATED_SEEK_TO_CURRENT_ERROR_HANDLER](spring-kafka/SPRING_DEPRECATED_SEEK_TO_CURRENT_ERROR_HANDLER.md) | WARNING | HIGH | bytecode | SeekToCurrentErrorHandler shipped its last bug fix in 2021. |
| [SPRING_EHD_LISTENER_NULL_NOT_CHECKED](spring-kafka/SPRING_EHD_LISTENER_NULL_NOT_CHECKED.md) | WARNING | MEDIUM | bytecode + config-file | `ErrorHandlingDeserializer` returns `null` for poison pills — and your listener processes them as legitimate records. 🤝 |
| [SPRING_KAFKA_TEMPLATE_SEND_BLOCKING_GET](spring-kafka/SPRING_KAFKA_TEMPLATE_SEND_BLOCKING_GET.md) | WARNING | HIGH | bytecode | `send(...).get()` turns an async producer back into a synchronous one, one record at a time. 🤝 |
| [SPRING_KAFKA_TEMPLATE_SEND_NO_CALLBACK](spring-kafka/SPRING_KAFKA_TEMPLATE_SEND_NO_CALLBACK.md) | WARNING | MEDIUM | bytecode | `kafkaTemplate.send(...)` and walk away is fire-and-pray. |
| [SPRING_KSTREAMS_BINDER_DEFAULT_DEH](spring-kafka/SPRING_KSTREAMS_BINDER_DEFAULT_DEH.md) | WARNING | MEDIUM | config-file | Spring Cloud Stream Kafka Streams binder defaults to `logAndFail` — first poison pill takes down the function. 🤝 |
| [SPRING_LISTENER_ASYNC_ANNOTATION](spring-kafka/SPRING_LISTENER_ASYNC_ANNOTATION.md) | ERROR | HIGH | annotation | @Async on a listener breaks every offset guarantee Kafka gives you. |
| [SPRING_LISTENER_AUTOSTARTUP_FALSE](spring-kafka/SPRING_LISTENER_AUTOSTARTUP_FALSE.md) | WARNING | MEDIUM | annotation | A listener you forgot to start is a feature flag that's always off. |
| [SPRING_LISTENER_BATCH_SIGNATURE_MISMATCH](spring-kafka/SPRING_LISTENER_BATCH_SIGNATURE_MISMATCH.md) | ERROR | HIGH | annotation + bytecode + config-file | A batch listener that takes one record will never deploy. |
| [SPRING_LISTENER_CONCURRENCY_EXCEEDS_PARTITIONS](spring-kafka/SPRING_LISTENER_CONCURRENCY_EXCEEDS_PARTITIONS.md) | WARNING | CONTEXT | annotation | Extra concurrency past partition count just hires idle threads. |
| [SPRING_LISTENER_DIRECT_PRODUCER](spring-kafka/SPRING_LISTENER_DIRECT_PRODUCER.md) | ERROR | HIGH | bytecode | `new KafkaProducer(...)` inside a Spring app is a memory leak with delivery guarantees. 🤝 |
| [SPRING_LISTENER_EMPTY_TOPICS](spring-kafka/SPRING_LISTENER_EMPTY_TOPICS.md) | ERROR | HIGH | annotation | An empty topics array subscribes to nothing, loudly. |
| [SPRING_LISTENER_MANUAL_ACK_NEVER_CALLED](spring-kafka/SPRING_LISTENER_MANUAL_ACK_NEVER_CALLED.md) | ERROR | HIGH | bytecode | AckMode.MANUAL without calling acknowledge() is just AckMode.NEVER. |
| [SPRING_LISTENER_MISSING_GROUP_ID](spring-kafka/SPRING_LISTENER_MISSING_GROUP_ID.md) | ERROR | HIGH | annotation + bytecode + config-file | A listener with no group is just a private subscription nobody else knows about. |
| [SPRING_LISTENER_RETURN_VALUE_IGNORED](spring-kafka/SPRING_LISTENER_RETURN_VALUE_IGNORED.md) | WARNING | HIGH | annotation + bytecode | A non-void listener with no @SendTo is a return statement nobody reads. |
| [SPRING_LISTENER_SENDTO_NO_REPLY_TEMPLATE](spring-kafka/SPRING_LISTENER_SENDTO_NO_REPLY_TEMPLATE.md) | WARNING | MEDIUM | annotation + bytecode + config-file | @SendTo without a reply template sends your replies into the void. |
| [SPRING_LISTENER_SWALLOWS_EXCEPTION](spring-kafka/SPRING_LISTENER_SWALLOWS_EXCEPTION.md) | WARNING | MEDIUM | bytecode | `try { ... } catch (Exception e) { log.error(e); }` inside a `@KafkaListener` deletes the error handler. 🤝 |
| [SPRING_LISTENER_THREAD_SLEEP](spring-kafka/SPRING_LISTENER_THREAD_SLEEP.md) | WARNING | MEDIUM | bytecode | Thread.sleep() in a listener is a self-inflicted rebalance. |
| [SPRING_NO_ERROR_HANDLING_DESERIALIZER](spring-kafka/SPRING_NO_ERROR_HANDLING_DESERIALIZER.md) | WARNING | MEDIUM | config-file | Without ErrorHandlingDeserializer, one bad byte stops every consumer in the group. |
| [SPRING_RETRYABLE_TOPIC_NO_KAFKA_TEMPLATE](spring-kafka/SPRING_RETRYABLE_TOPIC_NO_KAFKA_TEMPLATE.md) | ERROR | HIGH | annotation + bytecode | @RetryableTopic without a template bean fails at startup — every time. |
| [SPRING_RETRYABLE_TOPIC_WITH_BATCH](spring-kafka/SPRING_RETRYABLE_TOPIC_WITH_BATCH.md) | ERROR | HIGH | annotation + bytecode + config-file | @RetryableTopic + batch listeners = unsupported, per the docs. |
| [SPRING_SERIALIZER_MISMATCH](spring-kafka/SPRING_SERIALIZER_MISMATCH.md) | WARNING | MEDIUM | config-file | JsonSerializer in, StringDeserializer out — the wire becomes a Rorschach test. |
| [SPRING_TRANSACTIONAL_WITHOUT_KTM](spring-kafka/SPRING_TRANSACTIONAL_WITHOUT_KTM.md) | ERROR | MEDIUM | annotation + bytecode + config-file | @Transactional + kafkaTemplate.send() without a transactional producer is decoration. 🤝 |

### quarkus-kafka (36)

| Rule | Severity | Confidence | Detection | Tagline |
|---|---|---|---|---|
| [QK_AUTO_COMMIT_ENABLED](quarkus-kafka/QK_AUTO_COMMIT_ENABLED.md) | WARNING | HIGH | config-file | Let SmallRye commit. Don't hand the wheel to Kafka. |
| [QK_AUTO_OFFSET_RESET_LATEST](quarkus-kafka/QK_AUTO_OFFSET_RESET_LATEST.md) | WARNING | MEDIUM | config-file | auto.offset.reset=latest skips everything on a fresh consumer group. |
| [QK_AUTO_REGISTER_SCHEMAS](quarkus-kafka/QK_AUTO_REGISTER_SCHEMAS.md) | WARNING | HIGH | config-file | auto.register.schemas in prod = anyone can mint a new schema. Anyone. |
| [QK_AWAIT_INDEFINITELY_ON_IO](quarkus-kafka/QK_AWAIT_INDEFINITELY_ON_IO.md) | ERROR | HIGH | annotation + bytecode | await().indefinitely() in a reactive pipeline is a deadlock waiting for tomorrow. |
| [QK_BACKPRESSURE_NONE_STRATEGY](quarkus-kafka/QK_BACKPRESSURE_NONE_STRATEGY.md) | WARNING | HIGH | annotation | @OnOverflow(NONE) is reactive in name only. |
| [QK_BLOCKING_ON_IO_THREAD](quarkus-kafka/QK_BLOCKING_ON_IO_THREAD.md) | ERROR | HIGH | annotation + bytecode | @Blocking on the IO thread isn't reactive — it's surrender. |
| [QK_CHANNEL_NAME_COLLISION](quarkus-kafka/QK_CHANNEL_NAME_COLLISION.md) | ERROR | HIGH | bytecode + config-file | Same channel name for in and out wires your app to itself. |
| [QK_CHECKED_EXCEPTION_FROM_INCOMING](quarkus-kafka/QK_CHECKED_EXCEPTION_FROM_INCOMING.md) | WARNING | HIGH | bytecode | A checked exception that escapes @Incoming is a nack you didn't plan for. |
| [QK_COMMIT_STRATEGY_IGNORE](quarkus-kafka/QK_COMMIT_STRATEGY_IGNORE.md) | WARNING | HIGH | config-file | commit-strategy=ignore: you process everything, you remember nothing. 🤝 |
| [QK_DEVSERVICES_IN_PROD](quarkus-kafka/QK_DEVSERVICES_IN_PROD.md) | ERROR | HIGH | config-file | Dev Services in production is a Kafka in a sidecar that nobody told ops about. |
| [QK_EMITTER_NO_ONOVERFLOW](quarkus-kafka/QK_EMITTER_NO_ONOVERFLOW.md) | WARNING | HIGH | annotation + bytecode | 256 messages is not your back-pressure plan. |
| [QK_EMITTER_UNBOUNDED_BUFFER](quarkus-kafka/QK_EMITTER_UNBOUNDED_BUFFER.md) | ERROR | HIGH | annotation | UNBOUNDED_BUFFER is the docs' own [DANGER ZONE]. Read that as: OOM. |
| [QK_FAILURE_DLQ_NO_TOPIC](quarkus-kafka/QK_FAILURE_DLQ_NO_TOPIC.md) | WARNING | HIGH | config-file | `failure-strategy=dead-letter-queue` with no `dead-letter-queue.topic` sends every failure to a topic named `dead-letter-topic-$channel` — and no one will think to look there. |
| [QK_FAILURE_FAIL_PROD_NO_HEALTH](quarkus-kafka/QK_FAILURE_FAIL_PROD_NO_HEALTH.md) | WARNING | MEDIUM | config-file | `failure-strategy=fail` in production with no health-driven restart and no DLQ on the side is "one bad record = service offline until I notice." |
| [QK_FAILURE_STRATEGY_FAIL_NO_DLQ](quarkus-kafka/QK_FAILURE_STRATEGY_FAIL_NO_DLQ.md) | WARNING | MEDIUM | config-file | The default fails loud — make sure you actually want the channel to die. |
| [QK_FAILURE_STRATEGY_IGNORE](quarkus-kafka/QK_FAILURE_STRATEGY_IGNORE.md) | ERROR | HIGH | config-file | failure-strategy=ignore is data loss with a smile. |
| [QK_FAIL_ON_DESER_NO_DLQ](quarkus-kafka/QK_FAIL_ON_DESER_NO_DLQ.md) | ERROR | HIGH | config-file | One poison pill, one dead channel — unless you've planned for it. |
| [QK_HEALTH_DISABLED](quarkus-kafka/QK_HEALTH_DISABLED.md) | WARNING | HIGH | config-file | A channel without health checks is a channel without an alarm. |
| [QK_INCOMING_RETURNS_VOID](quarkus-kafka/QK_INCOMING_RETURNS_VOID.md) | WARNING | MEDIUM | bytecode | Void return = pre-processing ack = at-most-once if you blink wrong. |
| [QK_KSTREAMS_MULTIPLE_TOPOLOGY_BEANS](quarkus-kafka/QK_KSTREAMS_MULTIPLE_TOPOLOGY_BEANS.md) | ERROR | HIGH | annotation + bytecode | One Topology to rule them all — extras get ignored. |
| [QK_KSTREAMS_NO_APPLICATION_ID](quarkus-kafka/QK_KSTREAMS_NO_APPLICATION_ID.md) | ERROR | HIGH | config-file | A Streams app without an application-id is a consumer group named "guess". |
| [QK_LOCALHOST_IN_PROD](quarkus-kafka/QK_LOCALHOST_IN_PROD.md) | ERROR | HIGH | config-file | localhost:9092 in production: this isn't your laptop. |
| [QK_MESSAGE_NEVER_ACKED](quarkus-kafka/QK_MESSAGE_NEVER_ACKED.md) | ERROR | HIGH | bytecode | A Message<T> you don't ack is a partition you'll never advance. |
| [QK_MISSING_TOPIC](quarkus-kafka/QK_MISSING_TOPIC.md) | WARNING | MEDIUM | config-file | No topic property? Hope your channel name is the topic name you wanted. |
| [QK_MISSING_VALUE_DESERIALIZER](quarkus-kafka/QK_MISSING_VALUE_DESERIALIZER.md) | ERROR | HIGH | config-file | No value.deserializer? Hope you wanted Strings. |
| [QK_MUTINY_UNI_NEVER_SUBSCRIBED](quarkus-kafka/QK_MUTINY_UNI_NEVER_SUBSCRIBED.md) | ERROR | HIGH | bytecode | A Uni you don't subscribe to is a message you don't send. |
| [QK_NATIVE_REFLECTION_MISSING](quarkus-kafka/QK_NATIVE_REFLECTION_MISSING.md) | WARNING | MEDIUM | annotation + bytecode | GraalVM doesn't read your mind — register that class. |
| [QK_OUTGOING_ACKS_LE_ONE](quarkus-kafka/QK_OUTGOING_ACKS_LE_ONE.md) | ERROR for acks=0, WARNING for acks=1 | HIGH | config-file | acks=1 trusts one broker. acks=0 trusts nothing. 🤝 |
| [QK_PARTITIONS_DEPRECATED](quarkus-kafka/QK_PARTITIONS_DEPRECATED.md) | WARNING | HIGH | config-file | `partitions` is dead. Long live `concurrency`. |
| [QK_PRODUCER_NO_KEY](quarkus-kafka/QK_PRODUCER_NO_KEY.md) | WARNING | MEDIUM | bytecode | No key, no partition affinity, no ordering. Fine for fire-hoses, fatal for streams. |
| [QK_SCHEMA_REGISTRY_URL_MISSING](quarkus-kafka/QK_SCHEMA_REGISTRY_URL_MISSING.md) | ERROR | HIGH | config-file | Avro without a schema registry URL is just bytes pretending to be schema'd. |
| [QK_THROTTLED_HEALTH_DISABLED](quarkus-kafka/QK_THROTTLED_HEALTH_DISABLED.md) | WARNING | HIGH | config-file | Disabling unprocessed-record-max-age is welcoming the OOM in. |
| [QK_TRACING_DISABLED](quarkus-kafka/QK_TRACING_DISABLED.md) | WARNING | MEDIUM | config-file | tracing-enabled=false is "I solemnly swear to debug from logs alone." |
| [QK_TRANSACTIONAL_ID_NOT_UNIQUE](quarkus-kafka/QK_TRANSACTIONAL_ID_NOT_UNIQUE.md) | ERROR | HIGH | config-file | A shared transactional.id is two producers fencing each other off forever. 🤝 |
| [QK_UNNECESSARY_BLOCKING](quarkus-kafka/QK_UNNECESSARY_BLOCKING.md) | WARNING | MEDIUM | annotation + bytecode | @Blocking on pure compute is a free thread hop you pay for. |
| [QK_WAIT_FOR_WRITE_COMPLETION_FALSE](quarkus-kafka/QK_WAIT_FOR_WRITE_COMPLETION_FALSE.md) | WARNING | HIGH | config-file | waitForWriteCompletion=false un-does acks=all from the Quarkus side. |

### observability (14)

| Rule | Severity | Confidence | Detection | Tagline |
|---|---|---|---|---|
| [ASYNC_INCOMING_VOID_SUBSCRIBE](observability/ASYNC_INCOMING_VOID_SUBSCRIBE.md) | ERROR | HIGH | bytecode | `@Incoming` returning `void` while calling `.subscribe()` on a `Multi` is fire-and-forget — and the framework can't ack what it doesn't see. |
| [ASYNC_LISTENER_MONO_BLOCK](observability/ASYNC_LISTENER_MONO_BLOCK.md) | WARNING | HIGH | bytecode | `Mono.block()` inside a `@KafkaListener` defeats reactive — and the error path with it. |
| [DESER_JSON_TYPE_INFO_NO_ALLOWLIST](observability/DESER_JSON_TYPE_INFO_NO_ALLOWLIST.md) | ERROR | HIGH | bytecode + config-file | `JsonDeserializer` with `__TypeId__` headers and no trusted-packages allowlist is RCE via a Kafka header. 🤝 |
| [DESER_SR_NO_AUTH_CREDENTIALS](observability/DESER_SR_NO_AUTH_CREDENTIALS.md) | WARNING | MEDIUM | config-file | Schema Registry without `basic.auth.credentials.source` is an HTTPS URL that says "trust me, no creds needed." |
| [DESER_STRING_FOR_JSON_PAYLOAD](observability/DESER_STRING_FOR_JSON_PAYLOAD.md) | WARNING | MEDIUM | bytecode | `StringDeserializer` on a JSON topic catches no structural errors — your "deserializer" is a UTF-8 decoder. |
| [LAMBDA_PRODUCER_CALLBACK_EMPTY](observability/LAMBDA_PRODUCER_CALLBACK_EMPTY.md) | ERROR | HIGH | bytecode | `producer.send(record, (md, ex) -> {})` is the lambda spelling of "I don't want to know if it failed." |
| [LAMBDA_RECOVERER_RETURNS_NULL](observability/LAMBDA_RECOVERER_RETURNS_NULL.md) | ERROR | HIGH | bytecode | A `BiConsumer<ConsumerRecord, Exception>` recoverer that does nothing is a black hole with extra steps. 🤝 |
| [LAMBDA_SEND_GET_WITH_CALLBACK](observability/LAMBDA_SEND_GET_WITH_CALLBACK.md) | WARNING | HIGH | bytecode | `producer.send(record, callback).get()` is one too many error-handling paths. Pick one. |
| [OBS_NO_CONSUMER_INTERCEPTORS](observability/OBS_NO_CONSUMER_INTERCEPTORS.md) | WARNING | CONTEXT | config-file | A consumer with no `interceptor.classes` is the second half of a broken trace. |
| [OBS_NO_METRIC_REPORTERS](observability/OBS_NO_METRIC_REPORTERS.md) | WARNING | CONTEXT | config-file | Kafka exposes 100+ metrics by default — and ships exactly zero of them unless you ask. |
| [OBS_NO_PRODUCER_INTERCEPTORS](observability/OBS_NO_PRODUCER_INTERCEPTORS.md) | WARNING | CONTEXT | config-file | No `interceptor.classes` on the producer is "I want tracing — just not in this app." |
| [OBS_OPENTELEMETRY_AGENT_MISMATCH](observability/OBS_OPENTELEMETRY_AGENT_MISMATCH.md) | WARNING | MEDIUM | config-file + pom-dependency | Declaring `opentelemetry-api` without the agent *or* the Kafka instrumentation library is owning the boxing gloves without ever stepping in the ring. |
| [OBS_SPRING_MICROMETER_NOT_BOUND](observability/OBS_SPRING_MICROMETER_NOT_BOUND.md) | WARNING | MEDIUM | bytecode + pom-dependency | A custom `ProducerFactory` without `MicrometerProducerListener` is `KafkaMetricsAutoConfiguration` waving goodbye. 🤝 |
| [STREAMS_NO_HEALTH_ENDPOINT](observability/STREAMS_NO_HEALTH_ENDPOINT.md) | WARNING | MEDIUM | bytecode + pom-dependency | Without a `/health/ready` probe, Kubernetes restarts your Streams app during normal rebalances. |

### schema-registry (7)

| Rule | Severity | Confidence | Detection | Tagline |
|---|---|---|---|---|
| [AVRO_LOGICAL_TYPE_BIGDECIMAL_PRIMITIVE](schema-registry/AVRO_LOGICAL_TYPE_BIGDECIMAL_PRIMITIVE.md) | ERROR | HIGH | bytecode | `setAmount(0.0)` writes a `double` to a `BigDecimal` field — money has lost its precision. |
| [AVRO_LOGICAL_TYPE_INSTANT_PRIMITIVE](schema-registry/AVRO_LOGICAL_TYPE_INSTANT_PRIMITIVE.md) | ERROR | HIGH | bytecode | `Avro 1.12+` generated setters expect `java.time.Instant` — `Long.valueOf(...)` and `Math.max(...)` are compile errors with a paper trail. |
| [AVRO_LOGICAL_TYPE_LOCALDATE_PRIMITIVE](schema-registry/AVRO_LOGICAL_TYPE_LOCALDATE_PRIMITIVE.md) | ERROR | HIGH | bytecode | An `int` to a `LocalDate` setter is a 1970-01-01 record waiting to be produced. |
| [SR_CUSTOM_SERIALIZER_BYPASS](schema-registry/SR_CUSTOM_SERIALIZER_BYPASS.md) | WARNING | MEDIUM | bytecode | A hand-rolled `Serializer<T>` that does `JSON.stringify` is Confluent's Category E — schemas you can't evolve and consumers you can't migrate. |
| [SR_JSON_VALUE_TYPE_MISSING](schema-registry/SR_JSON_VALUE_TYPE_MISSING.md) | ERROR | HIGH | bytecode + config-file | Without `json.value.type`, your consumer gets `LinkedHashMap` and `getCustomerId()` throws `ClassCastException`. |
| [SR_PROTOBUF_VALUE_TYPE_MISSING](schema-registry/SR_PROTOBUF_VALUE_TYPE_MISSING.md) | WARNING | HIGH | bytecode + config-file | Without `specific.protobuf.value.type`, your consumer gets `DynamicMessage` instead of the generated class — every typed access goes through `getField()`. |
| [SR_USE_LATEST_VERSION_MISSING](schema-registry/SR_USE_LATEST_VERSION_MISSING.md) | WARNING | MEDIUM | config-file | `auto.register.schemas=false` without `use.latest.version=true` — your producer is stuck on schema ID v1 forever. |

### warpstream (4)

| Rule | Severity | Confidence | Detection | Tagline |
|---|---|---|---|---|
| [WARPSTREAM_BATCH_AND_LINGER_DEFAULTS](warpstream/WARPSTREAM_BATCH_AND_LINGER_DEFAULTS.md) | WARNING | CONTEXT | config-file | Default `batch.size=16384` + `linger.ms=0` against object storage = one S3 PUT per record. 🤝 |
| [WARPSTREAM_CLIENT_ID_NO_AZ](warpstream/WARPSTREAM_CLIENT_ID_NO_AZ.md) | WARNING | CONTEXT | config-file | No `ws_az=<az>` in `client.id` — every byte routed cross-AZ is $0.05/GB, paid silently in your AWS bill. |
| [WARPSTREAM_FETCH_MIN_BYTES_SET](warpstream/WARPSTREAM_FETCH_MIN_BYTES_SET.md) | WARNING | HIGH | config-file | `fetch.min.bytes` is silently ignored on WarpStream — use `fetch.max.wait.ms` to get the batching you wanted. |
| [WARPSTREAM_IDEMPOTENCE_ENABLED](warpstream/WARPSTREAM_IDEMPOTENCE_ENABLED.md) | WARNING | CONTEXT | config-file | `enable.idempotence=true` against ~250ms object-storage latency = 5 in-flight × 4 round-trips/sec = 20 RPS. 🤝 |

### versions (28)

| Rule | Severity | Confidence | Detection | Tagline |
|---|---|---|---|---|
| [APICURIO_SERDE_V1](versions/APICURIO_SERDE_V1.md) | WARNING | HIGH | pom-dependency | Apicurio Registry Serdes 1.x is on a different package — and its lights went off in 2022. |
| [CONFLUENT_AVRO_SERDE_EOL](versions/CONFLUENT_AVRO_SERDE_EOL.md) | WARNING | HIGH | pom-dependency | kafka-avro-serializer 5.x belongs to Confluent Platform 5 — twelve releases out of date. |
| [CONFLUENT_CLIENT_MIX](versions/CONFLUENT_CLIENT_MIX.md) | WARNING | MEDIUM | pom-dependency | Mixing Confluent Platform clients with Apache Kafka clients is asking two vendors to share one classpath. |
| [JAVA_VERSION_TOO_LOW](versions/JAVA_VERSION_TOO_LOW.md) | ERROR | HIGH | pom-property | Kafka 4 needs JDK 17. Spring 6 needs 17. Quarkus 3 needs 17. JDK 11 is not enough anymore. |
| [KAFKA_CLIENTS_CVE_BUFFER_POOL](versions/KAFKA_CLIENTS_CVE_BUFFER_POOL.md) | ERROR | HIGH | pom-dependency | CVE-2026-35554 — your producer ships messages to the wrong topic and never tells you. |
| [KAFKA_CLIENTS_CVE_CONFIG_PROVIDER](versions/KAFKA_CLIENTS_CVE_CONFIG_PROVIDER.md) | ERROR | HIGH | pom-dependency | CVE-2024-31141 — ConfigProvider was happy to read whatever file you named. |
| [KAFKA_CLIENTS_CVE_JNDI_LDAP](versions/KAFKA_CLIENTS_CVE_JNDI_LDAP.md) | ERROR | HIGH | pom-dependency | CVE-2023-25194 / CVE-2025-27818 — the Kafka spiritual successor to Log4Shell. |
| [KAFKA_CLIENTS_CVE_SASL_OAUTHBEARER](versions/KAFKA_CLIENTS_CVE_SASL_OAUTHBEARER.md) | ERROR | HIGH | pom-dependency | CVE-2025-27817 — your kafka-clients can be told to read any file on disk. |
| [KAFKA_CLIENTS_CVE_SCRAM_REPLAY](versions/KAFKA_CLIENTS_CVE_SCRAM_REPLAY.md) | WARNING | HIGH | pom-dependency | CVE-2024-56128 — SCRAM without TLS is a replay attack waiting to happen. |
| [KAFKA_CLIENTS_DRIFT_IN_MULTIMODULE](versions/KAFKA_CLIENTS_DRIFT_IN_MULTIMODULE.md) | WARNING | MEDIUM | pom-dependency | Two modules in the same reactor on two different kafka-clients is half a fix away from a classloader fight. |
| [KAFKA_CLIENTS_EOL](versions/KAFKA_CLIENTS_EOL.md) | ERROR | HIGH | pom-dependency | A Kafka client older than the bugs you'd file against it. |
| [KAFKA_CLIENTS_PRE_KIP679](versions/KAFKA_CLIENTS_PRE_KIP679.md) | WARNING | HIGH | pom-dependency | A client from before durable defaults — your producer is fire-and-forget unless you said otherwise. |
| [KAFKA_CLIENTS_SNAPSHOT](versions/KAFKA_CLIENTS_SNAPSHOT.md) | WARNING | HIGH | pom-dependency | A SNAPSHOT or milestone client in production is a contract you didn't sign. |
| [KAFKA_CLIENT_BROKER_LAG](versions/KAFKA_CLIENT_BROKER_LAG.md) | WARNING | CONTEXT | pom-property | Your client is forward-compatible with a broker eighteen months in the future — but missing four bug fixes. |
| [KAFKA_CLIENT_TYPO_GROUP](versions/KAFKA_CLIENT_TYPO_GROUP.md) | ERROR | HIGH | pom-dependency | There is no io.confluent:kafka-clients. There never has been. |
| [KAFKA_SCALA_BUNDLE_IMPORTED](versions/KAFKA_SCALA_BUNDLE_IMPORTED.md) | ERROR | HIGH | pom-dependency | You wanted a Kafka client. You pulled in Scala, the broker, and a small star system. |
| [KAFKA_STREAMS_CACHE_CONFIG_RENAMED](versions/KAFKA_STREAMS_CACHE_CONFIG_RENAMED.md) | WARNING | HIGH | bytecode + config-file + pom-dependency | cache.max.bytes.buffering was renamed in Streams 3.4 — using the old name silently disables your cache. |
| [KAFKA_TEST_UTILS_RUNTIME_SCOPE](versions/KAFKA_TEST_UTILS_RUNTIME_SCOPE.md) | WARNING | HIGH | pom-dependency | kafka-streams-test-utils in runtime scope ships an embedded broker into production. |
| [QUARKUS_EOL](versions/QUARKUS_EOL.md) | ERROR | HIGH | pom-dependency | Quarkus releases monthly; if you're not on an LTS, you're EOL within four weeks. |
| [QUARKUS_KAFKA_CLIENT_OVERRIDE](versions/QUARKUS_KAFKA_CLIENT_OVERRIDE.md) | WARNING | HIGH | pom-dependency | The Quarkus BOM pins kafka-clients; overriding it can derail native compilation. |
| [QUARKUS_KAFKA_EXTENSION_RENAMED](versions/QUARKUS_KAFKA_EXTENSION_RENAMED.md) | WARNING | HIGH | pom-dependency | quarkus-smallrye-reactive-messaging-kafka was renamed to quarkus-messaging-kafka — use the new name. |
| [QUARKUS_STREAMS_MISSING_CLIENT](versions/QUARKUS_STREAMS_MISSING_CLIENT.md) | ERROR | HIGH | pom-dependency | quarkus-kafka-streams requires quarkus-kafka-client — and won't tell you nicely. |
| [SMALLRYE_RM_OVERRIDE](versions/SMALLRYE_RM_OVERRIDE.md) | WARNING | HIGH | pom-dependency | Quarkus 3 ships SmallRye Reactive Messaging 4 — pinning your own version breaks the build steps. |
| [SPRING_BOOT_EOL](versions/SPRING_BOOT_EOL.md) | ERROR | HIGH | pom-dependency | An EOL Spring Boot means an EOL kafka-clients underneath, and you don't get to pick. |
| [SPRING_BOOT_KAFKA_CLIENT_OVERRIDE](versions/SPRING_BOOT_KAFKA_CLIENT_OVERRIDE.md) | WARNING | HIGH | pom-dependency + pom-property | Boot picks the kafka-clients spring-kafka was tested against; pinning your own is signing a private contract. |
| [SPRING_KAFKA_BOOT_MISMATCH](versions/SPRING_KAFKA_BOOT_MISMATCH.md) | ERROR | HIGH | pom-dependency + pom-property | Mixing spring-kafka and Spring Boot major versions is a slow-motion classpath collision. |
| [SPRING_KAFKA_DUPLICATE_DECLARATION](versions/SPRING_KAFKA_DUPLICATE_DECLARATION.md) | WARNING | HIGH | pom-dependency | Declaring spring-kafka and spring-boot-starter-kafka is one of them too many. |
| [SPRING_KAFKA_EOL](versions/SPRING_KAFKA_EOL.md) | ERROR | HIGH | pom-dependency | spring-kafka's OSS life is tied to Spring Boot's — and Boot 2.x stopped getting patches three years ago. |

### good-practices (24)

| Rule | Severity | Confidence | Detection | Tagline |
|---|---|---|---|---|
| [CONSUMER_EOS_READ_COMMITTED](good-practices/CONSUMER_EOS_READ_COMMITTED.md) | WARNING | MEDIUM | bytecode + config-file | Reading from a transactional producer with the default isolation is reading drafts. 🤝 |
| [CONSUMER_KIP_848_AWARENESS](good-practices/CONSUMER_KIP_848_AWARENESS.md) | WARNING | CONTEXT | config-file | KIP-848 is the future. It's just not your present yet. |
| [CONSUMER_POLL_TUNING_EXPLICIT](good-practices/CONSUMER_POLL_TUNING_EXPLICIT.md) | WARNING | CONTEXT | config-file | `max.poll.records` and `max.poll.interval.ms` should be deliberate, not whatever defaults you forgot were there. |
| [CONSUMER_STATIC_MEMBERSHIP](good-practices/CONSUMER_STATIC_MEMBERSHIP.md) | WARNING | CONTEXT | bytecode + config-file | Static membership turns N rolling-restart rebalances into zero. |
| [FOLLOWER_FETCHING_CLIENT_RACK](good-practices/FOLLOWER_FETCHING_CLIENT_RACK.md) | WARNING | CONTEXT | config-file | Cross-AZ fetches are billed by the gigabyte. `client.rack` is the line that turns that bill into local traffic. |
| [INFRA_BOOTSTRAP_SERVERS_NOT_LOCALHOST](good-practices/INFRA_BOOTSTRAP_SERVERS_NOT_LOCALHOST.md) | WARNING | MEDIUM | config-file | `localhost` in `src/main` is a deploy-time outage waiting for Friday. |
| [INFRA_QUARKUS_DEVSERVICES_PROFILE_SCOPED](good-practices/INFRA_QUARKUS_DEVSERVICES_PROFILE_SCOPED.md) | WARNING | HIGH | config-file | Dev Services live in `%dev.` and `%test.`. Anywhere else is a sidecar Kafka nobody told ops about. |
| [INFRA_SPRING_EMBEDDED_KAFKA_TEST_ONLY](good-practices/INFRA_SPRING_EMBEDDED_KAFKA_TEST_ONLY.md) | WARNING | HIGH | bytecode + pom-dependency | `EmbeddedKafkaBroker` is a test fixture. It does not belong in `src/main`. |
| [OBS_CLIENT_ID_SET](good-practices/OBS_CLIENT_ID_SET.md) | WARNING | MEDIUM | bytecode + config-file | `client.id` is the difference between `producer-1` and `orders-svc-prod-pod-3` in broker logs. |
| [OBS_INTERCEPTORS_CONFIGURED](good-practices/OBS_INTERCEPTORS_CONFIGURED.md) | WARNING | CONTEXT | bytecode + config-file | Interceptors are how the trace context survives the partition boundary. |
| [OBS_METRIC_REPORTERS_CONFIGURED](good-practices/OBS_METRIC_REPORTERS_CONFIGURED.md) | WARNING | CONTEXT | bytecode + config-file + pom-dependency | Kafka exposes 100+ metrics. Configure one reporter, or you publish to /dev/null. |
| [OBS_QUARKUS_CLIENT_ID_PREFIX](good-practices/OBS_QUARKUS_CLIENT_ID_PREFIX.md) | WARNING | HIGH | config-file | Quarkus has a `client-id` knob too. SmallRye RM won't write one for you. |
| [OBS_SPRING_CLIENT_ID_PREFIX](good-practices/OBS_SPRING_CLIENT_ID_PREFIX.md) | WARNING | HIGH | config-file | Spring Boot has a `client-id` knob. Use it before the broker logs see `producer-1` again. |
| [PRODUCER_DURABILITY_BUNDLE](good-practices/PRODUCER_DURABILITY_BUNDLE.md) | WARNING | MEDIUM | bytecode + config-file + pom-dependency | Durability is a five-key chord. Miss one note and the whole song goes flat. |
| [PRODUCER_THROUGHPUT_BUNDLE](good-practices/PRODUCER_THROUGHPUT_BUNDLE.md) | WARNING | MEDIUM | bytecode + config-file | A producer with default batching settings is one-record-at-a-time pretending to be a stream. |
| [PRODUCER_TRANSACTIONAL_BUNDLE](good-practices/PRODUCER_TRANSACTIONAL_BUNDLE.md) | WARNING | MEDIUM | bytecode + config-file | A `transactional.id` is a contract with the broker. Sign it properly or don't sign it. 🤝 |
| [STREAMS_APP_ID_STABLE_AND_VERSIONED](good-practices/STREAMS_APP_ID_STABLE_AND_VERSIONED.md) | WARNING | MEDIUM | config-file | `application.id` is your stream's identity. Treat it like a deployment artifact, not a runtime variable. |
| [STREAMS_EOS_V2_ENABLED](good-practices/STREAMS_EOS_V2_ENABLED.md) | WARNING | CONTEXT | config-file | `exactly_once_v2` is one config line and an entire set of operational obligations. 🤝 |
| [STREAMS_EXCEPTION_HANDLERS_WIRED](good-practices/STREAMS_EXCEPTION_HANDLERS_WIRED.md) | WARNING | HIGH | config-file | Every Streams app needs three handlers wired, not one missing. |
| [STREAMS_NAMED_OPERATORS](good-practices/STREAMS_NAMED_OPERATORS.md) | WARNING | HIGH | bytecode | Every stateful operator gets a stable name, or your topology is one edit away from data loss. |
| [STREAMS_NUM_THREADS_EXPLICIT](good-practices/STREAMS_NUM_THREADS_EXPLICIT.md) | WARNING | MEDIUM | config-file | `num.stream.threads` should be a sized choice, not a forgotten default. |
| [STREAMS_REPLICATION_FACTOR_EXPLICIT](good-practices/STREAMS_REPLICATION_FACTOR_EXPLICIT.md) | WARNING | MEDIUM | config-file | Replication factor of 3 (or broker default of -1) is the floor for a Streams app worth restarting. |
| [VERSIONS_LET_BOM_MANAGE_CLIENTS](good-practices/VERSIONS_LET_BOM_MANAGE_CLIENTS.md) | WARNING | HIGH | pom-dependency | Spring Boot, Quarkus, and Confluent all ship a BOM. Use it, don't fight it. |
| [VERSIONS_SCHEMA_REGISTRY_SERDE_PINNED](good-practices/VERSIONS_SCHEMA_REGISTRY_SERDE_PINNED.md) | WARNING | MEDIUM | pom-dependency | Schema Registry serdes must match the broker family, not your random Maven cache. |

## Cross-cutting indices

### Rules with a 🤝 Consult-a-friend block

These rules carry questions a reviewer should ask before applying the obvious fix. They cluster in EOS, transactions, isolation level, idempotence, and a handful of error-handler / observability rules where the silent-failure mode is non-obvious.

- [`CONSUMER_GROUP_INSTANCE_ID_MISSING`](kafka-clients/CONSUMER_GROUP_INSTANCE_ID_MISSING.md) — Without group.instance.id, every rolling restart is a full rebalance you didn't need.
- [`CONSUMER_ISOLATION_READ_UNCOMMITTED_WITH_TXN`](kafka-clients/CONSUMER_ISOLATION_READ_UNCOMMITTED_WITH_TXN.md) — read_uncommitted from a transactional topic is reading the draft, not the final.
- [`PRODUCER_ACKS_ONE`](kafka-clients/PRODUCER_ACKS_ONE.md) — acks=1 means "the leader saw it" — and then the leader died.
- [`PRODUCER_ACKS_ZERO`](kafka-clients/PRODUCER_ACKS_ZERO.md) — acks=0 is fire-and-pray.
- [`PRODUCER_IDEMPOTENCE_DISABLED`](kafka-clients/PRODUCER_IDEMPOTENCE_DISABLED.md) — Turning idempotence off in 2026 is undoing five years of work the client did for you.
- [`PRODUCER_MAX_IN_FLIGHT_TOO_HIGH`](kafka-clients/PRODUCER_MAX_IN_FLIGHT_TOO_HIGH.md) — The broker only remembers your last 5 sequences. Sixth one rewrites history.
- [`PRODUCER_RETRIES_ZERO`](kafka-clients/PRODUCER_RETRIES_ZERO.md) — retries=0 means every transient network blip is a permanent failure.
- [`PRODUCER_TXN_ID_WITHOUT_IDEMPOTENCE`](kafka-clients/PRODUCER_TXN_ID_WITHOUT_IDEMPOTENCE.md) — A transactional.id without idempotence is a transaction that isn't.
- [`STREAMS_COMMIT_INTERVAL_TOO_LOW`](kafka-streams/STREAMS_COMMIT_INTERVAL_TOO_LOW.md) — Committing every millisecond means the broker commits a million times a millisecond.
- [`STREAMS_DESER_HANDLER_BLANKET_CONTINUE`](kafka-streams/STREAMS_DESER_HANDLER_BLANKET_CONTINUE.md) — A custom `DeserializationExceptionHandler` that always returns `CONTINUE` is data loss with a stack trace.
- [`STREAMS_EOS_COMMIT_INTERVAL_SET`](kafka-streams/STREAMS_EOS_COMMIT_INTERVAL_SET.md) — Setting `commit.interval.ms` under EOS overrides a correctness-critical default.
- [`STREAMS_EOS_TRANSACTION_TIMEOUT_DEFAULT`](kafka-streams/STREAMS_EOS_TRANSACTION_TIMEOUT_DEFAULT.md) — 10s `transaction.timeout.ms` under EOS is a state-wipe cascade waiting for the first slow record.
- [`STREAMS_EOS_V1_DEPRECATED`](kafka-streams/STREAMS_EOS_V1_DEPRECATED.md) — `exactly_once` (v1) is a per-task producer. `exactly_once_v2` is the only one you should pick.
- [`STREAMS_GROUP_PROTOCOL_STREAMS_INCOMPAT`](kafka-streams/STREAMS_GROUP_PROTOCOL_STREAMS_INCOMPAT.md) — `group.protocol=streams` plus static membership is a `ConfigException` you can ship.
- [`STREAMS_NO_SHUTDOWN_HOOK`](kafka-streams/STREAMS_NO_SHUTDOWN_HOOK.md) — Without a shutdown hook, you commit offsets via SIGKILL.
- [`STREAMS_PROCESSING_EXCEPTION_HANDLER_MISSING`](kafka-streams/STREAMS_PROCESSING_EXCEPTION_HANDLER_MISSING.md) — A KIP-1034 handler is the only thing between a lambda NPE and a fleet-wide restart loop.
- [`STREAMS_PRODUCTION_HANDLER_MISSING`](kafka-streams/STREAMS_PRODUCTION_HANDLER_MISSING.md) — `default.production.exception.handler` unset means the first `RecordTooLargeException` kills the topology.
- [`STREAMS_PROD_HANDLER_BLANKET_CONTINUE`](kafka-streams/STREAMS_PROD_HANDLER_BLANKET_CONTINUE.md) — A `ProductionExceptionHandler` that always returns `CONTINUE` swallows every produce failure — including the ones that mean the broker is gone.
- [`STREAMS_REPLICATION_FACTOR_ONE`](kafka-streams/STREAMS_REPLICATION_FACTOR_ONE.md) — `replication.factor=1` is a state store with an expiration date.
- [`STREAMS_UNCAUGHT_HANDLER_DEPRECATED_API`](kafka-streams/STREAMS_UNCAUGHT_HANDLER_DEPRECATED_API.md) — `setUncaughtExceptionHandler(Thread.UncaughtExceptionHandler)` is a JDK API. Streams gave you a real one in KIP-671 — use it.
- [`SPRING_AFTER_ROLLBACK_PROCESSOR_DEFAULT`](spring-kafka/SPRING_AFTER_ROLLBACK_PROCESSOR_DEFAULT.md) — Transactional listeners don't use `DefaultErrorHandler` — they use `DefaultAfterRollbackProcessor`, with its own equally-bad default backoff.
- [`SPRING_BOOT_PRODUCER_ACKS_NOT_ALL`](spring-kafka/SPRING_BOOT_PRODUCER_ACKS_NOT_ALL.md) — `spring.kafka.producer.acks=1` ships durability away one record at a time.
- [`SPRING_DEH_DEFAULT_BACKOFF`](spring-kafka/SPRING_DEH_DEFAULT_BACKOFF.md) — `new DefaultErrorHandler()` retries ten times with zero delay — that's not a backoff, that's a tantrum.
- [`SPRING_DEH_NO_DLT_RECOVERER`](spring-kafka/SPRING_DEH_NO_DLT_RECOVERER.md) — `DefaultErrorHandler` with no recoverer logs your data loss and calls it a feature.
- [`SPRING_EHD_LISTENER_NULL_NOT_CHECKED`](spring-kafka/SPRING_EHD_LISTENER_NULL_NOT_CHECKED.md) — `ErrorHandlingDeserializer` returns `null` for poison pills — and your listener processes them as legitimate records.
- [`SPRING_KAFKA_TEMPLATE_SEND_BLOCKING_GET`](spring-kafka/SPRING_KAFKA_TEMPLATE_SEND_BLOCKING_GET.md) — `send(...).get()` turns an async producer back into a synchronous one, one record at a time.
- [`SPRING_KSTREAMS_BINDER_DEFAULT_DEH`](spring-kafka/SPRING_KSTREAMS_BINDER_DEFAULT_DEH.md) — Spring Cloud Stream Kafka Streams binder defaults to `logAndFail` — first poison pill takes down the function.
- [`SPRING_LISTENER_DIRECT_PRODUCER`](spring-kafka/SPRING_LISTENER_DIRECT_PRODUCER.md) — `new KafkaProducer(...)` inside a Spring app is a memory leak with delivery guarantees.
- [`SPRING_LISTENER_SWALLOWS_EXCEPTION`](spring-kafka/SPRING_LISTENER_SWALLOWS_EXCEPTION.md) — `try { ... } catch (Exception e) { log.error(e); }` inside a `@KafkaListener` deletes the error handler.
- [`SPRING_TRANSACTIONAL_WITHOUT_KTM`](spring-kafka/SPRING_TRANSACTIONAL_WITHOUT_KTM.md) — @Transactional + kafkaTemplate.send() without a transactional producer is decoration.
- [`QK_COMMIT_STRATEGY_IGNORE`](quarkus-kafka/QK_COMMIT_STRATEGY_IGNORE.md) — commit-strategy=ignore: you process everything, you remember nothing.
- [`QK_OUTGOING_ACKS_LE_ONE`](quarkus-kafka/QK_OUTGOING_ACKS_LE_ONE.md) — acks=1 trusts one broker. acks=0 trusts nothing.
- [`QK_TRANSACTIONAL_ID_NOT_UNIQUE`](quarkus-kafka/QK_TRANSACTIONAL_ID_NOT_UNIQUE.md) — A shared transactional.id is two producers fencing each other off forever.
- [`DESER_JSON_TYPE_INFO_NO_ALLOWLIST`](observability/DESER_JSON_TYPE_INFO_NO_ALLOWLIST.md) — `JsonDeserializer` with `__TypeId__` headers and no trusted-packages allowlist is RCE via a Kafka header.
- [`LAMBDA_RECOVERER_RETURNS_NULL`](observability/LAMBDA_RECOVERER_RETURNS_NULL.md) — A `BiConsumer<ConsumerRecord, Exception>` recoverer that does nothing is a black hole with extra steps.
- [`OBS_SPRING_MICROMETER_NOT_BOUND`](observability/OBS_SPRING_MICROMETER_NOT_BOUND.md) — A custom `ProducerFactory` without `MicrometerProducerListener` is `KafkaMetricsAutoConfiguration` waving goodbye.
- [`WARPSTREAM_BATCH_AND_LINGER_DEFAULTS`](warpstream/WARPSTREAM_BATCH_AND_LINGER_DEFAULTS.md) — Default `batch.size=16384` + `linger.ms=0` against object storage = one S3 PUT per record.
- [`WARPSTREAM_IDEMPOTENCE_ENABLED`](warpstream/WARPSTREAM_IDEMPOTENCE_ENABLED.md) — `enable.idempotence=true` against ~250ms object-storage latency = 5 in-flight × 4 round-trips/sec = 20 RPS.
- [`CONSUMER_EOS_READ_COMMITTED`](good-practices/CONSUMER_EOS_READ_COMMITTED.md) — Reading from a transactional producer with the default isolation is reading drafts.
- [`PRODUCER_TRANSACTIONAL_BUNDLE`](good-practices/PRODUCER_TRANSACTIONAL_BUNDLE.md) — A `transactional.id` is a contract with the broker. Sign it properly or don't sign it.
- [`STREAMS_EOS_V2_ENABLED`](good-practices/STREAMS_EOS_V2_ENABLED.md) — `exactly_once_v2` is one config line and an entire set of operational obligations.

### EOS / transaction family

Rules touching exactly-once semantics, transactional producers, isolation level, idempotence, or static-membership / group-instance-id (a transactional client's identity). Read these together when designing an EOS topology — the constraints chain.

**kafka-clients**
- [`PRODUCER_ACKS_ZERO`](kafka-clients/PRODUCER_ACKS_ZERO.md) 🤝 — acks=0 is fire-and-pray.
- [`PRODUCER_ACKS_ONE`](kafka-clients/PRODUCER_ACKS_ONE.md) 🤝 — acks=1 means "the leader saw it" — and then the leader died.
- [`PRODUCER_IDEMPOTENCE_DISABLED`](kafka-clients/PRODUCER_IDEMPOTENCE_DISABLED.md) 🤝 — Turning idempotence off in 2026 is undoing five years of work the client did for you.
- [`PRODUCER_MAX_IN_FLIGHT_TOO_HIGH`](kafka-clients/PRODUCER_MAX_IN_FLIGHT_TOO_HIGH.md) 🤝 — The broker only remembers your last 5 sequences. Sixth one rewrites history.
- [`PRODUCER_RETRIES_ZERO`](kafka-clients/PRODUCER_RETRIES_ZERO.md) 🤝 — retries=0 means every transient network blip is a permanent failure.
- [`PRODUCER_TXN_ID_WITHOUT_IDEMPOTENCE`](kafka-clients/PRODUCER_TXN_ID_WITHOUT_IDEMPOTENCE.md) 🤝 — A transactional.id without idempotence is a transaction that isn't.
- [`CONSUMER_ISOLATION_READ_UNCOMMITTED_WITH_TXN`](kafka-clients/CONSUMER_ISOLATION_READ_UNCOMMITTED_WITH_TXN.md) 🤝 — read_uncommitted from a transactional topic is reading the draft, not the final.
- [`CONSUMER_GROUP_INSTANCE_ID_MISSING`](kafka-clients/CONSUMER_GROUP_INSTANCE_ID_MISSING.md) 🤝 — Without group.instance.id, every rolling restart is a full rebalance you didn't need.

**kafka-streams**
- [`STREAMS_EOS_V1_DEPRECATED`](kafka-streams/STREAMS_EOS_V1_DEPRECATED.md) 🤝 — `exactly_once` (v1) is a per-task producer. `exactly_once_v2` is the only one you should pick.
- [`STREAMS_EOS_COMMIT_INTERVAL_SET`](kafka-streams/STREAMS_EOS_COMMIT_INTERVAL_SET.md) 🤝 — Setting `commit.interval.ms` under EOS overrides a correctness-critical default.
- [`STREAMS_EOS_TRANSACTION_TIMEOUT_DEFAULT`](kafka-streams/STREAMS_EOS_TRANSACTION_TIMEOUT_DEFAULT.md) 🤝 — 10s `transaction.timeout.ms` under EOS is a state-wipe cascade waiting for the first slow record.
- [`STREAMS_MAX_POLL_LT_TRANSACTION_TIMEOUT`](kafka-streams/STREAMS_MAX_POLL_LT_TRANSACTION_TIMEOUT.md) — Under EOS, `max.poll.interval.ms` smaller than `transaction.timeout.ms` is a guaranteed cascade.
- [`STREAMS_GROUP_PROTOCOL_NOT_STREAMS`](kafka-streams/STREAMS_GROUP_PROTOCOL_NOT_STREAMS.md) — On AK 4.2+ / CP 8.2+, classic rebalancing is 50-80% slower for no reason.
- [`STREAMS_GROUP_PROTOCOL_STREAMS_INCOMPAT`](kafka-streams/STREAMS_GROUP_PROTOCOL_STREAMS_INCOMPAT.md) 🤝 — `group.protocol=streams` plus static membership is a `ConfigException` you can ship.

**spring-kafka**
- [`SPRING_TRANSACTIONAL_WITHOUT_KTM`](spring-kafka/SPRING_TRANSACTIONAL_WITHOUT_KTM.md) 🤝 — @Transactional + kafkaTemplate.send() without a transactional producer is decoration.
- [`SPRING_AFTER_ROLLBACK_PROCESSOR_DEFAULT`](spring-kafka/SPRING_AFTER_ROLLBACK_PROCESSOR_DEFAULT.md) 🤝 — Transactional listeners don't use `DefaultErrorHandler` — they use `DefaultAfterRollbackProcessor`, with its own equally-bad default backoff.

**quarkus-kafka**
- [`QK_TRANSACTIONAL_ID_NOT_UNIQUE`](quarkus-kafka/QK_TRANSACTIONAL_ID_NOT_UNIQUE.md) 🤝 — A shared transactional.id is two producers fencing each other off forever.

**warpstream**
- [`WARPSTREAM_IDEMPOTENCE_ENABLED`](warpstream/WARPSTREAM_IDEMPOTENCE_ENABLED.md) 🤝 — `enable.idempotence=true` against ~250ms object-storage latency = 5 in-flight × 4 round-trips/sec = 20 RPS.

**good-practices**
- [`PRODUCER_TRANSACTIONAL_BUNDLE`](good-practices/PRODUCER_TRANSACTIONAL_BUNDLE.md) 🤝 — A `transactional.id` is a contract with the broker. Sign it properly or don't sign it.
- [`STREAMS_EOS_V2_ENABLED`](good-practices/STREAMS_EOS_V2_ENABLED.md) 🤝 — `exactly_once_v2` is one config line and an entire set of operational obligations.
- [`CONSUMER_EOS_READ_COMMITTED`](good-practices/CONSUMER_EOS_READ_COMMITTED.md) 🤝 — Reading from a transactional producer with the default isolation is reading drafts.
- [`CONSUMER_STATIC_MEMBERSHIP`](good-practices/CONSUMER_STATIC_MEMBERSHIP.md) — Static membership turns N rolling-restart rebalances into zero.

### CVE family

All rules in `versions/` matching `KAFKA_CLIENTS_CVE_*`, plus `observability/DESER_JSON_TYPE_INFO_NO_ALLOWLIST` (CVE-2023-34040). Severity is `ERROR` unless the CVE has been graded `MODERATE` upstream.

- [`KAFKA_CLIENTS_CVE_SASL_OAUTHBEARER`](versions/KAFKA_CLIENTS_CVE_SASL_OAUTHBEARER.md) — CVE-2025-27817 — your kafka-clients can be told to read any file on disk.
- [`KAFKA_CLIENTS_CVE_JNDI_LDAP`](versions/KAFKA_CLIENTS_CVE_JNDI_LDAP.md) — CVE-2023-25194 / CVE-2025-27818 — the Kafka spiritual successor to Log4Shell.
- [`KAFKA_CLIENTS_CVE_CONFIG_PROVIDER`](versions/KAFKA_CLIENTS_CVE_CONFIG_PROVIDER.md) — CVE-2024-31141 — ConfigProvider was happy to read whatever file you named.
- [`KAFKA_CLIENTS_CVE_BUFFER_POOL`](versions/KAFKA_CLIENTS_CVE_BUFFER_POOL.md) — CVE-2026-35554 — your producer ships messages to the wrong topic and never tells you.
- [`KAFKA_CLIENTS_CVE_SCRAM_REPLAY`](versions/KAFKA_CLIENTS_CVE_SCRAM_REPLAY.md) — CVE-2024-56128 — SCRAM without TLS is a replay attack waiting to happen.
- [`DESER_JSON_TYPE_INFO_NO_ALLOWLIST`](observability/DESER_JSON_TYPE_INFO_NO_ALLOWLIST.md) — `JsonDeserializer` with `__TypeId__` headers and no trusted-packages allowlist is RCE via a Kafka header.

## Conventions for future contributors

On adding a new rule:

- Pick the right directory. `kafka-clients` is the lowest level; if the rule is framework-specific, prefer the framework dir. Cross-cutting concerns (interceptors, lambda hygiene, deserialization safety, async/reactive bridge) live under `observability/`.
- Follow the doc template: `**Severity**` / `**Confidence**` / `**Detection**` / `**Tagline**` header lines, then TL;DR, mechanism, operational impact, how to fix, when it's a false positive, detection strategy, references.
- Severity values: `ERROR`, `WARNING`, `INFO`. Use conditional phrasing in the same line if it depends on a value (`ERROR for acks=0, WARNING for acks=1`).
- Confidence values: `HIGH`, `MEDIUM`, `CONTEXT`. Default to `MEDIUM` if you find yourself reaching for it.
- Detection values: atomic strategies joined by ` + ` in alphabetical order. No invented strategy names.
- Add a `## Consult a friend?` block iff the fix has a non-obvious correctness cost (EOS / transactions / isolation / idempotence).
- ID convention: `<FRAMEWORK_OR_ENGINE>_<SURFACE>_<DEFECT>`, ALL_CAPS, underscore-separated. No abbreviations beyond established ones (`EOS`, `DEH`, `EHD`, `DLT`, `SR`, `OBS`, `INFRA`, `QK`).
- Update this catalog (`_CATALOG.md`) **and** the directory's `_INDEX.md`. The catalog is the canonical view; the index is the in-directory navigation.
