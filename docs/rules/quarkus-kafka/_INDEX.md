# Quarkus / SmallRye Reactive Messaging Kafka — Rule Index

Build-time linter rules targeting Quarkus applications using `quarkus-messaging-kafka` (SmallRye Reactive Messaging) and `quarkus-kafka-streams`.

## Legend

**Severity**
- `ERROR` — data loss, crashes, or production incidents almost certain.
- `WARNING` — degraded behavior, sometimes legitimate, operator judgement required.

**Confidence**
- `HIGH` — single signal, low false-positive risk.
- `MEDIUM` — needs context (config + bytecode cross-check).
- `CONTEXT` — interprocedural / semantic, may need user suppression.

**Detection** — `bytecode`, `annotation`, `config-file`, or a combination (` + `, alphabetical).

A trailing 🤝 in the tagline means the rule's doc carries a `## Consult a friend?` block — read it before changing prod config.

## Catalog (36)

### Annotation / bytecode — consumer side

| Rule | Severity | Confidence | Detection | Tagline |
|---|---|---|---|---|
| [QK_BLOCKING_ON_IO_THREAD](./QK_BLOCKING_ON_IO_THREAD.md) | ERROR | HIGH | annotation + bytecode | @Blocking on the IO thread isn't reactive — it's surrender. |
| [QK_UNNECESSARY_BLOCKING](./QK_UNNECESSARY_BLOCKING.md) | WARNING | MEDIUM | annotation + bytecode | @Blocking on pure compute is a free thread hop you pay for. |
| [QK_MESSAGE_NEVER_ACKED](./QK_MESSAGE_NEVER_ACKED.md) | ERROR | HIGH | bytecode | A Message<T> you don't ack is a partition you'll never advance. |
| [QK_INCOMING_RETURNS_VOID](./QK_INCOMING_RETURNS_VOID.md) | WARNING | MEDIUM | bytecode | Void return = pre-processing ack = at-most-once if you blink wrong. |
| [QK_AWAIT_INDEFINITELY_ON_IO](./QK_AWAIT_INDEFINITELY_ON_IO.md) | ERROR | HIGH | annotation + bytecode | await().indefinitely() in a reactive pipeline is a deadlock waiting for tomorrow. |
| [QK_CHECKED_EXCEPTION_FROM_INCOMING](./QK_CHECKED_EXCEPTION_FROM_INCOMING.md) | WARNING | HIGH | bytecode | A checked exception that escapes @Incoming is a nack you didn't plan for. |

### Annotation / bytecode — producer side

| Rule | Severity | Confidence | Detection | Tagline |
|---|---|---|---|---|
| [QK_EMITTER_NO_ONOVERFLOW](./QK_EMITTER_NO_ONOVERFLOW.md) | WARNING | HIGH | annotation + bytecode | 256 messages is not your back-pressure plan. |
| [QK_EMITTER_UNBOUNDED_BUFFER](./QK_EMITTER_UNBOUNDED_BUFFER.md) | ERROR | HIGH | annotation | UNBOUNDED_BUFFER is the docs' own [DANGER ZONE]. Read that as: OOM. |
| [QK_BACKPRESSURE_NONE_STRATEGY](./QK_BACKPRESSURE_NONE_STRATEGY.md) | WARNING | HIGH | annotation | @OnOverflow(NONE) is reactive in name only. |
| [QK_MUTINY_UNI_NEVER_SUBSCRIBED](./QK_MUTINY_UNI_NEVER_SUBSCRIBED.md) | ERROR | HIGH | bytecode | A Uni you don't subscribe to is a message you don't send. |
| [QK_PRODUCER_NO_KEY](./QK_PRODUCER_NO_KEY.md) | WARNING | MEDIUM | bytecode | No key, no partition affinity, no ordering. Fine for fire-hoses, fatal for streams. |

### Config-file driven (channel-level)

| Rule | Severity | Confidence | Detection | Tagline |
|---|---|---|---|---|
| [QK_FAILURE_STRATEGY_IGNORE](./QK_FAILURE_STRATEGY_IGNORE.md) | ERROR | HIGH | config-file | failure-strategy=ignore is data loss with a smile. |
| [QK_FAILURE_STRATEGY_FAIL_NO_DLQ](./QK_FAILURE_STRATEGY_FAIL_NO_DLQ.md) | WARNING | MEDIUM | config-file | The default fails loud — make sure you actually want the channel to die. |
| [QK_FAILURE_DLQ_NO_TOPIC](./QK_FAILURE_DLQ_NO_TOPIC.md) | WARNING | HIGH | config-file | `failure-strategy=dead-letter-queue` with no `dead-letter-queue.topic` sends every failure to a topic named `dead-letter-topic-$channel` — and no one will think to look there. |
| [QK_FAILURE_FAIL_PROD_NO_HEALTH](./QK_FAILURE_FAIL_PROD_NO_HEALTH.md) | WARNING | MEDIUM | config-file | `failure-strategy=fail` in production with no health-driven restart and no DLQ on the side is "one bad record = service offline until I notice." |
| [QK_COMMIT_STRATEGY_IGNORE](./QK_COMMIT_STRATEGY_IGNORE.md) | WARNING | HIGH | config-file | commit-strategy=ignore: you process everything, you remember nothing. 🤝 |
| [QK_AUTO_COMMIT_ENABLED](./QK_AUTO_COMMIT_ENABLED.md) | WARNING | HIGH | config-file | Let SmallRye commit. Don't hand the wheel to Kafka. |
| [QK_FAIL_ON_DESER_NO_DLQ](./QK_FAIL_ON_DESER_NO_DLQ.md) | ERROR | HIGH | config-file | One poison pill, one dead channel — unless you've planned for it. |
| [QK_MISSING_VALUE_DESERIALIZER](./QK_MISSING_VALUE_DESERIALIZER.md) | ERROR | HIGH | config-file | No value.deserializer? Hope you wanted Strings. |
| [QK_THROTTLED_HEALTH_DISABLED](./QK_THROTTLED_HEALTH_DISABLED.md) | WARNING | HIGH | config-file | Disabling unprocessed-record-max-age is welcoming the OOM in. |
| [QK_HEALTH_DISABLED](./QK_HEALTH_DISABLED.md) | WARNING | HIGH | config-file | A channel without health checks is a channel without an alarm. |
| [QK_TRACING_DISABLED](./QK_TRACING_DISABLED.md) | WARNING | MEDIUM | config-file | tracing-enabled=false is "I solemnly swear to debug from logs alone." |
| [QK_AUTO_OFFSET_RESET_LATEST](./QK_AUTO_OFFSET_RESET_LATEST.md) | WARNING | MEDIUM | config-file | auto.offset.reset=latest skips everything on a fresh consumer group. |
| [QK_MISSING_TOPIC](./QK_MISSING_TOPIC.md) | WARNING | MEDIUM | config-file | No topic property? Hope your channel name is the topic name you wanted. |
| [QK_CHANNEL_NAME_COLLISION](./QK_CHANNEL_NAME_COLLISION.md) | ERROR | HIGH | bytecode + config-file | Same channel name for in and out wires your app to itself. |
| [QK_PARTITIONS_DEPRECATED](./QK_PARTITIONS_DEPRECATED.md) | WARNING | HIGH | config-file | `partitions` is dead. Long live `concurrency`. |

### Config-file driven (producer / durability)

| Rule | Severity | Confidence | Detection | Tagline |
|---|---|---|---|---|
| [QK_OUTGOING_ACKS_LE_ONE](./QK_OUTGOING_ACKS_LE_ONE.md) | ERROR for acks=0, WARNING for acks=1 | HIGH | config-file | acks=1 trusts one broker. acks=0 trusts nothing. 🤝 |
| [QK_WAIT_FOR_WRITE_COMPLETION_FALSE](./QK_WAIT_FOR_WRITE_COMPLETION_FALSE.md) | WARNING | HIGH | config-file | waitForWriteCompletion=false un-does acks=all from the Quarkus side. |
| [QK_TRANSACTIONAL_ID_NOT_UNIQUE](./QK_TRANSACTIONAL_ID_NOT_UNIQUE.md) | ERROR | HIGH | config-file | A shared transactional.id is two producers fencing each other off forever. 🤝 |

### Schema registry

| Rule | Severity | Confidence | Detection | Tagline |
|---|---|---|---|---|
| [QK_SCHEMA_REGISTRY_URL_MISSING](./QK_SCHEMA_REGISTRY_URL_MISSING.md) | ERROR | HIGH | config-file | Avro without a schema registry URL is just bytes pretending to be schema'd. |
| [QK_AUTO_REGISTER_SCHEMAS](./QK_AUTO_REGISTER_SCHEMAS.md) | WARNING | HIGH | config-file | auto.register.schemas in prod = anyone can mint a new schema. Anyone. |

### Kafka Streams (`quarkus-kafka-streams`)

| Rule | Severity | Confidence | Detection | Tagline |
|---|---|---|---|---|
| [QK_KSTREAMS_NO_APPLICATION_ID](./QK_KSTREAMS_NO_APPLICATION_ID.md) | ERROR | HIGH | config-file | A Streams app without an application-id is a consumer group named "guess". |
| [QK_KSTREAMS_MULTIPLE_TOPOLOGY_BEANS](./QK_KSTREAMS_MULTIPLE_TOPOLOGY_BEANS.md) | ERROR | HIGH | annotation + bytecode | One Topology to rule them all — extras get ignored. |

### Dev mode / environment hygiene

| Rule | Severity | Confidence | Detection | Tagline |
|---|---|---|---|---|
| [QK_DEVSERVICES_IN_PROD](./QK_DEVSERVICES_IN_PROD.md) | ERROR | HIGH | config-file | Dev Services in production is a Kafka in a sidecar that nobody told ops about. |
| [QK_LOCALHOST_IN_PROD](./QK_LOCALHOST_IN_PROD.md) | ERROR | HIGH | config-file | localhost:9092 in production: this isn't your laptop. |

### Native image

| Rule | Severity | Confidence | Detection | Tagline |
|---|---|---|---|---|
| [QK_NATIVE_REFLECTION_MISSING](./QK_NATIVE_REFLECTION_MISSING.md) | WARNING | MEDIUM | annotation + bytecode | GraalVM doesn't read your mind — register that class. |

## Cross-cutting detection notes

- **Profile-aware config parsing**: rules touching `application.properties` MUST understand `%dev.`, `%test.`, `%prod.` prefixes. A key set under `%dev.` is not a prod problem; an unprofiled key applies everywhere.
- **YAML support**: Quarkus also reads `application.yml`. The detector normalizes both.
- **Multiple connectors**: the rules assume `connector=smallrye-kafka`. Other connectors (`smallrye-in-memory`, `smallrye-amqp`, …) should not trigger Kafka-specific rules.
- **Suppression**: support inline suppression in properties via leading comments (`# kafka-lint:disable QK_FAILURE_STRATEGY_IGNORE`) and on Java elements via `@KafkaLintIgnore("QK_X")` (extension point).

## SmallRye / Quarkus version anchors

- SmallRye Reactive Messaging 4.x (Quarkus 3.x): `commit-strategy` defaults to `throttled` when `enable.auto.commit` is false (`ignore` otherwise).
- `partitions` deprecated in favor of `concurrency` since SmallRye Reactive Messaging 3.x.
- `@RunOnVirtualThread` requires JDK 21+ and Quarkus 3.2+.
- `KafkaTransactions<T>` API stable since Quarkus 2.16.
- Apicurio v2 client URL format: `http://host/apis/registry/v2` (v3 changes the path; not yet covered).

## Related catalogs

- [`../observability/`](../observability/) — `ASYNC_*`, `LAMBDA_*`, `DESER_*` rules cross-apply to reactive Quarkus handlers.
- [`../versions/`](../versions/) — `QUARKUS_EOL`, `QUARKUS_KAFKA_EXTENSION_RENAMED`, `QUARKUS_KAFKA_CLIENT_OVERRIDE`, `QUARKUS_STREAMS_MISSING_CLIENT`, `SMALLRYE_RM_OVERRIDE`.
- [`../good-practices/`](../good-practices/) — `OBS_QUARKUS_CLIENT_ID_PREFIX`, `INFRA_QUARKUS_DEVSERVICES_PROFILE_SCOPED`.
