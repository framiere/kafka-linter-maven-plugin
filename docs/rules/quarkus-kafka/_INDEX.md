# Quarkus / SmallRye Reactive Messaging Kafka — Rule Index

Build-time linter rules targeting Quarkus apps using `quarkus-messaging-kafka` (SmallRye Reactive Messaging) and `quarkus-kafka-streams`.

Severity: **ERROR** = data loss, crashes, or production incidents almost certain. **WARNING** = degraded behavior, sometimes legitimate, operator judgement required.
Confidence: **HIGH** = single signal, low false-positive risk. **MEDIUM** = needs context (config + bytecode cross-check). **CONTEXT** = interprocedural / semantic, may need user suppression.

## Annotation / Bytecode-driven (consumer side)

| Rule | Severity | Confidence | Summary |
|---|---|---|---|
| [QK_BLOCKING_ON_IO_THREAD](QK_BLOCKING_ON_IO_THREAD.md) | ERROR | HIGH | JDBC/JPA/Thread.sleep in an `@Incoming` method without `@Blocking` or `@RunOnVirtualThread`. |
| [QK_UNNECESSARY_BLOCKING](QK_UNNECESSARY_BLOCKING.md) | WARNING | MEDIUM | `@Blocking` annotation on a method that doesn't actually block. |
| [QK_MESSAGE_NEVER_ACKED](QK_MESSAGE_NEVER_ACKED.md) | ERROR | HIGH | `Message<T>` parameter handler with paths that exit without `ack()` or `nack()`. |
| [QK_INCOMING_RETURNS_VOID](QK_INCOMING_RETURNS_VOID.md) | WARNING | MEDIUM | `void` `@Incoming` method that fires async work and returns before completion. |
| [QK_AWAIT_INDEFINITELY_ON_IO](QK_AWAIT_INDEFINITELY_ON_IO.md) | ERROR | HIGH | `Uni.await().indefinitely()` inside a reactive handler on the IO thread. |
| [QK_CHECKED_EXCEPTION_FROM_INCOMING](QK_CHECKED_EXCEPTION_FROM_INCOMING.md) | WARNING | HIGH | `@Incoming` method declares `throws CheckedException` with default `failure-strategy=fail`. |

## Annotation / Bytecode-driven (producer side)

| Rule | Severity | Confidence | Summary |
|---|---|---|---|
| [QK_EMITTER_NO_ONOVERFLOW](QK_EMITTER_NO_ONOVERFLOW.md) | WARNING | HIGH | `Emitter`/`MutinyEmitter` injected without explicit `@OnOverflow` — implicit 256-entry buffer. |
| [QK_EMITTER_UNBOUNDED_BUFFER](QK_EMITTER_UNBOUNDED_BUFFER.md) | ERROR | HIGH | `@OnOverflow(UNBOUNDED_BUFFER)` — docs' own `[DANGER ZONE]`. |
| [QK_BACKPRESSURE_NONE_STRATEGY](QK_BACKPRESSURE_NONE_STRATEGY.md) | WARNING | HIGH | `@OnOverflow(NONE)` skips back-pressure entirely. |
| [QK_MUTINY_UNI_NEVER_SUBSCRIBED](QK_MUTINY_UNI_NEVER_SUBSCRIBED.md) | ERROR | HIGH | `MutinyEmitter.send()` result discarded without subscription. |
| [QK_PRODUCER_NO_KEY](QK_PRODUCER_NO_KEY.md) | WARNING | MEDIUM | Producing entity records without a key → ordering lost. |

## Config-file driven (channel-level)

| Rule | Severity | Confidence | Summary |
|---|---|---|---|
| [QK_FAILURE_STRATEGY_IGNORE](QK_FAILURE_STRATEGY_IGNORE.md) | ERROR | HIGH | `failure-strategy=ignore` → silent data loss. |
| [QK_FAILURE_STRATEGY_FAIL_NO_DLQ](QK_FAILURE_STRATEGY_FAIL_NO_DLQ.md) | WARNING | MEDIUM | Default `fail` strategy with no DLQ → channel dies on first nack. |
| [QK_COMMIT_STRATEGY_IGNORE](QK_COMMIT_STRATEGY_IGNORE.md) | WARNING | HIGH | `commit-strategy=ignore` without auto-commit or transactional path → never commits. |
| [QK_AUTO_COMMIT_ENABLED](QK_AUTO_COMMIT_ENABLED.md) | WARNING | HIGH | `enable.auto.commit=true` — Kafka commits on wall-clock, not on ack. |
| [QK_FAIL_ON_DESER_NO_DLQ](QK_FAIL_ON_DESER_NO_DLQ.md) | ERROR | HIGH | Default deser-failure handling without DLQ → poison pill crashloop. |
| [QK_MISSING_VALUE_DESERIALIZER](QK_MISSING_VALUE_DESERIALIZER.md) | ERROR | HIGH | `value.deserializer` unset and autodetection can't pick one. |
| [QK_THROTTLED_HEALTH_DISABLED](QK_THROTTLED_HEALTH_DISABLED.md) | WARNING | HIGH | `throttled.unprocessed-record-max-age.ms ≤ 0` disables stuck-record OOM safeguard. |
| [QK_HEALTH_DISABLED](QK_HEALTH_DISABLED.md) | WARNING | HIGH | `health-enabled=false` AND `health-readiness-enabled=false` — channel invisible to probes. |
| [QK_TRACING_DISABLED](QK_TRACING_DISABLED.md) | WARNING | MEDIUM | `tracing-enabled=false` — trace propagation broken across the channel. |
| [QK_AUTO_OFFSET_RESET_LATEST](QK_AUTO_OFFSET_RESET_LATEST.md) | WARNING | MEDIUM | `auto.offset.reset=latest` (default) skips history on a new group. |
| [QK_MISSING_TOPIC](QK_MISSING_TOPIC.md) | WARNING | MEDIUM | No `topic`/`topics`/`pattern` set → channel name used as fallback. |
| [QK_CHANNEL_NAME_COLLISION](QK_CHANNEL_NAME_COLLISION.md) | ERROR | HIGH | Same channel name on incoming and outgoing wires app to itself. |
| [QK_PARTITIONS_DEPRECATED](QK_PARTITIONS_DEPRECATED.md) | WARNING | HIGH | `partitions` property is deprecated; use `concurrency`. |

## Config-file driven (producer / durability)

| Rule | Severity | Confidence | Summary |
|---|---|---|---|
| [QK_OUTGOING_ACKS_LE_ONE](QK_OUTGOING_ACKS_LE_ONE.md) | WARNING | HIGH | `acks=0` or `acks=1` weakens durability; SmallRye's default is `1`. |
| [QK_WAIT_FOR_WRITE_COMPLETION_FALSE](QK_WAIT_FOR_WRITE_COMPLETION_FALSE.md) | WARNING | HIGH | `waitForWriteCompletion=false` un-does `acks=all` from the upstream side. |
| [QK_TRANSACTIONAL_ID_NOT_UNIQUE](QK_TRANSACTIONAL_ID_NOT_UNIQUE.md) | ERROR | MEDIUM | `transactional.id` shared across replicas → fence loop. |

## Schema registry

| Rule | Severity | Confidence | Summary |
|---|---|---|---|
| [QK_SCHEMA_REGISTRY_URL_MISSING](QK_SCHEMA_REGISTRY_URL_MISSING.md) | ERROR | HIGH | Avro/Protobuf deserializer configured without registry URL. |
| [QK_AUTO_REGISTER_SCHEMAS](QK_AUTO_REGISTER_SCHEMAS.md) | WARNING | HIGH | `auto.register.schemas=true` in prod → schema sprawl, unreviewed evolutions. |

## Kafka Streams (quarkus-kafka-streams)

| Rule | Severity | Confidence | Summary |
|---|---|---|---|
| [QK_KSTREAMS_NO_APPLICATION_ID](QK_KSTREAMS_NO_APPLICATION_ID.md) | ERROR | HIGH | No `quarkus.kafka-streams.application-id` set → defaults collide. |
| [QK_KSTREAMS_MULTIPLE_TOPOLOGY_BEANS](QK_KSTREAMS_MULTIPLE_TOPOLOGY_BEANS.md) | ERROR | HIGH | Multiple `@Produces Topology` CDI beans → ambiguous or silently dropped. |

## Dev mode / environment hygiene

| Rule | Severity | Confidence | Summary |
|---|---|---|---|
| [QK_DEVSERVICES_IN_PROD](QK_DEVSERVICES_IN_PROD.md) | ERROR | HIGH | `quarkus.kafka.devservices.enabled=true` unprofiled → ephemeral broker in prod. |
| [QK_LOCALHOST_IN_PROD](QK_LOCALHOST_IN_PROD.md) | ERROR | HIGH | `kafka.bootstrap.servers=localhost:9092` in prod/unprofiled config. |

## Native image

| Rule | Severity | Confidence | Summary |
|---|---|---|---|
| [QK_NATIVE_REFLECTION_MISSING](QK_NATIVE_REFLECTION_MISSING.md) | WARNING | MEDIUM | Custom Serializer/Deserializer/Partitioner referenced by string without `@RegisterForReflection`. |

## Cross-cutting detection notes

- **Profile-aware config parsing**: rules touching `application.properties` MUST understand `%dev.`, `%test.`, `%prod.` prefixes. A key set under `%dev.` is not a prod problem; an unprofiled key applies everywhere.
- **YAML support**: Quarkus also reads `application.yml`. Detector should normalize both.
- **Multiple connectors**: the rules assume `connector=smallrye-kafka`. Other connectors (`smallrye-in-memory`, `smallrye-amqp`, etc.) should not trigger Kafka-specific rules.
- **Suppression**: support inline suppression in properties via leading comments (`# kafka-lint:disable QK_FAILURE_STRATEGY_IGNORE`) and on Java elements via `@KafkaLintIgnore("QK_X")` (extension point).

## SmallRye / Quarkus version anchors

- SmallRye Reactive Messaging 4.x (Quarkus 3.x): `commit-strategy` defaults to `throttled` when `enable.auto.commit` is false (`ignore` otherwise).
- `partitions` deprecated in favor of `concurrency` since SmallRye Reactive Messaging 3.x.
- `@RunOnVirtualThread` requires JDK 21+ and Quarkus 3.2+.
- `KafkaTransactions<T>` API stable since Quarkus 2.16.
- Apicurio v2 client URL format: `http://host/apis/registry/v2` (v3 changes the path; not yet covered).
