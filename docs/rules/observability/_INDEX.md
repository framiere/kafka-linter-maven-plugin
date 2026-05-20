# Observability, Error Handlers, Deserialization & Lambda Anti-Patterns

This catalog covers four overlapping concerns that are easy to get wrong in production Kafka deployments:

1. **Monitoring & observability** — interceptors, metric reporters, Micrometer/OpenTelemetry wiring, consumer lag visibility.
2. **Error handlers** — Spring Kafka (`DefaultErrorHandler`, `CommonErrorHandler`, `DefaultAfterRollbackProcessor`), Kafka Streams (`DeserializationExceptionHandler`, `ProductionExceptionHandler`, `StreamsUncaughtExceptionHandler`), Quarkus / SmallRye Reactive Messaging (`failure-strategy`).
3. **Deserialization safety** — `ErrorHandlingDeserializer`, the `springDeserializerExceptionKey` header convention, `JsonDeserializer` type-info attacks (CVE-2023-34040), Schema Registry credentials.
4. **Lambdas as silent error swallowers** — empty `Callback` producer lambdas, `peek` used as an error log, recoverer lambdas that "just log," `Mono.block()` and `void`+`subscribe()` patterns.

These rules share a common shape: a configuration default or coding pattern that *looks* safe (compiles, tests pass, looks idiomatic) but produces silent data loss, unrecoverable restart loops, or invisible failures in production.

## Legend

**Severity**

- `ERROR` — production data loss or unrecoverable failure mode. Fix before shipping.
- `WARNING` — silent failure mode, observability gap, or footgun. Fix unless explicitly opted into.
- `INFO` — best-practice suggestion.

**Confidence**

- `HIGH` — descriptor-level match. Very few false positives.
- `MEDIUM` — heuristic. Requires human review.
- `CONTEXT` — depends on runtime / project intent; the linter cannot decide alone.

**Detection**

- `config-file` — scan `application.yml/.properties`, `bootstrap.yml`, etc.
- `bytecode` — ASM-level inspection of compiled classes (annotations, method bodies, lambda synthetic methods).
- `pom-dependency` — `pom.xml` dependency presence/absence.
- `annotation` — annotation presence (`@KafkaListener`, `@Incoming`).
- `combination` — multiple detection methods AND'd together.

## The Three Magic Tricks Spring Does With Errors

Before reading the Spring rules below, internalize these three abstractions. They are responsible for most of the confusion in Spring Kafka error handling — and most of the rules in this catalog exist because at least one of them is missing, misconfigured, or quietly bypassed.

### Trick 1 — `CommonErrorHandler` ate `ErrorHandler` and `BatchErrorHandler`

Spring Kafka **2.8** unified the consumer-side error handling SPI under a single interface: `org.springframework.kafka.listener.CommonErrorHandler`. Before 2.8, you had:

- `ErrorHandler` for record listeners.
- `BatchErrorHandler` for batch listeners.
- `ContainerAwareErrorHandler`, `RemainingRecordsErrorHandler`, etc., as variants.

All of these are **deprecated**. `CommonErrorHandler` replaces them. The container detects whether you're in batch or record mode and routes accordingly.

Key methods on `CommonErrorHandler`:

- `handleRecord(Exception, ConsumerRecord, Consumer, MessageListenerContainer)` — record mode.
- `handleBatch(Exception, ConsumerRecords, Consumer, MessageListenerContainer, Runnable)` — batch mode.
- **`handleOtherException(Exception, Consumer, MessageListenerContainer, boolean batchListener)`** — exceptions *not* tied to a specific record (commit failures, listener auth issues, framework-level errors). This is the one most custom implementations forget to override; the default delegates to `handleRecord` with a *null* record, which crashes inside any custom handler that dereferences `record.topic()`. See `SPRING_CUSTOM_HANDLER_MISSING_OTHER_EXCEPTION`.

The default implementation is `DefaultErrorHandler` (a `CommonErrorHandler` subclass), which replaced both `SeekToCurrentErrorHandler` (record) and `RecoveringBatchErrorHandler` (batch).

### Trick 2 — `DefaultErrorHandler` vs `DefaultAfterRollbackProcessor`

Spring Kafka has **two distinct error-handling pipelines**, depending on whether the container is transactional.

- **Non-transactional listener** (`@KafkaListener` with no `KafkaTransactionManager`): when the listener throws, `DefaultErrorHandler` kicks in. It uses a `BackOff` for retries, then calls the configured `ConsumerRecordRecoverer` (typically `DeadLetterPublishingRecoverer`). Offsets are committed normally after success/recovery.

- **Transactional listener** (container has `transactionManager` set, or `@Transactional` listener method with a Kafka transaction manager): when the transaction rolls back, the *transaction* is gone — but the offset hasn't been committed yet either. Spring needs a separate component to decide what to do with the record(s) that just rolled back. That's `DefaultAfterRollbackProcessor`. It also takes a `BackOff` and a recoverer.

The two are NOT interchangeable:

- `DefaultErrorHandler` does not run inside a rolled-back transaction. Calling DLT-publish from it uses the *outer* (non-transactional) producer factory.
- `DefaultAfterRollbackProcessor` runs *after* rollback, on the next poll. If you want DLT publishing to be part of the same transaction (so the DLT record is atomically committed with the offset advance), you need the *processor*, not the handler.

For an EOS or transactional topology, configuring `DefaultErrorHandler` and forgetting `DefaultAfterRollbackProcessor` leaves the rollback path running on Spring's built-in default (`new FixedBackOff(0L, 9)` + log-and-skip recoverer). See `SPRING_AFTER_ROLLBACK_PROCESSOR_DEFAULT`.

### Trick 3 — `ErrorHandlingDeserializer` and the `springDeserializerExceptionKey` header

Kafka's native `Deserializer<T>` returns `T` or throws. If it throws inside `KafkaConsumer.poll()`, the consumer is wedged — same offset, same poison record, every poll. This pre-dates `DefaultErrorHandler` in the sense that `DefaultErrorHandler` only runs *after* the listener has been invoked; if deserialization fails, the listener never runs.

Spring Kafka's solution: `org.springframework.kafka.support.serializer.ErrorHandlingDeserializer`. It wraps the real deserializer:

```java
props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ErrorHandlingDeserializer.class);
props.put(ErrorHandlingDeserializer.KEY_DESERIALIZER_CLASS, StringDeserializer.class);
props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ErrorHandlingDeserializer.class);
props.put(ErrorHandlingDeserializer.VALUE_DESERIALIZER_CLASS, JsonDeserializer.class);
```

When the delegate throws:

1. `ErrorHandlingDeserializer` returns `null` to the consumer (no exception propagates).
2. It adds the original exception to the record's headers under one of:
   - `springDeserializerExceptionKey` (key deserialization failed)
   - `springDeserializerExceptionValue` (value deserialization failed)
3. The listener receives the record with `value() == null` AND the exception header set.
4. The `DefaultErrorHandler` recognizes these headers in `setClassifications(...)` and routes via the recoverer (DLT).

**This implies a contract**: a listener that uses `ErrorHandlingDeserializer` MUST either let `DefaultErrorHandler` handle the deserialization failure, OR explicitly check `null` + the header. If neither, a `NullPointerException` on `order.getCustomerId()` in the listener body shadows the real deserialization error. See `SPRING_EHD_LISTENER_NULL_NOT_CHECKED`.

The constants live on `org.springframework.kafka.support.serializer.SerializationUtils`:

- `VALUE_DESERIALIZER_EXCEPTION_HEADER = "springDeserializerExceptionValue"`
- `KEY_DESERIALIZER_EXCEPTION_HEADER = "springDeserializerExceptionKey"`

These header names are also what `DeadLetterPublishingRecoverer` reads when copying exception metadata to DLT headers.

## Catalog

### Monitoring & observability (6)

| Rule | Severity | Confidence | Detection | Tagline |
|---|---|---|---|---|
| [OBS_NO_PRODUCER_INTERCEPTORS](OBS_NO_PRODUCER_INTERCEPTORS.md) | INFO | CONTEXT | config-file | Producer `interceptor.classes` is empty — no tracing, no audit, no client-side metrics interception. |
| [OBS_NO_CONSUMER_INTERCEPTORS](OBS_NO_CONSUMER_INTERCEPTORS.md) | INFO | CONTEXT | config-file | Consumer `interceptor.classes` is empty — same blind spot as the producer side. |
| [OBS_NO_METRIC_REPORTERS](OBS_NO_METRIC_REPORTERS.md) | WARNING | CONTEXT | config-file | `metric.reporters` unset and no Micrometer/JMX bridge — Kafka's own MBeans go nowhere. |
| [OBS_SPRING_MICROMETER_NOT_BOUND](OBS_SPRING_MICROMETER_NOT_BOUND.md) | WARNING | MEDIUM | bytecode | Custom `@Bean ProducerFactory`/`ConsumerFactory` without `Micrometer*Listener` — auto-config bypassed. |
| [OBS_OPENTELEMETRY_AGENT_MISMATCH](OBS_OPENTELEMETRY_AGENT_MISMATCH.md) | WARNING | MEDIUM | pom-dependency | `opentelemetry-api` on the classpath but neither the agent nor `opentelemetry-kafka-clients-2.6` — spans never start. |
| [OBS_RECORDS_LAG_NOT_EXPOSED](OBS_RECORDS_LAG_NOT_EXPOSED.md) | WARNING | CONTEXT | combination | `records-lag-max` MBean exists but nothing exports it — flying blind on consumer lag. |

### Spring Kafka error handling (6)

| Rule | Severity | Confidence | Detection | Tagline |
|---|---|---|---|---|
| [SPRING_DEH_DEFAULT_BACKOFF](SPRING_DEH_DEFAULT_BACKOFF.md) | WARNING | HIGH | bytecode | `new DefaultErrorHandler()` keeps the implicit `FixedBackOff(0L, 9)` — 10 instant retries, then a noisy log. |
| [SPRING_DEH_NO_DLT_RECOVERER](SPRING_DEH_NO_DLT_RECOVERER.md) | ERROR | HIGH | bytecode | `DefaultErrorHandler` built without a recoverer — failed records are logged then forgotten. |
| [SPRING_CUSTOM_HANDLER_MISSING_OTHER_EXCEPTION](SPRING_CUSTOM_HANDLER_MISSING_OTHER_EXCEPTION.md) | WARNING | HIGH | bytecode | Custom `CommonErrorHandler` doesn't override `handleOtherException` — commit failures hit a null record. |
| [SPRING_LISTENER_SWALLOWS_EXCEPTION](SPRING_LISTENER_SWALLOWS_EXCEPTION.md) | ERROR | MEDIUM | bytecode | `@KafkaListener` with `try { ... } catch (Exception e) { log.error(...); }` — error handler never sees the throw. |
| [SPRING_EHD_LISTENER_NULL_NOT_CHECKED](SPRING_EHD_LISTENER_NULL_NOT_CHECKED.md) | WARNING | MEDIUM | combination | `ErrorHandlingDeserializer` wired but listener doesn't check `null` payload + the `springDeserializerException*` header. |
| [SPRING_AFTER_ROLLBACK_PROCESSOR_DEFAULT](SPRING_AFTER_ROLLBACK_PROCESSOR_DEFAULT.md) | WARNING | HIGH | combination | Transactional listener with the default `DefaultAfterRollbackProcessor` — rollback retries are not transactional. |

### Spring Cloud Stream binder (1)

| Rule | Severity | Confidence | Detection | Tagline |
|---|---|---|---|---|
| [SPRING_KSTREAMS_BINDER_DEFAULT_DEH](SPRING_KSTREAMS_BINDER_DEFAULT_DEH.md) | WARNING | MEDIUM | config-file | Spring Cloud Stream Kafka Streams binder defaults to `logAndFail` — first poison pill takes down the function. |

### Kafka Streams error handling (4)

| Rule | Severity | Confidence | Detection | Tagline |
|---|---|---|---|---|
| [STREAMS_PRODUCTION_HANDLER_MISSING](STREAMS_PRODUCTION_HANDLER_MISSING.md) | WARNING | HIGH | config-file | `default.production.exception.handler` unset — the default *always* FAILs, even on retriable producer errors. |
| [STREAMS_DESER_HANDLER_BLANKET_CONTINUE](STREAMS_DESER_HANDLER_BLANKET_CONTINUE.md) | ERROR | HIGH | bytecode | Custom `DeserializationExceptionHandler` returns `CONTINUE` for every input — silent data loss with no audit trail. |
| [STREAMS_PROD_HANDLER_BLANKET_CONTINUE](STREAMS_PROD_HANDLER_BLANKET_CONTINUE.md) | ERROR | HIGH | bytecode | Custom `ProductionExceptionHandler` returns `CONTINUE` for every output failure — output records vanish. |
| [STREAMS_UNCAUGHT_HANDLER_DEPRECATED_API](STREAMS_UNCAUGHT_HANDLER_DEPRECATED_API.md) | WARNING | HIGH | bytecode | `setUncaughtExceptionHandler(Thread.UncaughtExceptionHandler)` is deprecated — use the KIP-671 `StreamsUncaughtExceptionHandler`. |
| [STREAMS_NO_HEALTH_ENDPOINT](STREAMS_NO_HEALTH_ENDPOINT.md) | WARNING | MEDIUM | combination | No `/health/live` + `/health/ready` HTTP probes backed by `KafkaStreams.state()` — K8s restarts pods during normal rebalances. |

### Quarkus / SmallRye Reactive Messaging (2)

| Rule | Severity | Confidence | Detection | Tagline |
|---|---|---|---|---|
| [QK_FAILURE_DLQ_NO_TOPIC](QK_FAILURE_DLQ_NO_TOPIC.md) | WARNING | HIGH | config-file | `failure-strategy=dead-letter-queue` with no `dead-letter-queue.topic` — DLT routes to `dead-letter-topic-$channel`, often unprovisioned. |
| [QK_FAILURE_FAIL_PROD_NO_HEALTH](QK_FAILURE_FAIL_PROD_NO_HEALTH.md) | WARNING | MEDIUM | combination | `failure-strategy=fail` in production without health-check integration — channel dies silent. |

### Deserialization safety (3)

| Rule | Severity | Confidence | Detection | Tagline |
|---|---|---|---|---|
| [DESER_JSON_TYPE_INFO_NO_ALLOWLIST](DESER_JSON_TYPE_INFO_NO_ALLOWLIST.md) | ERROR | HIGH | combination | `JsonDeserializer` honoring type headers with no `TRUSTED_PACKAGES` allowlist — CVE-2023-34040 surface. |
| [DESER_STRING_FOR_JSON_PAYLOAD](DESER_STRING_FOR_JSON_PAYLOAD.md) | WARNING | MEDIUM | combination | `StringDeserializer` + `ObjectMapper.readValue` in the listener — no DLT path for bad JSON. |
| [DESER_SR_NO_AUTH_CREDENTIALS](DESER_SR_NO_AUTH_CREDENTIALS.md) | WARNING | HIGH | config-file | Schema Registry URL is HTTPS but `basic.auth.credentials.source` is unset — auth never sent. |

### Lambda anti-patterns (4)

| Rule | Severity | Confidence | Detection | Tagline |
|---|---|---|---|---|
| [LAMBDA_PRODUCER_CALLBACK_EMPTY](LAMBDA_PRODUCER_CALLBACK_EMPTY.md) | ERROR | HIGH | bytecode | `producer.send(rec, (md, ex) -> {})` — fire-and-forget with no error path. |
| [LAMBDA_SEND_GET_WITH_CALLBACK](LAMBDA_SEND_GET_WITH_CALLBACK.md) | INFO | HIGH | bytecode | `send(rec, cb).get()` — the `.get()` already throws; the callback is redundant. |
| [LAMBDA_RECOVERER_RETURNS_NULL](LAMBDA_RECOVERER_RETURNS_NULL.md) | ERROR | HIGH | bytecode | Recoverer lambda is empty or only logs — record committed, evidence rotated away. |
| [LAMBDA_PEEK_AS_ERROR_LOG](LAMBDA_PEEK_AS_ERROR_LOG.md) | WARNING | MEDIUM | bytecode | `Stream.peek(x -> log.error(...))` — peek is inspection, not handling. |

### Async / reactive bridge (2)

| Rule | Severity | Confidence | Detection | Tagline |
|---|---|---|---|---|
| [ASYNC_LISTENER_MONO_BLOCK](ASYNC_LISTENER_MONO_BLOCK.md) | WARNING | HIGH | bytecode | `Mono.block()` inside `@KafkaListener` defeats reactive — and the error path with it. |
| [ASYNC_INCOMING_VOID_SUBSCRIBE](ASYNC_INCOMING_VOID_SUBSCRIBE.md) | ERROR | HIGH | bytecode | `@Incoming` returning `void` while calling `.subscribe()` on a `Multi` is fire-and-forget — the framework can't ack what it doesn't see. |

## Cross-references

- Conduktor Config Advisor catalogues many of the broker-side and client-side defaults flagged here: <https://kafka-options-explorer.conduktor.io/config-advisor/>
- KIP-210 (production exception handler), KIP-671 (Streams uncaught exception handler), KIP-1033 (processing exception handler).
- CVE-2023-34040 — Spring Kafka deserialization vulnerability.
