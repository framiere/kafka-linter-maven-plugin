# Observability, Deserialization Safety & Lambda Anti-Patterns

This catalog covers four overlapping concerns that are easy to get wrong in production Kafka deployments:

1. **Monitoring & observability** — interceptors, metric reporters, Micrometer/OpenTelemetry wiring.
2. **Deserialization safety** — `ErrorHandlingDeserializer`-style headers, `JsonDeserializer` type-info attacks (CVE-2023-34040), Schema Registry credentials, `StringDeserializer` + ad-hoc `ObjectMapper.readValue` patterns.
3. **Lambdas as silent error swallowers** — empty `Callback` producer lambdas, recoverer lambdas that "just log," `Mono.block()` and `void`+`subscribe()` patterns.
4. **Async / reactive bridge** — `Mono.block()` inside `@KafkaListener`, `void` `@Incoming` returning before subscription completes.

These rules share a common shape: a configuration default or coding pattern that *looks* safe (compiles, tests pass, looks idiomatic) but produces silent data loss, unrecoverable restart loops, or invisible failures in production.

Note: framework-specific error-handler rules (`SPRING_DEH_*`, `SPRING_AFTER_ROLLBACK_PROCESSOR_DEFAULT`, `SPRING_EHD_LISTENER_NULL_NOT_CHECKED`, `SPRING_KSTREAMS_BINDER_DEFAULT_DEH`, `STREAMS_PRODUCTION_HANDLER_MISSING`, `STREAMS_DESER_HANDLER_BLANKET_CONTINUE`, `STREAMS_PROD_HANDLER_BLANKET_CONTINUE`, `STREAMS_UNCAUGHT_HANDLER_DEPRECATED_API`, `QK_FAILURE_DLQ_NO_TOPIC`, `QK_FAILURE_FAIL_PROD_NO_HEALTH`) now live with their respective frameworks — see [`../spring-kafka/_INDEX.md`](../spring-kafka/_INDEX.md), [`../kafka-streams/_INDEX.md`](../kafka-streams/_INDEX.md), and [`../quarkus-kafka/_INDEX.md`](../quarkus-kafka/_INDEX.md). The Three-Magic-Tricks walk-through below is retained because it remains the single most useful piece of background reading before touching any of those rules.

## Legend

**Severity**
- `ERROR` — production data loss or unrecoverable failure mode. Fix before shipping.
- `WARNING` — silent failure mode, observability gap, or footgun. Fix unless explicitly opted into.
- `INFO` — best-practice suggestion.

**Confidence**
- `HIGH` — descriptor-level match. Very few false positives.
- `MEDIUM` — heuristic. Requires human review.
- `CONTEXT` — depends on runtime / project intent; the linter cannot decide alone.

**Detection** — `bytecode`, `annotation`, `config-file`, `pom-dependency`, or a combination (` + `, alphabetical).

A trailing 🤝 in the tagline means the rule's doc carries a `## Consult a friend?` block — read it before changing prod config.

## The Three Magic Tricks Spring Does With Errors

Before reading the Spring rules (now in [`../spring-kafka/_INDEX.md`](../spring-kafka/_INDEX.md)), internalize these three abstractions. They are responsible for most of the confusion in Spring Kafka error handling — and most of the framework rules in this codebase exist because at least one of them is missing, misconfigured, or quietly bypassed.

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

## Catalog (14)

### Monitoring & observability

| Rule | Severity | Confidence | Detection | Tagline |
|---|---|---|---|---|
| [OBS_NO_PRODUCER_INTERCEPTORS](./OBS_NO_PRODUCER_INTERCEPTORS.md) | WARNING | CONTEXT | config-file | No `interceptor.classes` on the producer is "I want tracing — just not in this app." |
| [OBS_NO_CONSUMER_INTERCEPTORS](./OBS_NO_CONSUMER_INTERCEPTORS.md) | WARNING | CONTEXT | config-file | A consumer with no `interceptor.classes` is the second half of a broken trace. |
| [OBS_NO_METRIC_REPORTERS](./OBS_NO_METRIC_REPORTERS.md) | WARNING | CONTEXT | config-file | Kafka exposes 100+ metrics by default — and ships exactly zero of them unless you ask. |
| [OBS_SPRING_MICROMETER_NOT_BOUND](./OBS_SPRING_MICROMETER_NOT_BOUND.md) | WARNING | MEDIUM | bytecode + pom-dependency | A custom `ProducerFactory` without `MicrometerProducerListener` is `KafkaMetricsAutoConfiguration` waving goodbye. 🤝 |
| [OBS_OPENTELEMETRY_AGENT_MISMATCH](./OBS_OPENTELEMETRY_AGENT_MISMATCH.md) | WARNING | MEDIUM | config-file + pom-dependency | Declaring `opentelemetry-api` without the agent *or* the Kafka instrumentation library is owning the boxing gloves without ever stepping in the ring. |

### Kafka Streams operability

| Rule | Severity | Confidence | Detection | Tagline |
|---|---|---|---|---|
| [STREAMS_NO_HEALTH_ENDPOINT](./STREAMS_NO_HEALTH_ENDPOINT.md) | WARNING | MEDIUM | bytecode + pom-dependency | Without a `/health/ready` probe, Kubernetes restarts your Streams app during normal rebalances. |

### Deserialization safety

| Rule | Severity | Confidence | Detection | Tagline |
|---|---|---|---|---|
| [DESER_JSON_TYPE_INFO_NO_ALLOWLIST](./DESER_JSON_TYPE_INFO_NO_ALLOWLIST.md) | ERROR | HIGH | bytecode + config-file | `JsonDeserializer` with `__TypeId__` headers and no trusted-packages allowlist is RCE via a Kafka header. 🤝 |
| [DESER_STRING_FOR_JSON_PAYLOAD](./DESER_STRING_FOR_JSON_PAYLOAD.md) | WARNING | MEDIUM | bytecode | `StringDeserializer` on a JSON topic catches no structural errors — your "deserializer" is a UTF-8 decoder. |
| [DESER_SR_NO_AUTH_CREDENTIALS](./DESER_SR_NO_AUTH_CREDENTIALS.md) | WARNING | MEDIUM | config-file | Schema Registry without `basic.auth.credentials.source` is an HTTPS URL that says "trust me, no creds needed." |

### Lambda anti-patterns

| Rule | Severity | Confidence | Detection | Tagline |
|---|---|---|---|---|
| [LAMBDA_PRODUCER_CALLBACK_EMPTY](./LAMBDA_PRODUCER_CALLBACK_EMPTY.md) | ERROR | HIGH | bytecode | `producer.send(record, (md, ex) -> {})` is the lambda spelling of "I don't want to know if it failed." |
| [LAMBDA_SEND_GET_WITH_CALLBACK](./LAMBDA_SEND_GET_WITH_CALLBACK.md) | WARNING | HIGH | bytecode | `producer.send(record, callback).get()` is one too many error-handling paths. Pick one. |
| [LAMBDA_RECOVERER_RETURNS_NULL](./LAMBDA_RECOVERER_RETURNS_NULL.md) | ERROR | HIGH | bytecode | A `BiConsumer<ConsumerRecord, Exception>` recoverer that does nothing is a black hole with extra steps. 🤝 |

### Async / reactive bridge

| Rule | Severity | Confidence | Detection | Tagline |
|---|---|---|---|---|
| [ASYNC_LISTENER_MONO_BLOCK](./ASYNC_LISTENER_MONO_BLOCK.md) | WARNING | HIGH | bytecode | `Mono.block()` inside a `@KafkaListener` defeats reactive — and the error path with it. |
| [ASYNC_INCOMING_VOID_SUBSCRIBE](./ASYNC_INCOMING_VOID_SUBSCRIBE.md) | ERROR | HIGH | bytecode | `@Incoming` returning `void` while calling `.subscribe()` on a `Multi` is fire-and-forget — and the framework can't ack what it doesn't see. |

## Cross-references

- Conduktor Config Advisor catalogues many of the broker-side and client-side defaults flagged here: <https://kafka-options-explorer.conduktor.io/config-advisor/>
- KIP-210 (production exception handler), KIP-671 (Streams uncaught exception handler), KIP-1033 (processing exception handler).
- CVE-2023-34040 — Spring Kafka deserialization vulnerability — see `DESER_JSON_TYPE_INFO_NO_ALLOWLIST`.
- Spring-side error-handler rules: [`../spring-kafka/_INDEX.md`](../spring-kafka/_INDEX.md) (`SPRING_DEH_DEFAULT_BACKOFF`, `SPRING_DEH_NO_DLT_RECOVERER`, `SPRING_CUSTOM_HANDLER_MISSING_OTHER_EXCEPTION`, `SPRING_LISTENER_SWALLOWS_EXCEPTION`, `SPRING_EHD_LISTENER_NULL_NOT_CHECKED`, `SPRING_AFTER_ROLLBACK_PROCESSOR_DEFAULT`, `SPRING_KSTREAMS_BINDER_DEFAULT_DEH`).
- Streams-side error-handler rules: [`../kafka-streams/_INDEX.md`](../kafka-streams/_INDEX.md) (`STREAMS_PRODUCTION_HANDLER_MISSING`, `STREAMS_DESER_HANDLER_BLANKET_CONTINUE`, `STREAMS_PROD_HANDLER_BLANKET_CONTINUE`, `STREAMS_UNCAUGHT_HANDLER_DEPRECATED_API`).
- Quarkus-side error-handler rules: [`../quarkus-kafka/_INDEX.md`](../quarkus-kafka/_INDEX.md) (`QK_FAILURE_DLQ_NO_TOPIC`, `QK_FAILURE_FAIL_PROD_NO_HEALTH`, `QK_FAIL_ON_DESER_NO_DLQ`).
