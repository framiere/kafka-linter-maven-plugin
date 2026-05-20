# SPRING_DEPRECATED_SEEK_TO_CURRENT_ERROR_HANDLER

**Severity**: WARNING
**Confidence**: HIGH
**Detection**: bytecode
**Tagline**: SeekToCurrentErrorHandler shipped its last bug fix in 2021.

## TL;DR

The linter flags references to `SeekToCurrentErrorHandler`, `SeekToCurrentBatchErrorHandler`, `RecoveringBatchErrorHandler`, `ContainerStoppingErrorHandler`, and `LoggingErrorHandler` from spring-kafka 2.x. These were deprecated when the `CommonErrorHandler` interface was introduced in 2.8 and are removed in 3.x.

## What's happening (the mechanism)

Spring Kafka 2.8 unified the error handler hierarchy. The legacy split between `ErrorHandler` (record) and `BatchErrorHandler` (batch) became `CommonErrorHandler`, and `DefaultErrorHandler` replaced both `SeekToCurrentErrorHandler` and `RecoveringBatchErrorHandler` as the new default.

Replacement table (from the spring-kafka docs):

| Legacy | Replacement |
|---|---|
| `SeekToCurrentErrorHandler` | `DefaultErrorHandler` |
| `SeekToCurrentBatchErrorHandler` | `DefaultErrorHandler` with infinite `BackOff` |
| `RecoveringBatchErrorHandler` | `DefaultErrorHandler` |
| `ContainerStoppingErrorHandler` | `CommonContainerStoppingErrorHandler` |
| `LoggingErrorHandler` | `CommonLoggingErrorHandler` |

Default retry behavior of `DefaultErrorHandler` is `FixedBackOff(0L, 9)` — 9 retries with no back off, then log at `ERROR` and skip the record (the framework calls this "logging recoverer").

Code that still references the legacy classes:

- Fails to compile against spring-kafka 3.x (classes removed). Catches at build time, so this is a transitive-dependency / shaded-jar risk.
- Compiles against 2.8.x but emits deprecation warnings that get lost in the noise.
- May reference behaviors (`setCommitRecovered(...)`, custom `recovererCallback`) that have different default semantics in `DefaultErrorHandler`.

Static-detection win: catches a lib upgrade landmine before the build breaks at Maven Central.

## Operational impact

- Build break when bumping `spring-kafka` past 3.0.
- Subtle behavior drift: `DefaultErrorHandler` skips the record after the recoverer succeeds; `SeekToCurrentErrorHandler` in older versions did the same but with different commit semantics.
- Logs full of `[DEPRECATION]` warnings from the framework — drowning out real issues.

## How to fix

```java
// BAD — legacy
factory.setErrorHandler(new SeekToCurrentErrorHandler(
        new FixedBackOff(1_000L, 2L)));

// GOOD — current API
factory.setCommonErrorHandler(new DefaultErrorHandler(
        new FixedBackOff(1_000L, 2L)));

// GOOD — with a DLT recoverer
DeadLetterPublishingRecoverer recoverer = new DeadLetterPublishingRecoverer(
        kafkaTemplate,
        (record, ex) -> new TopicPartition(record.topic() + "-dlt", record.partition()));
factory.setCommonErrorHandler(new DefaultErrorHandler(recoverer,
        new FixedBackOff(1_000L, 2L)));
```

If you relied on `ContainerStoppingErrorHandler`, switch to `CommonContainerStoppingErrorHandler` — same semantics, new interface.

## When this might be a false positive

- A project locked to spring-kafka 2.7.x for compatibility reasons — the rule still flags but the team can suppress with a `kafka-linter.suppress` comment. The deprecation is real.

## Detection strategy

- Bytecode: scan for `INVOKESPECIAL` / type references to:
  - `org/springframework/kafka/listener/SeekToCurrentErrorHandler`
  - `org/springframework/kafka/listener/SeekToCurrentBatchErrorHandler`
  - `org/springframework/kafka/listener/RecoveringBatchErrorHandler`
  - `org/springframework/kafka/listener/ContainerStoppingErrorHandler`
  - `org/springframework/kafka/listener/LoggingErrorHandler`
- Also flag calls to `ConcurrentKafkaListenerContainerFactory.setErrorHandler(...)` (deprecated in favor of `setCommonErrorHandler`).
- Confidence: HIGH — the class names are unambiguous.

## References

- Spring Kafka — Handling Exceptions: https://docs.spring.io/spring-kafka/reference/kafka/annotation-error-handling.html
- Spring Kafka — Migrating from 2.x: https://docs.spring.io/spring-kafka/reference/whats-new.html
