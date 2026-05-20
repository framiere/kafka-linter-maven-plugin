# SPRING_DEH_NO_DLT_RECOVERER

**Severity**: WARNING
**Confidence**: HIGH
**Detection**: bytecode
**Tagline**: `DefaultErrorHandler` with no recoverer logs your data loss and calls it a feature.

## TL;DR

The linter flags `DefaultErrorHandler` constructed without a recoverer argument. After retries are exhausted, the default behavior is: **log the failed record at ERROR level, commit the offset, move on**. The record is gone. A `DeadLetterPublishingRecoverer` (or any `ConsumerRecordRecoverer`) is required to actually preserve failed records — otherwise this is silent data loss with extra steps.

## The setup

The team set up retry-with-backoff. They've fixed `SPRING_DEH_DEFAULT_BACKOFF` and the retries are tasteful. The downstream is sometimes down for hours, longer than any reasonable backoff. The retries exhaust. The record is "handled" — i.e. dropped — and the consumer moves on. Two weeks later, a customer asks "where's order 12345?" and the only trace is one ERROR line in `application.log`.

## What's actually happening

`DefaultErrorHandler`'s default recoverer is `LoggingRecoverer`: it writes one ERROR-level log line with the record key/topic/partition/offset and the exception, then returns. The container interprets this as "handled" — the offset is committed and `poll()` moves to the next record.

From the docs: *"By default, after ten failures, the failed record is logged (at the ERROR level)."*

The fix is to pass a `DeadLetterPublishingRecoverer` (or a custom `ConsumerRecordRecoverer`) to the constructor:

```java
new DefaultErrorHandler(new DeadLetterPublishingRecoverer(kafkaTemplate), backOff)
```

`DeadLetterPublishingRecoverer` publishes the record to `<topic>-dlt` (default destination) on the same partition, with additional headers carrying the exception cause, original topic, original offset, etc.

## Why this is subtle

- "Failed records are logged" sounds like a feature. The word "log" hides "dropped."
- The default DLT topic name `<topic>-dlt` doesn't exist until a record is sent there for the first time — so log "topic does not exist" warnings may make engineers think the recoverer is broken when in fact it just hasn't fired yet.
- "It worked in test" — tests usually don't exhaust retries because the failure mode under test is something the test will eventually fix (re-enable the mock).
- Spring metrics show "consumer is healthy" — the offset is committed, lag is zero, no exception bubbles up.

## Operational impact

- Failed records gone. No DLT, no audit trail, only an ERROR log.
- Lag metrics look healthy because offsets are advancing.
- The only forensic trail is grep on the application log file — which has likely rotated.

## Failure scenarios (walkthrough)

1. **The vendor outage.** Downstream payment API is hard-down for 6 hours. Retries: 10 immediate (or 5 exponential to 30s). Either way: ~5 minutes per record max. Records consumed during the outage: dropped with an ERROR log. When the outage ends, the consumer keeps consuming; the dropped records are not recoverable. Customer-impacting outage extends from "we held the data" to "we lost the data."

2. **The poison pill.** One record has a schema-incompatible payload. `ErrorHandlingDeserializer` correctly wraps the deserialization, the listener gets a null value, throws, retries exhaust, record dropped. With a DLT, the operator can inspect the byte payload and fix the producer. Without, the only signal is one ERROR log already drowned in 99 retry log lines.

## How to fix

```java
// BAD — no recoverer
@Bean
DefaultErrorHandler errorHandler() {
    return new DefaultErrorHandler(new FixedBackOff(2_000L, 3L));
    // exhausted retries -> log ERROR, commit, move on
}

// GOOD — explicit DLT recoverer
@Bean
DefaultErrorHandler errorHandler(KafkaTemplate<Object, Object> template) {
    DeadLetterPublishingRecoverer recoverer = new DeadLetterPublishingRecoverer(template);
    return new DefaultErrorHandler(recoverer, new FixedBackOff(2_000L, 3L));
}

// ALSO GOOD — custom destination resolver for typed DLT routing
@Bean
DefaultErrorHandler errorHandler(KafkaTemplate<Object, Object> template) {
    DeadLetterPublishingRecoverer recoverer = new DeadLetterPublishingRecoverer(template,
        (record, ex) -> ex instanceof PoisonPillException
            ? new TopicPartition(record.topic() + ".dlt.poison", record.partition())
            : new TopicPartition(record.topic() + ".dlt.transient", record.partition()));
    return new DefaultErrorHandler(recoverer, new FixedBackOff(2_000L, 3L));
}
```

The recoverer-published DLT record includes these headers (constants on `KafkaHeaders`):

- `DLT_EXCEPTION_FQCN`, `DLT_EXCEPTION_MESSAGE`, `DLT_EXCEPTION_STACKTRACE`
- `DLT_ORIGINAL_TOPIC`, `DLT_ORIGINAL_PARTITION`, `DLT_ORIGINAL_OFFSET`, `DLT_ORIGINAL_TIMESTAMP`
- `DLT_ORIGINAL_CONSUMER_GROUP`

Use these in your DLT reprocessor to know what to replay.

## Consult a friend?

> Slow down. A `DeadLetterPublishingRecoverer` needs:
> 1. The DLT topic to exist with enough partitions to accept the original partition number (or use a non-partition-preserving resolver).
> 2. A `KafkaTemplate<Object, Object>` (not the typed one your producer uses) so it can publish bytes for poison-pill cases.
> 3. Awareness of transactions — if the listener is transactional, the DLT publish should be in the *same* transaction (`commitRecovered=true` on `DefaultAfterRollbackProcessor`). Otherwise you can publish to DLT and then roll back the offset commit, leading to duplicate DLT entries.
>
> Verify these three before merging.

## When this might be a false positive

- A custom `ConsumerRecordRecoverer` (`BiConsumer<ConsumerRecord<?, ?>, Exception>`) is passed instead of `DeadLetterPublishingRecoverer` — still recovers, still acceptable.
- Test code where a `LoggingRecoverer` is the explicit intent.
- The application has a `@RetryableTopic` annotation that manages retries+DLT on topic side and bypasses this error handler.

## Detection strategy

- bytecode: `DefaultErrorHandler.<init>` with arguments only of type `BackOff` (no `ConsumerRecordRecoverer`/`BiConsumer` argument).
- bytecode: `DefaultErrorHandler.<init>` with no arguments (covered by `SPRING_DEH_DEFAULT_BACKOFF`, but also lacks recoverer).
- Confidence HIGH — constructor signature is checked statically.

## References

- Spring Kafka — `DeadLetterPublishingRecoverer`: https://docs.spring.io/spring-kafka/reference/kafka/annotation-error-handling.html
- `KafkaHeaders` constants: https://docs.spring.io/spring-kafka/api/org/springframework/kafka/support/KafkaHeaders.html
- Confluent — Handling deserialization errors with Spring: https://www.confluent.io/blog/spring-kafka-can-your-kafka-consumers-handle-a-poison-pill/
