# STREAMS_DESER_HANDLER_BLANKET_CONTINUE

**Severity**: ERROR
**Confidence**: HIGH
**Detection**: bytecode
**Tagline**: A custom `DeserializationExceptionHandler` that always returns `CONTINUE` is data loss with a stack trace.

## TL;DR

The linter flags `DeserializationExceptionHandler` implementations whose `handle()` method always returns `CONTINUE` regardless of the exception. The shipped `LogAndContinueExceptionHandler` already does this — using it in production is acceptable only as a temporary mitigation. Implementing your own and writing `return CONTINUE` unconditionally is even worse: it makes the silent drop look like a deliberate design.

## The setup

Team hits the poison-pill problem: one bad record kills the topology (the default `LogAndFailExceptionHandler` returns `FAIL`). They write `class MyDeserHandler implements DeserializationExceptionHandler { public DeserializationHandlerResponse handle(...) { return CONTINUE; } }`. Topology no longer dies. Schema drift goes unnoticed; producer-side bugs become "data feels weird" support tickets weeks later.

## What's actually happening

`DeserializationExceptionHandler.handle()` returns either `CONTINUE` (drop the record, advance the offset) or `FAIL` (shut down the thread). The built-in choices:

- `LogAndFailExceptionHandler` (default) — logs at ERROR, returns FAIL.
- `LogAndContinueExceptionHandler` — logs at WARN, returns CONTINUE.

Both are blanket policies. A production-ready handler should:
1. Distinguish recoverable vs unrecoverable: a Jackson `JsonProcessingException` on one record is recoverable; an `UnknownTopicOrPartitionException` is not.
2. Publish failed records to a DLT (Spring Kafka's `RecoveringDeserializationExceptionHandler` does this).
3. Emit a metric per drop — silent drops are a data-loss audit gap.
4. Optionally rate-limit: if 99% of records are failing, that's a producer or schema issue, not a poison pill — falling out fast is better than draining the topic.

## Why this is subtle

- "Continue on every exception" looks defensive — "we won't let one bad record kill us."
- Without metrics, the drop rate is invisible. A producer migration that breaks 10% of payloads silently loses 10% of data.
- `LogAndContinueExceptionHandler` is a built-in name, which makes it look officially endorsed for production. It is not — Confluent's docs explicitly call it a "log-and-skip" handler suitable only when downstream tolerates losing records.
- For aggregations and joins, silent drops corrupt aggregates without any indication.

## Operational impact

- Topology runs through any input. Producer schema drift, encoding bugs, accidental gzip — all silently dropped.
- Aggregates and joins produce wrong-but-plausible numbers.
- The only signal is log volume (which is rotated, not alerted on).
- DLT topic empty (nothing was published there).

## Failure scenarios (walkthrough)

1. **The schema drift.** Upstream producer rolls out a new schema version that requires consumer-side schema-registry update. Consumer side missed the update. 80% of records fail to deserialize. Topology runs happily — `CONTINUE` on each one. Downstream aggregate shows traffic at 20% of normal. Business notices "metrics are weird" three days later.

2. **The double-encoding.** A producer accidentally serializes JSON twice (`JSON-of-JSON`). Deserializer fails. Handler returns CONTINUE. Downstream aggregate undercounts. Three weeks to root-cause.

## How to fix

Distinguish exceptions, publish to DLT, emit a metric. The simplest production-ready handler:

```java
public class TopicAwareDeserHandler implements DeserializationExceptionHandler {
    private final KafkaProducer<byte[], byte[]> dltProducer;
    private final MeterRegistry meter;
    @Override
    public DeserializationHandlerResponse handle(
            ErrorHandlerContext ctx, ConsumerRecord<byte[], byte[]> record, Exception e) {
        meter.counter("streams.deser.error",
            "topic", record.topic(),
            "exception", e.getClass().getSimpleName()).increment();
        // publish to DLT
        dltProducer.send(new ProducerRecord<>(record.topic() + ".dlt",
            record.partition(), record.key(), record.value(),
            withDltHeaders(record, e)));
        return DeserializationHandlerResponse.CONTINUE;
    }
    @Override public void configure(Map<String, ?> configs) {}
}
```

For Spring users, `RecoveringDeserializationExceptionHandler` does this with a `DeadLetterPublishingRecoverer`:

```java
@Bean
public RecoveringDeserializationExceptionHandler deserHandler(
        KafkaTemplate<Object, Object> template) {
    RecoveringDeserializationExceptionHandler handler =
        new RecoveringDeserializationExceptionHandler(new DeadLetterPublishingRecoverer(template));
    return handler;
}
```

## When this might be a false positive

- Genuine "log and skip" tier (analytics topic where 1% loss is acceptable). Make this an explicit annotation/comment so future-you knows it's a policy choice.
- Custom handler that returns `CONTINUE` for *specific* exception types and `FAIL` otherwise — not a blanket continue.

## Detection strategy

- bytecode: scan implementations of `org.apache.kafka.streams.errors.DeserializationExceptionHandler`.
- For each `handle` method override, build a simple SSA-ish view. If every return path returns `DeserializationHandlerResponse.CONTINUE` (no `FAIL`, no exception type dispatch), flag.
- Whitelist: `LogAndContinueExceptionHandler` (Apache built-in — covered separately by an INFO-level "production usage" rule). Spring's `RecoveringDeserializationExceptionHandler` (acceptable when wired with a recoverer).
- Confidence HIGH — return-statement analysis is exact.

## Consult a friend?

> 🤝 **Slow down.** Replacing a blanket-CONTINUE handler with a DLT-publishing one is the right shape, but the DLT side has its own correctness traps — especially under EOS.
> - Under `processing.guarantee=exactly_once_v2`, the DLT publish is *not* part of the topology's transaction (the deser handler runs before the processor enters the transactional region). A successful DLT send followed by a topology crash means the offset isn't committed, so the next instance reprocesses the same record and double-publishes to the DLT. Is the DLT consumer idempotent?
> - Per-exception classification ("Jackson → CONTINUE, anything else → FAIL") is what good handlers do — but if `Exception` is the only thing caught in the parent stack, you've collapsed the dispatch to FAIL-only. Verify each exception type actually surfaces with its real class, not wrapped in `KafkaException`.
> - EOS slogan: "exactly once is exactly once *if it's processed at all*". `CONTINUE` means "0 times" — for a financial / audit pipeline, that's a contract violation. Confirm with the data owner that drops are acceptable, or use `FAIL` and fix the upstream.
> - Read KIP-1033 (3.9+ processing-exception handler) — it's the same shape of trap one layer in.

## References

- Apache Kafka — `DeserializationExceptionHandler` (2.6 javadoc): https://kafka.apache.org/26/javadoc/org/apache/kafka/streams/errors/DeserializationExceptionHandler.html
- Confluent — Kafka Streams error handling: https://developer.confluent.io/courses/kafka-streams/error-handling/
- Spring Kafka — `RecoveringDeserializationExceptionHandler`: https://docs.spring.io/spring-kafka/api/org/springframework/kafka/listener/RecoveringDeserializationExceptionHandler.html
- Baeldung — Kafka Streams exception handling: https://www.baeldung.com/java-kafka-streams-exception-handling
