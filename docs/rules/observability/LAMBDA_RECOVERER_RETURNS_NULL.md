# LAMBDA_RECOVERER_RETURNS_NULL

**Severity**: ERROR
**Confidence**: HIGH
**Detection**: bytecode
**Tagline**: A `BiConsumer<ConsumerRecord, Exception>` recoverer that does nothing is a black hole with extra steps.

## TL;DR

The linter flags lambdas passed as `ConsumerRecordRecoverer` (`BiConsumer<ConsumerRecord<?, ?>, Exception>`) to `DefaultErrorHandler`, `DefaultAfterRollbackProcessor`, or `SeekToCurrentErrorHandler`, where the lambda body either (a) is empty, (b) doesn't touch the `Exception` parameter, or (c) only logs at DEBUG/INFO without sending to DLT, incrementing a metric, or persisting evidence. Such "recoverers" complete the framework's contract (no thrown exception → record is treated as handled, offset committed) but leave zero forensic trail. This is silent data loss disguised as recovery.

## The setup

Team configures a `DefaultErrorHandler` with a custom recoverer because the default `DeadLetterPublishingRecoverer` setup feels heavy. They write `(record, ex) -> log.error("recover {}", record.key())`. Tests pass — failed records are "recovered" — and life moves on. Two weeks later: data loss audit.

## What's actually happening

`DefaultErrorHandler` (and `DefaultAfterRollbackProcessor`) treat the recoverer as a `BiConsumer<ConsumerRecord<?, ?>, Exception>`. After retries are exhausted, the recoverer is called once. If it returns normally:

- Container commits the offset.
- Container marks the record as handled.
- Next poll moves on.

If the recoverer *throws*, the container restarts the retry cycle (potentially infinite loop, depending on `resetStateOnRecoveryFailure`).

So a lambda that does nothing but log:
1. Doesn't throw → record is committed → moved on.
2. Logs once at ERROR (typical) → log file is the only record.
3. No DLT, no metric, no audit row.

The pattern matches the producer-callback case (`LAMBDA_PRODUCER_CALLBACK_EMPTY`) but on the consumer side and with worse consequences — the record is genuinely gone from the consumer's perspective.

## Why this is subtle

- The lambda looks active: it has a logger call, it has the exception as a parameter, it satisfies the type signature.
- `DefaultErrorHandler`'s built-in default recoverer (when none provided) literally just logs at ERROR — so writing a lambda that does the same thing matches the framework's "default" behavior, which feels safe.
- The behavior is identical to `SPRING_DEH_NO_DLT_RECOVERER`, but harder to detect because there *is* a recoverer — just an empty one.
- Tests usually verify "after retries, record is committed" — the no-recoverer and empty-recoverer cases pass the same test.

## Operational impact

- Failed records permanently lost.
- No DLT to replay from.
- Audit/compliance can show only log lines (which rotate).
- No metric counter → no alert on "recover rate spiking" — invisible to monitoring.

## Failure scenarios (walkthrough)

1. **The DLT-less migration.** Team is moving away from a homegrown DLT to Spring's. They replace the homegrown `DeadLetterPublishingRecoverer` with a placeholder lambda `(r, e) -> log.error("recover {}", r)`. They plan to put the real one in next sprint. Next sprint slips. Three months later: outage replays show 4,000 records were "recovered" with no DLT in sight.

2. **The intern's fix.** "Why is this error handler failing in tests?" New engineer simplifies the recoverer to `(r, e) -> {}` so tests pass. Code review approves. Behavior in prod: data loss.

## How to fix

```java
// BAD
new DefaultErrorHandler((record, ex) -> {});
new DefaultErrorHandler((record, ex) -> log.error("recover", ex));

// GOOD — use the framework recoverer
new DefaultErrorHandler(new DeadLetterPublishingRecoverer(template), backOff);

// GOOD — explicit custom recoverer that actually does something
new DefaultErrorHandler((record, ex) -> {
    failureRepository.save(new FailedRecord(record, ex));  // DB row
    meterRegistry.counter("kafka.recovered",
        "topic", record.topic(),
        "exception", ex.getClass().getSimpleName()).increment();
    log.error("Recovered failed record on topic {} offset {}",
        record.topic(), record.offset(), ex);
}, backOff);
```

## Consult a friend?

> 🤝 **Slow down.** A recoverer should produce *evidence*. If a year from now an auditor asks "where did record X with key Y at offset Z go on date W?", what is your answer?
> - DLT — replayable, broker-side, durable.
> - Database row — queryable, has business-context columns.
> - External SIEM event — alertable.
>
> An ERROR log line is not evidence — it's rotated, unsearchable across a fleet, and easy to lose. Decide your evidence path before choosing a recoverer.

## When this might be a false positive

- Lambda body persists evidence via a captured `failureRepository` field; bytecode shows it as a virtual call to a non-logger.
- The `DefaultErrorHandler` is for a topic where "log and skip" is the documented policy.

## Detection strategy

- bytecode: locate `new DefaultErrorHandler(`, `new DefaultAfterRollbackProcessor(`, or any `setRecoverer(`.
- Resolve the recoverer argument:
  - If lambda: visit synthetic method body. Flag if body is empty.
  - Flag if the only INVOKE in the body is on `org.slf4j.Logger` / `java.util.logging.Logger` / `org.apache.commons.logging.Log` AND the exception parameter is used only as an argument to that logger call (no DLT publish, no metric increment).
- Confidence HIGH for empty body, MEDIUM for log-only body.

## References

- Spring Kafka — `DefaultErrorHandler` recoverer: https://docs.spring.io/spring-kafka/reference/kafka/annotation-error-handling.html
- `DeadLetterPublishingRecoverer`: https://docs.spring.io/spring-kafka/api/org/springframework/kafka/listener/DeadLetterPublishingRecoverer.html
- Related: `SPRING_DEH_NO_DLT_RECOVERER` (no recoverer at all)
- Related: `LAMBDA_PRODUCER_CALLBACK_EMPTY` (same pattern on producer side)
