# SPRING_DEH_DEFAULT_BACKOFF

**Severity**: WARNING
**Confidence**: HIGH
**Detection**: bytecode
**Tagline**: `new DefaultErrorHandler()` retries ten times with zero delay — that's not a backoff, that's a tantrum.

## TL;DR

The linter flags `new DefaultErrorHandler()` (or DI bean of type `DefaultErrorHandler`) constructed without an explicit `BackOff`. The implicit default is `FixedBackOff(0L, 9)` — 9 retries with 0 ms between them, so 10 immediate redelivery attempts in microseconds. This is almost never useful: it doesn't give the downstream service time to recover from a transient blip, but it does flood logs with ten identical stack traces per failed record.

## The setup

Team wires up Spring Kafka error handling. They see `DefaultErrorHandler` in the docs, instantiate it with `new DefaultErrorHandler()`, attach a `DeadLetterPublishingRecoverer`, and call it done. On the next transient downstream timeout, they see ten stack traces back-to-back for the same record, then a DLT publish — all within a millisecond. The retry "succeeded in retrying" but actually accomplished nothing because the downstream was still down for the full ten attempts.

## What's actually happening

The Spring Kafka `DefaultErrorHandler` constructor without arguments sets:

```java
new DefaultErrorHandler(null /* no recoverer */, new FixedBackOff(0L, 9L))
```

The `FixedBackOff(interval=0, maxAttempts=9)` translates to: retry 9 times after the initial failure (10 total deliveries), 0 ms apart. The intent of a backoff is to give the failure mode time to clear — DNS resolution, downstream cold-start, network reachability — and 0 ms achieves none of that.

Spring documentation explicitly says: *"For a record listener, this will retry a delivery up to 2 times (3 delivery attempts) with a back off of 1 second, instead of the default configuration (FixedBackOff(0L, 9))."* In other words, the docs treat the default as a counter-example.

The recommended replacement, available since spring-kafka 2.7.3, is `ExponentialBackOffWithMaxRetries(maxRetries)`:

```java
ExponentialBackOffWithMaxRetries bo = new ExponentialBackOffWithMaxRetries(6);
bo.setInitialInterval(1_000L);
bo.setMultiplier(2.0);
bo.setMaxInterval(10_000L);
// => retries after 1, 2, 4, 8, 10, 10 seconds
```

## Why this is subtle

- `new DefaultErrorHandler()` *looks* like sensible behavior — there's a retry, there's a backoff, there's a default. All true. The defaults are just terrible.
- The default-default-default is **also** to have no recoverer, so after the 10 immediate retries the record is dropped with only an ERROR log (see `SPRING_DEH_NO_DLT_RECOVERER`).
- Tests pass because in-memory tests with fast-failing mocks never need backoff.
- Production failures look like "we retried, retried failed, sent to DLT" — which sounds correct, masking that the retries were useless.

## Operational impact

- Burst of identical stack traces in logs (10 within microseconds) — alerts that count "errors per minute" spike.
- Downstream service receives 10 retry requests in microseconds — can amplify load during incident recovery.
- Records appear in DLT for transient failures that a real backoff would have survived.
- Real-life "retry" pattern is missing — DLT becomes a noisy crash report instead of a poison-pill log.

## Failure scenarios (walkthrough)

1. **Downstream cold-start.** A canary deploy of the downstream service takes 12 seconds to start serving. During those 12 seconds, the consumer with default backoff fires 10 retries in microseconds for every failing record, sends them all to DLT, and the operator has to replay from DLT. With `ExponentialBackOffWithMaxRetries(6)` topping at 10s, two records might end up in DLT — the rest succeed on retry.

2. **DNS blip.** Brief DNS resolution failure for the downstream HTTP API (1 second). Default backoff: every record consumed during that second fires 10 retries instantly, all 10 fail, DLT'd. Real backoff: one record per second of blip, retries succeed on the second attempt.

## How to fix

```java
// BAD
@Bean
public DefaultErrorHandler errorHandler(KafkaTemplate<String, ?> template) {
    return new DefaultErrorHandler(new DeadLetterPublishingRecoverer(template));
    // implicit FixedBackOff(0L, 9) — 10 retries at 0ms
}

// GOOD — exponential, capped
@Bean
public DefaultErrorHandler errorHandler(KafkaTemplate<String, ?> template) {
    ExponentialBackOffWithMaxRetries bo = new ExponentialBackOffWithMaxRetries(5);
    bo.setInitialInterval(1_000L);
    bo.setMultiplier(2.0);
    bo.setMaxInterval(30_000L);
    return new DefaultErrorHandler(new DeadLetterPublishingRecoverer(template), bo);
}

// ALSO GOOD — minimal but explicit
@Bean
public DefaultErrorHandler errorHandler(KafkaTemplate<String, ?> template) {
    return new DefaultErrorHandler(
        new DeadLetterPublishingRecoverer(template),
        new FixedBackOff(2_000L, 3L));  // 3 retries, 2s apart
}
```

## Consult a friend?

> Slow down — if your listener is `@Transactional` (or the container has a `KafkaTransactionManager`), exceptions roll back the transaction and `DefaultErrorHandler` is **not** invoked. The retry/recover path goes through `DefaultAfterRollbackProcessor` instead, which has its own `FixedBackOff(0L, 9)` default and its own DLT story. Verify which path your listener takes before tuning the backoff.

## When this might be a false positive

- Test code where 10-immediate retries simulate "exhausted retries quickly".
- The constructor takes a recoverer + explicit `BackOff` argument — already correct.
- Listener has `@RetryableTopic` — the framework manages retries on topic side and the error handler runs only for non-retryable exceptions.

## Detection strategy

- bytecode: visit constructors `DefaultErrorHandler.<init>()` with zero or one argument. With zero args: always flag. With one arg: check the argument type — if it's a `BiConsumer` (recoverer only, no BackOff), flag.
- bytecode: also flag `setBackOff(new FixedBackOff(0L, 9L))` literal calls — explicit re-statement of the bad default.
- Confidence HIGH — the constructor signature is unambiguous.

## References

- Spring Kafka — `DefaultErrorHandler` defaults: https://docs.spring.io/spring-kafka/reference/kafka/annotation-error-handling.html
- Spring Kafka — `ExponentialBackOffWithMaxRetries`: https://docs.spring.io/spring-kafka/reference/kafka/annotation-error-handling.html#exp-backoff
- `FixedBackOff`: https://docs.spring.io/spring-framework/docs/current/javadoc-api/org/springframework/util/backoff/FixedBackOff.html
