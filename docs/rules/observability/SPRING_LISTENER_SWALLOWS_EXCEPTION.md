# SPRING_LISTENER_SWALLOWS_EXCEPTION

**Severity**: WARNING
**Confidence**: MEDIUM
**Detection**: bytecode
**Tagline**: `try { ... } catch (Exception e) { log.error(e); }` inside a `@KafkaListener` deletes the error handler.

## TL;DR

The linter flags `@KafkaListener`-annotated methods whose top-level body is `try { ... } catch (Exception | Throwable e) { /* log only */ }`. The framework's retry / DLT / `CommonErrorHandler` pipeline is triggered **only when an exception escapes the listener method**. Catching at the listener level — even just to log — silences the entire error path. The retry never fires. The DLT recoverer never publishes. The "errors per minute" counter you wired in your error handler stays at zero.

## The setup

Engineer writes a `@KafkaListener`. They put a try/catch around the body "for safety, so the listener doesn't crash." They log the exception. They return normally. They feel responsible. Then they wonder why their `DefaultErrorHandler` retry config has no effect — because their retry config is dead code.

## What's actually happening

Spring's listener container wraps each invocation of the user method like this (simplified):

```java
try {
    invokeListener(record);
    ack(record);  // commit offset
} catch (Throwable t) {
    errorHandler.handleOne(t, record, consumer, container);
    // handler decides: retry (seek), recover (DLT), skip (ack), stop
}
```

If the user method catches the exception itself, `invokeListener` returns normally, `ack` runs, the offset advances. The error handler's pipeline is never entered. The configured `BackOff`, the `DeadLetterPublishingRecoverer`, the custom `handleOne` — all are bypassed.

The same applies to:
- A lambda body inside a chain (`Stream.forEach(record -> { try { ... } catch (...) {} })`).
- A `BiConsumer<ConsumerRecord, Acknowledgment>` registered as a `@KafkaListener` method reference.
- An `@RetryableTopic` annotated method that catches internally — retries are configured but never trigger.

## Why this is subtle

- "Catch and log" is the most universally-taught Java defensive idiom. It's almost reflexive.
- Tests usually pass — the exception is thrown, the test sees the log, the listener returns. Nothing about Spring's machinery is tested.
- Code review usually approves: "good, you're handling errors."
- Metrics and alerting based on Spring's error handler counters (`spring.kafka.listener.error`) under-report.
- For `@RetryableTopic`, the retry topics are *created* (the framework registers them eagerly) but no records ever land there. The infrastructure looks correctly provisioned.

## Operational impact

- No retries.
- No DLT publishing.
- Failed records committed and gone.
- Error-handler-based metrics silent.
- `@RetryableTopic` retry topics empty (operational confusion: "why are these topics empty?").

## Failure scenarios (walkthrough)

1. **The transient downstream blip.** Downstream HTTP API is flaky. Listener method calls it, catches `IOException`, logs "downstream failed, skipping". Record committed. Customer-visible behavior: missing data. Real cause: a 50-ms blip that one retry would have masked.

2. **The poison pill.** `ErrorHandlingDeserializer` correctly puts a `null` value and a deserialization-exception header on the record. Listener body: `if (record.value() == null) return;`. Record committed. Poison pill silently dropped. DLT empty.

3. **The retry-topic confusion.** Listener has `@RetryableTopic(attempts = "5")`. Listener body catches all exceptions. Retry topics `topic-retry-0` through `topic-retry-4` are auto-created on startup, sitting empty for years. New team member asks "what are these for?" and removes them, breaking the next migration.

## How to fix

```java
// BAD
@KafkaListener(topics = "orders")
public void handle(Order order) {
    try {
        orderService.process(order);
    } catch (Exception e) {
        log.error("Failed to process order {}", order.id(), e);
        // implicit: return, ack, move on — no retry, no DLT
    }
}

// GOOD — let the framework see the exception
@KafkaListener(topics = "orders")
public void handle(Order order) {
    orderService.process(order);
    // DefaultErrorHandler will catch, retry, recover-to-DLT
}

// GOOD — selective catching only for "this is fine" cases
@KafkaListener(topics = "orders")
public void handle(Order order) {
    try {
        orderService.process(order);
    } catch (DuplicateOrderException ignore) {
        // genuinely idempotent — safe to swallow
    }
    // other exceptions still escape
}
```

If you have *transactional* concerns (the listener does DB writes that must roll back), use `@Transactional` and let the exception escape — the rollback handler (`DefaultAfterRollbackProcessor`) does the right thing.

## Consult a friend?

> Slow down. If your `@KafkaListener` writes to a database, your "catch and log" was probably trying to preserve the DB write while skipping the record. Verify:
> 1. Is the DB write idempotent on replay? If yes — let the exception escape and rely on retry.
> 2. If not — your real bug is the lack of idempotency, not the missing catch. Fix that, *then* remove the catch.
> 3. If the listener spans multiple resources (DB + downstream HTTP + send-to-topic), look up "Spring Kafka chained transaction manager" before removing the catch.

## When this might be a false positive

- The catch block re-throws (visible in bytecode: `ATHROW` at end of handler).
- The catch block calls `acknowledgment.nack(...)` — that's the manual-ack escape hatch for "treat this as a failure even though I caught."
- The catch is narrow (`catch (SpecificBusinessException e)`) and the comment / metric counter shows intent.
- The `@KafkaListener` is set up for `errorHandler = "..."` argument that points to a `KafkaListenerErrorHandler` — that's the per-method error path; the broad catch may be deliberate.

## Detection strategy

- bytecode: visit methods annotated `@KafkaListener` (or registered via `@KafkaListener` on a class with `@KafkaHandler`).
- bytecode: build a CFG; if a top-level try block covers the entire method body, and its catch block does not throw (no `ATHROW`) and does not call `Acknowledgment.nack`, flag.
- Trickier with lambdas — if the listener delegates via `ConsumerRecord -> handler.accept(record)` and the lambda has the try/catch, follow the lambda.
- Confidence MEDIUM — many catches are legitimately narrow; need the throw-or-nack heuristic to keep false positives down.

## References

- Spring Kafka — error handling: https://docs.spring.io/spring-kafka/reference/kafka/annotation-error-handling.html
- Spring Kafka — `@RetryableTopic`: https://docs.spring.io/spring-kafka/reference/kafka/retrytopic.html
- Spring Kafka — `KafkaListenerErrorHandler`: https://docs.spring.io/spring-kafka/api/org/springframework/kafka/listener/KafkaListenerErrorHandler.html
