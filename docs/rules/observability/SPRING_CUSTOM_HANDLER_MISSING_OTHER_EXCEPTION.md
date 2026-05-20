# SPRING_CUSTOM_HANDLER_MISSING_OTHER_EXCEPTION

**Severity**: WARNING
**Confidence**: HIGH
**Detection**: bytecode
**Tagline**: A custom `CommonErrorHandler` that skips `handleOtherException` is half an error handler.

## TL;DR

The linter flags classes implementing `org.springframework.kafka.listener.CommonErrorHandler` that override `handleOne` / `handleRemaining` / `handleBatch` but do **not** override `handleOtherException`. `handleOtherException` is invoked when an exception occurs *outside* the record-processing path — consumer errors, commit failures, rebalance callbacks. Default implementation logs at ERROR and returns. Without your override, these container-level exceptions never reach your DLT / metrics / alerting pipeline.

## The setup

A team wants custom error handling: maybe routing different exception types to different DLT topics, maybe a circuit-breaker pattern. They write `class OrderErrorHandler implements CommonErrorHandler`. They override `handleOne` and `handleRemaining`. Their tests pass. They deploy. Eventually a consumer-side error happens — a commit timeout, a rebalance failure — and the handler's `handleOne` is never invoked. The exception is logged once at ERROR and forgotten.

## What's actually happening

`CommonErrorHandler` (Spring Kafka 2.8+, replacement for the deprecated `ErrorHandler` and `BatchErrorHandler`) has these key methods:

| Method | Invoked when |
|--------|--------------|
| `handleOne(Exception, ConsumerRecord, Consumer, MessageListenerContainer)` | Record listener throws |
| `handleRemaining(Exception, List<ConsumerRecord>, Consumer, MessageListenerContainer)` | Listener throws with remaining records to process |
| `handleBatch(Exception, ConsumerRecords, Consumer, MessageListenerContainer)` | Batch listener throws |
| **`handleOtherException(Exception, Consumer, MessageListenerContainer, boolean batchListener)`** | Anything else: commit failure, consumer poll exception, rebalance callback failure, `WakeupException` propagation |

The default `handleOtherException` implementation (from Spring's abstract base) just logs at ERROR level and returns. Container continues. The listener never sees the error. Your DLT/metrics/alerting code in `handleOne` doesn't run.

From the docs: *"`handleOtherException()` — handles exceptions outside record processing scope (e.g., consumer errors). Required for all custom implementations."*

## Why this is subtle

- The interface offers default methods. You can implement just one and the rest are stubs. Compilation passes, tests pass (because tests focus on record paths).
- Documentation phrases it as "should override" rather than "must override" — easy to skip.
- The exceptions that come through `handleOtherException` are exactly the ones you most want to alert on: commit failures, group coordinator errors, broker disconnects.
- Unit testing this path requires injecting a `WakeupException` or simulating a coordinator failure — not part of typical test coverage.

## Operational impact

- Commit failures swallowed. The consumer may be hitting "offset commit failed because group rebalanced" repeatedly with no visible signal beyond ERROR logs.
- Rebalance callback exceptions logged once and ignored.
- Custom metrics for "errors handled" undercount the real error rate.
- Dashboards based on `handleOne` counters look healthy during container-level distress.

## Failure scenarios (walkthrough)

1. **The commit-storm.** During rebalance, offset commits race the new generation. `CommitFailedException` is thrown out-of-band of any record. Without `handleOtherException`, your "errors per minute" metric stays flat, but in reality every commit is failing and the consumer is reading the same records over and over after each rebalance.

2. **The broker disconnect.** Network partition kills the connection to the group coordinator. Heartbeats fail. The error surfaces as a poll-time exception, routed to `handleOtherException`. Default behavior: ERROR log. Your alerting is silent.

## How to fix

```java
// BAD — only record paths overridden
public class OrderErrorHandler implements CommonErrorHandler {
    @Override
    public void handleOne(Exception ex, ConsumerRecord<?, ?> record,
                          Consumer<?, ?> consumer, MessageListenerContainer container) {
        sendToDlt(record, ex);
        meterRegistry.counter("kafka.listener.error").increment();
    }
    // handleOtherException not overridden → default ERROR log, swallowed
}

// GOOD — handle every path
public class OrderErrorHandler implements CommonErrorHandler {
    @Override
    public void handleOne(Exception ex, ConsumerRecord<?, ?> record,
                          Consumer<?, ?> consumer, MessageListenerContainer container) {
        sendToDlt(record, ex);
        meterRegistry.counter("kafka.listener.error", "kind", "record").increment();
    }

    @Override
    public void handleOtherException(Exception ex, Consumer<?, ?> consumer,
                                     MessageListenerContainer container, boolean batchListener) {
        log.error("Container-level Kafka error in {} (batch={})",
                  container.getListenerId(), batchListener, ex);
        meterRegistry.counter("kafka.listener.error", "kind", "container").increment();
        alertingClient.fireKafkaContainerError(container.getListenerId(), ex);
    }

    @Override
    public boolean seeksAfterHandling() { return false; }  // honest about behavior
}
```

If you only want to *log differently* but keep default container behavior, extend `DefaultErrorHandler` instead of implementing `CommonErrorHandler` from scratch — you inherit the right `handleOtherException` behavior and only customize what you need.

## When this might be a false positive

- The class extends `DefaultErrorHandler` and overrides only specific methods — `DefaultErrorHandler` has a sensible `handleOtherException` already.
- The class implements `CommonErrorHandler` but delegates to a `DefaultErrorHandler` field for every method (composition pattern).

## Detection strategy

- bytecode: scan classes implementing `org.springframework.kafka.listener.CommonErrorHandler` directly (not extending `DefaultErrorHandler` or `AbstractKafkaListenerErrorHandler`).
- bytecode: enumerate overridden methods; flag if `handleOtherException` is absent.
- Whitelist: composition via a delegate field of type `CommonErrorHandler` whose methods are forwarded.
- Confidence HIGH — method enumeration is exact.

## References

- Spring Kafka — `CommonErrorHandler`: https://docs.spring.io/spring-kafka/api/org/springframework/kafka/listener/CommonErrorHandler.html
- Spring Kafka — error handling reference: https://docs.spring.io/spring-kafka/reference/kafka/annotation-error-handling.html
- Migration from legacy `ErrorHandler`: https://docs.spring.io/spring-kafka/reference/kafka/annotation-error-handling.html#legacy-eh
