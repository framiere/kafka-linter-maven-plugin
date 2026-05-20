# SPRING_LISTENER_MANUAL_ACK_NEVER_CALLED

**Severity**: ERROR
**Confidence**: HIGH
**Detection**: bytecode
**Tagline**: AckMode.MANUAL without calling acknowledge() is just AckMode.NEVER.

## TL;DR

The linter flags `@KafkaListener` methods that declare an `Acknowledgment` parameter but never invoke `acknowledge()` (or `nack(...)`) on any code path. Offsets never advance and the partition replays from the same offset on every restart and rebalance.

## What's happening (the mechanism)

When the container is configured with `AckMode.MANUAL` or `AckMode.MANUAL_IMMEDIATE`, Spring injects an `Acknowledgment` parameter into the listener method. The framework commits offsets only when the application calls `ack.acknowledge()` (or stages a `nack(...)` for redelivery).

If `acknowledge()` is never called:
- With `AckMode.MANUAL`: the offset commit is queued for the next poll cycle — but nothing is queued, so nothing commits. The in-memory consumer position advances, but `__consumer_offsets` is never updated.
- With `AckMode.MANUAL_IMMEDIATE`: same — no commit happens, ever.
- On consumer restart / rebalance / partition reassignment, the partition reads from the last committed offset (or `auto.offset.reset` if no offsets exist), and the application reprocesses everything since startup.

Because the poll loop keeps running, the application appears to be processing fine — until the first restart, when the same window of records is replayed. In high-throughput topics this can mean millions of duplicates.

There is an additional ordering hazard: per the docs, *"when using `AckMode.MANUAL` or `AckMode.MANUAL_IMMEDIATE`, the acknowledgments must be acknowledged in order, because Kafka does not maintain state for each record"*. Skipping an `acknowledge()` for one record stalls commits for all later records in that partition too.

## Operational impact

- Consumer lag in `kafka-consumer-groups --describe` is huge (or grows) despite the application processing records — the committed offset is the one from startup.
- On rolling restart: every replica reprocesses everything since the last graceful commit, which for a never-acknowledged listener is everything since the group's last earlier owner.
- Spring Actuator metric `spring.kafka.listener` shows records consumed but `__consumer_offsets` topic shows no progress.
- Idempotency-required downstreams (DB writes, payment processing) suffer N-fold duplicate side effects after restart.

## How to fix

```java
// BAD — Acknowledgment is injected but ignored
@KafkaListener(topics = "orders", groupId = "g",
               containerFactory = "manualAckFactory")
public void onOrder(Order o, Acknowledgment ack) {
    process(o);
    // forgot ack.acknowledge();
}

// GOOD — acknowledge in the success path
@KafkaListener(topics = "orders", groupId = "g",
               containerFactory = "manualAckFactory")
public void onOrder(Order o, Acknowledgment ack) {
    process(o);
    ack.acknowledge();
}

// BETTER — let the container commit on successful return (BATCH/RECORD)
// Remove the Acknowledgment parameter and use AckMode.BATCH (default)
@KafkaListener(topics = "orders", groupId = "g")
public void onOrder(Order o) {
    process(o);
}
```

If you only have manual ack because you want to retry on failure: prefer `DefaultErrorHandler` (default in spring-kafka 3.x+) with a `BackOff` — it gives you retry-then-DLT semantics without manual offset gymnastics.

## When this might be a false positive

- The method delegates to an async pipeline that calls `acknowledge()` later (e.g., stored in a `Map<Acknowledgment, Future<?>>` and acked on completion). Bytecode analysis must follow the `Acknowledgment` reference into the helper.
- Conditional ack paths controlled by a flag — both paths should call `acknowledge()` or `nack(...)`. If only the error path is missing, downgrade to WARNING.

## Detection strategy

- Bytecode: identify `@KafkaListener` methods with parameter type `org.springframework.kafka.support.Acknowledgment`. Use an ASM `MethodVisitor` to track `ALOAD` of that local var and check whether any subsequent `INVOKEINTERFACE Acknowledgment.acknowledge()V` (or `nack(...)`) is reachable.
- Pessimistic version: any read of the `Acknowledgment` parameter slot is "good enough" — eliminates false positives from helpers but introduces a few false negatives.
- If the method passes the `Acknowledgment` to another method, fall back to checking that other method (intra-class) before reporting.
- Confidence: HIGH when no read at all; MEDIUM when read but no `acknowledge()` directly visible.

## References

- Spring Kafka — Manually Committing Offsets: https://docs.spring.io/spring-kafka/reference/kafka/receiving-messages/ooo-commits.html
- Spring Kafka — `Acknowledgment` Javadoc: https://docs.spring.io/spring-kafka/docs/current/api/org/springframework/kafka/support/Acknowledgment.html
