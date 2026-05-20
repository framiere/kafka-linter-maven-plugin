# QK_CHECKED_EXCEPTION_FROM_INCOMING

**Severity**: WARNING
**Confidence**: HIGH
**Detection**: bytecode
**Tagline**: A checked exception that escapes @Incoming is a nack you didn't plan for.

## TL;DR

An `@Incoming` method declaring `throws SQLException` (or any checked exception) is contractually saying "I can throw — handle me." SmallRye will nack the message and invoke the `failure-strategy`. If the default `fail` is in effect with no DLQ, the channel dies on the first thrown exception.

## What's happening (the mechanism)

Java's checked-exception contract surfaces at the call site. SmallRye's reactive runtime catches any throwable from a handler and routes it through:

1. The configured `failure-strategy` (default `fail`).
2. `failure-strategy=fail` halts the channel; offset uncommitted; restart re-runs the bad record.

Checked exceptions advertise this as a known code path — but operators often write `throws SQLException` and forget to configure a `dead-letter-queue` to absorb it. The compile-time signature is screaming at you and you're tuning it out.

## Operational impact

- Same as QK_FAILURE_STRATEGY_FAIL_NO_DLQ — channel halt + crashloop on the bad record.
- The checked exception declaration is the *evidence in the code* that failures are expected, making the missing DLQ even more glaring.

## How to fix

Two combine-able fixes:

```java
// GOOD — handle the checked exception in code
@Incoming("orders")
public void handle(Order o) {
    try {
        db.persist(o);
    } catch (SQLException e) {
        // log, route to DLQ via emitter, or rethrow as a typed runtime exception
        throw new OrderPersistenceException(e);
    }
}
```

```properties
# GOOD — configure DLQ at the channel level
mp.messaging.incoming.orders.failure-strategy=dead-letter-queue
mp.messaging.incoming.orders.dead-letter-queue.topic=orders-dlq
```

## When this might be a false positive

- The channel is explicitly configured with `failure-strategy=dead-letter-queue` or `delayed-retry-topic`.
- The handler catches and rethrows as runtime; the linter sees the `throws` but the catch is present.

## Detection strategy

- Bytecode: `@Incoming` method whose `Exceptions` attribute lists any checked exception (not `RuntimeException`/`Error` subclass).
- Suppress when matching channel has a non-default `failure-strategy`.
- Confidence: HIGH for the combo of `throws` + default `fail` strategy.

## References

- https://quarkus.io/guides/kafka (Failure strategies)
- https://smallrye.io/smallrye-reactive-messaging/latest/concepts/error-handling/
