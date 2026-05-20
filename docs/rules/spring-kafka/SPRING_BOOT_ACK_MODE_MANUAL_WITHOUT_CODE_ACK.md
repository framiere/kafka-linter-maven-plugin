# SPRING_BOOT_ACK_MODE_MANUAL_WITHOUT_CODE_ACK

**Severity**: ERROR
**Confidence**: MEDIUM
**Detection**: annotation + bytecode + config-file
**Tagline**: ack-mode=MANUAL plus no acknowledge() means no commits, ever.

## TL;DR

The linter flags `spring.kafka.listener.ack-mode=MANUAL` (or `MANUAL_IMMEDIATE`) in properties, when no `@KafkaListener` method in the codebase declares an `Acknowledgment` parameter or calls `acknowledge()`. Offsets are never committed.

## What's happening (the mechanism)

`ack-mode=MANUAL` tells the container "I will commit offsets myself". The container hands an `Acknowledgment` to the listener method. If the listener doesn't accept the parameter or never calls `acknowledge()`:

- The container's offset is in memory but never sent to `__consumer_offsets`.
- On restart / rebalance, the partition reads from the last committed offset — possibly the group's first-ever assignment, which means re-processing back to the broker's retention horizon.
- The poll loop continues to work — no exception, no warning.

This is the dual of `SPRING_LISTENER_MANUAL_ACK_NEVER_CALLED`: that rule fires when the listener has the parameter but doesn't call ack; this one fires when the global property says MANUAL but no listener takes the parameter at all.

The combination is the "I changed the property but forgot to update code" case.

## Operational impact

- Same as `SPRING_LISTENER_MANUAL_ACK_NEVER_CALLED`: consumer lag huge after restart, downstream duplicate side effects, etc.
- More dangerous because the misconfiguration is in YAML — a deploy that flips the property triggers massive replay on next restart.

## How to fix

```yaml
# BAD — ack-mode is MANUAL but no listener takes Acknowledgment
spring:
  kafka:
    listener:
      ack-mode: MANUAL
```

```java
// Listener as it stands today
@KafkaListener(topics = "orders", groupId = "g")
public void onOrder(Order o) { ... }
```

Fix A — remove the property (default BATCH commits):

```yaml
spring:
  kafka:
    listener:
      # remove the line — default ack-mode is BATCH
```

Fix B — update the listener to ack:

```java
@KafkaListener(topics = "orders", groupId = "g")
public void onOrder(Order o, Acknowledgment ack) {
    process(o);
    ack.acknowledge();
}
```

## When this might be a false positive

- Listener method delegates the `Acknowledgment` to a helper class — bytecode scan must cross-reference helpers.
- Listener defined in a JAR dependency, not in the scanned classpath. The linter sees only properties and reports a false positive. Allow suppression at the property level.
- Multiple container factories: the property may apply to only one, with another factory taking BATCH.

## Detection strategy

- Config: scan for `spring.kafka.listener.ack-mode` with value `MANUAL` or `MANUAL_IMMEDIATE`.
- Bytecode: enumerate `@KafkaListener` methods and check whether any of them declare a parameter of type `org.springframework.kafka.support.Acknowledgment`.
- If none, flag at ERROR.
- If some do, the rule is satisfied (it's about the global property having no consumer-side support at all).
- Confidence: MEDIUM — the static check can't know if a `Acknowledgment` is captured by a helper class.

## References

- Spring Kafka — Manually Committing Offsets: https://docs.spring.io/spring-kafka/reference/kafka/receiving-messages/ooo-commits.html
- Spring Boot — `spring.kafka.listener.ack-mode`: https://docs.spring.io/spring-boot/reference/messaging/kafka.html
- Related rule: `SPRING_LISTENER_MANUAL_ACK_NEVER_CALLED`.
