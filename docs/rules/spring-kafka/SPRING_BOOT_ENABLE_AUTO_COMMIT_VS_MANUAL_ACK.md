# SPRING_BOOT_ENABLE_AUTO_COMMIT_VS_MANUAL_ACK

**Severity**: ERROR
**Confidence**: HIGH
**Detection**: annotation + bytecode + config-file
**Tagline**: `enable-auto-commit=true` + Acknowledgment in code = two cooks, no kitchen.

## TL;DR

The linter flags applications that set `spring.kafka.consumer.enable-auto-commit=true` AND have at least one `@KafkaListener` method declaring an `Acknowledgment` parameter. The configurations contradict each other.

## What's happening (the mechanism)

Spring Kafka uses one of two offset-commit strategies:

1. **Kafka client auto-commit** (`enable.auto.commit=true`): the consumer client commits offsets every `auto.commit.interval.ms` (default 5_000 ms) in the background. Spring's container is bypassed.
2. **Spring-managed commit** (`enable.auto.commit=false`, the Spring default when used via `@KafkaListener`): the container commits at the boundaries defined by `AckMode` (`BATCH`, `RECORD`, `MANUAL`, etc.). Manual modes hand control to the application via `Acknowledgment`.

When `enable-auto-commit=true` is set explicitly AND a listener uses `Acknowledgment`:

- The client's background thread commits offsets every 5 s regardless of whether `acknowledge()` was called.
- A listener that *forgets* to ack still commits, defeating the safety net.
- A listener that *acks late* — the offset was already committed seconds ago; `acknowledge()` is now a no-op (or worse, depending on AckMode interactions).
- Spring's `AckMode.MANUAL` semantics are silently broken; the `Acknowledgment` instance still exists but its `acknowledge()` calls don't drive commits.

Spring Boot defaults `enable-auto-commit` to false when `@KafkaListener` is used — this rule fires when someone explicitly set it to true.

## Operational impact

- "MANUAL ack doesn't work" — developer asserts records are reprocessed after restart, but actually offsets are getting committed by the background thread.
- Conversely: a record fails, the developer calls `nack(...)` to retry, but the offset was committed 4 seconds ago — the next poll skips the record entirely.
- Container logs show no errors; the misbehavior is purely semantic.

## How to fix

```properties
# BAD
spring.kafka.consumer.enable-auto-commit=true
# with @KafkaListener(topics="x") void handle(X x, Acknowledgment ack)

# GOOD A — let Spring manage commits, pick AckMode in code
# (remove the enable-auto-commit line; Spring defaults to false)
spring.kafka.listener.ack-mode=MANUAL

# GOOD B — keep auto-commit, drop manual Acknowledgment
# spring.kafka.consumer.enable-auto-commit=true
# (remove Acknowledgment parameter from listeners)
```

If you genuinely want the Kafka client to auto-commit (rare), do not also use `Acknowledgment` — the two are mutually exclusive.

## When this might be a false positive

- Mixed application with multiple container factories: one factory has `enable.auto.commit=true` (used for some listeners), another sets it to `false` and uses `MANUAL` ack. The static check sees the global property only; it can't tell which factory each listener uses. Flag at MEDIUM confidence when both annotations and properties are detected.

## Detection strategy

- Config: parse `application*.properties|yml` for `spring.kafka.consumer.enable-auto-commit=true` (and `spring.kafka.consumer.properties[enable.auto.commit]=true`).
- Bytecode: scan `@KafkaListener` methods for an `Acknowledgment` parameter (type `org.springframework.kafka.support.Acknowledgment`).
- Flag when both are present.
- Confidence: HIGH if the global property is true and at least one `Acknowledgment` parameter exists; downgrade to MEDIUM if a custom `containerFactory` is referenced.

## References

- Spring Kafka — Manually Committing Offsets: https://docs.spring.io/spring-kafka/reference/kafka/receiving-messages/ooo-commits.html
- Spring Boot — `spring.kafka.consumer.enable-auto-commit`: https://docs.spring.io/spring-boot/reference/messaging/kafka.html
- Apache Kafka — `enable.auto.commit`: https://kafka.apache.org/documentation/#consumerconfigs_enable.auto.commit
