# CONSUMER_NOT_CLOSED

**Severity**: WARNING
**Confidence**: HIGH
**Detection**: bytecode
**Tagline**: If you don't close the consumer, the group waits 30s+ to forget you.

## TL;DR

The linter flags `KafkaConsumer` instances that are constructed but never closed. On JVM exit, the group coordinator only learns the consumer is gone after `session.timeout.ms`, stalling partition reassignment.

## What's happening (the mechanism)

`KafkaConsumer.close()`:
1. Commits any pending auto-commit offsets.
2. Sends an explicit `LeaveGroup` request to the coordinator → immediate rebalance.
3. Releases network resources.

Without `close()`:
- The coordinator only realizes the member is dead after `session.timeout.ms` (default 45s in Kafka 3.0+) of missing heartbeats.
- During that window, the dead member's partitions are stuck — no other consumer in the group can fetch them.
- Static members (`group.instance.id` set) intentionally skip `LeaveGroup`, so the "don't close" anti-pattern partially merges with the static membership pattern — but you still leak resources locally.

`KafkaConsumer` implements `AutoCloseable`.

## Operational impact

- 45-second downstream stall on every restart for dynamic consumers.
- Partition lag spikes on every rolling deploy.
- For static consumers, also: orphan resources, file descriptor leaks under restart loops.

## How to fix

```java
// BAD
KafkaConsumer<String, String> c = new KafkaConsumer<>(props);
c.subscribe(List.of("topic"));
while (running) { c.poll(Duration.ofSeconds(1)); }
// no close — group hangs for 45s

// GOOD — try-with-resources around the run loop
try (KafkaConsumer<String, String> c = new KafkaConsumer<>(props)) {
    c.subscribe(List.of("topic"));
    while (!shutdownRequested.get()) {
        c.poll(Duration.ofSeconds(1));
    }
} // close() runs on exit

// GOOD — wakeup pattern for clean shutdown from another thread
KafkaConsumer<String, String> c = new KafkaConsumer<>(props);
Runtime.getRuntime().addShutdownHook(new Thread(c::wakeup));
try {
    while (true) { c.poll(Duration.ofSeconds(1)); }
} catch (WakeupException e) {
    // clean shutdown
} finally {
    c.close(Duration.ofSeconds(5));
}
```

## When this might be a false positive

- Framework-managed consumer (Spring `@KafkaListener`, Quarkus `@Incoming`). Suppress if the consumer reference is registered with a known framework container.

## Detection strategy

- Bytecode: scan for `new KafkaConsumer(...)`. Track the reference; flag if no `close()` is reachable.
- Suppress for try-with-resources patterns.
- HIGH for local-scoped consumer; MEDIUM when reference escapes.

## References

- KafkaConsumer.close() javadoc: https://kafka.apache.org/40/javadoc/org/apache/kafka/clients/consumer/KafkaConsumer.html#close--
- Apache Kafka — Consumer group rebalancing: https://kafka.apache.org/documentation/#impl_consumer
