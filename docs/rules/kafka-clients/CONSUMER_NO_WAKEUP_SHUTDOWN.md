# CONSUMER_NO_WAKEUP_SHUTDOWN

**Severity**: WARNING
**Confidence**: MEDIUM
**Detection**: bytecode
**Tagline**: Without wakeup(), poll() never gets the memo to stop.

## TL;DR

The linter flags consumer poll loops that have no path to call `consumer.wakeup()` from a shutdown hook or signal handler. Without it, a thread blocked in `poll()` cannot be interrupted cleanly — JVM shutdown kills it mid-fetch.

## What's happening (the mechanism)

`KafkaConsumer.poll(Duration)` blocks for up to the timeout while it waits for records. The duration is a hint; if a rebalance callback is running, `poll()` ignores it. The standard pattern to exit cleanly is:

1. Another thread calls `consumer.wakeup()` (the only thread-safe method).
2. The next `poll()` throws `WakeupException`.
3. The consumer loop catches it, breaks out, and calls `close()`.

Without `wakeup()`, shutdown options are:
- `Thread.interrupt()` — works for some blocking calls but is generally not safe for `KafkaConsumer`.
- Setting a `volatile boolean running` to false — only effective when `poll()` returns naturally (after `timeout`). Pathological case: long `poll()` timeout means long shutdown.

## Operational impact

- `kubectl delete pod` / SIGTERM forces JVM to be killed after `terminationGracePeriodSeconds`, often before `poll()` returns.
- Final offset commit may be skipped → duplicate processing on next start.
- For static members, the missed `close()` does not even trigger a `LeaveGroup`, so partitions stay assigned to the dead instance for `session.timeout.ms`.

## How to fix

```java
// BAD — no escape path
KafkaConsumer<K, V> c = new KafkaConsumer<>(props);
c.subscribe(topics);
while (true) {
    c.poll(Duration.ofSeconds(10)); // blocks; nothing can stop it cleanly
}

// GOOD
KafkaConsumer<K, V> c = new KafkaConsumer<>(props);
Runtime.getRuntime().addShutdownHook(new Thread(c::wakeup));
c.subscribe(topics);
try {
    while (true) {
        ConsumerRecords<K, V> r = c.poll(Duration.ofSeconds(10));
        // process...
    }
} catch (WakeupException e) {
    // expected during shutdown
} finally {
    try { c.commitSync(); } finally { c.close(Duration.ofSeconds(5)); }
}
```

## When this might be a false positive

- Framework-managed consumers (Spring container, Quarkus connector) where the framework handles the wakeup. Suppress if `@KafkaListener`, `@Incoming`, etc. is found in the same project.
- One-shot consumers with very short `poll()` timeout (e.g., < 100ms) that rely on a `running` flag — borderline acceptable.

## Detection strategy

- Bytecode: locate `KafkaConsumer.poll(...)` calls inside infinite loops; verify the same consumer reference has a reachable `wakeup()` call (in a shutdown hook, `SignalHandler`, or a thread that holds a reference). MEDIUM (heuristic).
- Suppress when the consumer is owned by a framework wrapper.

## References

- KafkaConsumer.wakeup() javadoc: https://kafka.apache.org/40/javadoc/org/apache/kafka/clients/consumer/KafkaConsumer.html#wakeup--
- KafkaConsumer "Multi-threaded Processing" section: https://kafka.apache.org/40/javadoc/org/apache/kafka/clients/consumer/KafkaConsumer.html
