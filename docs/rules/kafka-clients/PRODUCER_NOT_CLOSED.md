# PRODUCER_NOT_CLOSED

**Severity**: ERROR
**Confidence**: HIGH
**Detection**: bytecode
**Tagline**: An unclosed producer is unflushed buffers waiting for JVM shutdown to lose them.

## TL;DR

The linter flags `KafkaProducer` instances that are constructed but never closed via `close()` or try-with-resources. On JVM exit, in-flight records sit in the accumulator and are lost.

## What's happening (the mechanism)

`KafkaProducer.close()` does three things:
1. Blocks until in-flight records are sent and acked (up to `close(Duration)` timeout).
2. Sends a clean disconnect to the broker.
3. Releases the sender thread, the network thread, and the producer-side buffer pool.

Without `close()`, on JVM shutdown:
- The sender thread is killed mid-batch.
- Records in the accumulator are silently dropped.
- The broker sees a TCP RST instead of a clean close.
- For a transactional producer, the in-flight transaction is left open; the broker fences it after `transaction.timeout.ms` (default 60s), during which downstream `read_committed` consumers stall on the LSO.

`KafkaProducer` implements `AutoCloseable` since 0.10 — try-with-resources is the idiomatic answer.

## Operational impact

- Lost messages on every restart, scaling event, or deploy.
- Stuck transactions for the transaction timeout window — downstream consumers lag without obvious cause.
- TCP RST in broker logs and brief connection churn.

## How to fix

```java
// BAD
KafkaProducer<String, String> p = new KafkaProducer<>(props);
p.send(record);
// no close — buffered records lost on shutdown

// GOOD — try-with-resources
try (KafkaProducer<String, String> p = new KafkaProducer<>(props)) {
    p.send(record).get();
}

// GOOD — long-lived producer with explicit shutdown hook
KafkaProducer<String, String> producer = new KafkaProducer<>(props);
Runtime.getRuntime().addShutdownHook(new Thread(() -> producer.close(Duration.ofSeconds(10))));
```

## When this might be a false positive

- Producer wrapped by a framework (Spring `KafkaTemplate`, Quarkus emitter) that manages lifecycle. Suppress if the producer reference is returned / passed to a framework registration.
- Static singleton producers managed by a shutdown hook elsewhere in the code base.

## Detection strategy

- Bytecode: scan for `new KafkaProducer(...)` allocations. Track the resulting reference; flag if it does not escape (returned, stored in a field, passed as arg) AND no `close()` invocation is reachable on it.
- Suppress for try-with-resources (`ASTORE` followed by an exception table marking the close).
- HIGH for locally-scoped allocations without close; MEDIUM when reference escapes (cannot prove externally managed).

## References

- KafkaProducer.close() javadoc: https://kafka.apache.org/40/javadoc/org/apache/kafka/clients/producer/KafkaProducer.html#close--
- Confluent — Kafka Producer Reliability: https://docs.confluent.io/platform/current/clients/producer.html
