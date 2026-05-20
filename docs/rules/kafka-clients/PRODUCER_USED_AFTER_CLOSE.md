# PRODUCER_USED_AFTER_CLOSE

**Severity**: ERROR
**Confidence**: HIGH
**Detection**: bytecode
**Tagline**: A closed producer answers "no" to every send. Forever.

## TL;DR

The linter flags `producer.send(...)` (or `flush()`, `beginTransaction()`, etc.) reachable after `producer.close()` on the same instance. Closed producers reject all subsequent calls with `IllegalStateException`.

## What's happening (the mechanism)

`KafkaProducer` has a one-way lifecycle: `OPEN` → `CLOSED`. After `close()` (or `close(Duration)`), the sender thread is stopped, the accumulator is drained, and the internal `closed` flag is set. Any subsequent call to:

- `send()`
- `flush()`
- `beginTransaction()`
- `partitionsFor()`
- `metrics()` (some versions)

throws `IllegalStateException: Cannot perform operation after producer has been closed`.

Common failure pattern: a try-with-resources that captures the producer reference into a callback executed after the try block exits.

## Operational impact

- `IllegalStateException` on the post-close `send()`; if it happens in an async callback, the exception can be swallowed silently.
- Lost messages.

## How to fix

```java
// BAD
try (KafkaProducer<K, V> p = new KafkaProducer<>(props)) {
    futureSubmit(() -> p.send(record)); // executor runs this after try-with-resources closes p
}

// GOOD — keep the producer alive until all senders complete
KafkaProducer<K, V> p = new KafkaProducer<>(props);
try {
    Future<?> f = futureSubmit(() -> p.send(record));
    f.get();
} finally {
    p.close(Duration.ofSeconds(10));
}
```

## When this might be a false positive

- The post-close call is in an unreachable error path. Rare.

## Detection strategy

- Bytecode: track linear control flow within a method; after a `KafkaProducer.close(...)` call, flag any subsequent invocation of `send`, `flush`, `beginTransaction`, `commitTransaction`, `abortTransaction`, `initTransactions` on the same instance. HIGH.
- Also flag post-close calls via captured-lambda escape (HIGH if the lambda is submitted before close, executed after).

## References

- KafkaProducer.close() javadoc: https://kafka.apache.org/40/javadoc/org/apache/kafka/clients/producer/KafkaProducer.html#close--
