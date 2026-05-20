# COMMIT_ASYNC_NO_FINAL_SYNC

**Severity**: WARNING
**Confidence**: MEDIUM
**Detection**: bytecode
**Tagline**: commitAsync is fast and forgetful — pair it with one commitSync on the way out.

## TL;DR

The linter flags consumers that call `commitAsync()` in the main loop but never call `commitSync()` (or `commitAsync` with a callback that re-tries) in shutdown. The last asynchronous commit can be lost if the consumer closes before the broker responds.

## What's happening (the mechanism)

`commitAsync()` fires the offset commit request and returns immediately. The result lands in a callback. If the consumer closes before the response is received and acknowledged, the broker may not have persisted the commit — the next start of the same group re-processes the uncommitted records.

The canonical pattern:
1. Use `commitAsync()` inside the loop for throughput.
2. Use `commitSync()` in a `finally` block to ensure the last commit is durable.
3. Optionally, use `commitAsync(OffsetCommitCallback)` and retry on `RetriableException`.

## Operational impact

- Duplicate processing on restart (`commitAsync()` race lost during shutdown).
- Hard to reproduce — only fires when shutdown beats the commit response.
- `kafka.consumer:type=consumer-coordinator-metrics:commit-rate` shows commits but the broker `__consumer_offsets` lags behind.

## How to fix

```java
// BAD
try {
    while (running) {
        ConsumerRecords<K, V> records = consumer.poll(Duration.ofSeconds(1));
        process(records);
        consumer.commitAsync();
    }
} finally {
    consumer.close();
}

// GOOD
try {
    while (running) {
        ConsumerRecords<K, V> records = consumer.poll(Duration.ofSeconds(1));
        process(records);
        consumer.commitAsync(); // fast path
    }
} catch (WakeupException e) {
    // expected on shutdown
} finally {
    try {
        consumer.commitSync(); // make the last commit durable
    } finally {
        consumer.close();
    }
}
```

## When this might be a false positive

- Consumers that store offsets externally (database, custom store) and don't rely on Kafka commits.
- `enable.auto.commit=true` consumers (different anti-pattern entirely — covered elsewhere).

## Detection strategy

- Bytecode: locate `KafkaConsumer.commitAsync(...)` calls. Check the surrounding method / class for a `commitSync(...)` call reachable in a `finally` block or shutdown path. If none, flag. MEDIUM.

## References

- KafkaConsumer.commitAsync() javadoc: https://kafka.apache.org/40/javadoc/org/apache/kafka/clients/consumer/KafkaConsumer.html#commitAsync--
- "Kafka: The Definitive Guide" — Combining sync + async commits pattern.
- Confluent — consumer offset management: https://docs.confluent.io/platform/current/clients/consumer.html
