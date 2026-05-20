# CONSUMER_SEEK_BEFORE_POLL

**Severity**: ERROR
**Confidence**: MEDIUM
**Detection**: bytecode
**Tagline**: seek() before poll() is asking to skip to a chapter of a book you haven't opened.

## TL;DR

The linter flags `consumer.seek(...)` calls reachable before any `consumer.poll(...)` has executed. The consumer has no partition assignment until the first `poll()` completes the group join, so `seek()` throws `IllegalStateException`.

## What's happening (the mechanism)

For consumers using `subscribe()` (dynamic group membership):
- Partition assignment is established during the first `poll()` (group coordinator dance).
- Before that, `assignment()` is empty and `seek(tp, offset)` throws `IllegalStateException: No current assignment for partition <tp>`.

The correct pattern is:
1. `subscribe()` with a `ConsumerRebalanceListener`.
2. `poll(0)` (or `poll(Duration.ZERO)`) to drive the group join — discard records.
3. `seek()` inside `onPartitionsAssigned`, or in the main loop after the first non-empty poll, or use `ConsumerRebalanceListener.onPartitionsAssigned`.

For consumers using `assign()` (manual partition assignment), `seek()` immediately after `assign()` is fine.

## Operational impact

- `IllegalStateException` on startup, consumer thread dies before processing any records.
- If retried in a loop, the group enters a join/leave thrash.

## How to fix

```java
// BAD
consumer.subscribe(List.of("topic"));
consumer.seek(new TopicPartition("topic", 0), 100); // No current assignment

// GOOD — manual assign
consumer.assign(List.of(new TopicPartition("topic", 0)));
consumer.seek(new TopicPartition("topic", 0), 100);

// GOOD — subscribe + seek inside callback
consumer.subscribe(List.of("topic"), new ConsumerRebalanceListener() {
    public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
        for (TopicPartition tp : partitions) {
            consumer.seek(tp, lookupOffset(tp));
        }
    }
    public void onPartitionsRevoked(Collection<TopicPartition> partitions) {}
});
while (running) { consumer.poll(Duration.ofSeconds(1)); }
```

## When this might be a false positive

- Manual `assign()` path (legitimate); the rule must specifically detect `subscribe()`-then-`seek()`-no-`poll()`.

## Detection strategy

- Bytecode: in a method that calls `KafkaConsumer.subscribe(...)`, locate the first reachable `seek(...)` and the first reachable `poll(...)`. If `seek()` precedes `poll()` in linear control flow AND the seek is not inside a `ConsumerRebalanceListener.onPartitionsAssigned` implementation, flag. MEDIUM.
- Distinguish from `assign()`-based code (which is fine).

## References

- KafkaConsumer.seek() javadoc — "Lookup the offset for the given partition...": https://kafka.apache.org/40/javadoc/org/apache/kafka/clients/consumer/KafkaConsumer.html#seek-org.apache.kafka.common.TopicPartition-long-
- KafkaConsumer "Manual Partition Assignment" section: https://kafka.apache.org/40/javadoc/org/apache/kafka/clients/consumer/KafkaConsumer.html
