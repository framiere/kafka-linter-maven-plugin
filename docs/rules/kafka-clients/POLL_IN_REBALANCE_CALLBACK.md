# POLL_IN_REBALANCE_CALLBACK

**Severity**: ERROR
**Confidence**: HIGH
**Detection**: bytecode
**Tagline**: Calling poll() inside onPartitionsRevoked is asking the consumer to interrupt itself.

## TL;DR

The linter flags `consumer.poll(...)` invocations reachable inside a `ConsumerRebalanceListener.onPartitionsRevoked` / `onPartitionsAssigned` / `onPartitionsLost` callback. The callback runs on the poll thread mid-rebalance; calling `poll()` recursively corrupts the rebalance state.

## What's happening (the mechanism)

`ConsumerRebalanceListener` callbacks fire inside `poll()` — the same thread that was already in `poll()` when the rebalance was triggered. The callback is allowed to use the consumer for `commitSync()`, `committed()`, `position()`, `seek()`, etc., but NOT for recursive `poll()`.

Calling `poll()` from inside the callback:
- Re-enters the rebalance state machine while it is in the middle of a phase.
- Triggers `IllegalStateException` in some Kafka versions, undefined behavior in others.
- At minimum, breaks the documented "callback completes successfully → assignment finalized" contract.

The other consumer-mutating operations are allowed (and commit-on-revoke is the canonical pattern), so the rule must specifically target `poll()`.

## Operational impact

- `IllegalStateException` at runtime, kills the consumer thread.
- Group enters a rebalance crashloop because every new owner trips the same bug.
- Difficult to reproduce in unit tests — only fires under real rebalance triggers.

## How to fix

```java
// BAD
consumer.subscribe(topics, new ConsumerRebalanceListener() {
    @Override
    public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
        consumer.poll(Duration.ofMillis(10)); // ILLEGAL
    }
    @Override
    public void onPartitionsAssigned(Collection<TopicPartition> partitions) {}
});

// GOOD — commit, seek, query offsets — but never poll
consumer.subscribe(topics, new ConsumerRebalanceListener() {
    @Override
    public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
        consumer.commitSync(currentOffsets);
    }
    @Override
    public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
        for (TopicPartition tp : partitions) {
            consumer.seek(tp, loadSavedOffset(tp));
        }
    }
});
```

## When this might be a false positive

- None — `poll()` inside the callback is always wrong.

## Detection strategy

- Bytecode: identify implementations of `org.apache.kafka.clients.consumer.ConsumerRebalanceListener` (the three method signatures `onPartitionsRevoked`, `onPartitionsAssigned`, `onPartitionsLost`). Inside those methods, look for `INVOKEVIRTUAL org/apache/kafka/clients/consumer/Consumer.poll` or `KafkaConsumer.poll`. HIGH.
- Also inspect lambdas registered via `consumer.subscribe(topics, rebalanceListener)` if the rebalance listener is a SAM lambda.

## References

- ConsumerRebalanceListener javadoc: https://kafka.apache.org/40/javadoc/org/apache/kafka/clients/consumer/ConsumerRebalanceListener.html
- KafkaConsumer javadoc — "Storing Offsets Outside Kafka": https://kafka.apache.org/40/javadoc/org/apache/kafka/clients/consumer/KafkaConsumer.html
