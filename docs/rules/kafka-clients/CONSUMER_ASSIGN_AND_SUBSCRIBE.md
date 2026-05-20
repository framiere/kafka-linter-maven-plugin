# CONSUMER_ASSIGN_AND_SUBSCRIBE

**Severity**: ERROR
**Confidence**: HIGH
**Detection**: bytecode
**Tagline**: Pick one. assign() and subscribe() are mutually exclusive — Kafka will tell you.

## TL;DR

The linter flags consumers that call both `assign(...)` and `subscribe(...)` on the same instance. Kafka rejects this with `IllegalStateException`.

## What's happening (the mechanism)

`KafkaConsumer` supports two assignment models:

- `subscribe(topics)` — dynamic group membership; coordinator assigns partitions.
- `assign(partitions)` — manual; the application owns the partition list.

Once a consumer enters one mode, calling the other throws `IllegalStateException: Subscription to topics, partitions and pattern are mutually exclusive`. Switching modes requires `unsubscribe()` or `assign(emptyList())` first.

This is a common mistake when copy-pasting examples or when refactoring from one model to the other.

## Operational impact

- `IllegalStateException` at startup, consumer dies.
- Mode switch in middle of run: lost work mid-batch.

## How to fix

```java
// BAD
consumer.subscribe(List.of("topic"));
consumer.assign(List.of(new TopicPartition("topic", 0))); // IllegalStateException

// GOOD — manual partition control
consumer.assign(List.of(new TopicPartition("topic", 0)));

// GOOD — switching modes safely
consumer.unsubscribe();
consumer.assign(List.of(new TopicPartition("topic", 0)));
```

## When this might be a false positive

- None within a single execution path.

## Detection strategy

- Bytecode: in a single method body (or across methods touching the same consumer field), detect both `KafkaConsumer.subscribe(...)` and `KafkaConsumer.assign(...)` calls reachable on the same instance without an intervening `unsubscribe()`. HIGH.

## References

- KafkaConsumer.subscribe() javadoc: https://kafka.apache.org/40/javadoc/org/apache/kafka/clients/consumer/KafkaConsumer.html#subscribe-java.util.Collection-
- KafkaConsumer.assign() javadoc: https://kafka.apache.org/40/javadoc/org/apache/kafka/clients/consumer/KafkaConsumer.html#assign-java.util.Collection-
