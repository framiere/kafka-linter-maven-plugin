# PRODUCER_RECORD_PARTITION_AND_KEY

**Severity**: WARNING
**Confidence**: MEDIUM
**Detection**: bytecode
**Tagline**: Specifying both partition and key means you don't trust the key. Pick one.

## TL;DR

The linter flags `new ProducerRecord(topic, partition, key, value)` calls where the partition is provided alongside a non-null key. The explicit partition overrides key-based partitioning — usually a mistake.

## What's happening (the mechanism)

`ProducerRecord` constructor overloads:

- `ProducerRecord(topic, value)` — partition chosen by partitioner using sticky/random.
- `ProducerRecord(topic, key, value)` — partitioner uses `murmur2(key) % numPartitions`.
- `ProducerRecord(topic, partition, key, value)` — explicit partition; **key is ignored for partitioning**, only passed as the record key.

The explicit-partition form is rarely correct in application code:
- It assumes the writer knows the current partition count (breaks on partition expansion).
- It defeats per-key ordering guarantees if mixed with key-only writes.
- It bypasses adaptive partitioning (KIP-794) which steers to faster brokers.

Most uses of the four-arg form are accidental — the developer wanted to compute "their" partition but didn't realize the key-based form already does that hashing.

## Operational impact

- Hot partition: a constant or low-cardinality `partition` argument concentrates traffic on one broker.
- Out-of-order processing per key when the same key is written with and without an explicit partition.
- Breaks on `kafka-topics --alter --partitions` — old code keeps writing to partitions [0..N-1] while consumers see [0..M-1].

## How to fix

```java
// BAD — manual partition computation, ignores the partitioner
int p = Math.abs(key.hashCode()) % numPartitions;
producer.send(new ProducerRecord<>(topic, p, key, value));

// GOOD — let the partitioner handle it
producer.send(new ProducerRecord<>(topic, key, value));

// LEGITIMATE — when explicit partition is genuinely required (rare):
// e.g., explicit fan-out to all partitions, replay tooling
producer.send(new ProducerRecord<>(topic, partition, null, value));
```

## When this might be a false positive

- Tools that intentionally write to a specific partition: replay/repair scripts, partition-affinity admin operations.
- The legitimate "partition without key" form (`new ProducerRecord(topic, p, null, value)`) is not flagged.

## Detection strategy

- Bytecode: detect `INVOKESPECIAL org/apache/kafka/clients/producer/ProducerRecord <init>(Ljava/lang/String;Ljava/lang/Integer;Ljava/lang/Object;Ljava/lang/Object;)V` (and the headers overload `...Iterable;)V`). Verify the third arg (key) is not `null` (i.e., not `ACONST_NULL` immediately preceding). MEDIUM.
- Suppress for replay/admin tools (heuristic: file path contains `replay`, `repair`, `admin`).

## References

- ProducerRecord javadoc: https://kafka.apache.org/40/javadoc/org/apache/kafka/clients/producer/ProducerRecord.html
- Apache Kafka — Partitioning: https://kafka.apache.org/documentation/#producer_partitioning
- KIP-794: https://cwiki.apache.org/confluence/display/KAFKA/KIP-794
