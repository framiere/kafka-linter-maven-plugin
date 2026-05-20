# PRODUCER_TXN_ID_WITHOUT_IDEMPOTENCE

**Severity**: ERROR
**Confidence**: HIGH
**Detection**: both
**Tagline**: A transactional.id without idempotence is a transaction that isn't.

## TL;DR

The linter flags producers that set `transactional.id` while also setting `enable.idempotence=false`, or that obtain a transactional producer and never call `initTransactions()`.

## What's happening (the mechanism)

Transactions require idempotence as their substrate — exactly-once for the producer-to-broker leg is what makes a transaction atomic across partitions. Setting `transactional.id` without idempotence is rejected by the client (`ConfigException`).

Separately, a transactional producer's state machine requires `initTransactions()` to be the first call. It registers the `transactional.id` with the broker, fences any previous instance of the same id, and obtains a fresh producer ID and epoch. Skipping it makes the first `beginTransaction()` throw `IllegalStateException`.

A transactional producer is also expected to:
1. `initTransactions()` once at startup.
2. Wrap every batch in `beginTransaction()` / `commitTransaction()` (or `abortTransaction()` on error).
3. `acks=all` (default since 3.0).

Skipping any of these breaks the guarantee.

## Operational impact

- `ConfigException` at startup (best case) — caught by the build/IT.
- `IllegalStateException` on first send — worst case, surfaces under prod load.
- Zombie-instance fencing fails: a stale producer can still write to the partition if the new one never called `initTransactions()`, breaking exactly-once for the whole pipeline (Kafka Streams, transactional consumer-producer chains).
- Downstream consumers with `isolation.level=read_committed` see records but the abort/commit markers are inconsistent.

## How to fix

```java
// BAD
props.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, "orders-tx-1");
props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "false");
KafkaProducer<K, V> p = new KafkaProducer<>(props);
p.beginTransaction(); // IllegalStateException

// GOOD
props.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, "orders-tx-1");
// idempotence default-true is sufficient
KafkaProducer<K, V> p = new KafkaProducer<>(props);
p.initTransactions();             // exactly once at startup
try {
    p.beginTransaction();
    p.send(rec);
    p.commitTransaction();
} catch (KafkaException e) {
    p.abortTransaction();
    throw e;
}
```

## When this might be a false positive

- None. This is always a misconfiguration.

## Detection strategy

- Config: presence of `transactional.id` paired with `enable.idempotence=false` in the same Properties source. HIGH.
- Bytecode: `new KafkaProducer(props)` where `props.put("transactional.id", ...)` is reachable AND `initTransactions()` is never called on the resulting producer reference (intra-method or via field). HIGH for single-method scope, MEDIUM if the producer reference escapes.

## References

- Apache Kafka producer configs — `transactional.id`: https://kafka.apache.org/documentation/#producerconfigs_transactional.id
- KafkaProducer javadoc — `initTransactions()`: https://kafka.apache.org/40/javadoc/org/apache/kafka/clients/producer/KafkaProducer.html#initTransactions--
- KIP-98 — Exactly Once Delivery and Transactional Messaging: https://cwiki.apache.org/confluence/display/KAFKA/KIP-98+-+Exactly+Once+Delivery+and+Transactional+Messaging
