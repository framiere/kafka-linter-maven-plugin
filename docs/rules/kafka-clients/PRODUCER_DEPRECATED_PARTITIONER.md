# PRODUCER_DEPRECATED_PARTITIONER

**Severity**: WARNING
**Confidence**: HIGH
**Detection**: both
**Tagline**: DefaultPartitioner is older than your phone. Stop naming it.

## TL;DR

The linter flags `partitioner.class` set to `org.apache.kafka.clients.producer.internals.DefaultPartitioner` or `org.apache.kafka.clients.producer.UniformStickyPartitioner` — both deprecated by KIP-794 (Kafka 3.3).

## What's happening (the mechanism)

KIP-794 (Kafka 3.3) introduced the "strictly uniform sticky partitioner" as the built-in default and deprecated:

- `DefaultPartitioner` — the original sticky partitioner that overloaded slow brokers.
- `UniformStickyPartitioner` — the explicit-uniform variant.

Both classes still work for back-compat but log a deprecation warning. The new behavior is built into `KafkaProducer` itself when `partitioner.class` is unset (the default in 3.3+ is `null`). The new logic uses `partitioner.adaptive.partitioning.enable` (default `true`) to steer records toward faster brokers.

Migration:
- To replace `DefaultPartitioner`: remove the `partitioner.class` setting entirely.
- To replace `UniformStickyPartitioner`: remove `partitioner.class` and set `partitioner.ignore.keys=true`.

`Partitioner.onNewBatch` is also deprecated and now a no-op.

## Operational impact

- Slower brokers receive disproportionate traffic with the old sticky partitioners — uneven partition lag, hot replicas.
- Deprecation log noise (`WARN`) at every producer startup.
- Locked out of `partitioner.availability.timeout.ms` — the new mechanism marks unresponsive partitions unavailable.
- Will break on future major version that removes the deprecated classes.

## How to fix

```java
// BAD
props.put(ProducerConfig.PARTITIONER_CLASS_CONFIG,
    "org.apache.kafka.clients.producer.internals.DefaultPartitioner");

// BAD
props.put(ProducerConfig.PARTITIONER_CLASS_CONFIG,
    "org.apache.kafka.clients.producer.UniformStickyPartitioner");

// GOOD — remove the property entirely; the built-in adaptive logic kicks in.

// GOOD — if you want UniformStickyPartitioner behavior (ignore keys):
props.put("partitioner.ignore.keys", "true");
```

## When this might be a false positive

- Brokers older than 3.3 — the new default behavior was added on the client side and works fine talking to older brokers, so this is rare.
- Test fixtures asserting specific partition assignment with the old logic.

## Detection strategy

- Config: `partitioner.class` equals one of the deprecated FQCNs. HIGH.
- Bytecode: `Properties.put("partitioner.class", LDC "<deprecated FQCN>")` or `ProducerConfig.PARTITIONER_CLASS_CONFIG`. HIGH.
- Also flag direct `Class.forName("...DefaultPartitioner")` or `new DefaultPartitioner()` references.

## References

- KIP-794 — Strictly Uniform Sticky Partitioner: https://cwiki.apache.org/confluence/display/KAFKA/KIP-794
- Apache Kafka producer configs — `partitioner.class`, `partitioner.adaptive.partitioning.enable`, `partitioner.ignore.keys`: https://kafka.apache.org/documentation/#producerconfigs_partitioner.class
- Apache Kafka 3.3 release notes: https://kafka.apache.org/blog#apache_kafka_330_release_announcement
