# STREAMS_DOUBLE_REPARTITION

**Severity**: WARNING
**Confidence**: MEDIUM
**Detection**: dsl-chain
**Tagline**: Every `groupBy()` is a network round-trip. Two in a row is two too many.

## TL;DR

The linter flags chains that repartition multiple times — `selectKey().groupBy()`, `groupBy().groupBy()`, `selectKey().selectKey().groupByKey()`, or a `repartition()` followed by another key-changing operator — because each repartition spawns an internal topic and an extra broker round-trip.

## What's happening (the mechanism)

Any operator that *can* change the partitioning key — `selectKey`, `map`, `flatMap`, `transform`, `groupBy`, `repartition` — sets an internal "needs repartition" flag on the resulting `KStream`. When a downstream stateful operator (`groupByKey`, `join`, `aggregate`) reads such a stream, Kafka Streams inserts a sink-to-internal-topic and a source-from-internal-topic, producing a *repartition topic*. Each repartition is a full produce + fetch across the cluster.

Chaining two key-changing operators back-to-back means the data crosses the broker twice. The first repartition is paid for, then immediately invalidated by the second `selectKey`. With `topology.optimization=all` and KIP-295, the planner *may* collapse some of these, but only when the optimization rules apply (consecutive key-changers with no intervening stateful operator). Don't rely on it.

## Operational impact

- Per-task throughput halves (data crosses the network twice).
- `kafka.streams:type=stream-task-metrics,task-id=...:process-rate` looks fine on each task in isolation, but cluster-level `BytesIn`/`BytesOut` is double what you'd expect from the input rate.
- Broker disk pressure: two internal topics with `cleanup.policy=delete` and a retention window. Operators see "phantom" topics named `<app-id>-KSTREAM-KEY-SELECT-XXXX-repartition` they didn't create.
- Rebalance time grows: more tasks to assign, more partitions to reassign.

## How to fix

Combine key-changers into one, and `repartition()` only at the end.

```java
// BAD — two repartitions
stream
    .selectKey((k, v) -> v.getRegion())
    .groupBy((k, v) -> v.getCustomerId() + "|" + v.getRegion())  // re-keys again
    .count(Materialized.as("counts-by-region-customer"));

// GOOD — one selectKey, then groupByKey (no re-key)
stream
    .selectKey((k, v) -> v.getCustomerId() + "|" + v.getRegion())
    .groupByKey()
    .count(Materialized.as("counts-by-region-customer"));

// GOOD — when you genuinely need two passes, use repartition() with a name
KStream<String, Order> byCustomer = stream
    .selectKey((k, v) -> v.getCustomerId())
    .repartition(Repartitioned.<String, Order>as("by-customer")
        .withNumberOfPartitions(12));
```

Also enable topology optimization:
```properties
topology.optimization=all
```

## When this might be a false positive

- A pipeline that genuinely needs to aggregate by key A, then re-aggregate the result by key B (two distinct stateful steps). Two repartitions is correct here — but each should be at a clear stateful boundary, not consecutive key-changers.
- `KTable.toStream().selectKey().groupByKey()` is sometimes needed when the table's key is not the desired grouping key.

## Detection strategy

- DSL chain: two consecutive calls within the same builder chain to any of `{selectKey, map, flatMap, groupBy, repartition}` with no stateful operator between them.
- ASM: track method invocations on `KStream` instances. Flag if `KEY_CHANGERS.contains(insn.name)` and the previous DSL call on the same stack slot was also in `KEY_CHANGERS`.
- Suppress when `topology.optimization=all` is set AND the chain matches a pattern the optimizer collapses (rare to verify statically — keep WARNING).
- Confidence: MEDIUM — sometimes the second key-changer is genuinely needed.

## References

- KIP-221 — `repartition()` operator and `through()` deprecation: https://cwiki.apache.org/confluence/display/KAFKA/KIP-221:+Enhance+DSL+with+Connecting+Topic+Creation+and+Repartition+Hint
- KIP-295 — Topology optimization: https://cwiki.apache.org/confluence/display/KAFKA/KIP-295:+Add+Streams+Configuration+Allowing+for+Optional+Topology+Optimization
- Confluent — Optimizing topologies: https://docs.confluent.io/platform/current/streams/developer-guide/optimizing-streams.html
