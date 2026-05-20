# STREAMS_KTABLE_REGROUP_BY_SAME_KEY

**Severity**: WARNING
**Confidence**: MEDIUM
**Detection**: dsl-chain
**Tagline**: `toStream().groupByKey()` is a repartition you didn't need to pay for.

## TL;DR

The linter flags `KTable#toStream()` immediately followed by `.groupByKey()` (or `.groupBy(sameKey)`). Streams may insert a repartition topic even though the data is already co-partitioned by key.

## What's happening (the mechanism)

When you call `KTable.toStream()`, the resulting `KStream`'s key is still the table's key. If you then call `groupByKey()`, Streams sees a `KStream` and conservatively assumes the upstream "may have changed the key" — triggering a repartition. This adds a network round-trip for data that didn't need to move.

The same defect with `groupBy((k, v) -> k)` is even more explicit: the user is *declaring* they want to re-group by the same key.

If you genuinely need to re-aggregate, use the `KTable.groupBy` operator directly (it's smarter about key tracking), or use `KTable.aggregate` if you can express the second pass over the table values.

## Operational impact

- Extra repartition topic (`<app-id>-...-repartition`) with full input traffic going through.
- Cluster-level bandwidth doubles for that operator.
- Extra task in the topology DAG → extra rebalance overhead.
- For windowed re-aggregation, doubles changelog volume.

## How to fix

```java
// BAD — KTable.toStream().groupByKey() forces a repartition
KTable<String, Long> counts = stream.groupByKey().count(Materialized.as("c1"));
KStream<String, Long> asStream = counts.toStream();
KTable<String, Long> recounts = asStream.groupByKey().reduce(Long::sum,
    Materialized.as("c2"));

// GOOD — chain KTable operators directly
KTable<String, Long> counts = stream.groupByKey().count(Materialized.as("c1"));
KTable<String, Long> recounts = counts.groupBy(
    (k, v) -> KeyValue.pair(k, v),
    Grouped.with(Serdes.String(), Serdes.Long()))
    .reduce(Long::sum, (k, v, agg) -> agg - v, Materialized.as("c2"));
```

If the use case is "re-emit the KTable as a stream", just use `toStream()` — no `groupByKey` needed.

## When this might be a false positive

- The downstream is `groupBy((k,v) -> newKey)` where the new key is genuinely different. Already handled by `STREAMS_DOUBLE_REPARTITION`.

## Detection strategy

- DSL chain: `KTable.toStream()` invocation followed in the same builder chain by `.groupByKey()` or `.groupBy(lambda)` where the lambda returns the input key unchanged (`(k,v) -> k` or `KeyValue.pair(k, ...)` preserving k).
- Bytecode: detect the call sequence; for `groupBy`, decompile the lambda to see if it preserves the key.
- Confidence: HIGH for `toStream().groupByKey()`. MEDIUM for `groupBy(lambda)` (depends on lambda body).

## References

- KTable javadoc — toStream / groupBy: https://kafka.apache.org/40/javadoc/org/apache/kafka/streams/kstream/KTable.html
- KIP-295 — Topology optimization: https://cwiki.apache.org/confluence/display/KAFKA/KIP-295:+Add+Streams+Configuration+Allowing+for+Optional+Topology+Optimization
