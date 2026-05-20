# STREAMS_GROUPBY_NULL_KEY

**Severity**: WARNING
**Confidence**: MEDIUM
**Detection**: dsl-chain
**Tagline**: `groupByKey()` on null keys silently eats your records.

## TL;DR

The linter flags `KStream#groupByKey()` (or any stateful aggregation) on a stream whose upstream operator can produce null keys, because such records are silently dropped during repartition.

## What's happening (the mechanism)

Kafka Streams' contract for `KGroupedStream` is explicit in the javadoc: *"if a record key is null the record will not be included in the resulting `KGroupedStream`"*. The same is true after a `selectKey`/`map`/`groupBy` whose `KeyValueMapper` returns null — `KStream#repartition()` itself silently filters null-keyed records before writing them to the repartition topic (see KAFKA-13024).

Why? Because a null key cannot be hashed to a partition deterministically — `DefaultPartitioner` would round-robin it, but Streams needs key-stable partitioning for stateful operators. So the engine drops the record rather than route it to an unpredictable task. No exception, no metric, no log line at INFO.

A related failure mode: if the upstream key extractor (`selectKey(...)`) throws NPE because it dereferences a null field, the StreamThread dies. That's the same defect class but louder.

## Operational impact

- Records vanish between input and aggregate. Input rate `kafka.streams:type=stream-source-node-metrics,node-id=...,process-rate` does not match downstream `process-rate` on the aggregation node.
- The aggregation's changelog grows slower than expected. `kafka.streams:type=stream-state-store-metrics,...put-rate` is lower than `process-rate` upstream.
- Counts/aggregates are systematically low. Reports look "close but wrong" — the hardest class of bug to diagnose.
- If null keys come from a malformed upstream record, the symptom only surfaces in production where bad data exists.

## How to fix

Filter explicitly, or guarantee a key. Be loud about nulls.

```java
// BAD — silently drops null-keyed records
inputStream
    .selectKey((k, v) -> v.getCustomerId())   // may return null
    .groupByKey()
    .count(Materialized.as("counts-by-customer"));

// GOOD — explicit filter, optional DLQ branch
KStream<String, Order> keyed = inputStream
    .selectKey((k, v) -> v.getCustomerId());

keyed.filter((k, v) -> k == null)
     .to("orders.dlq.null-customer");

keyed.filter((k, v) -> k != null)
     .groupByKey()
     .count(Materialized.as("counts-by-customer"));
```

## When this might be a false positive

- The upstream key extractor is provably non-null (e.g. `selectKey((k,v) -> v.getId().toString())` where `id` is a `@NotNull` field). Static analysis can't always prove this — hence MEDIUM confidence.
- The stream's source is a topic whose producer guarantees non-null keys.

## Detection strategy

- DSL chain: `KStream.selectKey(...)` / `KStream.map(...)` / `KStream.groupBy(...)` immediately followed by `groupByKey()` / aggregation, where the mapper lambda's return type or implementation is not provably non-null.
- ASM: detect `INVOKEINTERFACE org/apache/kafka/streams/kstream/KStream.selectKey` followed in the same builder chain by `INVOKEINTERFACE org/apache/kafka/streams/kstream/KStream.groupByKey` with no intervening `filter`.
- Confidence: MEDIUM by default; HIGH if the mapper lambda contains a path that returns `null` explicitly (e.g. `return v == null ? null : v.id`).

## References

- KStream javadoc — `groupByKey`: https://kafka.apache.org/40/javadoc/org/apache/kafka/streams/kstream/KStream.html#groupByKey--
- KAFKA-13024 — repartition silently drops null keys: https://issues.apache.org/jira/browse/KAFKA-13024
- Confluent: Naming stateful operations: https://developer.confluent.io/tutorials/naming-stateful-operations/kstreams.html
