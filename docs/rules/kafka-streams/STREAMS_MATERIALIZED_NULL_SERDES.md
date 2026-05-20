# STREAMS_MATERIALIZED_NULL_SERDES

**Severity**: WARNING
**Confidence**: MEDIUM
**Detection**: bytecode
**Tagline**: `Materialized.with(null, null)` says "trust me, the default serdes are right." They aren't.

## TL;DR

The linter flags `Materialized.with(null, null)` and any pattern that relies on `default.key.serde` / `default.value.serde` for a stateful operator's serdes. It works until the topology grows a second store with a different value type, then the defaults can't be both.

## What's happening (the mechanism)

When you don't pass key/value serdes to a stateful operator (or pass `null`), Streams falls back to `default.key.serde` / `default.value.serde` from the config. For a small app with one type everywhere (`Serdes.String()` keys, `Serdes.Long()` values), this works.

As soon as the topology has two stores with different value types — a `count()` (Long) and an `aggregate(... Object ...)` — the defaults can only point at one Serde. The other store either fails at first record or, worse, silently mis-serializes (e.g. uses a generic `JsonSerde<Object>` that loses type info).

The same defect bites changelog topics: they inherit the value Serde of the store. If your "default" serde doesn't match the store's actual type, the changelog is written with the wrong bytes, and on restore you get garbage.

## Operational impact

- `ClassCastException` at first put / get to the store.
- Mis-typed changelog records persisting forever — even fixing the code later can't repair the topic without a reset.
- `kafka.streams:type=stream-state-metrics,...:put-rate` is normal; deserialization on consume yields garbage, breaking downstream.

## How to fix

```java
// BAD
KTable<String, Long> counts = stream
    .groupByKey()
    .count(Materialized.as("counts"));   // relies on defaults

// BAD
KTable<String, Order> orders = stream
    .groupByKey()
    .aggregate(Order::new, (k, v, agg) -> agg.add(v),
        Materialized.<String, Order, KeyValueStore<Bytes, byte[]>>with(null, null));

// GOOD — explicit serdes per store
KTable<String, Long> counts = stream
    .groupByKey()
    .count(Materialized.<String, Long, KeyValueStore<Bytes, byte[]>>as("counts")
        .withKeySerde(Serdes.String())
        .withValueSerde(Serdes.Long()));

KTable<String, Order> orders = stream
    .groupByKey()
    .aggregate(Order::new, (k, v, agg) -> agg.add(v),
        Materialized.<String, Order, KeyValueStore<Bytes, byte[]>>as("orders-aggregated")
            .withKeySerde(Serdes.String())
            .withValueSerde(orderSerde));
```

## When this might be a false positive

- Truly homogeneous-type apps where the defaults are the only serdes ever used. Acceptable but fragile.
- Test code that uses `Serdes.String()` and `Serdes.Long()` as defaults intentionally.

## Detection strategy

- Bytecode: `INVOKESTATIC org/apache/kafka/streams/kstream/Materialized.with` with both arguments `ACONST_NULL`. HIGH confidence "fragile".
- Bytecode: `Materialized.as(name)` with no chained `.withKeySerde` or `.withValueSerde`. MEDIUM confidence (depends on whether defaults suffice).
- Confidence: MEDIUM.

## References

- Materialized javadoc: https://kafka.apache.org/40/javadoc/org/apache/kafka/streams/kstream/Materialized.html
- Confluent — Default Serdes pitfalls: https://docs.confluent.io/platform/current/streams/developer-guide/datatypes.html
