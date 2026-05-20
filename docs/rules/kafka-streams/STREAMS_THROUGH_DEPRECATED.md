# STREAMS_THROUGH_DEPRECATED

**Severity**: WARNING
**Confidence**: HIGH
**Detection**: bytecode
**Tagline**: `through()` is dead. Long live `repartition()`.

## TL;DR

The linter flags any call to `KStream#through(...)` — deprecated since 2.6 (KIP-221) and removed in later Kafka versions. The replacement is `KStream#repartition(...)`.

## What's happening (the mechanism)

`KStream.through(topic)` was syntactic sugar for `.to(topic)` + `StreamsBuilder.stream(topic)` — write the data to an intermediate topic and read it back. It had three problems: (1) you had to create the topic yourself, (2) you couldn't easily change its partition count, (3) it didn't trigger automatic key-aware partitioning when used after a key-changer.

KIP-221 introduced `repartition(Repartitioned)` which Kafka Streams manages itself: it creates the topic, names it after the application, sets `cleanup.policy=delete`, lets you specify `numberOfPartitions`, `keySerde`, `valueSerde`, and `StreamPartitioner`. The method exists in 2.6+ and is the replacement for `through()`. The `through()` method itself was removed entirely in Kafka 4.0.

## Operational impact

- On Kafka 4.x and newer: the build fails (`through` no longer exists on `KStream`).
- On 2.6–3.x: deprecation warnings at compile time, but the bigger issue is operational: the developer manually created an intermediate topic with arbitrary partition count, retention, and replication. That topic is now part of the topology contract — changing it requires coordination Streams doesn't help with.
- Cannot benefit from KIP-295 topology optimization which understands `repartition` but not `through`.

## How to fix

```java
// BAD — deprecated, removed in 4.x
stream.through("intermediate-topic")
      .groupByKey()
      .count(...);

// GOOD — Streams manages the topic
stream.repartition(Repartitioned.<String, Order>as("orders-rekeyed")
        .withNumberOfPartitions(12)
        .withKeySerde(Serdes.String())
        .withValueSerde(orderSerde))
      .groupByKey()
      .count(...);

// GOOD — when you genuinely own the topic (cross-application boundary)
stream.to("public.orders.rekeyed", Produced.with(Serdes.String(), orderSerde));
// In another app: builder.stream("public.orders.rekeyed", ...)
```

## When this might be a false positive

None worth coding around. If you find a legitimate use case (cross-app intermediate topic with explicit lifecycle), use `to()` + `stream()`, not `through()`.

## Detection strategy

- Bytecode: `INVOKEINTERFACE org/apache/kafka/streams/kstream/KStream.through (...)Lorg/apache/kafka/streams/kstream/KStream;` — any overload.
- Confidence: HIGH — the method is unconditionally deprecated and removed in newer versions.

## References

- KIP-221 — Enhance DSL with Connecting Topic Creation and Repartition Hint: https://cwiki.apache.org/confluence/display/KAFKA/KIP-221:+Enhance+DSL+with+Connecting+Topic+Creation+and+Repartition+Hint
- Confluent — Streams upgrade guide (2.6): https://docs.confluent.io/platform/current/streams/upgrade-guide.html
