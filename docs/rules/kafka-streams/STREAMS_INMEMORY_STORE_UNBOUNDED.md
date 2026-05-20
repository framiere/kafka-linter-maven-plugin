# STREAMS_INMEMORY_STORE_UNBOUNDED

**Severity**: WARNING
**Confidence**: MEDIUM
**Detection**: bytecode
**Tagline**: In-memory stores have one knob: heap.

## TL;DR

The linter flags `Stores.inMemoryKeyValueStore(...)` / `Stores.inMemoryWindowStore(...)` / `Stores.inMemorySessionStore(...)` for any aggregation/join store. In-memory stores grow without bound until heap exhausts — use `Stores.persistentKeyValueStore` (RocksDB-backed) unless you've proven the dataset bounded and small.

## What's happening (the mechanism)

In-memory stores keep all entries in an on-heap `ConcurrentHashMap`-like structure. They're great for:
- Test code.
- Genuinely small lookup tables (a few thousand entries, bounded by domain).
- KTables built from globally compacted small topics.

They are catastrophic for:
- Aggregations over high-cardinality keys (user IDs, session IDs, device IDs).
- Windowed aggregations whose retention window holds millions of windows.
- Stream-stream join stores (they retain every record within the join window).

The store can be restored from a changelog without bound — so restart performance is also poor: you pull every changelog record into heap before processing the first real input.

The persistent (RocksDB) variant offloads to disk, uses bounded block cache, and survives restarts via the local state directory.

## Operational impact

- Heap usage climbs steadily, never plateaus. Full GC frequency increases; eventually OOM kill.
- `kafka.streams:type=stream-state-store-metrics,store-id=...:put-rate` looks fine, but JVM `java.lang:type=Memory,HeapMemoryUsage` curves upward.
- After OOM/restart: full re-load of every store from changelog before any work happens.
- No bounded block cache → no ability to tune memory usage independently of dataset.

## How to fix

```java
// BAD — in-memory, unbounded
Topology topology = new Topology()
    .addStateStore(Stores.keyValueStoreBuilder(
        Stores.inMemoryKeyValueStore("counts"),
        Serdes.String(), Serdes.Long()));

// GOOD — RocksDB-backed
topology.addStateStore(Stores.keyValueStoreBuilder(
    Stores.persistentKeyValueStore("counts"),
    Serdes.String(), Serdes.Long()));

// DSL form:
// BAD
stream.groupByKey()
      .count(Materialized.as(Stores.inMemoryKeyValueStore("counts")));

// GOOD
stream.groupByKey()
      .count(Materialized.<String, Long>as("counts")
          .withKeySerde(Serdes.String())
          .withValueSerde(Serdes.Long()));   // defaults to persistent
```

## When this might be a false positive

- Bounded small reference tables (currency rates, country codes). Document the bound.
- Low-cardinality counts (e.g. status codes).
- `TopologyTestDriver` tests.

## Detection strategy

- Bytecode: `INVOKESTATIC org/apache/kafka/streams/state/Stores.inMemoryKeyValueStore`, `.inMemoryWindowStore`, `.inMemorySessionStore`.
- Higher severity when seen on a `KGroupedStream#aggregate`/`count`/`reduce` with windowing, or on a stream-stream join store.
- Confidence: MEDIUM — sometimes legitimate.

## References

- Stores javadoc: https://kafka.apache.org/40/javadoc/org/apache/kafka/streams/state/Stores.html
- Confluent — State stores: https://docs.confluent.io/platform/current/streams/developer-guide/dsl-api.html#state-stores

## Cross-reference

Also recommended by Confluent agent-skills — see kafka-streams-programming/references/debugging.md § State store OOM.
