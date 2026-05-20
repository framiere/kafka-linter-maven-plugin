# STREAMS_ROCKSDB_NO_CONFIG_SETTER

**Severity**: WARNING
**Confidence**: MEDIUM
**Detection**: config-file
**Tagline**: A `RocksDBConfigSetter` is the difference between tuned RocksDB and 64 MB of "default".

## TL;DR

The linter flags Streams apps with persistent state stores but no `rocksdb.config.setter` set. RocksDB defaults are tuned for a single embedded use case; multi-store Streams apps with default settings often suffer from block-cache contention, write-stall pauses, and oversized memtables.

## What's happening (the mechanism)

Each persistent state store in a Streams instance creates its own RocksDB instance with default options:
- Block cache: 50 MB per store.
- Memtable: 16 MB per store.
- Max open files: -1 (unlimited).
- Write buffer manager: not shared across stores.

In a topology with 10 stores × 4 partitions × 2 instances, that's 80 RocksDB instances on disk, each holding 50 MB block cache + 16 MB memtable → 5.3 GB before counting OS page cache. None of it is shared. The block cache is too small for any one store's working set and too duplicated across the cluster to be efficient.

`rocksdb.config.setter` lets you point Streams at a `RocksDBConfigSetter` implementation that sets shared resources (`WriteBufferManager`, shared `Cache`) so all stores in the instance use one bounded memory budget.

## Operational impact

- `kafka.streams:type=stream-state-metrics,...:rocksdb-block-cache-usage` shows constant 100% per store, with eviction churn.
- p99 store-get latency rises with dataset size because everything misses the block cache.
- Write stalls visible as spiky `put-latency-avg`; backpressure propagates upstream.
- Memory usage grows uncontrollably across multiple stores; no single knob to bound it.

## How to fix

```java
public class BoundedRocksDBConfig implements RocksDBConfigSetter {
    // Shared across all stores in this JVM
    private static final Cache CACHE = new LRUCache(256L * 1024 * 1024); // 256 MB total
    private static final WriteBufferManager WRITE_BUFFER_MANAGER =
        new WriteBufferManager(64L * 1024 * 1024, CACHE);

    public void setConfig(String storeName, Options options, Map<String, Object> configs) {
        BlockBasedTableConfig tableConfig = (BlockBasedTableConfig) options.tableFormatConfig();
        tableConfig.setBlockCache(CACHE);
        tableConfig.setCacheIndexAndFilterBlocks(true);
        options.setTableFormatConfig(tableConfig);
        options.setWriteBufferManager(WRITE_BUFFER_MANAGER);
        options.setMaxWriteBufferNumber(2);
    }

    public void close(String storeName, Options options) {}
}

// Wire it up
props.put(StreamsConfig.ROCKSDB_CONFIG_SETTER_CLASS_CONFIG, BoundedRocksDBConfig.class);
```

## When this might be a false positive

- Tiny topologies (1–2 stores, low partition count). Defaults work.
- Apps where RocksDB tuning is delegated to an external library (Quarkus, Spring with a custom auto-config).

## Detection strategy

- Config files: persistent state stores in use (any `Materialized` / DSL aggregation) AND `rocksdb.config.setter` not set. MEDIUM confidence.
- Bytecode: no `Properties.put(StreamsConfig.ROCKSDB_CONFIG_SETTER_CLASS_CONFIG, ...)`.
- Combine with topology size: more stores = more pressing.
- Confidence: MEDIUM.

## References

- RocksDBConfigSetter javadoc: https://kafka.apache.org/40/javadoc/org/apache/kafka/streams/state/RocksDBConfigSetter.html
- Confluent — RocksDB tuning: https://docs.confluent.io/platform/current/streams/developer-guide/memory-mgmt.html#rocksdb
- Bruno Cadonna blog — Tuning RocksDB for Kafka Streams: https://www.confluent.io/blog/how-to-tune-rocksdb-kafka-streams-state-stores-performance/

## Cross-reference

Also recommended by Confluent agent-skills — see kafka-streams-programming/references/architecture.md § RocksDB memory accounting, kafka-streams-programming/references/config-baseline.md § rocksdb.config.setter.
