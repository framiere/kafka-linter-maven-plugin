# KAFKA_STREAMS_CACHE_CONFIG_RENAMED

**Severity**: WARNING
**Confidence**: HIGH
**Detection**: bytecode + config-file + pom-dependency
**Tagline**: cache.max.bytes.buffering was renamed in Streams 3.4 — using the old name silently disables your cache.

## TL;DR

KIP-770 (Streams 3.4) renamed `cache.max.bytes.buffering` to `statestore.cache.max.bytes`. The old key still parses (deprecated) up to Streams 3.x; in Streams 4.0 it stops being recognized and is silently ignored — your state store has no cache, blowing up write amplification and broker traffic.

## The setup

A team configured Streams long ago with `cache.max.bytes.buffering=10485760` (the default). They upgraded `kafka-streams` to 4.x. Their pom and config file weren't touched. Streams starts, processes records, but the state-store changelog now writes every single update instead of batching.

## What's actually happening

The Streams state-store cache is a write-back cache in front of RocksDB. It coalesces multiple updates to the same key before flushing them to the changelog topic. The cache size is a property:

| Streams version | Property name | Behavior |
|-----------------|---------------|----------|
| < 3.4 | `cache.max.bytes.buffering` | Honored |
| 3.4 – 3.x | `cache.max.bytes.buffering` (deprecated) **or** `statestore.cache.max.bytes` | Both honored; warning if old name |
| 4.0+ | `statestore.cache.max.bytes` only | Old name silently ignored |

The cache default is 10 MiB per instance. Setting it correctly is one of the bigger throughput levers in a Streams app — turning it off (effectively, by ignoring the config in 4.0) is a measurable performance cliff.

## Why this is subtle

- The 4.0 release notes warn about the rename. The deprecation message has been in 3.4+ for two years. Yet "the config name silently changed" is exactly the kind of thing that gets buried.
- The symptom isn't a crash. The application keeps working, processing throughput drops, changelog topic write rate doubles or triples. Teams blame "rocksdb tuning" or "broker congestion".
- Unknown configs in Streams are *not* an error by default — they're logged at WARN. Production log volume swallows the warning.

## Operational impact

- **Higher write amplification** — every state-store update becomes a changelog record. Compaction can't keep up.
- **Higher broker network and disk I/O** — the changelog topic's ingest rate spikes.
- **Higher RocksDB write throughput** — flushes more frequently, increases compaction work.
- **Degraded latency on aggregations** — the cache also acts as a coalescing buffer for downstream `KTable` updates. Without it, more downstream traffic.

## How to fix

Update the config:

```properties
# BAD (4.0+)
cache.max.bytes.buffering=10485760

# GOOD
statestore.cache.max.bytes=10485760
```

Or in code:

```java
// BAD (4.0+)
props.put("cache.max.bytes.buffering", 10485760);

// GOOD
props.put(StreamsConfig.STATESTORE_CACHE_MAX_BYTES_CONFIG, 10485760);
```

## When this might be a false positive

- Apps that explicitly want no caching set the value to `0`. The linter should still flag the old key name (it's wrong) but note that the effective behavior is unchanged in 4.0.
- Streams `< 3.4` (the old key is the only valid one — no rule fires).

## Detection strategy

- If `kafka-streams` resolved version `>= 4.0` AND the config (file or code) sets `cache.max.bytes.buffering`: WARNING (config silently ignored).
- If `kafka-streams` resolved version `[3.4, 4.0)` AND same config key: INFO (deprecated but honored).
- If both keys are set: WARNING regardless of version (ambiguous; let `statestore.cache.max.bytes` win but flag).

## References

- [KIP-770 — Replace cache.max.bytes.buffering with statestore.cache.max.bytes](https://cwiki.apache.org/confluence/display/KAFKA/KIP-770)
- [Streams configuration reference](https://kafka.apache.org/documentation/#streamsconfigs)
- [Kafka 4.0 release notes](https://kafka.apache.org/blog)
