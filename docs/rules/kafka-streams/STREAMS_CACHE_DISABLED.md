# STREAMS_CACHE_DISABLED

**Severity**: WARNING
**Confidence**: MEDIUM
**Detection**: config-file
**Tagline**: Cache=0 makes brokers cry. Update rates explode without it.

## TL;DR

The linter flags `cache.max.bytes.buffering=0` (deprecated key) or `statestore.cache.max.bytes=0` (KIP-770 replacement). Disabling the cache causes every record update to flush downstream and produce a changelog record, often multiplying broker write load by 10–100x.

## What's happening (the mechanism)

Kafka Streams buffers KTable updates in an in-memory cache before flushing to RocksDB and to the changelog topic. The cache deduplicates rapid updates to the same key: if you `count()` 100 increments to key `K` within the buffer window, only the final value is written. Disable the cache and every increment is a separate KTable update → a separate changelog write → a separate downstream record on `KTable.toStream()`.

Defaults:
- Pre-3.4: `cache.max.bytes.buffering=10485760` (10 MB) per Streams instance, divided across threads.
- 3.4+: `statestore.cache.max.bytes` (KIP-770) replaces the above key with identical semantics and default.
- Both keys still accepted; the old one is deprecated.

The KIP also added `input.buffer.max.bytes` (512 MB) — unrelated to the cache, but often confused.

## Operational impact

- Changelog topic write throughput multiplies. `kafka.streams:type=stream-state-store-metrics,...:put-rate` matches incoming record rate (no dedup).
- Broker disk/network pressure on the changelog topic; ISR shrinkage if brokers can't keep up.
- Downstream consumers of `KTable.toStream()` see every intermediate update instead of debounced final values → consumer-side processing overhead.
- Suppress operators (`Suppressed.untilTimeLimit`) interact with the cache; disabling cache while expecting debounce is a bug.

## How to fix

```properties
# BAD — disables cache, hammers brokers
cache.max.bytes.buffering=0
statestore.cache.max.bytes=0

# GOOD — leave at default (10 MB) or tune up for high-update workloads
# (omit to use default)

# GOOD — explicit and modern (3.4+)
statestore.cache.max.bytes=33554432
```

If you genuinely need every intermediate update for downstream consumers, use the Processor API to forward explicitly, not "disable the cache".

## When this might be a false positive

- Tests using `TopologyTestDriver` where you want deterministic per-record output. Suppress for test code.
- Specialized low-latency use cases where the buffering delay is the bottleneck — but usually the right fix is `commit.interval.ms`, not cache=0.

## Detection strategy

- Config files: `cache.max.bytes.buffering=0` or `statestore.cache.max.bytes=0`.
- Bytecode: `Properties.put` with key `cache.max.bytes.buffering`/`statestore.cache.max.bytes` / `StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG`/`STATESTORE_CACHE_MAX_BYTES_CONFIG` and value `LDC 0` / `LCONST_0`.
- Also flag the deprecated key in use even with non-zero value (lower severity, suggests migration).
- Confidence: MEDIUM — sometimes a deliberate trade-off.

## References

- KIP-770 — Replace cache.max.bytes.buffering: https://cwiki.apache.org/confluence/pages/viewpage.action?pageId=186878390
- Streams config — statestore.cache.max.bytes: https://kafka.apache.org/documentation/streams/developer-guide/config-streams.html#statestore-cache-max-bytes
- Confluent — Memory Management in Kafka Streams: https://docs.confluent.io/platform/current/streams/developer-guide/memory-mgmt.html

## Cross-reference

Also recommended by Confluent agent-skills — see kafka-streams-programming/references/config-baseline.md § statestore.cache.max.bytes.
