# PRODUCER_COMPRESSION_NONE_EXPLICIT

**Severity**: WARNING
**Confidence**: MEDIUM
**Detection**: both
**Tagline**: compression.type=none isn't a default — it's a choice. Make sure you meant it.

## TL;DR

The linter flags producers that explicitly set `compression.type=none`. The default is also `none`, so writing it out usually signals copy-paste or "I tried something and reverted it" rather than a deliberate decision.

## What's happening (the mechanism)

`compression.type` chooses the codec applied to the batch payload before it is sent to the broker: `none`, `gzip`, `snappy`, `lz4`, `zstd`. Compression operates per-batch — bigger batches compress dramatically better. With the default of `none`, the producer ships raw bytes, the broker stores raw bytes, and every replica fetcher and every consumer also moves raw bytes over the wire.

The companion rule `PRODUCER_NO_COMPRESSION` (already implemented) flags the implicit no-compression case. This rule covers the explicit case — someone typed `compression.type=none` on purpose, which is more often a leftover than a real choice.

For JSON / Avro / Protobuf payloads, `lz4` or `zstd` typically yields 3–10x reduction with minimal CPU. `zstd` (Kafka 2.1+, KIP-110) is the best ratio/CPU tradeoff for most workloads.

## Operational impact

- 3–10x higher broker disk and network usage than a `zstd`-compressed pipeline carrying the same payload.
- Inter-broker replication bandwidth grows in lockstep — replication-bound clusters need more brokers than they should.
- Cloud egress costs scale linearly with payload size (high $$).
- `kafka.network:type=RequestMetrics,name=ResponseSendTimeMs,request=Fetch` slowly degrades as topic size grows.

## How to fix

```java
// BAD — explicit, signals the team disabled compression at some point
props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "none");

// GOOD
props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "zstd");  // or lz4 for lower CPU
```

Topic-level `compression.type` overrides producer-level if set on the broker. Confirm both layers.

## When this might be a false positive

- Pre-compressed payloads (already-gzipped blobs in the value bytes). Document and downgrade.
- Workloads where CPU is the binding resource and bytes are already minimal. Rare.

## Detection strategy

- Config: `compression.type=none` literal. MEDIUM.
- Bytecode: `Properties.put("compression.type", "none")` (LDC `"none"`). MEDIUM.
- HIGH if combined with large `batch.size` (clear contradiction).

## References

- Apache Kafka producer configs — `compression.type`: https://kafka.apache.org/documentation/#producerconfigs_compression.type
- KIP-110 — zstd compression: https://cwiki.apache.org/confluence/display/KAFKA/KIP-110%3A+Adding+support+for+ZStandard+Compression
- Confluent — Compression in Kafka: https://www.confluent.io/blog/apache-kafka-message-compression/
