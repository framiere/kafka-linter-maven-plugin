# PRODUCER_LINGER_ZERO_NO_BATCH

**Severity**: WARNING
**Confidence**: MEDIUM
**Detection**: config-file
**Tagline**: linger.ms=0 with default batch.size means every record is a network round trip.

## TL;DR

The linter flags producers that explicitly set `linger.ms=0` without raising `batch.size` — disabling the producer's own batching and turning every `send()` into an independent broker request.

## What's happening (the mechanism)

The producer accumulates records per partition into batches up to `batch.size` (default 16 KiB) and ships a batch when either (a) the batch is full or (b) `linger.ms` has elapsed since the first record arrived. With `linger.ms=0` the sender thread fires as soon as it sees a non-empty accumulator — which on a low-rate or bursty producer is once per record.

Apache Kafka 4.0 changed the default `linger.ms` from `0` to `5` exactly because the original default produced one request per record on most workloads. Explicitly setting `linger.ms=0` in 2026 is reverting to that broken behavior.

Per-record overhead with no batching:
- One produce request per record → broker TPS dominated by request rate, not byte rate.
- One compression block per record → effectively no compression benefit.
- One `ProduceResponse` per record → producer thread spends most of its time waiting for acks.

## Operational impact

- 10–100x increase in broker `RequestsPerSec` for the same byte throughput.
- Broker `request-handler-avg-idle-percent` drops, request queue length climbs.
- Producer `producer-metrics:batch-size-avg` stays near the record size; `records-per-request-avg` ≈ 1.
- p99 latency goes up because requests queue at the broker.

## How to fix

```java
// BAD — no batching, no compression benefit
props.put(ProducerConfig.LINGER_MS_CONFIG, "0");

// GOOD — accept default (5ms in Kafka 4.0+) or tune
props.put(ProducerConfig.LINGER_MS_CONFIG, "10");
props.put(ProducerConfig.BATCH_SIZE_CONFIG, Integer.toString(64 * 1024));
props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "lz4");
```

## When this might be a false positive

- True ultra-low-latency producers (financial tick, control plane) where the 5 ms linger budget is a hard miss. Document and downgrade.
- Producers that have already raised `batch.size` very high (so the size trigger fires before linger would). MEDIUM-confidence rule should account for this.

## Detection strategy

- Config: `linger.ms=0` AND no `batch.size` override (or `batch.size` at default 16384). MEDIUM.
- Bytecode: `Properties.put("linger.ms", "0")` — flag and check sibling puts in the same method. MEDIUM.
- Confidence drops to LOW if `batch.size` is also being set very large in the same Properties source — that combination is occasionally intentional.

## References

- Apache Kafka producer configs — `linger.ms`, `batch.size`: https://kafka.apache.org/documentation/#producerconfigs_linger.ms
- Apache Kafka 4.0 upgrade notes — linger default change: https://kafka.apache.org/40/documentation.html#upgrade
- Confluent — Optimizing Throughput: https://docs.confluent.io/cloud/current/client-apps/optimizing/throughput.html
