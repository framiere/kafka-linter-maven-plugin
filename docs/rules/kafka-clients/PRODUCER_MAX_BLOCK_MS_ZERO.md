# PRODUCER_MAX_BLOCK_MS_ZERO

**Severity**: ERROR
**Confidence**: HIGH
**Detection**: bytecode + config-file
**Tagline**: max.block.ms=0 means "fail before you even ask the broker."

## TL;DR

The linter flags `max.block.ms=0` (or values < 100). `KafkaProducer.send()` must block while it fetches partition metadata and while the accumulator is full — zero block time means the first `send()` after startup throws `TimeoutException`.

## What's happening (the mechanism)

`max.block.ms` (default 60000) bounds how long `send()`, `partitionsFor()`, and `initTransactions()` will block waiting for:

1. Initial metadata fetch from any bootstrap broker (cold start).
2. Buffer pool space when `buffer.memory` is exhausted (back-pressure).

Setting it to 0 (or absurdly low) makes the producer give up on the metadata round trip before the bootstrap broker can respond. The first `send()` throws `TimeoutException: Topic <X> not present in metadata after 0 ms.`

This is occasionally mistakenly used as a "fail fast on full buffer" knob — but it also kills the cold-start metadata bootstrap.

## Operational impact

- Producer never produces — first record fails, application crashes or enters infinite retry on startup.
- Looks like a connectivity problem but isn't.
- Metric: `producer-metrics:record-error-total` spikes with `TimeoutException` immediately at startup.

## How to fix

```java
// BAD
props.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, "0");

// GOOD — default is fine for most apps
// (omit)

// GOOD — fail fast on full buffer, but allow metadata bootstrap
props.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, "5000");
```

If the intent is "drop on back-pressure", combine a small `buffer.memory` with a non-blocking producer wrapper at the application layer; do not zero `max.block.ms`.

## When this might be a false positive

- Smoke-test wiring that wants the producer to fail fast if the cluster is unreachable. Even then, use ≥ 500ms.

## Detection strategy

- Config: `max.block.ms < 100`. HIGH.
- Bytecode: `Properties.put("max.block.ms", "<small>")`. HIGH for literal 0; MEDIUM otherwise.

## References

- Apache Kafka producer configs — `max.block.ms`: https://kafka.apache.org/documentation/#producerconfigs_max.block.ms
- KafkaProducer.send() javadoc — blocking conditions: https://kafka.apache.org/40/javadoc/org/apache/kafka/clients/producer/KafkaProducer.html#send-org.apache.kafka.clients.producer.ProducerRecord-
