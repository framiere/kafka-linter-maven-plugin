# PRODUCER_BUFFER_MEMORY_MISCONFIG

**Severity**: WARNING
**Confidence**: MEDIUM
**Detection**: config-file
**Tagline**: buffer.memory below batch.size is "I asked for back-pressure on every send."

## TL;DR

The linter flags producer configurations where `buffer.memory` is set absurdly small (≤ `batch.size`) or absurdly large (> 1 GiB without justification).

## What's happening (the mechanism)

`buffer.memory` (default 33554432 = 32 MiB) is the total memory the producer can use to buffer records waiting to be sent. When exhausted, `send()` blocks for up to `max.block.ms` (default 60s), then throws `TimeoutException`.

Two failure modes:

1. **Too small** (e.g. < 1 MiB): every burst of records hits the back-pressure path. The producer spends most of its time blocking in `send()` and times out under any spike.
2. **Too large** (e.g. > 1 GiB): consumes JVM heap that should be available for processing. On OOM, the producer's accumulator dies with the JVM and all buffered records are lost.

Common rule of thumb: `buffer.memory >= batch.size * number_of_partitions_actively_written`.

## Operational impact

- Too small: producer-side back-pressure under any traffic spike; `TimeoutException: Expiring N records: <topic-partition>` in logs. Metric: `producer-metrics:buffer-available-bytes` near zero.
- Too large: JVM heap pressure, longer GC pauses, OOM under crash.

## How to fix

```java
// BAD — too small
props.put(ProducerConfig.BUFFER_MEMORY_CONFIG, "65536"); // 64 KiB

// BAD — too large without rationale
props.put(ProducerConfig.BUFFER_MEMORY_CONFIG, "2147483648"); // 2 GiB

// GOOD — default
// (omit)

// GOOD — tuned to known workload
props.put(ProducerConfig.BUFFER_MEMORY_CONFIG, "67108864"); // 64 MiB
props.put(ProducerConfig.BATCH_SIZE_CONFIG, "131072");      // 128 KiB
```

## When this might be a false positive

- High-fan-out producers writing to thousands of partitions concurrently legitimately need large buffers.
- Embedded systems with tight memory budgets where the producer is rate-limited externally.

## Detection strategy

- Config: `buffer.memory < 1048576` (1 MiB) OR `buffer.memory > 1073741824` (1 GiB). MEDIUM.
- Cross-key: `buffer.memory < batch.size` is always wrong. HIGH.

## References

- Apache Kafka producer configs — `buffer.memory`: https://kafka.apache.org/documentation/#producerconfigs_buffer.memory
- Confluent — Optimizing Throughput: https://docs.confluent.io/cloud/current/client-apps/optimizing/throughput.html
