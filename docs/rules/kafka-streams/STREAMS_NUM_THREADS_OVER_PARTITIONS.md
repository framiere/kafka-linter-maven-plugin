# STREAMS_NUM_THREADS_OVER_PARTITIONS

**Severity**: WARNING
**Confidence**: CONTEXT
**Detection**: annotation + config-file
**Tagline**: Threads with nothing to do still cost you a heap.

## TL;DR

The linter flags `num.stream.threads` > the largest input topic's partition count. Excess threads sit idle but consume heap, RocksDB block cache (one per task), and JMX metric churn.

## What's happening (the mechanism)

Streams parallelism is bounded by the input topic's partition count: at most one task per input partition per sub-topology. If a sub-topology has 4 input partitions and you set `num.stream.threads=8`, four threads sit empty. They still:
- Hold per-thread state (`StreamThread` objects, polling loops).
- Pre-allocate consumer / producer per-thread resources.
- Receive rebalance protocol events.
- Show up in metrics, doubling the cardinality of `stream-thread-metrics`.

Worse, when scaling out the cluster, you exceed input partitions across instances combined: instances acquire 0 tasks and just keep the consumer group fat.

## Operational impact

- Idle threads show `process-rate=0` while alive in `alive-stream-threads`.
- Increased rebalance time proportional to total thread count (cooperative rebalance protocol negotiations).
- Extra JVM heap baseline for no benefit.
- Operator confusion: "we have 8 threads but only 4 are working".

## How to fix

```java
// BAD — 8 threads, only 4 input partitions
props.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, 8);

// GOOD — match max input partitions
props.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, 4);
```

For horizontal scaling, the total of `num.stream.threads × instances` should be `<=` max partitions. To go beyond that, increase the partition count of the bottleneck input topic.

## When this might be a false positive

- Apps where input partition count is expected to grow soon (pre-provisioning threads).
- Multi-topology apps where the maximum is across all sub-topologies, not one.

## Detection strategy

- Combination: `num.stream.threads` value from config + the largest input topic partition count (known via `@KafkaListener` annotations, explicit `topic.partitions` configs, or Kafka Connect manifests). When linter has access to the topic metadata (e.g. via a sidecar config), compare.
- Without topic metadata: just flag values > 16 as MEDIUM confidence (typical input topics are <= 16 partitions).
- Confidence: MEDIUM.

## References

- Streams config — num.stream.threads: https://kafka.apache.org/documentation/streams/developer-guide/config-streams.html#num-stream-threads
- Confluent — Capacity planning for Kafka Streams: https://docs.confluent.io/platform/current/streams/sizing.html
