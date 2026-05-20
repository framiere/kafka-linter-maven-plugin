# STREAMS_NUM_THREADS_ONE

**Severity**: WARNING
**Confidence**: MEDIUM
**Detection**: config-file
**Tagline**: One stream thread is one heart attack from offline.

## TL;DR

The linter flags `num.stream.threads=1` (or absent — the default is 1) on production-flagged configs. A single thread is a single point of failure: when it dies on a deserialization error, the whole instance stops processing.

## What's happening (the mechanism)

`num.stream.threads` controls how many `StreamThread`s a `KafkaStreams` instance runs. Each thread polls a slice of the assigned tasks. Default is **1**.

If that one thread dies (poison pill, OOM in a processor, RocksDB I/O error) and `StreamsUncaughtExceptionHandler` returns `SHUTDOWN_CLIENT` (the default), the entire JVM instance becomes idle. Even with `REPLACE_THREAD`, you have a single point of contention for everything the instance handles — no I/O parallelism, no CPU parallelism, and the punctuator + the processor + the commit all serialize.

You should target roughly `min(cores_available, max_partitions_per_topic)`. More threads than input partitions waste resources (idle threads). Fewer threads serialize work that could parallelize.

## Operational impact

- `kafka.streams:type=stream-metrics,client-id=...:alive-stream-threads` falls to 0 the moment the single thread dies → no processing, lag grows linearly.
- `kafka.streams:type=stream-thread-metrics,...:process-rate` is bounded by one CPU core regardless of available capacity.
- During a rebalance, the entire instance is stuck on one thread's reassignment.
- p99 latency dominated by punctuator runs (no concurrency to absorb them).

## How to fix

```properties
# BAD — default, or explicit 1
num.stream.threads=1

# GOOD — match cores and partitions
num.stream.threads=4
```

```java
// GOOD — derived from runtime
int partitions = 12;            // largest input topic partition count
int cores = Runtime.getRuntime().availableProcessors();
props.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, Math.min(partitions, cores));
```

Pair with KIP-671's `StreamsUncaughtExceptionHandler` returning `REPLACE_THREAD` for transient errors so that one bad record doesn't take down the herd.

## When this might be a false positive

- Single-partition input topics: more threads than partitions = idle threads. One thread is correct.
- Local development.
- Embedded use cases where the application is a leaf in a larger system and concurrency is managed elsewhere.

## Detection strategy

- Config files: `num.stream.threads=1`, or absent (the implicit default is 1). Lower confidence when absent — could be intentional or just unset.
- Bytecode: `Properties.put("num.stream.threads", "1")` / `StreamsConfig.NUM_STREAM_THREADS_CONFIG` set to `LDC "1"` or `ICONST_1` boxed.
- Pair-detect: if input topic partition count is known (e.g. from `Topic` annotations or explicit `partitions=`), suppress when partitions == 1.
- Confidence: MEDIUM — sometimes legitimate.

## References

- Streams config — num.stream.threads: https://kafka.apache.org/documentation/streams/developer-guide/config-streams.html#num-stream-threads
- KIP-671 — Streams-specific uncaught exception handler: https://cwiki.apache.org/confluence/display/KAFKA/KIP-671:+Introduce+Kafka+Streams+Specific+Uncaught+Exception+Handler

## Cross-reference

Also recommended by Confluent agent-skills — see kafka-streams-programming/references/config-baseline.md § Thread sizing, kafka-streams-programming/references/architecture.md § Threading model.
