# STREAMS_SUPPRESS_UNBOUNDED_BUFFER
**Severity**: ERROR
**Confidence**: HIGH
**Detection**: bytecode
**Tagline**: `Suppressed.BufferConfig.unbounded()` in production is a heap-OOM with a paper trail.
**Source**: Confluent agent-skills — kafka-streams-programming/references/topology-patterns.md § Suppression, kafka-streams-programming/references/debugging.md § Memory Issues.

## TL;DR

The linter flags any call to `Suppressed.untilWindowCloses(BufferConfig.unbounded())` (or any other suppression with `BufferConfig.unbounded()`) outside of test code. Confluent's pattern reference explicitly prescribes bounded buffers for production:

> .suppress(Suppressed.untilWindowCloses(BufferConfig.maxRecords(10000).shutDownWhenFull())) // Bounded for production

`unbounded()` keeps every in-flight window in-heap until the grace period expires; for any non-trivial cardinality, that is an OOM kill in production.

## What's happening (the mechanism)

`Suppressed.untilWindowCloses(...)` holds aggregation outputs in a buffer until the window closes (plus grace). The buffer's job is to suppress the per-record intermediate updates that the cache would normally emit — only the final value per window is emitted downstream.

`BufferConfig.unbounded()` says: hold as many records as needed, no eviction, no cap.

In practice:
- Cardinality is key count × concurrent open windows. For a 5-min tumbling window on user_id with 10M active users, that's 10M open buffer entries — easily 5-10 GB of heap.
- Memory keeps growing until grace expires. For grace of `Duration.ofHours(1)`, that's an hour of accumulation before any evictions.
- JVM heap fills, GC pressure climbs, container OOM-kills the pod. The replacement pod starts cold, restores state from changelog (slow), then begins accumulating again. Repeats.

Confluent's debugging.md guide names "Unbounded suppression buffer" explicitly as a memory-growth root cause:

> Unbounded suppression buffer: Using `Suppressed.BufferConfig.unbounded()` in production. Fix: use `maxRecords(N).shutDownWhenFull()`.

## Operational impact

- Container OOM kill (Kubernetes `OOMKilled` event). JVM heap saturation if the limit is unlimited.
- After OOM kill, full state restoration on restart → restoration cascade (see `STREAMS_STATE_DIR_TMP`-style fallout when state is on ephemeral storage).
- No graceful degradation — the buffer either fits or the JVM dies.

## How to fix (bad → good code)

```java
// BAD — unbounded buffer
KTable<Windowed<String>, Long> windowed = stream
    .groupByKey()
    .windowedBy(TimeWindows.ofSizeAndGrace(Duration.ofMinutes(5), Duration.ofMinutes(1)))
    .count(Materialized.as("counts"))
    .suppress(Suppressed.untilWindowCloses(BufferConfig.unbounded()));   // OOM waiting to happen

// GOOD — bounded, fail-fast if exceeded
KTable<Windowed<String>, Long> windowed = stream
    .groupByKey()
    .windowedBy(TimeWindows.ofSizeAndGrace(Duration.ofMinutes(5), Duration.ofMinutes(1)))
    .count(Materialized.as("counts"))
    .suppress(Suppressed.untilWindowCloses(
        BufferConfig.maxRecords(10_000).shutDownWhenFull()));   // bounded; loud crash if cardinality grows

// GOOD — bounded by bytes, also fail-fast
.suppress(Suppressed.untilWindowCloses(
    BufferConfig.maxBytes(100L * 1024 * 1024).shutDownWhenFull()));
```

Choose `maxRecords` for predictable cardinality, `maxBytes` for unpredictable record sizes. **Always** use `.shutDownWhenFull()` (or accept the default `emitEarlyWhenFull` if duplicate emissions downstream are tolerable) — never `unbounded()` in production.

## When this might be a false positive

- Test code using `TopologyTestDriver` where the dataset is small and bounded.
- Demo / educational code where the trade-off is documented.
- A user has confirmed via measurement that cardinality is bounded by external factors (rare; this is a fragile assumption).

## Detection strategy

- Bytecode: `INVOKESTATIC org/apache/kafka/streams/kstream/Suppressed$BufferConfig.unbounded()` reachable from a `KafkaStreams.start()`-bound topology.
- Match also: any `BufferConfig` constructed without `maxRecords(...)` or `maxBytes(...)` constraints.
- Test-code exclusion: skip classes in `src/test/`. HIGH confidence outside tests.

## References

- Confluent agent-skills — kafka-streams-programming/references/topology-patterns.md § Suppression
- Confluent agent-skills — kafka-streams-programming/references/debugging.md § Memory Issues
- Apache Kafka docs — `Suppressed`: https://kafka.apache.org/40/javadoc/org/apache/kafka/streams/kstream/Suppressed.html
- Related rule: [STREAMS_SUPPRESS_NO_GRACE](STREAMS_SUPPRESS_NO_GRACE.md) — the windowing side of the same suppression concern.
