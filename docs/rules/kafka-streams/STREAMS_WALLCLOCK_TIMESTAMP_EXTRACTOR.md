# STREAMS_WALLCLOCK_TIMESTAMP_EXTRACTOR

**Severity**: WARNING
**Confidence**: HIGH
**Detection**: config-file
**Tagline**: `WallclockTimestampExtractor` says "ignore the data's timestamp, use mine."

## TL;DR

The linter flags `default.timestamp.extractor=org.apache.kafka.streams.processor.WallclockTimestampExtractor` (or explicit `timestamp.extractor` in `Consumed`) in pipelines that perform windowing, joins, or any event-time-correct computation. Replay determinism is gone, and "out of order" no longer has a meaningful definition.

## What's happening (the mechanism)

`WallclockTimestampExtractor` calls `System.currentTimeMillis()` and returns that as the record's timestamp — completely ignoring the producer-assigned timestamp (`CreateTime`) and broker-assigned (`LogAppendTime`).

Consequences:
- Stream time = wall-clock time at processing → on replay, all records "happened just now".
- Windows are bucketed by processing time, not event time.
- Joins use processing time → late-arriving events join with whatever happens to be in the join store *right now*, not the events that were actually concurrent.
- The whole topology is non-deterministic across reprocessing.

The built-in alternatives are:
- `FailOnInvalidTimestamp` — *default*. Uses CreateTime/LogAppendTime; throws if invalid.
- `LogAndSkipOnInvalidTimestamp` — uses CreateTime/LogAppendTime; skips invalid records.
- `UsePartitionTimeOnInvalidTimestamp` — uses CreateTime; falls back to partition time on invalid.

`WallclockTimestampExtractor` is appropriate only when the source topic has no usable timestamp (legacy producers from pre-0.10) *and* you don't need event-time correctness.

## Operational impact

- Reprocessing produces different results than the original run. Audits fail.
- Windowed aggregates emit results bucketed by when the consumer happened to process them — different across pods, different across restarts.
- `kafka.streams:type=stream-task-metrics,...:enforced-processing-rate` is normal but the *contents* of the windows are wrong.
- Stream-stream joins emit "false positive" matches between events that were never actually concurrent.

## How to fix

```properties
# BAD
default.timestamp.extractor=org.apache.kafka.streams.processor.WallclockTimestampExtractor

# GOOD — default behavior; use CreateTime/LogAppendTime
# (omit, or set explicitly)
default.timestamp.extractor=org.apache.kafka.streams.processor.FailOnInvalidTimestamp

# GOOD — for sources that may have invalid timestamps
default.timestamp.extractor=org.apache.kafka.streams.processor.LogAndSkipOnInvalidTimestamp
```

For per-source override, prefer per-`Consumed` extractor:
```java
KStream<String, Order> orders = builder.stream("orders",
    Consumed.with(Serdes.String(), orderSerde)
            .withTimestampExtractor(new OrderEventTimeExtractor()));
```

## When this might be a false positive

- Legacy topics with `CreateTime=-1` everywhere and no event-time-correct downstream. Document explicitly.

## Detection strategy

- Config files: `default.timestamp.extractor` or `timestamp.extractor` value ending in `WallclockTimestampExtractor`.
- Bytecode: `LDC "org.apache.kafka.streams.processor.WallclockTimestampExtractor"` paired with `default.timestamp.extractor` key or passed to `Consumed.withTimestampExtractor(new WallclockTimestampExtractor())`.
- Confidence: HIGH — almost always a mistake when the topology has windows/joins.

## References

- WallclockTimestampExtractor javadoc: https://kafka.apache.org/40/javadoc/org/apache/kafka/streams/processor/WallclockTimestampExtractor.html
- KIP-32 — Add timestamps to Kafka message: https://cwiki.apache.org/confluence/display/KAFKA/KIP-32+-+Add+timestamps+to+Kafka+message
- Confluent — Time semantics in Kafka Streams: https://docs.confluent.io/platform/current/streams/concepts.html#time
