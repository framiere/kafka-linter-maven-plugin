# STREAMS_JOIN_WINDOW_LEGACY_API

**Severity**: WARNING
**Confidence**: HIGH
**Detection**: bytecode
**Tagline**: `JoinWindows.of(...)` gives you a 24-hour grace period for free, whether you wanted it or not.

## TL;DR

The linter flags `JoinWindows.of(Duration)` and `TimeWindows.of(Duration)` — both deprecated in favor of the explicit-grace factory methods. The old API silently picks a 24-hour grace period; results don't appear until 24h after the window ends.

## What's happening (the mechanism)

Prior to KIP-633, `JoinWindows.of(Duration timeDifference)` returned a `JoinWindows` with grace = `24h - timeDifference`. That made stream-stream join outputs lag by up to 24 hours, which surprised everyone who built a "real time" join and discovered nothing emitted for a day.

KIP-633 (Kafka 3.0) deprecated `JoinWindows.of` (and the matching `TimeWindows.of` / `SessionWindows.with`) and added explicit factory methods:

- `JoinWindows.ofTimeDifferenceAndGrace(Duration timeDifference, Duration grace)` — explicit grace.
- `JoinWindows.ofTimeDifferenceWithNoGrace(Duration timeDifference)` — zero grace; out-of-order records past window end are dropped.
- `TimeWindows.ofSizeAndGrace`, `TimeWindows.ofSizeWithNoGrace` — same idea for tumbling/hopping.

The new API also enables "spurious result fixes" for left/outer stream-stream joins: results are emitted only after the grace period, not eagerly.

## Operational impact

- Stream-stream join output appears 24 hours later than intended (legacy 24h default). Pages on-call: "the join isn't working" → actually it is, just on tomorrow.
- For windowed aggregations using `TimeWindows.of`, late records are accepted for 24h, blowing up state store size unexpectedly. `kafka.streams:type=stream-state-store-metrics,...:put-rate` is way above expected.
- Spurious left-join results: with the legacy API, left/outer joins emit `(left, null)` eagerly, then update later. Downstream consumers see "null then non-null" sequences they have to deduplicate.

## How to fix

```java
// BAD — deprecated, 24h hidden grace
left.join(right,
    (l, r) -> l + r,
    JoinWindows.of(Duration.ofMinutes(5)));

// GOOD — explicit no grace
left.join(right,
    (l, r) -> l + r,
    JoinWindows.ofTimeDifferenceWithNoGrace(Duration.ofMinutes(5)));

// GOOD — explicit grace
left.join(right,
    (l, r) -> l + r,
    JoinWindows.ofTimeDifferenceAndGrace(Duration.ofMinutes(5), Duration.ofSeconds(30)));

// BAD — windowed aggregation, 24h hidden grace
stream.groupByKey()
      .windowedBy(TimeWindows.of(Duration.ofMinutes(1)))
      .count();

// GOOD
stream.groupByKey()
      .windowedBy(TimeWindows.ofSizeAndGrace(Duration.ofMinutes(1), Duration.ofSeconds(10)))
      .count(Materialized.as("counts-1m"));
```

## When this might be a false positive

- Code targeting Kafka < 3.0 where the new API doesn't exist. Suppress per-module.

## Detection strategy

- Bytecode:
  - `INVOKESTATIC org/apache/kafka/streams/kstream/JoinWindows.of (Ljava/time/Duration;)Lorg/apache/kafka/streams/kstream/JoinWindows;`
  - `INVOKESTATIC org/apache/kafka/streams/kstream/TimeWindows.of (...)` similarly.
  - `INVOKESTATIC org/apache/kafka/streams/kstream/SessionWindows.with (...)`.
- Also flag `JoinWindows.grace(Duration)` chained onto `JoinWindows.of` — works but uses the deprecated path.
- Confidence: HIGH (deprecated, replacement straightforward).

## References

- KIP-633 — Deprecate 24 hour default grace period: https://cwiki.apache.org/confluence/display/KAFKA/KIP-633%3A+Deprecate+24+hour+default+grace+period+for+windowed+operations
- JoinWindows javadoc: https://kafka.apache.org/40/javadoc/org/apache/kafka/streams/kstream/JoinWindows.html
- Israel Ekpo — Kafka Summit 2021 talk: https://www.confluent.io/events/kafka-summit-americas-2021/the-new-way-of-configuring-grace-periods-for-windowed-operations-in-kafka/
