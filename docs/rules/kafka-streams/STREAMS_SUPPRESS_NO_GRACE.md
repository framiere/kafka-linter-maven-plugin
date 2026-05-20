# STREAMS_SUPPRESS_NO_GRACE

**Severity**: WARNING
**Confidence**: MEDIUM
**Detection**: dsl-chain
**Tagline**: `suppress(untilWindowCloses)` without explicit grace is a 24-hour pause.

## TL;DR

The linter flags `Suppressed.untilWindowCloses(...)` applied to a window that was constructed via the deprecated `JoinWindows.of` / `TimeWindows.of` (no explicit grace). The suppress will only flush after window-end + grace, and the legacy grace is 24 hours.

## What's happening (the mechanism)

`suppress(Suppressed.untilWindowCloses(BufferConfig))` holds back emissions until the window's grace period has elapsed (in stream-time). It needs the window to have a *fixed* end + grace so it knows when results are final.

If the window was built with `TimeWindows.of(Duration.ofMinutes(1))`, the legacy grace of 24h applies. The suppress therefore buffers results for at least 24h before emitting a single one. Combined with another classic pitfall — suppress only flushes when stream-time advances, which requires new input on the relevant partition — you can end up never emitting at all in a low-traffic stream.

The new `untilWindowCloses` is also strict in another way: you must use a `StrictBufferConfig` (`unbounded()`, `shutDownWhenFull()`, etc.). Using `Suppressed.untilTimeLimit` instead of `untilWindowCloses` does *not* give the "final result" guarantee.

## Operational impact

- Downstream consumers see no output for a day after the topology starts. Operators conclude "the suppress is broken" and remove it, losing the dedup behavior they wanted.
- If buffer config is unbounded and grace is 24h, RAM grows for 24h. OOM possible.
- For low-traffic topics, stream-time may never advance enough — suppress holds forever.

## How to fix

```java
// BAD — legacy window, 24h grace, suppress waits 24h
stream.groupByKey()
      .windowedBy(TimeWindows.of(Duration.ofMinutes(5)))
      .count(Materialized.as("counts-5m"))
      .suppress(Suppressed.untilWindowCloses(BufferConfig.unbounded()))
      .toStream();

// GOOD — explicit grace, suppress flushes shortly after window-end
stream.groupByKey()
      .windowedBy(TimeWindows.ofSizeAndGrace(Duration.ofMinutes(5), Duration.ofSeconds(30)))
      .count(Materialized.as("counts-5m"))
      .suppress(Suppressed.untilWindowCloses(BufferConfig.unbounded().shutDownWhenFull()))
      .toStream();
```

For low-volume streams that may never advance stream-time enough, prefer `Suppressed.untilTimeLimit` with a wall-clock-driven flush or wire up a keep-alive producer.

## When this might be a false positive

- Topologies built against Kafka < 3.0 where the legacy API is the only one.

## Detection strategy

- DSL chain: a `suppress(Suppressed.untilWindowCloses(...))` whose upstream `windowedBy` argument was built via `JoinWindows.of` / `TimeWindows.of` / `SessionWindows.with`.
- Bytecode: trace the `KStream` / `KGroupedStream` to the `.windowedBy(...)` argument; check if that argument was produced by a deprecated factory.
- Confidence: HIGH when both deprecated factory and `untilWindowCloses` are present.

## References

- KIP-328 — Ability to suppress updates for KTables: https://cwiki.apache.org/confluence/display/KAFKA/KIP-328:+Ability+to+suppress+updates+for+KTables
- KIP-633 — Deprecate 24 hour default grace: https://cwiki.apache.org/confluence/display/KAFKA/KIP-633%3A+Deprecate+24+hour+default+grace+period+for+windowed+operations
- Suppressed javadoc: https://kafka.apache.org/40/javadoc/org/apache/kafka/streams/kstream/Suppressed.html
