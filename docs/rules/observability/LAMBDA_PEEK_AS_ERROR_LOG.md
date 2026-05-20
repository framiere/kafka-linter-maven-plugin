# LAMBDA_PEEK_AS_ERROR_LOG

**Severity**: WARNING
**Confidence**: MEDIUM
**Detection**: bytecode
**Tagline**: `Stream.peek(x -> log.error(...))` is not error handling — `peek` is for inspection, not action.

## TL;DR

The linter flags `Stream.peek` and `KStream.peek` calls whose lambda contains a logging call at WARN/ERROR level — typically used as a poor-man's "log every record that flows through." Two problems: (a) `Stream.peek` is called *for every record that the terminal operator pulls*, so a peek that "logs errors" actually logs the entire stream (or worse, the stream is consumed twice and the peek fires twice); (b) `peek` is intentionally a side-effect-only operator with no return value influence — the record continues regardless of whether the "error" was "handled."

## The setup

Team adds a Streams topology. They want to log records that fail a filter. They write `.filter(this::isValid).peek((k, v) -> log.error("invalid {}", v))` — but `peek` is *before* the filter rejects anything, so it logs every record, not just invalid ones. Or they write `.peek((k, v) -> { if (!isValid(v)) log.error(...); })` thinking it's a side-channel for errors, and the topology continues processing the bad records as if peek had filtered them out.

## What's actually happening

`KStream.peek(ForeachAction)` and `java.util.stream.Stream.peek(Consumer)` are *non-terminal*, *side-effecting* operators. They:

1. Receive every record that the upstream emits.
2. Invoke the lambda.
3. Pass the record downstream unchanged.

The lambda's return value (if any) is discarded. There's no way for the lambda to "drop" a record or signal an error to the topology. If the lambda throws:
- `Stream.peek` (JDK): exception propagates to the terminal operator. The stream stops.
- `KStream.peek`: exception propagates to the deserializer/processor pipeline. The stream thread dies (subject to `StreamsUncaughtExceptionHandler`).

A "log errors via peek" pattern is therefore misleading on two axes:

- The error path (lambda throws) kills the topology — usually not what was intended.
- The "log and continue" path doesn't actually distinguish good records from bad — it logs every record under an `error()` call.

Worse for `java.util.stream.Stream`: if the stream is consumed multiple times (`stream.collect(...); stream.forEach(...)`, illegal but common bug), peek fires multiple times.

## Why this is subtle

- `peek` reads like "look at this without affecting it" — exactly the right semantic for "I want to log without changing the data flow."
- The fix isn't obvious — you want a separate processor / side-output, not peek.
- Tests that consume the stream once make peek look correct.
- The IDE doesn't warn about `peek` with an action that "could be a side effect."
- For `KStream.peek`, it's often used (correctly!) for observability spans — but using it for error reporting confuses the two roles.

## Operational impact

- Every record logged at ERROR — log volume explodes.
- "Error rate" alerts trigger constantly (or, after a noise-reduction PR, never).
- If the lambda throws on bad data, the topology dies on the first bad record instead of dropping it.
- Real error paths (DeserializationExceptionHandler, ProductionExceptionHandler) bypassed in favor of peek-based logging.

## Failure scenarios (walkthrough)

1. **The misplaced peek.** Topology: `source.filter(this::isValid).peek((k, v) -> log.error("processing {}", v)).to("out")`. Intended: log invalid records. Actual: logs every valid record (peek runs *after* filter, on the records that pass). Inversely placed: `source.peek(...).filter(...)` logs every record before filter. Either way, ERROR-level log spam.

2. **The peek-as-tombstone-counter.** `.peek((k, v) -> { if (v == null) meter.count("tombstones"); })` — peek is run lazily; if the downstream operator never pulls, the metric stays at zero. This is rare for `KStream` (Streams is push) but a classic bug for `java.util.stream.Stream`.

3. **The exception-in-peek.** `.peek((k, v) -> validate(v))` where `validate` throws on bad data. The throw propagates up. Stream thread dies. Same record on restart. Infinite loop.

## How to fix

Pick the right operator for your intent:

```java
// BAD — peek as error log
source.filter(this::isValid)
      .peek((k, v) -> log.error("processed: {}", v))
      .to("out");

// GOOD — branch for observability + main flow
source.peek((k, v) -> meter.counter("source.received").increment())
      .filter(this::isValid)
      .to("out");
// peek used correctly: side-effect observation, not error handling

// GOOD — explicit branch for "bad" records to a side topic
KStream<K, V>[] branches = source.branch(
    (k, v) -> isValid(v),       // good branch
    (k, v) -> true              // catch-all bad branch
);
branches[0].to("out");
branches[1].peek((k, v) -> meter.counter("invalid").increment())
           .to("out.dlq");

// GOOD — use process() for actual processing-side concerns
source.process(() -> new BadRecordCounter(), Named.as("counter"));
```

For `java.util.stream.Stream`, prefer `filter` + `forEach` over `peek` for anything observable.

## When this might be a false positive

- `peek` is used legitimately for observability (span attribute, metric counter) and the logger call is at INFO/DEBUG (not WARN/ERROR).
- One-off debugging code that should be removed before merge but isn't yet.

## Detection strategy

- bytecode: visit calls to `org.apache.kafka.streams.kstream.KStream.peek` and `java.util.stream.Stream.peek`.
- Resolve the lambda. If the lambda's body contains a call to a `Logger.error(...)` or `Logger.warn(...)`, flag.
- Lower the confidence if the lambda also calls `MeterRegistry.counter(...)` — could be legitimate.
- Confidence MEDIUM — peek is a legitimate operator and the log-level heuristic is imperfect.

## References

- Apache Kafka Streams — `KStream.peek` API: https://kafka.apache.org/40/javadoc/org/apache/kafka/streams/kstream/KStream.html#peek
- JDK `Stream.peek` API + caveats: https://docs.oracle.com/en/java/javase/21/docs/api/java.base/java/util/stream/Stream.html#peek(java.util.function.Consumer)
- Related: `STREAMS_FOREACH_FOR_STATE` (similar misuse of side-effecting operator)
