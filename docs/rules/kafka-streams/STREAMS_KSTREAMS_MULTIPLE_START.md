# STREAMS_KSTREAMS_MULTIPLE_START

**Severity**: ERROR
**Confidence**: HIGH
**Detection**: bytecode
**Tagline**: `KafkaStreams#start()` is one-shot. Calling it twice throws `IllegalStateException`.

## TL;DR

The linter flags code paths that may call `KafkaStreams#start()` more than once on the same instance, or call `start()` after `close()` has been called. Both throw `IllegalStateException("Stream is already running")` / `("Stream has already been started")`.

## What's happening (the mechanism)

`KafkaStreams` is a single-use lifecycle object: `CREATED → RUNNING → NOT_RUNNING`. The state machine forbids:
- `start()` when already in any state past `CREATED`.
- `start()` after `close()` (state is terminal).

Restart-on-failure logic that catches a `StreamsUncaughtException`, calls `close()`, and then calls `start()` on the *same* instance does not work — you must build a new `KafkaStreams` from the same `Topology`.

A related defect is mutating the `StreamsBuilder` after `build()` has been called: adding sources/sinks after `.build()` is silently ignored. The compiled topology is whatever was in the builder at `.build()` time.

## Operational impact

- `IllegalStateException` at startup blocking the entire app.
- Restart loops where each retry crashes faster than the last.
- Topologies missing operators that "should be there" because they were added post-`build()`.

## How to fix

```java
// BAD — second start throws
KafkaStreams streams = new KafkaStreams(topology, props);
streams.start();
// ... later, on some failure ...
streams.close();
streams.start();   // IllegalStateException

// GOOD — build a new instance
private KafkaStreams streams;
private final Topology topology;
private final Properties props;

public void restart() {
    if (streams != null) streams.close();
    streams = new KafkaStreams(topology, props);
    streams.setUncaughtExceptionHandler(myHandler);
    streams.start();
}

// BAD — mutating builder after build
StreamsBuilder builder = new StreamsBuilder();
builder.stream("a").to("b");
Topology t = builder.build();
builder.stream("c").to("d");   // silently absent from t
```

## When this might be a false positive

- None — these are always bugs.

## Detection strategy

- Bytecode: data-flow analysis. Find a `KafkaStreams` local/field and look for two paths that both invoke `start()` on it without a re-construction (`NEW org/apache/kafka/streams/KafkaStreams`) in between.
- Bytecode: `StreamsBuilder.build()` invocation followed by another mutating call (`stream`, `table`, `addProcessor`, etc.) on the same `StreamsBuilder` instance.
- Confidence: HIGH when both paths are statically provable.

## References

- KafkaStreams javadoc: https://kafka.apache.org/40/javadoc/org/apache/kafka/streams/KafkaStreams.html
- KIP-696 — Update Streams FSM: https://cwiki.apache.org/confluence/display/KAFKA/KIP-696%3A+Update+Kafka+Streams+FSM+to+clarify+ERROR+state
