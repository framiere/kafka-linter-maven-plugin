# STREAMS_PUNCTUATOR_WRONG_TIME

**Severity**: WARNING
**Confidence**: MEDIUM
**Detection**: bytecode
**Tagline**: Wall-clock punctuators fire even when no data arrives. Stream-time punctuators don't.

## TL;DR

The linter flags `ProcessorContext.schedule(...)` where the choice of `PunctuationType` (`STREAM_TIME` vs `WALL_CLOCK_TIME`) likely doesn't match the intent — usually evidenced by stream-time punctuators inside topologies that need wall-clock heartbeats, or wall-clock punctuators inside event-time-correctness topologies.

## What's happening (the mechanism)

`PunctuationType.STREAM_TIME` advances *only* when records arrive (it's driven by the timestamp extractor). On a low-traffic input topic, your "every 1 minute" punctuator may fire zero times an hour.

`PunctuationType.WALL_CLOCK_TIME` advances via the polling loop independent of records. It's best-effort (granularity bounded by `poll.ms`) but fires regardless of input. It's the right choice for:
- Periodic emit / heartbeat / health beacon.
- Timeout-based cleanup of session state.
- "Send something every minute" UX requirements.

It's the wrong choice for:
- Time-travel and reprocessing — wall-clock breaks determinism across replays.
- Event-time-correctness windows — stream-time is the canonical clock.

A related anti-pattern: calling `System.currentTimeMillis()` *inside* a processor to compute event times. That makes the topology non-deterministic and breaks reprocessing. Use `ctx.currentStreamTimeMs()` or `record.timestamp()` instead.

A second anti-pattern: scheduling a punctuator with a tiny interval (e.g. `Duration.ofMillis(1)`) — effectively a busy loop. Kafka Streams runs punctuators on the StreamThread; tight intervals starve actual record processing.

## Operational impact

- Stream-time punctuator on low-traffic topic: emits/cleanups never happen → state grows, sessions never time out, `kafka.streams:type=stream-task-metrics,...:punctuate-rate` is 0.
- Wall-clock punctuator on a backfill/replay run: emits real-time data into historical processing → wrong outputs, undetectable until you compare to a golden dataset.
- Tight-interval punctuator: `kafka.streams:type=stream-thread-metrics,...:process-rate` plummets; thread is CPU-bound on the punctuator.

## How to fix

```java
// BAD — stream-time punctuator that's meant to "tick every minute"
ctx.schedule(Duration.ofMinutes(1), PunctuationType.STREAM_TIME, ts -> {
    flushHealthBeacon();   // never fires if no data arrives
});

// GOOD — wall-clock for time-based housekeeping
ctx.schedule(Duration.ofMinutes(1), PunctuationType.WALL_CLOCK_TIME, ts -> {
    flushHealthBeacon();
});

// BAD — System.currentTimeMillis() in a processor breaks replay determinism
public void process(Record<String, Order> record) {
    long now = System.currentTimeMillis();
    ctx.forward(record.withValue(record.value().withProcessedAt(now)));
}

// GOOD — use stream time
public void process(Record<String, Order> record) {
    long now = ctx.currentStreamTimeMs();
    ctx.forward(record.withValue(record.value().withProcessedAt(now)));
}

// BAD — busy-loop punctuator
ctx.schedule(Duration.ofMillis(1), PunctuationType.WALL_CLOCK_TIME, ts -> { ... });

// GOOD — reasonable cadence (>= 100ms, usually seconds)
ctx.schedule(Duration.ofSeconds(10), PunctuationType.WALL_CLOCK_TIME, ts -> { ... });
```

## When this might be a false positive

- Stream-time punctuator is correct when the punctuator is part of event-time-correct aggregation logic (e.g. emitting on window-close).
- Wall-clock punctuator is correct for periodic housekeeping.

## Detection strategy

- Bytecode: `INVOKEINTERFACE org/apache/kafka/streams/processor/api/ProcessorContext.schedule (Ljava/time/Duration;Lorg/apache/kafka/streams/processor/PunctuationType;...)`.
  - Argument 2 is `PunctuationType.STREAM_TIME` AND the lambda body calls external I/O (network, JDBC, logger.info) → MEDIUM signal of "wall-clock was intended".
  - Argument 1 is a Duration < 100ms → busy-loop. MEDIUM confidence.
- Bytecode: `INVOKESTATIC java/lang/System.currentTimeMillis ()J` inside a class implementing `Processor`/`FixedKeyProcessor`/`Transformer`. HIGH suspicion of replay-non-determinism.
- Confidence: MEDIUM.

## References

- PunctuationType javadoc: https://kafka.apache.org/40/javadoc/org/apache/kafka/streams/processor/PunctuationType.html
- Confluent — Processor API: https://docs.confluent.io/platform/current/streams/developer-guide/processor-api.html
- Matthias J. Sax — Time semantics in Kafka Streams: https://www.confluent.io/blog/watermarks-tables-event-time-dataflow-model/
