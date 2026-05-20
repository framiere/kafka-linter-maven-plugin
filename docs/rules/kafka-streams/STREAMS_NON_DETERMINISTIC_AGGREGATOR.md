# STREAMS_NON_DETERMINISTIC_AGGREGATOR

**Severity**: WARNING
**Confidence**: MEDIUM
**Detection**: bytecode
**Tagline**: Aggregators with `Math.random()` give different answers on replay. That's a bug, not a feature.

## TL;DR

The linter flags `Aggregator`, `Reducer`, `Initializer`, `ValueMapper`, and filter `Predicate` implementations whose bytecode calls `Math.random`, `System.currentTimeMillis`, `Instant.now`, `UUID.randomUUID`, or mutates external static state. State stores are replayed from changelog on restore; non-deterministic functions produce different state each time.

## What's happening (the mechanism)

Kafka Streams guarantees that, given the same input topic with the same offsets, replaying from the changelog reproduces the exact same state. That guarantee holds only if every function the topology uses is a pure function of its inputs.

Common defects:
- `Math.random()` inside an aggregator's combine function → state varies across replays.
- `System.currentTimeMillis()` inside a `mapValues` → output timestamps differ between original and reprocess.
- Reading from a `static AtomicLong` counter or external cache → state depends on order of arrival, not just content.
- A filter `Predicate` that calls an HTTP service → non-deterministic and slow.

The damage compounds with restoration: after a broker failure forces a state restore, the *restored* state may diverge from the *online* state on another instance. Now reads via interactive queries return different answers depending on which instance you hit.

## Operational impact

- Aggregates "drift" — two instances of the same app converge on different values for the same key.
- Restoration after a node failure produces different state than what was there before → invisible data corruption.
- A/B tests fed by the aggregate are statistically meaningless.
- "It works locally / fails in prod" — local tests don't restore from changelog, prod does.

## How to fix

```java
// BAD — non-deterministic
stream.groupByKey().aggregate(
    () -> 0L,
    (key, value, agg) -> agg + (Math.random() < 0.1 ? value : 0L),
    Materialized.as("sampled-sums"));

// GOOD — deterministic sampling using a stable hash
stream.groupByKey().aggregate(
    () -> 0L,
    (key, value, agg) -> (Math.abs(key.hashCode()) % 10 == 0) ? agg + value : agg,
    Materialized.as("sampled-sums"));

// BAD — System.currentTimeMillis in a mapValues
stream.mapValues(v -> v.withProcessedAt(System.currentTimeMillis()));

// GOOD — let downstream consumers do wall-clock annotation, or use a Processor with context().currentStreamTimeMs()
stream.process(() -> new ContextualProcessor<String, V, String, V>() {
    public void process(Record<String, V> record) {
        context().forward(record.withValue(record.value().withProcessedAt(context().currentStreamTimeMs())));
    }
});
```

## When this might be a false positive

- Functions that intentionally tag output with processing-time metadata are fine — but they shouldn't drive state.
- Code that uses `System.currentTimeMillis()` purely for logging.

## Detection strategy

- Bytecode: any class implementing `Aggregator`, `Reducer`, `Initializer`, `ValueMapper`, `ValueMapperWithKey`, `KeyValueMapper`, `Predicate`, or `Processor`/`FixedKeyProcessor` (specifically inside `aggregate`/`reduce`/`mapValues`/`filter`/`process` invocations) that:
  - Contains `INVOKESTATIC java/lang/Math.random ()D`
  - Contains `INVOKESTATIC java/lang/System.currentTimeMillis ()J` (lower severity in a Processor with no aggregation downstream)
  - Contains `INVOKESTATIC java/time/Instant.now ()Ljava/time/Instant;`
  - Contains `INVOKESTATIC java/util/UUID.randomUUID ()Ljava/util/UUID;`
  - Writes to a `PUTSTATIC` outside the Streams runtime's own classes.
- Confidence: MEDIUM (some uses are benign).

## References

- Confluent — Why my Kafka Streams app gives different results on restart: https://docs.confluent.io/platform/current/streams/faq.html
- Matthias J. Sax — Streams testing: https://www.confluent.io/blog/test-kafka-streams-with-topologytestdriver/
