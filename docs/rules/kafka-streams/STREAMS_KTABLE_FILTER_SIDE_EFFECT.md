# STREAMS_KTABLE_FILTER_SIDE_EFFECT

**Severity**: WARNING
**Confidence**: MEDIUM
**Detection**: bytecode
**Tagline**: `filter`/`mapValues` with side effects: rerun the changelog, rerun the side effect.

## TL;DR

The linter flags `KStream`/`KTable#filter`, `mapValues`, `foreach`, and `peek` whose lambda makes external side effects (HTTP call, DB write, file I/O, mutable static state). State stores replay from changelog on restore; every replay re-executes the lambda → duplicated side effects.

## What's happening (the mechanism)

Streams operators are expected to be pure data transformations. The runtime replays records from changelogs during restore, during rebalance, and (under ALOS) re-processes records after failure. Side effects in operator lambdas execute every time:
- Initial processing.
- Restore after a node fails.
- Reprocessing after task migration.
- (Under ALOS) at-least-once duplication.

A `foreach(record -> httpClient.post(...))` inside an aggregate that's later materialized means every restore-time replay re-issues the HTTP POST. Downstream systems can't tell the difference between a "real" message and a replay.

The right place for side effects is at the *sink*, via `to(...)` writing to a topic that another consumer (Kafka Connect, a sink service) is responsible for honoring idempotently. Or use `process()` with exactly-once semantics where the sink is a Kafka topic written transactionally.

## Operational impact

- Duplicate downstream side effects after a Streams instance restart.
- Replay storms when broker failures trigger restoration cluster-wide.
- Side-channel I/O serialized on the StreamThread → throughput collapses.
- "Why did we send the same email twice?" tickets.

## How to fix

```java
// BAD — side effect in mapValues
stream.mapValues(v -> {
    httpClient.post("/audit", v.toJson());   // re-runs on every replay
    return v.enrich();
});

// GOOD — separate the side effect to a sink topic
KStream<String, V> enriched = stream.mapValues(V::enrich);
enriched.to("audit-events", Produced.with(...));
// Then a separate consumer (Kafka Connect HTTP sink, etc.) handles the POST idempotently.

// BAD — peek that writes to a database
stream.peek((k, v) -> jdbcTemplate.update(...));

// GOOD — sink via Kafka Connect JDBC sink
stream.to("for-db", Produced.with(...));
```

## When this might be a false positive

- `peek` used purely for logging at a controlled rate (idempotent observation, not side effect). Lower severity.

## Detection strategy

- Bytecode: any class implementing `ValueMapper`, `ValueMapperWithKey`, `ForeachAction`, `Predicate`, `KeyValueMapper`, or a lambda inside `KStream.mapValues/filter/foreach/peek/transformValues/processValues`, where the body calls:
  - `INVOKE*` to known I/O classes (`java/net/`, `java/sql/`, `org/springframework/web/client/RestTemplate`, `okhttp3/`, `org/apache/http/`, JDBC `Connection`, `OkHttpClient`).
  - File I/O (`java/io/FileOutputStream`, `java/nio/file/Files`).
  - Mutating statics.
- Confidence: MEDIUM — logger calls and metrics counters are often false positives. Whitelist common observability classes (SLF4J `Logger`, Micrometer `Counter`).

## References

- Confluent — DSL side-effects pitfalls: https://docs.confluent.io/platform/current/streams/developer-guide/dsl-api.html
- Matthias J. Sax — Pure functions in Kafka Streams: https://www.confluent.io/blog/transactions-apache-kafka/
