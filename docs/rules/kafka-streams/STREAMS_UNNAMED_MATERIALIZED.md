# STREAMS_UNNAMED_MATERIALIZED

**Severity**: ERROR
**Confidence**: HIGH
**Detection**: dsl-chain
**Tagline**: An unnamed store is a store you'll lose on the next deploy.

## TL;DR

The linter flags stateful DSL operators (`count`, `aggregate`, `reduce`, `windowedBy().count`, `KTable.toTable`, joins) that don't pass a `Materialized.as("explicit-name")` or `Named.as(...)`, because the auto-generated name is positional and shifts the moment you touch the topology.

## What's happening (the mechanism)

In the Kafka Streams DSL, every operator gets a name. If you don't provide one, Streams generates one positionally — e.g. `KSTREAM-AGGREGATE-STATE-STORE-0000000003`. The trailing number is the operator's index in the topology DAG, padded to 10 characters. Internal topics derived from that name follow the pattern `<application.id>-<store-name>-changelog` and `<application.id>-<store-name>-repartition`.

The moment you insert a `filter`, a `peek`, a `mapValues` before that aggregation, every downstream index shifts. The store name changes from `...0000000003` to `...0000000004`. Kafka Streams sees this as a brand-new store: it creates a new changelog topic, starts restoring from offset 0 (empty), and the old changelog topic becomes orphaned. All accumulated state — counts, joins, aggregates — is lost. Silently.

Since Kafka Streams 3.7 there's `StreamsConfig.ENSURE_EXPLICIT_INTERNAL_RESOURCE_NAMING_CONFIG`: when `true`, the application refuses to start if any internal resource uses an auto-generated name. Use it as a belt to this rule's suspenders.

## Operational impact

- After a code change, you see `kafka.streams:type=stream-task-metrics,task-id=...:restore-records-rate` spike across every task. State restoration time goes from seconds to hours.
- Aggregation results emit "wrong" numbers — really, they're correct for the empty new store and the user is hitting the new instance.
- Old changelog topics linger as zombies (`__consumer_offsets` shows no group reading them; bytes never decrease). Quota and storage cost climbs.
- Cannot do a rolling upgrade safely — the new pod and the old pod disagree about which store is canonical.
- Interactive queries against `store("counts", ...)` return `InvalidStateStoreException` because the store name changed.

## How to fix

Always pass a stable name.

```java
// BAD — store name is positional, breaks on topology edit
stream.groupByKey()
      .count();

// BAD — even with a Materialized, the name is auto-generated
stream.groupByKey()
      .count(Materialized.with(Serdes.String(), Serdes.Long()));

// GOOD — explicit, stable, deploy-safe
stream.groupByKey()
      .count(Materialized.<String, Long, KeyValueStore<Bytes, byte[]>>as("orders-per-customer")
          .withKeySerde(Serdes.String())
          .withValueSerde(Serdes.Long()));

// GOOD — for joins, name the StreamJoined / Joined
left.join(right,
    (l, r) -> l + r,
    JoinWindows.ofTimeDifferenceAndGrace(Duration.ofMinutes(5), Duration.ofMinutes(1)),
    StreamJoined.<String, String, String>as("orders-payments-join")
        .withName("orders-payments")
        .withStoreName("orders-payments-store"));
```

For belt-and-suspenders, set:
```properties
ensure.explicit.internal.resource.naming=true
```

## When this might be a false positive

- A `count()` whose results never need to survive a redeploy (truly ephemeral counters that reset on every restart). Rare in practice.
- A test topology built with `TopologyTestDriver` where state is recreated per test.

## Detection strategy

- DSL chain: any of these called without a `Materialized.as(String)` / `Named.as(String)` / `StreamJoined.as(String)`:
  - `KGroupedStream#count()`, `count(Named)`, `count(Materialized)` where `Materialized` was built via `Materialized.with(...)` (no `.as`)
  - `KGroupedStream#reduce`, `aggregate`
  - `TimeWindowedKStream/SessionWindowedKStream#count|reduce|aggregate`
  - `KStream#join|leftJoin|outerJoin` (stream-stream) without `StreamJoined.as(...)` or `.withStoreName(...)`
  - `KTable#join|leftJoin|outerJoin` without `Materialized.as(...)`
  - `KTable#toTable` without `Named.as(...)`
- ASM: at the call site of `count()/aggregate()/reduce()`, walk back to the constructed `Materialized` instance; if it was obtained from `Materialized.with(...)` (not `as(...)`) the store name is generated. HIGH confidence.
- Config: presence of `ensure.explicit.internal.resource.naming=true` suppresses this rule.

## References

- Confluent — Naming stateful operations in Kafka Streams: https://developer.confluent.io/tutorials/naming-stateful-operations/kstreams.html
- Confluent — Naming DSL topologies: https://docs.confluent.io/platform/current/streams/developer-guide/dsl-topology-naming.html
- `Materialized` javadoc: https://kafka.apache.org/40/javadoc/org/apache/kafka/streams/kstream/Materialized.html

## Cross-reference

Also recommended by Confluent agent-skills — see kafka-streams-programming/references/topology-patterns.md § Naming stateful resources, kafka-streams-programming/SKILL.md § Invariant Checklist #4.
