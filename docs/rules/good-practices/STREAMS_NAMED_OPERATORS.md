# STREAMS_NAMED_OPERATORS

**Severity**: WARNING
**Confidence**: HIGH
**Detection**: bytecode
**Tagline**: Every stateful operator gets a stable name, or your topology is one edit away from data loss.

## TL;DR

The good-practice form of
[STREAMS_UNNAMED_MATERIALIZED](../kafka-streams/STREAMS_UNNAMED_MATERIALIZED.md).
Every stateful DSL operator (`count`, `aggregate`, `reduce`, `join`,
windowed variants, `KTable.toTable`) must be given an explicit name via
`Materialized.as("...")`, `Named.as("...")`, or `StreamJoined.as("...")`.

## Cross-reference

See [STREAMS_UNNAMED_MATERIALIZED](../kafka-streams/STREAMS_UNNAMED_MATERIALIZED.md)
for the mechanism (positional auto-generated names shift on topology
edits, orphaning state).

## What the good-practice adds

- The positive practice is to also set
  `ensure.explicit.internal.resource.naming=true` (Kafka 3.7+) so the
  app refuses to start if any internal resource has an auto-generated
  name. Belt and braces.
- A naming convention discipline: name operators by their **business
  meaning**, not their position. `orders-per-customer`, not
  `aggregate-1`. That way the JMX metrics, changelog topic names, and
  state store directories all read at a glance.

```java
// yes — named operator
stream.groupByKey()
      .count(Materialized.<String, Long, KeyValueStore<Bytes, byte[]>>as(
          "orders-per-customer"));
```

```properties
# yes — Streams config
ensure.explicit.internal.resource.naming=true
```

## References

See the linked anti-pattern doc and the Confluent naming tutorial.
