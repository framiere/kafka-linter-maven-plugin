# STREAMS_JOIN_DEFAULT_SERDES

**Severity**: WARNING
**Confidence**: MEDIUM
**Detection**: dsl-chain
**Tagline**: A join without explicit Serdes is a join with implicit assumptions.

## TL;DR

The linter flags `KStream#join` / `leftJoin` / `outerJoin` and `KTable.join(KTable, ...)` foreign-key joins called without an explicit `Joined` / `StreamJoined` / `Materialized` carrying key & value Serdes. The fallback is `default.key.serde` / `default.value.serde`, which silently misuse the wrong type if either stream has heterogeneous values.

## What's happening (the mechanism)

A stream-stream join needs to know how to serialize:
- The join key (for the internal join-store).
- The left value.
- The right value.

If you call `left.join(right, valueJoiner, joinWindows)` without a `StreamJoined`, Streams uses `default.key.serde` for the key and `default.value.serde` for both values. That works in toy examples (`Serdes.String()` everywhere) but breaks the moment the two streams have different value types — for example a `String`-valued left and an `Order`-valued right. You get `ClassCastException` deep in the join store, far from the source.

Foreign-key KTable joins (`KTable.join(KTable, foreignKeyExtractor, valueJoiner)`) have the same problem amplified: they create *two* internal subscription topics + a result topic; mis-Serded data is silently corrupted in transit.

## Operational impact

- `ClassCastException` from inside the join's `KeyValueStore` writer. Stack trace points to `Serdes$WrapperSerde.serialize` — confusing because the trace doesn't mention which join.
- For foreign-key joins, the response topic carries garbage bytes; the downstream operator deserializes wrong values.
- Tests pass (because they use a single Serde) and prod fails (because the value types diverge).

## How to fix

```java
// BAD — implicit defaults
left.join(right,
    (l, r) -> l + r,
    JoinWindows.ofTimeDifferenceWithNoGrace(Duration.ofMinutes(5)));

// GOOD — explicit StreamJoined
left.join(right,
    (l, r) -> l + r,
    JoinWindows.ofTimeDifferenceAndGrace(Duration.ofMinutes(5), Duration.ofSeconds(30)),
    StreamJoined.<String, Order, Payment>with(Serdes.String(), orderSerde, paymentSerde)
        .withName("orders-payments")
        .withStoreName("orders-payments-store"));

// BAD — foreign-key KTable join without serdes
ktable1.join(ktable2, foreignKeyExtractor, valueJoiner);

// GOOD
ktable1.join(ktable2,
    foreignKeyExtractor,
    valueJoiner,
    TableJoined.<String, String>as("orders-customers-join"),
    Materialized.<String, EnrichedOrder, KeyValueStore<Bytes, byte[]>>as("orders-customers-store")
        .withKeySerde(Serdes.String())
        .withValueSerde(enrichedOrderSerde));
```

## When this might be a false positive

- Truly homogeneous-type joins (both sides `String` values) where the default Serdes work. Still better practice to be explicit.

## Detection strategy

- Bytecode: `INVOKEINTERFACE org/apache/kafka/streams/kstream/KStream.join/leftJoin/outerJoin` whose overload does NOT take a `StreamJoined`/`Joined` argument.
- Same for `KTable.join` foreign-key overloads without a `TableJoined` + `Materialized`.
- Confidence: MEDIUM — works if defaults align, but fragile.

## References

- StreamJoined javadoc: https://kafka.apache.org/40/javadoc/org/apache/kafka/streams/kstream/StreamJoined.html
- KIP-213 — Foreign Key Join in KTable: https://cwiki.apache.org/confluence/display/KAFKA/KIP-213+Support+non-key+joining+in+KTable
- Confluent — DSL operators: joining: https://docs.confluent.io/platform/current/streams/developer-guide/dsl-api.html#joining
