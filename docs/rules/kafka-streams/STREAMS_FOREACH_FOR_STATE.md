# STREAMS_FOREACH_FOR_STATE

**Severity**: WARNING
**Confidence**: MEDIUM
**Detection**: bytecode
**Tagline**: `foreach` is a terminal sink. Don't put your state machine in there.

## TL;DR

The linter flags `KStream#foreach(ForeachAction)` whose lambda touches a Streams state store via `KafkaStreams.store(...)`, or which writes to external state that should be a Kafka topic. `foreach` is a terminal operator — no downstream, no exactly-once, no recovery. Anything stateful belongs in the Processor API.

## What's happening (the mechanism)

`foreach` (and `peek`) is for side-effect observation: emit metrics, log, fire-and-forget. It runs on the StreamThread, blocks the topology, isn't wrapped in the EOS transaction, and isn't replayable. If you put state logic in it:

- The "state" lives outside Streams — external map, JDBC table, etc. Restoration doesn't replay it.
- Under EOS, the side effect is not part of the transaction. On rollback, the side effect persists; on commit, it might not have happened yet.
- There's no `Materialized` → no changelog → no fault tolerance.

If you need state, use `processValues`/`process` with a state store registered via `Stores.keyValueStoreBuilder` and accessed through `context.getStateStore("name")`. The store is then changelog-backed and EOS-safe.

## Operational impact

- State drifts between Streams instances after partition reassignment (no restoration mechanism).
- EOS guarantees lie — your "exactly-once" pipeline has at-least-once side effects.
- Tests pass (single-instance, no rebalance) and prod fails (rebalances trigger drift).
- Side-channel I/O blocks the StreamThread → throughput tanks.

## How to fix

```java
// BAD — counter in a Map, not a state store
private static final Map<String, Long> COUNTS = new ConcurrentHashMap<>();
stream.foreach((k, v) -> COUNTS.merge(k, 1L, Long::sum));

// GOOD — Processor API + state store
StoreBuilder<KeyValueStore<String, Long>> storeBuilder = Stores.keyValueStoreBuilder(
    Stores.persistentKeyValueStore("counts"),
    Serdes.String(), Serdes.Long());
builder.addStateStore(storeBuilder);

stream.process(() -> new Processor<String, V, Void, Void>() {
    private KeyValueStore<String, Long> store;
    public void init(ProcessorContext<Void, Void> ctx) {
        this.store = ctx.getStateStore("counts");
    }
    public void process(Record<String, V> record) {
        Long current = store.get(record.key());
        store.put(record.key(), (current == null ? 0L : current) + 1L);
    }
}, "counts");

// Or just use the DSL:
stream.groupByKey()
      .count(Materialized.as("counts"));
```

## When this might be a false positive

- `foreach` used purely for logging, metrics, or alerting — no shared mutable state. OK.
- `peek` for the same.

## Detection strategy

- Bytecode: `INVOKEINTERFACE org/apache/kafka/streams/kstream/KStream.foreach (Lorg/apache/kafka/streams/kstream/ForeachAction;)V` where the lambda body:
  - Writes to a mutable static field, or
  - Calls JDBC / HTTP / file I/O methods (overlap with `STREAMS_KTABLE_FILTER_SIDE_EFFECT`), or
  - Calls `KafkaStreams.store(...)` (very rare; almost always wrong).
- Confidence: MEDIUM.

## References

- KStream.foreach javadoc: https://kafka.apache.org/40/javadoc/org/apache/kafka/streams/kstream/KStream.html#foreach-org.apache.kafka.streams.kstream.ForeachAction-
- Confluent — Processor API: https://docs.confluent.io/platform/current/streams/developer-guide/processor-api.html
