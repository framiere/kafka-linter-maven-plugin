# QK_PRODUCER_NO_KEY

**Severity**: WARNING
**Confidence**: MEDIUM
**Detection**: bytecode
**Tagline**: No key, no partition affinity, no ordering. Fine for fire-hoses, fatal for streams.

## TL;DR

Producing `Message<Order>` or `KafkaRecord<K,V>` without setting a key delegates partition selection to the round-robin / sticky partitioner. For any payload with an entity identity (order ID, user ID, account number), this scatters related records across partitions and breaks per-key ordering.

## What's happening (the mechanism)

The Kafka producer routes records by:
1. Explicit `partition` if set.
2. Hash of the key if set.
3. Sticky / round-robin partitioner if no key.

For Streams / event-sourced architectures, per-key ordering is the foundational invariant. If two updates to order `42` land on different partitions, the consumer can process them in arbitrary order across partitions.

Common bytecode shapes hitting this:

```java
// BAD — no key
emitter.send(order);

// BAD — Message without key metadata
emitter.send(Message.of(order));

// BAD — KafkaRecord with explicit null key
emitter.send(KafkaRecord.of(null, order));
```

## Operational impact

- Stream joins produce wrong results.
- KTable updates apply out of order under load.
- Materialized state drifts from source-of-truth.
- Almost impossible to detect from metrics — looks like a normal producer.

## How to fix

```java
// GOOD — explicit key
emitter.send(KafkaRecord.of(order.getId(), order));

// GOOD — propagate key from incoming Record
@Incoming("orders-raw")
@Outgoing("orders-normalized")
public Record<String, Order> normalize(Record<String, Order> in) {
    return Record.of(in.key(), in.value().normalize());
}

// GOOD — propagate-record-key in config
// (then producing payload only is fine; key comes from incoming)
```

```properties
mp.messaging.outgoing.orders-out.propagate-record-key=true
```

## When this might be a false positive

- Fan-out topics where any partition is acceptable (load distribution, no per-entity ordering required).
- Telemetry / metrics topics keyed by timestamp downstream.
- Producer with `propagate-record-key=true` and the upstream is providing the key — bytecode in this method won't show a key.

## Detection strategy

- Bytecode: `INVOKEVIRTUAL Emitter.send` / `MutinyEmitter.send` with a non-`Message`/`KafkaRecord` argument (payload only), OR a `Message.of(payload)` / `KafkaRecord.of(null, ...)` shape.
- Suppress when config sets `propagate-record-key=true` on the same channel.
- Confidence: MEDIUM — requires payload type heuristics.

## References

- https://smallrye.io/smallrye-reactive-messaging/latest/kafka/writing-kafka-records/
- https://kafka.apache.org/documentation/#producerconfigs_partitioner.class
