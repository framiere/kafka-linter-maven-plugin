# CONSUMER_AUTO_OFFSET_RESET_NONE_UNHANDLED

**Severity**: WARNING
**Confidence**: MEDIUM
**Detection**: bytecode
**Tagline**: auto.offset.reset=none is great — if you handle the exception.

## TL;DR

The linter flags consumers with `auto.offset.reset=none` whose code never catches `NoOffsetForPartitionException`. The setting is correct for forcing an explicit decision, but useless if the exception just crashes the consumer.

## What's happening (the mechanism)

`auto.offset.reset=none` causes the first `poll()` on a partition with no committed offset to throw `NoOffsetForPartitionException`. This is intentional — it forces the operator to choose explicitly between `seekToBeginning`, `seekToEnd`, or `seek(specific offset)` before resuming.

If the application does not catch the exception, the consumer thread dies, the group rebalances, the new owner immediately hits the same exception, and the group enters a rebalance crashloop.

## Operational impact

- Consumer crashloop on first start of a new group.
- Rebalance storm: every restart triggers a rebalance, every rebalance kills another consumer, group never reaches stable state.
- Symptom: `kafka.consumer:type=consumer-coordinator-metrics,client-id=*` `rebalance-rate-per-hour` very high; `assigned-partitions` flapping.

## How to fix

```java
// BAD
props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "none");
KafkaConsumer<K, V> c = new KafkaConsumer<>(props);
c.subscribe(List.of("topic"));
while (running) {
    c.poll(Duration.ofSeconds(1)); // NoOffsetForPartitionException → thread death
}

// GOOD
props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "none");
KafkaConsumer<K, V> c = new KafkaConsumer<>(props);
c.subscribe(List.of("topic"));
while (running) {
    try {
        c.poll(Duration.ofSeconds(1));
    } catch (NoOffsetForPartitionException e) {
        // Explicit operator policy: seek to beginning, or fail loud, or
        // alert + manual reset.
        c.seekToBeginning(e.partitions());
    }
}
```

## When this might be a false positive

- Frameworks (Spring, Quarkus) that install their own `ErrorHandlingDeserializer` / `ConsumerInterceptor` that handles the exception out of the application's view. Confidence drops to LOW when the consumer is consumed by a framework wrapper.

## Detection strategy

- Bytecode: locate methods where `Properties.put("auto.offset.reset", "none")` is set on a `Properties` later passed to `new KafkaConsumer(...)`; scan the resulting consumer's poll loop for `catch NoOffsetForPartitionException`. If none, flag.
- MEDIUM: cross-method analysis is brittle. Suppress if the consumer object escapes the method.

## References

- Apache Kafka consumer configs — `auto.offset.reset`: https://kafka.apache.org/documentation/#consumerconfigs_auto.offset.reset
- NoOffsetForPartitionException javadoc: https://kafka.apache.org/40/javadoc/org/apache/kafka/clients/consumer/NoOffsetForPartitionException.html
