# STREAMS_TRANSFORM_DEPRECATED

**Severity**: WARNING
**Confidence**: HIGH
**Detection**: bytecode
**Tagline**: Transformers are last decade's Processor. Use `process` / `processValues`.

## TL;DR

The linter flags `KStream#transform`, `flatTransform`, `transformValues`, `flatTransformValues`, and the corresponding `Transformer` / `ValueTransformer` / `*Supplier` interfaces — all deprecated by KIP-820 in favor of the strongly-typed `process(ProcessorSupplier)` / `processValues(FixedKeyProcessorSupplier)`.

## What's happening (the mechanism)

The legacy `Transformer<K, V, R>` API exposed the old weakly-typed `ProcessorContext` (`forward(Object, Object)`) and was the only DSL escape hatch for custom processing with state. KIP-478 introduced a strongly-typed `Processor<KIn, VIn, KOut, VOut>` plus `Record<K, V>`. KIP-820 then plumbed that new Processor API into the DSL via `KStream#process(ProcessorSupplier)` (key-changing, returns `KStream<KOut, VOut>`) and `KStream#processValues(FixedKeyProcessorSupplier)` (key-preserving, returns `KStream<K, VOut>`).

Why `processValues` matters: when you only modify values, the downstream stream is still co-partitioned by the original key. With the old `transformValues` this was a convention you had to trust; with `processValues` the `FixedKeyProcessorContext` *makes it impossible* to call `forward` with a new key. The compiler enforces correctness.

The deprecation timeline:
- 3.3 (KIP-820): `KStream#transform*` methods deprecated.
- KAFKA-16339 / KIP-1070: `Transformer`, `TransformerSupplier`, `ValueTransformer*` interfaces deprecated; planned removal in a future major (5.0 timeframe).

## Operational impact

- Compile warnings today, build failures on the next major version bump.
- Code that returns null from `transform` to "drop" a record is fragile — `process` callers use `forward()` explicitly or not at all, which is clearer.
- `transformValues` users sometimes silently *did* change the key (via mutable shared state) and broke co-partitioning. `processValues` makes that impossible.

## How to fix

```java
// BAD — deprecated transform
stream.transformValues(() -> new ValueTransformer<Order, String>() {
    private ProcessorContext ctx;
    public void init(ProcessorContext ctx) { this.ctx = ctx; }
    public String transform(Order o) { return o.toJson(); }
    public void close() {}
}, "my-store");

// GOOD — processValues with FixedKeyProcessor
stream.processValues(() -> new FixedKeyProcessor<String, Order, String>() {
    private FixedKeyProcessorContext<String, String> ctx;
    public void init(FixedKeyProcessorContext<String, String> ctx) { this.ctx = ctx; }
    public void process(FixedKeyRecord<String, Order> record) {
        ctx.forward(record.withValue(record.value().toJson()));
    }
    public void close() {}
}, Named.as("order-to-json"), "my-store");

// BAD — deprecated transform with key change
stream.transform(() -> new Transformer<String, Order, KeyValue<String, Event>>() { ... });

// GOOD — process returns KStream<KOut, VOut>
KStream<String, Event> events = stream.process(
    () -> new ContextualProcessor<String, Order, String, Event>() {
        public void process(Record<String, Order> record) {
            context().forward(new Record<>(record.key(), toEvent(record.value()), record.timestamp()));
        }
    },
    Named.as("order-to-event"),
    "my-store");
```

## When this might be a false positive

- Code targeting Kafka 2.x where `process*` overloads don't exist yet. If you're stuck on 2.x for other reasons, suppress per-module.

## Detection strategy

- Bytecode: any `INVOKEINTERFACE` to:
  - `org/apache/kafka/streams/kstream/KStream.transform`
  - `org/apache/kafka/streams/kstream/KStream.flatTransform`
  - `org/apache/kafka/streams/kstream/KStream.transformValues`
  - `org/apache/kafka/streams/kstream/KStream.flatTransformValues`
- Or any class implementing:
  - `org/apache/kafka/streams/kstream/Transformer`
  - `org/apache/kafka/streams/kstream/TransformerSupplier`
  - `org/apache/kafka/streams/kstream/ValueTransformer`
  - `org/apache/kafka/streams/kstream/ValueTransformerSupplier`
  - `org/apache/kafka/streams/kstream/ValueTransformerWithKey`
  - `org/apache/kafka/streams/kstream/ValueTransformerWithKeySupplier`
- Confidence: HIGH — all unconditionally deprecated.

## References

- KIP-820 — Extend KStream process with new Processor API: https://cwiki.apache.org/confluence/display/KAFKA/KIP-820:+Extend+KStream+process+with+new+Processor+API
- KIP-478 — Strongly typed Processor API: https://cwiki.apache.org/confluence/display/KAFKA/KIP-478+-+Strongly+typed+Processor+API
- KIP-1070 — Deprecate Transformer interfaces: https://cwiki.apache.org/confluence/display/KAFKA/KIP-1070:+deprecate+MockProcessorContext
- KAFKA-16339: https://issues.apache.org/jira/browse/KAFKA-16339
