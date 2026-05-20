# SPRING_EHD_LISTENER_NULL_NOT_CHECKED

**Severity**: WARNING
**Confidence**: MEDIUM
**Detection**: bytecode + config-file
**Tagline**: `ErrorHandlingDeserializer` returns `null` for poison pills — and your listener processes them as legitimate records.

## TL;DR

The linter flags `@KafkaListener` methods on consumers configured with `ErrorHandlingDeserializer` where the body does not check for `null` value (or, for batch listeners, does not inspect the per-record `springDeserializerExceptionValue` / `springDeserializerExceptionKey` headers). The wrapper signals a poison pill by setting the value to `null` and stashing the `DeserializationException` in a header — if you treat that null as a regular record, you'll either NPE downstream or, worse, treat "missing data" as semantically valid.

## The setup

Team correctly wires `ErrorHandlingDeserializer` over `JsonDeserializer<Order>` (fixing `SPRING_NO_ERROR_HANDLING_DESERIALIZER`). They then write `@KafkaListener void handle(Order order) { service.process(order); }`. First poison pill arrives, `order` is `null`, `service.process(null)` NPEs — *or worse*, accepts it and inserts a row with all null fields into a DB.

## What's actually happening

`ErrorHandlingDeserializer` (wrapper around your real deserializer) does:

1. Try delegate.
2. If delegate succeeds → return the deserialized value.
3. If delegate throws → return `null` and add two record headers:
   - `springDeserializerExceptionValue` (constant: `SerializationUtils.VALUE_DESERIALIZER_EXCEPTION_HEADER`) for value-side failures
   - `springDeserializerExceptionKey` (constant: `SerializationUtils.KEY_DESERIALIZER_EXCEPTION_HEADER`) for key-side failures
   - The header value is a serialized `DeserializationException` carrying the cause and the original bytes.

For **single-record listeners**, the container honors a built-in rule: if the value is `null` AND the deserialization-exception header is present AND `checkDeserExWhenValueNull=true` (default true *only when* `ErrorHandlingDeserializer` is detected — important CVE-2023-34040 caveat), it raises a `DeserializationException` *to the error handler*, not to the listener. **So the listener body should normally not see the null at all.**

But: this works only when the container can detect the configuration. For **batch listeners** (`@KafkaListener(... batch = "true")`), the container does **not** filter out the failed records — they appear in the `List<Order>` as `null` entries, and the listener must inspect headers per-record and decide what to do.

For **manual-ack listeners with `ConsumerRecord<K, V>`** signatures, the value can also surface as `null` to the listener directly.

## Why this is subtle

- Most documentation examples show single-record listeners — the container handles it for you, so the pattern looks safe.
- Switching to batch (for performance) silently changes the contract: `null` entries appear; per-record inspection is required.
- Pre-CVE-2023-34040 (Spring Kafka 3.0.9 and earlier), the container behavior was different and some `null`-handling code was written assuming the listener saw nulls.
- A `null` value can pattern-match other "missing data" scenarios (tombstones on compacted topics, especially). Treating poison pills and tombstones identically is a real bug.

## Operational impact

- NPE downstream → exception escapes → record gets retried + DLT (acceptable, but the listener should have been simpler).
- Silent insertion of all-null rows in a DB.
- Counters for "processed records" inflated by poison-pill count.
- Tombstone records on compacted topics — which legitimately have `null` value — accidentally treated as poison pills (or vice versa).

## Failure scenarios (walkthrough)

1. **The batch consumer.** Listener is `@KafkaListener(batch="true") void handle(List<Order> orders)`. A poison pill in a batch of 100 surfaces as one `null` in the list. The listener iterates and calls `service.process(order)` — the first `null` NPEs and the **entire batch** is retried because batch listeners are retried as a unit. Six healthy records get reprocessed once per poison pill.

2. **The tombstone confusion.** Topic is compacted; `null` value records signal deletion. Consumer configures `ErrorHandlingDeserializer`. Listener: `if (order == null) deleteFromCache(record.key()); else upsertIntoCache(order);`. On a poison pill, `deleteFromCache` runs — wrong semantics, real data deleted.

## How to fix

For **single-record listeners** with `value=null` semantically valid (tombstones), distinguish poison pills from tombstones:

```java
@KafkaListener(topics = "orders")
public void handle(ConsumerRecord<String, Order> record) {
    if (record.value() == null) {
        Header excHeader = record.headers().lastHeader(
            SerializationUtils.VALUE_DESERIALIZER_EXCEPTION_HEADER);
        if (excHeader != null) {
            // poison pill — let the container's error handler take it
            throw SerializationUtils.byteArrayToDeserializationException(log, excHeader);
        }
        // genuine tombstone
        cache.delete(record.key());
        return;
    }
    cache.upsert(record.key(), record.value());
}
```

For **batch listeners**, inspect each record:

```java
@KafkaListener(topics = "orders", batch = "true")
public void handle(List<ConsumerRecord<String, Order>> records) {
    for (int i = 0; i < records.size(); i++) {
        ConsumerRecord<String, Order> rec = records.get(i);
        if (rec.value() == null) {
            DeserializationException de = SerializationUtils.getExceptionFromHeader(rec,
                SerializationUtils.VALUE_DESERIALIZER_EXCEPTION_HEADER, log);
            if (de != null) {
                throw new BatchListenerFailedException("Deserialization", de, i);
            }
            // tombstone
            continue;
        }
        service.process(rec.value());
    }
}
```

`BatchListenerFailedException` with the failing index lets `DefaultErrorHandler` recover *just that one* record to DLT and continue processing the rest.

## Consult a friend?

> Slow down. The interaction between `ErrorHandlingDeserializer`, container ack mode, `checkDeserExWhenValueNull`, and tombstones on compacted topics is genuinely complex.
> 1. If your topic is compacted, you must distinguish tombstones from poison pills using the header.
> 2. Check your Spring Kafka version: pre-3.0.10, `checkDeserExWhenValueNull` behavior changed (CVE-2023-34040). Upgrade.
> 3. Batch + transactional + `ErrorHandlingDeserializer` is a four-way interaction — read the docs cover-to-cover before relying on it.

## When this might be a false positive

- Single-record listener with type-safe value parameter (`@KafkaListener void handle(Order order)`) — Spring container filters out nulls before calling user method, so the body cannot see a poison pill.
- Custom container factory that disables `checkDeserExWhenValueNull`.
- Test code with mock deserializers.

## Detection strategy

- config-file: detect `ErrorHandlingDeserializer` wrapping in `spring.kafka.consumer.value-deserializer` or the corresponding `spring.deserializer.value.delegate.class` property.
- bytecode: visit `@KafkaListener` methods on classes in the same project.
  - If the method signature uses `ConsumerRecord<K, V>` directly or `List<ConsumerRecord<K, V>>` (batch), scan for a header check using `lastHeader("springDeserializerException*")` or `SerializationUtils.VALUE_DESERIALIZER_EXCEPTION_HEADER`. Flag if absent.
  - If the method takes `List<V>` (batch with raw type), flag — the listener can't tell tombstones from poison pills.
- Confidence MEDIUM — single-record listeners with typed value are usually safe; batch always needs scrutiny.

## References

- Spring Kafka — `ErrorHandlingDeserializer`: https://docs.spring.io/spring-kafka/reference/kafka/serdes.html
- `SerializationUtils.VALUE_DESERIALIZER_EXCEPTION_HEADER`: https://docs.spring.io/spring-kafka/api/org/springframework/kafka/support/serializer/SerializationUtils.html
- CVE-2023-34040 — deserialization vulnerability and `checkDeserExWhenValueNull`: https://spring.io/security/cve-2023-34040/
- `BatchListenerFailedException`: https://docs.spring.io/spring-kafka/api/org/springframework/kafka/listener/BatchListenerFailedException.html
