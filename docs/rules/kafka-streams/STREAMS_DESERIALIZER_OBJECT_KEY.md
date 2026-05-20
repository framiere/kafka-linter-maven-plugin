# STREAMS_DESERIALIZER_OBJECT_KEY

**Severity**: WARNING
**Confidence**: MEDIUM
**Detection**: bytecode
**Tagline**: JSON keys don't hash the same on every JVM. Pick a stable representation.

## TL;DR

The linter flags `Serdes`/`Serializer` for record keys where the serialized form is not deterministic — typically JSON-encoded objects, or `Serdes.serdeFrom(serializer, deserializer)` with mismatched types. Keys must hash to the same partition byte-for-byte; non-deterministic encoding breaks co-partitioning silently.

## What's happening (the mechanism)

Kafka partitions by `hash(serializedKey) % numPartitions`. For Streams operations that require co-partitioning (joins, aggregations), two records with semantically equal keys must produce the same serialized bytes.

JSON breaks this:
- Property order isn't guaranteed across `ObjectMapper` configurations and JVMs.
- Whitespace, escape conventions, number formatting differ.
- Locale-dependent number formatting (decimal separator) varies.

So `{"customerId":"C1","tenant":"T1"}` from producer A and `{"tenant":"T1","customerId":"C1"}` from producer B partition to different partitions and never join.

A second flavor: `Serdes.serdeFrom(StringSerializer, LongDeserializer)` — Java's type system doesn't catch the mismatch, and `ClassCastException` shows up at the first record. The mismatched pair also silently works for round-trips if the user later forces the read side via casts.

## Operational impact

- Joins never produce a match. The downstream topic has zero records. `kafka.streams:type=stream-state-store-metrics,store-id=...,name=*-store:put-rate` for the join store is normal on both sides but the output is empty.
- Aggregations show "duplicate-but-different" keys (same semantic identity, different bytes) — counts are wrong.
- ClassCastException at runtime for mismatched serdeFrom pairings — usually on the first record.

## How to fix

```java
// BAD — JSON-encoded object as key
producer.send(new ProducerRecord<>(
    "orders", objectMapper.writeValueAsString(new CompositeKey(custId, tenant)), payload));

// GOOD — a deterministic single-string composite
producer.send(new ProducerRecord<>(
    "orders", tenant + "|" + custId, payload));

// GOOD — Avro / Protobuf with a stable schema (binary determinism)
producer.send(new ProducerRecord<>("orders", avroKey, payload));

// BAD — type mismatch
Serde<String> bad = Serdes.serdeFrom(
    new StringSerializer(), new LongDeserializer());

// GOOD
Serde<String> good = Serdes.String();
```

If you must use JSON for keys, enforce canonical JSON (sorted keys, no whitespace) via your own `Serializer<T>` that wraps Jackson with `SerializationFeature.ORDER_MAP_ENTRIES_BY_KEYS`.

## When this might be a false positive

- Single-partition topics where partitioning doesn't matter.
- Streams apps that never co-partition (no joins, no aggregations) — though it's still latent risk.

## Detection strategy

- Bytecode: `INVOKESTATIC org/apache/kafka/common/serialization/Serdes.serdeFrom (Ljava/lang/Class;Ljava/lang/Class;)Lorg/apache/kafka/common/serialization/Serde;` where the two `Class` arguments have different generic erasure (hard to prove statically; MEDIUM confidence).
- Bytecode: a `KStream.selectKey`/`map`/`groupBy` lambda whose return value is the result of `ObjectMapper.writeValueAsString(...)` or `gson.toJson(...)`. HIGH confidence "JSON key".
- Confidence: MEDIUM.

## References

- Serdes javadoc: https://kafka.apache.org/40/javadoc/org/apache/kafka/common/serialization/Serdes.html
- Confluent — Schema Registry & Avro keys: https://docs.confluent.io/platform/current/schema-registry/index.html
- RFC 8785 — JSON Canonicalization Scheme: https://datatracker.ietf.org/doc/rfc8785/
