# QK_MISSING_VALUE_DESERIALIZER

**Severity**: ERROR
**Confidence**: HIGH
**Detection**: config-file
**Tagline**: No value.deserializer? Hope you wanted Strings.

## TL;DR

`mp.messaging.incoming.<channel>.value.deserializer` is mandatory in SmallRye's Kafka connector. If autodetection doesn't pick a serde for your payload type (multiple Avro serdes, non-standard generics, custom DTOs without an `ObjectMapperSerializer` subclass), the connector either fails at startup or — worse — silently picks the wrong one.

## What's happening (the mechanism)

SmallRye autodetection works for: primitives, `String`, `UUID`, `ByteBuffer`, Vert.x types, Avro-generated classes (when exactly one serde is on the classpath), and subclasses of `ObjectMapperSerializer` / `JsonbSerializer`. Outside those cases:

- With Quarkus 2.x+, JSON serializer generation runs and may auto-create a serializer (controlled by `quarkus.messaging.kafka.serializer-generation.enabled`, default `true`).
- If generation fails or is disabled, the build fails at compile time.
- If multiple compatible serdes are on the classpath (e.g., both Apicurio and Confluent Avro), autodetection refuses to pick — startup fails or picks ambiguously.

The footgun: a developer copies a working example using `String`, swaps the payload to a DTO, and doesn't notice that the implicit `StringDeserializer` is now silently producing toString() on the bytes.

## Operational impact

- Cleanest case: startup fails with `KafkaException: Missing value deserializer for channel orders`.
- Worst case: `value.deserializer=StringDeserializer` is inherited from defaults, every record arrives as a String, downstream code throws `ClassCastException` deep in business logic.
- Native-image builds: serializer-generation can silently fail at runtime if reflection metadata is missing.

## How to fix

```properties
# GOOD — be explicit
mp.messaging.incoming.orders.value.deserializer=io.quarkiverse.kafka.serializer.OrderDeserializer
# or for JSON
mp.messaging.incoming.orders.value.deserializer=io.quarkus.kafka.client.serialization.ObjectMapperDeserializer
mp.messaging.incoming.orders.value.type=com.example.Order
```

For Apicurio / Confluent:

```properties
mp.messaging.incoming.orders.value.deserializer=io.apicurio.registry.serde.avro.AvroKafkaDeserializer
mp.messaging.incoming.orders.apicurio.registry.url=http://schema-registry:8080/apis/registry/v2
```

## When this might be a false positive

- Single Avro serde on the classpath with autodetection enabled → genuinely fine.
- `quarkus.messaging.kafka.serializer-generation.enabled=true` and the payload is a vanilla POJO — Quarkus generates the serde at build time.

The rule should only fire when (a) autodetection signals (single Avro, primitive types, generated subclass) are absent AND (b) `value.deserializer` is unset.

## Detection strategy

- Config: `mp.messaging.incoming.<channel>.connector=smallrye-kafka` AND `value.deserializer` unset.
- Suppress if `quarkus.messaging.kafka.serializer-generation.enabled` is true and a `value.type` is set.
- Confidence: HIGH for the explicit-mandatory case; MEDIUM where Quarkus generation might cover it.

## References

- https://smallrye.io/smallrye-reactive-messaging/latest/kafka/receiving-kafka-records/
- https://quarkus.io/guides/kafka (Serdes section)
