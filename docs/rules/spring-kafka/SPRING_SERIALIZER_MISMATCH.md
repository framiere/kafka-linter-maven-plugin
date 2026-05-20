# SPRING_SERIALIZER_MISMATCH

**Severity**: WARNING
**Confidence**: MEDIUM
**Detection**: config-file
**Tagline**: JsonSerializer in, StringDeserializer out — the wire becomes a Rorschach test.

## TL;DR

The linter flags serializer/deserializer mismatches in `spring.kafka.producer.*` vs `spring.kafka.consumer.*` properties — most commonly `JsonSerializer` on the producer side paired with `StringDeserializer` (the Spring default) on the consumer. The consumer sees a JSON string, not the typed object the producer thought it was sending.

## What's happening (the mechanism)

Producer and consumer (de)serializers don't have a handshake — they're configured independently. Common mismatches:

| Producer | Consumer | Result |
|---|---|---|
| `JsonSerializer` | `StringDeserializer` | Consumer gets `String` payload of raw JSON; type info headers ignored. Listener method signature `(MyObj obj)` triggers a Spring `MessageConversion` attempt, which may or may not work depending on `RecordMessageConverter`. |
| `StringSerializer` | `JsonDeserializer` | Consumer tries to parse a `String` as JSON; works if the string is JSON, fails for plain strings. Type header missing → falls back to default type if configured. |
| `ByteArraySerializer` | `StringDeserializer` | Consumer interprets bytes as UTF-8 string. Works for ASCII; mojibake otherwise. |
| `JsonSerializer` (producer A) | `JsonDeserializer` with no `default.type` and no `__TypeId__` on the wire | `IllegalStateException: No type information in headers and no default type provided`. |

The "JsonSerializer producer / StringDeserializer consumer" case is the most insidious because it *appears* to work — the JSON text is delivered as a string, and lazy code does `objectMapper.readValue(str, MyObj.class)` inside the listener. But you lose Spring's type-mapper integration, type headers, and any validation `JsonDeserializer` would do.

Spring Boot defaults are `StringSerializer` / `StringDeserializer` on both sides — so the mismatch only happens when one side has been customized.

## Operational impact

- Records produced as typed objects arrive as strings → listener gets `String` instead of expected POJO → `ClassCastException` at first invocation.
- With `RecordMessageConverter` wired, the conversion is silent — looks correct, but type headers are dropped. Downstream consumers of the same topic now have to use `JsonDeserializer` to recover types.
- Schema-registry-backed deserializers fail outright with `SerializationException: Unknown magic byte` when the wire format doesn't match what they expect.

## How to fix

```properties
# Choose one consistent pair across the entire application

# OPTION A — JSON typed
spring.kafka.producer.value-serializer=org.springframework.kafka.support.serializer.JsonSerializer
spring.kafka.consumer.value-deserializer=org.springframework.kafka.support.serializer.ErrorHandlingDeserializer
spring.kafka.consumer.properties.spring.deserializer.value.delegate.class=org.springframework.kafka.support.serializer.JsonDeserializer
spring.kafka.consumer.properties.spring.json.trusted.packages=com.example

# OPTION B — plain string (simplest)
spring.kafka.producer.value-serializer=org.apache.kafka.common.serialization.StringSerializer
spring.kafka.consumer.value-deserializer=org.apache.kafka.common.serialization.StringDeserializer

# OPTION C — Avro + Schema Registry
spring.kafka.producer.value-serializer=io.confluent.kafka.serializers.KafkaAvroSerializer
spring.kafka.consumer.value-deserializer=io.confluent.kafka.serializers.KafkaAvroDeserializer
```

For multi-app shared topics, declare the serdes pair as a contract in the topic's documentation.

## When this might be a false positive

- The producer and the consumer live in *different* apps and the linter only sees one side. The check is best run across both projects (or per-side with no comparison).
- Different per-topic factories: producer has multiple `KafkaTemplate` beans with different serializers; static check can't tell which template sends to which topic.

## Detection strategy

- Config: parse `spring.kafka.producer.value-serializer`, `spring.kafka.producer.key-serializer`, `spring.kafka.consumer.value-deserializer`, `spring.kafka.consumer.key-deserializer`.
- Build a compatibility matrix and flag known-broken pairs (e.g., `JsonSerializer` + `StringDeserializer`, `AvroSerializer` + `JsonDeserializer`).
- Suppress when both sides are explicitly typed and match.
- Confidence: MEDIUM — single-side analysis has blind spots when the producer or consumer lives in a different repo.

## References

- Spring Kafka — Serialization & Deserialization: https://docs.spring.io/spring-kafka/reference/kafka/serdes.html
- Spring Boot — `spring.kafka.consumer.value-deserializer`: https://docs.spring.io/spring-boot/reference/messaging/kafka.html
