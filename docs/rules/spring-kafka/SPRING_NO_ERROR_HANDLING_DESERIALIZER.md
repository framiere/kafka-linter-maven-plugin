# SPRING_NO_ERROR_HANDLING_DESERIALIZER

**Severity**: WARNING
**Confidence**: MEDIUM
**Detection**: config-file
**Tagline**: Without ErrorHandlingDeserializer, one bad byte stops every consumer in the group.

## TL;DR

The linter flags consumer configurations that use a structured deserializer (`JsonDeserializer`, `AvroDeserializer`, `ProtobufDeserializer`, etc.) but do not wrap it in `ErrorHandlingDeserializer`. A single malformed record — a poison pill — will jam the consumer in an infinite retry loop because the failure happens inside `poll()`, before Spring's error handler can intervene.

## What's happening (the mechanism)

When a deserializer throws, the exception is raised inside `KafkaConsumer.poll()`. The container's `CommonErrorHandler` only sees exceptions that surface from the listener method — it never sees `poll()`-time failures. The default behavior: the same poll is retried, the same record fails to deserialize, the same exception is thrown, forever.

`ErrorHandlingDeserializer` (since spring-kafka 2.2) wraps a delegate:

1. Call the delegate.
2. If it throws, return `null` and stash the exception + raw bytes in record headers.
3. The record reaches the container, where the configured error handler is invoked.

With this wrapper, a poison record is routed through `DefaultErrorHandler` to the DLT (or just logged-and-skipped) and the consumer keeps making progress.

Per the docs: *"When a deserializer fails to deserialize a message, Spring has no way to handle the problem, because it occurs before the `poll()` returns. To solve this problem, the `ErrorHandlingDeserializer` has been introduced."*

For high-volume topics with multiple producers, the probability of a poison pill (schema drift, partial deploy, gzipped-by-mistake payload) is non-trivial — the wrapper is a production necessity.

## Operational impact

- Container stuck. Consumer lag grows linearly with producer throughput. The pod looks healthy.
- Logs: same `DeserializationException` printed every `request.timeout.ms` for the same offset.
- Restart doesn't help — the offset is committed and the poll resumes at the same broken record.
- The only "fix" without `ErrorHandlingDeserializer` is operator intervention: `kafka-consumer-groups --reset-offsets --shift-by 1` to skip the record manually.

## How to fix

```properties
# BAD — direct JsonDeserializer
spring.kafka.consumer.value-deserializer=org.springframework.kafka.support.serializer.JsonDeserializer

# GOOD — wrap in ErrorHandlingDeserializer
spring.kafka.consumer.key-deserializer=org.springframework.kafka.support.serializer.ErrorHandlingDeserializer
spring.kafka.consumer.value-deserializer=org.springframework.kafka.support.serializer.ErrorHandlingDeserializer
spring.kafka.consumer.properties.spring.deserializer.key.delegate.class=org.apache.kafka.common.serialization.StringDeserializer
spring.kafka.consumer.properties.spring.deserializer.value.delegate.class=org.springframework.kafka.support.serializer.JsonDeserializer
spring.kafka.consumer.properties.spring.json.trusted.packages=com.example
```

```java
// GOOD — programmatic
JsonDeserializer<Order> json = new JsonDeserializer<>(Order.class);
json.addTrustedPackages("com.example");
ErrorHandlingDeserializer<Order> ehd = new ErrorHandlingDeserializer<>(json);

DefaultKafkaConsumerFactory<String, Order> cf = new DefaultKafkaConsumerFactory<>(
        props,
        new ErrorHandlingDeserializer<>(new StringDeserializer()),
        ehd);
```

Pair with a `DefaultErrorHandler` whose recoverer is `DeadLetterPublishingRecoverer` — the poison pill ends up on `<topic>.DLT` with the exception details in headers.

## When this might be a false positive

- `StringDeserializer` only — strings don't really fail (they take any bytes). The check should target *structured* delegates.
- A topic where the producer is the same application and the schema is locked — the probability of poison pill is low. But upstream pipeline changes break this assumption.

## Detection strategy

- Config: find `spring.kafka.consumer.value-deserializer` or `key-deserializer` set to one of:
  - `org.springframework.kafka.support.serializer.JsonDeserializer`
  - `io.confluent.kafka.serializers.KafkaAvroDeserializer`
  - `io.confluent.kafka.serializers.protobuf.KafkaProtobufDeserializer`
  - Apache Avro/Protobuf custom deserializers.
- Flag when the deserializer is not `ErrorHandlingDeserializer` (and not wrapped under a delegate property).
- If the value is `ErrorHandlingDeserializer`, verify `spring.deserializer.value.delegate.class` is set.
- Confidence: MEDIUM — programmatic configurations bypass the static check.

## References

- Spring Kafka — `ErrorHandlingDeserializer`: https://docs.spring.io/spring-kafka/reference/kafka/serdes.html
- Spring Kafka — Handling poison pill messages: https://docs.spring.io/spring-kafka/reference/kafka/annotation-error-handling.html
- Confluent — Handling deserialization errors: https://www.confluent.io/blog/spring-kafka-can-your-kafka-consumers-handle-a-poison-pill/
