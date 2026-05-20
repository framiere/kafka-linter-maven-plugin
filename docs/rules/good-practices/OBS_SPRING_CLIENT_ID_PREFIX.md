# OBS_SPRING_CLIENT_ID_PREFIX

**Severity**: WARNING
**Confidence**: HIGH
**Detection**: config-file
**Tagline**: Spring Boot has a `client-id` knob. Use it before the broker logs see `producer-1` again.

## TL;DR

The linter flags Spring Boot Kafka projects that don't set
`spring.kafka.client-id` (or per-section `spring.kafka.consumer.client-id`
and `spring.kafka.producer.client-id`). Without it, Spring Boot leaves
the auto-generated `client.id` unset and the underlying clients fall
back to `producer-N` / `consumer-N`.

## The setup

Spring Boot's `KafkaProperties` exposes top-level and per-section
`client-id` settings. They map to the underlying client's `client.id`
configuration. If unset, the resulting `ProducerConfig` / `ConsumerConfig`
gets no `client.id`, and the broker assigns a synthetic name.

This is the Spring-flavored variant of the kafka-clients
[CLIENT_ID_MISSING](../kafka-clients/CLIENT_ID_MISSING.md). Cross-link
for the operational impact.

## What's actually happening (the mechanism)

`spring.kafka.client-id` is the default. Per-section overrides take
precedence:

- `spring.kafka.producer.client-id` — wins for the producer factory.
- `spring.kafka.consumer.client-id` — wins for the consumer factory.
- `spring.kafka.streams.client-id` — wins for the Streams app.

Spring Boot does NOT auto-derive these from `spring.application.name`
(an old request that has not been merged). If you want
"orders-svc-producer-1", you have to wire it.

## How to fix (no → yes)

```yaml
# no — Spring Boot's default empty client.id
spring:
  application:
    name: orders-svc

# yes — explicit per-section
spring:
  application:
    name: orders-svc
  kafka:
    client-id: ${spring.application.name}
    consumer:
      client-id: ${spring.application.name}-consumer-${HOSTNAME:local}
    producer:
      client-id: ${spring.application.name}-producer-${HOSTNAME:local}
```

For per-listener (`@KafkaListener`), use the `clientIdPrefix` attribute:

```java
@KafkaListener(topics = "orders",
               groupId = "orders-svc",
               clientIdPrefix = "orders-svc-consumer")
public void onOrder(Order order) { ... }
```

## When this might be a false positive

- Local dev profile with no real broker (Spring's
  `EmbeddedKafkaBroker` test scaffolding).
- Application uses an OpenTelemetry javaagent that auto-attributes
  spans with `service.name` from a different source — the
  observability gap is filled, but the broker-side audit gap is not.

## Detection strategy

- **Config files:** flag if NONE of `spring.kafka.client-id`,
  `spring.kafka.consumer.client-id`, `spring.kafka.producer.client-id`,
  `spring.kafka.streams.client-id` are set.
- Suppress in `application-test.yml` / `application-local.yml`.
- **Confidence: HIGH** — provable from config absence.

## References

- Spring Boot — `spring.kafka` properties reference:
  https://docs.spring.io/spring-boot/appendix/application-properties/index.html#appendix.application-properties.integration
- Spring Kafka — `@KafkaListener#clientIdPrefix`:
  https://docs.spring.io/spring-kafka/reference/kafka/receiving-messages/listener-annotation.html
- Cross-link: [CLIENT_ID_MISSING](../kafka-clients/CLIENT_ID_MISSING.md)
