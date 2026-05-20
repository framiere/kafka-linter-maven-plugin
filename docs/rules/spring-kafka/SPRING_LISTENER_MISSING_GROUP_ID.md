# SPRING_LISTENER_MISSING_GROUP_ID

**Severity**: ERROR
**Confidence**: HIGH
**Detection**: combination
**Tagline**: A listener with no group is just a private subscription nobody else knows about.

## TL;DR

The linter flags `@KafkaListener` methods that declare neither `groupId` nor `id`, in projects that also don't set `spring.kafka.consumer.group-id`. The container will fail to start with `IllegalStateException: No group.id found...`.

## What's happening (the mechanism)

Spring Kafka resolves the consumer `group.id` in this order (since spring-kafka 2.0):

1. `@KafkaListener.groupId` — explicit, always wins.
2. `@KafkaListener.id` — if `idIsGroup` is `true` (default), the bean name is also used as `group.id`.
3. `spring.kafka.consumer.group-id` (Spring Boot property) or `ConsumerFactory` defaults.
4. If nothing of the above is set, the underlying `KafkaConsumer.subscribe(...)` call throws `InvalidGroupIdException: The configured groupId is invalid` and the container transitions to `failed`.

There is no Spring Boot default for `spring.kafka.consumer.group-id` — it must be provided.

Quoting the docs on `id`: *"the `id` property (if present) is used as the Kafka consumer `group.id` property, overriding the configured property in the consumer factory."* So in practice many teams rely on `id`, but that only works if `id` is set.

## Operational impact

- Container fails to start. Look for `o.s.k.l.KafkaMessageListenerContainer` logging `Stopping container due to an Error` with `InvalidGroupIdException` as cause.
- Spring Boot Actuator `/health` will not directly fail unless `KafkaHealthIndicator` is enabled — the app appears UP but doesn't consume.
- On Kubernetes, the pod stays Ready (HTTP listener is up) while no records are processed. Detection latency is consumer-lag-alert latency.
- Silent in dev with `spring.kafka.consumer.group-id` set in `application-dev.properties` but missing in the prod profile.

## How to fix

```java
// BAD — no group resolution path
@KafkaListener(topics = "orders")
public void onOrder(Order o) { ... }

// GOOD — explicit groupId
@KafkaListener(topics = "orders", groupId = "order-service")
public void onOrder(Order o) { ... }

// GOOD — id used as group (default behavior)
@KafkaListener(id = "order-service", topics = "orders")
public void onOrder(Order o) { ... }

// GOOD — set globally in application.yml
// spring.kafka.consumer.group-id: order-service
```

Prefer `groupId` over `id`. `id` doubles as the container bean name, which means renaming the container also renames the consumer group and resets offsets.

## When this might be a false positive

- A consumer factory bean explicitly sets `ConsumerConfig.GROUP_ID_CONFIG` programmatically — the static check sees neither annotation attribute nor Boot property. Downgrade to WARNING with a hint that programmatic factories should be inspected manually.
- Test code that uses `@EmbeddedKafka` with a generated group id.

## Detection strategy

- Annotation: read `@KafkaListener` `AnnotationNode.values` (alternating name/value list); flag when neither `groupId` nor `id` is present.
- Config: parse `application.properties` / `application.yml` / `application-*.yml` for `spring.kafka.consumer.group-id`. If present, suppress.
- Bytecode: low signal — programmatic `ConsumerFactory` configuration requires data-flow analysis. Mark CONTEXT and offer manual review pointer.
- Confidence: HIGH when annotation alone and no boot property; MEDIUM when a custom `containerFactory` is referenced (the factory may set it).

## References

- Spring Kafka — `@KafkaListener` Annotation: https://docs.spring.io/spring-kafka/reference/kafka/receiving-messages/listener-annotation.html
- Spring Kafka — Obtaining the Consumer `group.id`: https://docs.spring.io/spring-kafka/reference/kafka/receiving-messages/obtaining-group-id.html
- Spring Boot — `spring.kafka.consumer.group-id`: https://docs.spring.io/spring-boot/appendix/application-properties/index.html
