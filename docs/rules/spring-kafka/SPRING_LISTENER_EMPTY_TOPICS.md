# SPRING_LISTENER_EMPTY_TOPICS

**Severity**: ERROR
**Confidence**: HIGH
**Detection**: annotation
**Tagline**: An empty topics array subscribes to nothing, loudly.

## TL;DR

The linter flags `@KafkaListener` declared with an empty `topics = {}` array and no `topicPattern` or `topicPartitions` alternative. The container will fail to start.

## What's happening (the mechanism)

`@KafkaListener` requires exactly one topic-selection mechanism: `topics`, `topicPattern`, or `topicPartitions`. Spring's `KafkaListenerAnnotationBeanPostProcessor` validates this at bean post-processing time. When all three are empty/absent, it raises `IllegalStateException: Topics, topicPattern or topicPartitions must be provided`.

The quirky case is `topics = {}` — a developer who tried to externalize the list and ended up with an empty array (e.g., from `topics = "${app.topics}".split(",")` semantics gone wrong, or from migrating away from a config and forgetting to replace the attribute). Static check catches this before the app even tries to start.

A second variant: `topics = "${kafka.topics:}"` with no override in any profile — the resolved value is the empty string, which Spring treats as a single empty topic name and Kafka rejects (`InvalidTopicException`).

## Operational impact

- App fails to start. `ApplicationContextException` during refresh; stack trace points to the offending listener method.
- In Spring Boot, `spring.main.lazy-initialization=true` defers the failure until first dependency injection — sometimes the failure surfaces only when a controller is hit.
- Pod crash-loops in Kubernetes.

## How to fix

```java
// BAD
@KafkaListener(topics = {}, groupId = "g")
public void onOrder(Order o) { ... }

// BAD — resolves to empty string
@KafkaListener(topics = "${kafka.topics:}", groupId = "g")
public void onOrder(Order o) { ... }

// GOOD — explicit topic
@KafkaListener(topics = "orders", groupId = "g")
public void onOrder(Order o) { ... }

// GOOD — placeholder with mandatory binding
@KafkaListener(topics = "${app.kafka.orders-topic}", groupId = "g")
public void onOrder(Order o) { ... }
```

For mandatory placeholders, omit the default — Spring throws on context init if unresolved, which is exactly the failure you want at deploy time, not at first poll.

## When this might be a false positive

- Dynamic registration via `KafkaListenerEndpointRegistrar` where the topics list is computed at runtime and the annotation is intentionally a placeholder. Tag those listeners explicitly and suppress.

## Detection strategy

- Annotation: read `@KafkaListener` `AnnotationNode.values`. Locate `topics`, `topicPattern`, `topicPartitions`.
- Flag when `topics` is an empty array literal AND no `topicPattern` AND no `topicPartitions`.
- Optional: flag `topics = "${...:}"` patterns where the default is empty; cross-reference against application properties.
- Confidence: HIGH for the literal empty array.

## References

- Spring Kafka — `@KafkaListener` Annotation: https://docs.spring.io/spring-kafka/reference/kafka/receiving-messages/listener-annotation.html
- `KafkaListenerAnnotationBeanPostProcessor` Javadoc: https://docs.spring.io/spring-kafka/docs/current/api/org/springframework/kafka/annotation/KafkaListenerAnnotationBeanPostProcessor.html
