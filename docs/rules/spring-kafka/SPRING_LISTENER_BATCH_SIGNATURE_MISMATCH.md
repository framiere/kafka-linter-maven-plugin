# SPRING_LISTENER_BATCH_SIGNATURE_MISMATCH

**Severity**: ERROR
**Confidence**: HIGH
**Detection**: combination
**Tagline**: A batch listener that takes one record will never deploy.

## TL;DR

The linter flags `@KafkaListener` methods declared as batch listeners (via `batch = "true"`, factory `setBatchListener(true)`, or `spring.kafka.listener.type=batch`) whose signature doesn't accept a `List<...>` / `ConsumerRecords<?,?>` payload.

## What's happening (the mechanism)

A batch listener container delivers an entire `poll()` result to the listener in one call. The framework's `MessageHandlerMethodFactory` chooses an argument resolver based on the method signature:

- `List<ConsumerRecord<K,V>>` — full records with metadata.
- `List<Message<MyPojo>>` — messages with headers.
- `List<MyPojo>` — payloads only.
- `ConsumerRecords<K, V>` — raw collection (record-filter-strategy is ignored in this form, per docs).

If the container is configured as batch but the method takes a single record (`Order o`, `String s`, `ConsumerRecord<K,V> r`), Spring fails to bind the arguments and the container fails to start with `MethodArgumentNotValidException` or `KafkaException: Could not find a method argument resolver`.

`@KafkaListener(batch = "true")` was added in spring-kafka 2.8 to allow the same factory to be used for both record and batch listeners — which means the mismatch is easy to introduce by flipping `batch` without changing the method signature.

Per the docs: *"Non-Blocking Retries are not supported with batch listeners."* — also worth flagging if `@RetryableTopic` is co-located on a batch method (see `SPRING_RETRYABLE_TOPIC_WITH_BATCH`).

## Operational impact

- Application fails to start. `BeanInitializationException` at context refresh; stack trace mentions the listener method and the missing argument resolver.
- In `lazy-initialization` mode, the failure is deferred until first request.
- Common in code reviews that flip `spring.kafka.listener.type=batch` to fix a perceived perf issue without updating each listener.

## How to fix

```java
// BAD — batch=true but single-record signature
@KafkaListener(topics = "orders", groupId = "g", batch = "true")
public void onOrder(Order o) { ... }

// GOOD — accept a List
@KafkaListener(topics = "orders", groupId = "g", batch = "true")
public void onOrders(List<Order> orders) { ... }

// GOOD — accept ConsumerRecords for full metadata
@KafkaListener(topics = "orders", groupId = "g", batch = "true")
public void onOrders(ConsumerRecords<String, Order> records) { ... }

// GOOD — single-record listener, no batch override
@KafkaListener(topics = "orders", groupId = "g")
public void onOrder(Order o) { ... }
```

If you flipped `spring.kafka.listener.type=batch` globally, audit every `@KafkaListener` — or pin the listener type per factory and use `batch = "false"` on those that still want record-by-record.

## When this might be a false positive

- Custom argument resolvers that legitimately bind a single record from a batch (rare; usually a sign of confused design).
- Signature uses a custom `Iterable<T>` subtype Spring can resolve — verify with the method-argument resolver chain.

## Detection strategy

- Annotation: read `@KafkaListener.batch`. If `"true"`, inspect method signature; flag when no parameter is `java.util.List`, `org.apache.kafka.clients.consumer.ConsumerRecords`, or `org.springframework.messaging.Message` of a list.
- Config: also detect `spring.kafka.listener.type=batch` in application properties; in that case, flag any listener method on a default factory that uses a single-record signature.
- Bytecode: read method descriptor — if no `List` / `ConsumerRecords` parameter found and the batch flag is on, flag.
- Confidence: HIGH for the annotation case; MEDIUM for the global config case (a custom factory may override).

## References

- Spring Kafka — Batch Listeners: https://docs.spring.io/spring-kafka/reference/kafka/receiving-messages/listener-annotation.html
- Spring Kafka — `batch` attribute (since 2.8): https://docs.spring.io/spring-kafka/reference/kafka/receiving-messages/listener-annotation.html
