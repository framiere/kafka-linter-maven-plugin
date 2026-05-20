# SPRING_RETRYABLE_TOPIC_WITH_BATCH

**Severity**: ERROR
**Confidence**: HIGH
**Detection**: combination
**Tagline**: @RetryableTopic + batch listeners = unsupported, per the docs.

## TL;DR

The linter flags methods annotated `@RetryableTopic` that are also batch listeners (`batch = "true"` on the `@KafkaListener`, or the factory configured for batch). The framework explicitly does not support this combination.

## What's happening (the mechanism)

Non-blocking retries (`@RetryableTopic`, since spring-kafka 2.7) work by routing failed records to a sequence of retry topics with increasing back-off and ultimately to a dead-letter topic. The framework's `RetryTopicConfigurer` creates the retry/DLT topics and additional listener containers behind the scenes.

The retry mechanism reasons about one failed record at a time — extracting the original topic/partition/offset from headers and republishing. Batch listeners, by contrast, deliver many records per call, with no per-record "this one failed" signal accessible to the retry topic infrastructure.

Quoting the docs: *"Non-blocking retries are not supported with Batch Listeners."* and *"Non-Blocking Retries cannot combine with Container Transactions."*

The failure mode is one of:

- `@RetryableTopic` on a method with `batch = "true"`: framework throws `IllegalArgumentException` during configuration.
- `@RetryableTopic` on a method whose factory is globally configured for batch (e.g., `spring.kafka.listener.type=batch`): same failure at context init.
- A class-level `@KafkaListener` with `@RetryableTopic` and any `@KafkaHandler` method using a `List<>` signature: failure.

## Operational impact

- Application fails to start with `IllegalArgumentException` from `RetryTopicConfigurer`.
- Bean post-processing failure surfaces as `BeanCreationException` with the listener method in the cause chain.
- In `lazy-initialization` mode, the failure is deferred until first poll attempt.

## How to fix

```java
// BAD
@RetryableTopic(attempts = "5", kafkaTemplate = "kafkaTemplate")
@KafkaListener(topics = "orders", groupId = "g", batch = "true")
public void onOrders(List<Order> orders) { ... }

// GOOD A — keep batch, use blocking retry via DefaultErrorHandler
@KafkaListener(topics = "orders", groupId = "g", batch = "true")
public void onOrders(List<Order> orders) {
    for (Order o : orders) process(o); // throw -> DefaultErrorHandler retries
}
// + factory.setCommonErrorHandler(new DefaultErrorHandler(new FixedBackOff(1000, 3)));

// GOOD B — keep @RetryableTopic, switch to record listener
@RetryableTopic(attempts = "5", kafkaTemplate = "kafkaTemplate")
@KafkaListener(topics = "orders", groupId = "g")
public void onOrder(Order o) { ... }

// GOOD C — per-record DLT inside a batch
@KafkaListener(topics = "orders", groupId = "g", batch = "true")
public void onOrders(List<Order> orders) {
    for (int i = 0; i < orders.size(); i++) {
        try { process(orders.get(i)); }
        catch (Exception e) { throw new BatchListenerFailedException("bad", e, i); }
    }
}
// + factory.setCommonErrorHandler(new DefaultErrorHandler(deadLetterRecoverer, backoff));
```

`BatchListenerFailedException` tells `DefaultErrorHandler` exactly which record in the batch failed, and lets the recoverer send only that one to the DLT.

## When this might be a false positive

- None. The combination is explicitly unsupported by the framework.

## Detection strategy

- Annotation: scan for methods carrying both `@RetryableTopic` and `@KafkaListener` with `batch = "true"`.
- Annotation + config: also flag `@RetryableTopic` on methods declared in a class where the referenced `containerFactory` bean sets `setBatchListener(true)` — requires bean-graph reasoning. Bytecode scan of `@Configuration` classes catches this.
- Annotation + global property: flag when `spring.kafka.listener.type=batch` is set and any `@RetryableTopic` exists on a method using the default factory.
- Also flag `@RetryableTopic` co-existing with `KafkaTransactionManager` (the docs forbid this too).
- Confidence: HIGH.

## References

- Spring Kafka — Non-Blocking Retries: https://docs.spring.io/spring-kafka/reference/retrytopic.html
- Spring Kafka — Combining Blocking and Non-Blocking Retries: https://docs.spring.io/spring-kafka/reference/retrytopic/combine-blocking.html
- Spring Kafka — `BatchListenerFailedException`: https://docs.spring.io/spring-kafka/reference/kafka/annotation-error-handling.html
