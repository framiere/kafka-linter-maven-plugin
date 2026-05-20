# SPRING_LISTENER_RETURN_VALUE_IGNORED

**Severity**: WARNING
**Confidence**: HIGH
**Detection**: annotation + bytecode
**Tagline**: A non-void listener with no @SendTo is a return statement nobody reads.

## TL;DR

The linter flags `@KafkaListener` methods that declare a non-`void` return type, are not annotated with `@SendTo`, and are not wired for `replyTemplate` / `ReplyingKafkaTemplate`. The return value is computed and silently discarded.

## What's happening (the mechanism)

Spring Kafka recognizes three legitimate return-value paths:

1. **`@SendTo` annotation**: the return is forwarded to the named topic via the container factory's `replyTemplate`.
2. **`ReplyingKafkaTemplate` consumer side**: the return is sent back to the reply topic specified in the inbound message's `KafkaHeaders.REPLY_TOPIC` header.
3. **`Message<?>` return type with `KafkaHeaders.TOPIC` set**: similar to (1) but dynamically routed.

If none of these apply, the framework computes the return and throws it away. The listener author thought the return was meaningful; in reality the work was wasted.

The pattern often appears when developers refactor a request-response controller into a Kafka listener and forget the listener doesn't return to a caller. Or when a return statement is added speculatively "for future async response" that never materializes.

## Operational impact

- CPU spent computing an unused return value — typically small, but in hot loops over expensive payloads it adds up.
- Cognitive cost: readers think the return matters; debugging detours into "where does this go?" when the answer is "nowhere".
- For a `CompletableFuture` return, the future may be never observed — exceptions inside the future are silently dropped (only logged by `LoggingProducerListener` if it's a Kafka send failure).

## How to fix

```java
// BAD
@KafkaListener(topics = "orders", groupId = "g")
public OrderConfirmation onOrder(Order o) {
    return processAndConfirm(o);
}

// GOOD A — void return, side-effect only
@KafkaListener(topics = "orders", groupId = "g")
public void onOrder(Order o) {
    processAndConfirm(o);
}

// GOOD B — actually forward the result
@KafkaListener(topics = "orders", groupId = "g")
@SendTo("order-confirmations")
public OrderConfirmation onOrder(Order o) {
    return processAndConfirm(o);
}

// GOOD C — typed async return (since 3.2 supports CompletableFuture/Mono)
@KafkaListener(topics = "orders", groupId = "g")
public CompletableFuture<Void> onOrder(Order o) {
    return processAsync(o);
}
```

The async return type (C) is a special case where the framework awaits the future before committing — different semantics from "return discarded". Allow it when the listener signature is `CompletableFuture<?>` or `Mono<?>` and the container supports async returns.

## When this might be a false positive

- Listener returning `CompletableFuture<?>` / `Mono<?>` — the framework integrates with these for async ack semantics (since spring-kafka 3.2+).
- Class-level `@KafkaListener` with `@KafkaHandler` methods that return different types — Spring may be using the return for dispatch decisions in some configurations.
- Test scaffolding that intentionally returns to expose data to assertions.

## Detection strategy

- Annotation: scan for `@KafkaListener` methods (and `@KafkaHandler` methods on `@KafkaListener` classes).
- Bytecode: read the method descriptor's return type. Flag when:
  - Return type is not `V` (void).
  - No `@SendTo` annotation present on the method.
  - Return type is not `CompletableFuture<?>` / `Mono<?>` / `Flux<?>` (async ack support).
- Confidence: HIGH for plain non-void returns with no `@SendTo`; LOWER when async types involved.

## References

- Spring Kafka — Forwarding Listener Results using `@SendTo`: https://docs.spring.io/spring-kafka/reference/kafka/receiving-messages/listener-annotation.html
- Spring Kafka — Asynchronous `@KafkaListener` Return Types: https://docs.spring.io/spring-kafka/reference/kafka/receiving-messages/listener-annotation.html
