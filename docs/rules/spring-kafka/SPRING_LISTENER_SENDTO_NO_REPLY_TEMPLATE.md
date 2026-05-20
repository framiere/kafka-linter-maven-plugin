# SPRING_LISTENER_SENDTO_NO_REPLY_TEMPLATE

**Severity**: WARNING
**Confidence**: MEDIUM
**Detection**: combination
**Tagline**: @SendTo without a reply template sends your replies into the void.

## TL;DR

The linter flags methods annotated `@KafkaListener` + `@SendTo` when no `ReplyingKafkaTemplate` / reply `KafkaTemplate` is wired into the container factory. The return value is computed and silently dropped.

## What's happening (the mechanism)

`@SendTo` declares an output topic for the listener's return value. The container forwards the return value as a new `ProducerRecord` to that topic — but only if the container factory has a `KafkaTemplate` set via `factory.setReplyTemplate(template)` (or, in Spring Boot, when a `KafkaTemplate` bean exists and Spring Boot auto-wires it into the default factory).

When no reply template is configured:

- Custom container factory built programmatically without `setReplyTemplate(...)` — the return value is discarded.
- Multiple `KafkaTemplate` beans defined: the auto-configuration picks one (or fails); without `@Primary`, Spring Boot may not wire it into the factory at all.
- Listener return type is non-void — looks intentional but does nothing.

The framework does not log a warning at startup for the missing reply template; the only signal is "the reply topic stays empty".

`@SendTo` also works with `ReplyingKafkaTemplate` for request-reply semantics — same hazard, just on the reply side.

## Operational impact

- Reply topic has no records. Consumers of the reply topic see 0 throughput.
- No exceptions logged. Looks like the listener "works" — return value computed, no error.
- Request-reply pattern: callers time out waiting for replies that never come; the original `template.sendAndReceive(...)` returns a `TimeoutException` after `replyTimeout`.

## How to fix

```java
// BAD — @SendTo with no reply template wired
@KafkaListener(topics = "requests", groupId = "g")
@SendTo("replies")
public Reply handle(Request r) {
    return process(r);
}

// GOOD — wire reply template into the factory
@Bean
public ConcurrentKafkaListenerContainerFactory<String, Request> factory(
        ConsumerFactory<String, Request> cf,
        KafkaTemplate<String, Reply> replyTemplate) {
    var factory = new ConcurrentKafkaListenerContainerFactory<String, Request>();
    factory.setConsumerFactory(cf);
    factory.setReplyTemplate(replyTemplate);
    return factory;
}

// GOOD — Spring Boot: ensure a single KafkaTemplate bean (or mark @Primary)
// is autowired into the default factory
```

For request-reply, declare a `ReplyingKafkaTemplate<K, V, R>` bean and use `sendAndReceive(...)` on the caller side.

## When this might be a false positive

- The reply template is set programmatically on a non-default factory referenced via `@KafkaListener(containerFactory = "...")` — static check sees no global template binding but the listener is wired correctly. Requires bean-graph analysis.
- The `@SendTo` value is dynamically computed via SpEL — assume reply path is intentional and downgrade to INFO.

## Detection strategy

- Annotation: scan for methods with both `@KafkaListener` and `org.springframework.messaging.handler.annotation.SendTo`.
- Config + bean graph: detect the absence of a `KafkaTemplate`-typed bean OR an explicit `setReplyTemplate` invocation in any `@Configuration` class.
- Cross-reference `containerFactory` attribute: if it points to a factory bean whose `@Bean` method does call `setReplyTemplate(...)`, suppress.
- Confidence: MEDIUM — bean-graph reasoning has gaps.

## References

- Spring Kafka — Forwarding Listener Results using `@SendTo`: https://docs.spring.io/spring-kafka/reference/kafka/receiving-messages/listener-annotation.html
- Spring Kafka — `ReplyingKafkaTemplate`: https://docs.spring.io/spring-kafka/reference/kafka/sending-messages.html#replying-template
