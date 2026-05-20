# SPRING_KAFKA_TEMPLATE_SEND_NO_CALLBACK

**Severity**: WARNING
**Confidence**: MEDIUM
**Detection**: bytecode
**Tagline**: `kafkaTemplate.send(...)` and walk away is fire-and-pray.

## TL;DR

The linter flags call sites where the `CompletableFuture` returned by `KafkaTemplate.send(...)` is discarded — no `whenComplete`, `thenAccept`, `get()`, no assignment, no chaining. Send failures are silently swallowed.

## What's happening (the mechanism)

In spring-kafka 3.0+, `KafkaTemplate.send(...)` returns `CompletableFuture<SendResult<K, V>>` (it returned `ListenableFuture` before — see `SPRING_KAFKA_TEMPLATE_LISTENABLE_FUTURE` if applicable). The send itself goes onto the producer's internal accumulator and is sent asynchronously by the producer's IO thread.

If the application discards the returned future:

- Synchronous errors at enqueue time (serialization failure, buffer full, illegal partition) are exposed as a *failed* future — but a discarded future means no `whenComplete` handler runs, so the exception is invisible.
- Asynchronous errors (broker rejection, `NotEnoughReplicasException`, timeout, `TopicAuthorizationException`) similarly resolve into a failed future. Discarded = silent.
- The framework's `LoggingProducerListener` (default `ProducerListener` on the template) does log errors — but only if `setProducerListener(...)` hasn't been overridden, and only at `ERROR` level which is easy to filter out.

Without a `ProducerListener` and without consuming the future, a send that fails leaves no trace beyond a producer metric (`record-error-rate`).

## Operational impact

- Producer-side failures invisible at the application level. Records "sent" never arrive.
- Bad keys, broker timeouts, ACL changes all manifest as missing data downstream.
- `record-error-rate` and `record-error-total` micrometer metrics go up — must be explicitly alerted on.
- Particularly nasty with transactional templates: a failed send inside a transaction aborts the transaction, but if the future is discarded the calling code thinks the transaction committed.

## How to fix

```java
// BAD
kafkaTemplate.send("orders", order.id(), order);

// GOOD — async callback
kafkaTemplate.send("orders", order.id(), order)
    .whenComplete((result, ex) -> {
        if (ex != null) {
            log.error("send failed for {}", order.id(), ex);
            metrics.counter("kafka.send.failure").increment();
        }
    });

// GOOD — let a global ProducerListener log
@Bean
public ProducerListener<String, Object> producerListener() {
    return new ProducerListener<>() {
        public void onError(ProducerRecord<String, Object> r, RecordMetadata m, Exception e) {
            log.error("send failed topic={} key={}", r.topic(), r.key(), e);
        }
    };
}
// + kafkaTemplate.setProducerListener(producerListener);
```

For per-call semantics, prefer `whenComplete` — `ProducerListener` is good as a safety net but can't carry call-site context.

## When this might be a false positive

- A custom `ProducerListener` is wired into the template that does meaningful error reporting — static check can suppress when the bean-graph shows `setProducerListener` is called and the listener implements `onError`.
- Discard inside a method whose caller does its own error tracking via metrics + log aggregation.

## Detection strategy

- Bytecode: locate `INVOKEVIRTUAL org/springframework/kafka/core/KafkaTemplate send(...)Ljava/util/concurrent/CompletableFuture;` (and the related overloads on the `KafkaOperations` interface).
- Check whether the returned future is consumed: any of `whenComplete`, `thenAccept`, `thenApply`, `thenCompose`, `handle`, `get`, `join`, `exceptionally`, or assigned/returned. If immediately followed by `POP` or no use, flag.
- Suppress when a `ProducerListener` bean is detected in the bean graph (best-effort).
- Confidence: MEDIUM — the static check is precise at the bytecode level but can't reason about the `ProducerListener` safety net without bean-graph analysis.

## References

- Spring Kafka — `KafkaTemplate.send` (`CompletableFuture<SendResult>` since 3.0): https://docs.spring.io/spring-kafka/reference/kafka/sending-messages.html
- Spring Kafka — `ProducerListener` Javadoc: https://docs.spring.io/spring-kafka/docs/current/api/org/springframework/kafka/support/ProducerListener.html
- Related core rule: `PRODUCER_SEND_NO_CALLBACK` in kafka-clients ruleset.
