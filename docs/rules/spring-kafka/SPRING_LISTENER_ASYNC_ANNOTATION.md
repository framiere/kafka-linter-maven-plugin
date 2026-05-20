# SPRING_LISTENER_ASYNC_ANNOTATION

**Severity**: ERROR
**Confidence**: HIGH
**Detection**: annotation
**Tagline**: @Async on a listener breaks every offset guarantee Kafka gives you.

## TL;DR

The linter flags methods carrying both `@KafkaListener` and `@Async` (or any AOP proxy that runs the method off-thread). The framework will commit offsets the instant the method "returns" (which it does immediately, into another thread), so a crash mid-processing causes silent data loss.

## What's happening (the mechanism)

`@Async` (from `spring-context`) makes the proxy submit the method invocation to a `TaskExecutor` and return immediately — either `void` or a `CompletableFuture` placeholder. The actual work runs on a different thread.

Spring Kafka's listener container has no way to know the work hasn't finished. From its perspective:

- `RECORD` ack mode: the method "returned without exception", so the offset is committed.
- `BATCH` ack mode: the entire poll batch is considered processed when the synchronous return completes.
- `MANUAL` ack mode: the `Acknowledgment` parameter is captured by the async thread, but if the async thread crashes or hangs, no `acknowledge()` is ever called — and on container shutdown, in-flight async work is abandoned.

Net effect: the offset advances **before** the work is done. A JVM crash, a `OutOfMemoryError` in the async pool, or even normal container shutdown drops records on the floor. Worse, the offset commit is silent — no exception, no log line, just missing data downstream.

Same hazard for `@Transactional` declared on the listener method when the container is also transactional (double-proxying issues), but the canonical case is `@Async`.

## Operational impact

- Records ack'd as processed; downstream side effects never applied.
- Hard to detect: consumer lag stays at 0, the topic looks healthy. The bug surfaces in downstream gaps — orders missing, events not propagated.
- Application logs show `TaskRejectedException` if the async pool overflows — but only if you configured a bounded queue.
- On graceful shutdown, Spring shuts down the listener container before the `TaskExecutor`, so in-flight async work may complete — but if shutdown is forced (Kubernetes SIGKILL after `terminationGracePeriodSeconds`), in-flight work is gone.

## How to fix

```java
// BAD
@Async
@KafkaListener(topics = "orders", groupId = "g")
public void onOrder(Order o) {
    process(o);
}

// GOOD — process synchronously
@KafkaListener(topics = "orders", groupId = "g")
public void onOrder(Order o) {
    process(o);
}

// GOOD — true async via container concurrency
// Increase ConcurrentKafkaListenerContainerFactory.concurrency to parallelize
// across partitions, with each partition consumed by a dedicated thread.

// GOOD — controlled async with explicit ack
@KafkaListener(topics = "orders", groupId = "g",
               containerFactory = "manualAckFactory")
public void onOrder(Order o, Acknowledgment ack) {
    executor.submit(() -> {
        try { process(o); ack.acknowledge(); }
        catch (Exception e) { ack.nack(Duration.ofSeconds(5)); }
    });
}
```

Even the "controlled async" pattern is a smell — you've recreated the consumer threading model on top of Kafka's. Prefer container concurrency tied to partition count.

## When this might be a false positive

- `@Async` on a helper method that the listener calls — that's fine, the listener itself blocks on the result. Flag only when `@Async` is directly on the `@KafkaListener` method.
- Class-level `@Async` with the `@KafkaListener` method not overridden — same hazard, flag it.

## Detection strategy

- Annotation: read class- and method-level annotations. Flag any method (or its declaring class) carrying both `@KafkaListener` and `org.springframework.scheduling.annotation.Async`.
- Bonus: also flag `@Scheduled` on the same method (different but related anti-pattern).
- Confidence: HIGH — the two annotations are never legitimately combined on the same method.

## References

- Spring Kafka — Listener container threading model: https://docs.spring.io/spring-kafka/reference/kafka/receiving-messages/message-listener-container.html
- Spring Framework — `@Async`: https://docs.spring.io/spring-framework/reference/integration/scheduling.html#scheduling-annotation-support-async
- Spring Kafka GitHub issue — `@Async` with `@KafkaListener` is unsafe: https://github.com/spring-projects/spring-kafka/issues
