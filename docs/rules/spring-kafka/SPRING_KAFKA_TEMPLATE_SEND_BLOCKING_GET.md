# SPRING_KAFKA_TEMPLATE_SEND_BLOCKING_GET

**Severity**: WARNING
**Confidence**: HIGH
**Detection**: bytecode
**Tagline**: `send(...).get()` turns an async producer back into a synchronous one, one record at a time.

## TL;DR

The linter flags `kafkaTemplate.send(...).get()` (or `.join()`) without a bounded timeout — blocking the caller until the broker acknowledges. Throughput collapses to per-record round-trip time, and the calling thread can hang for `delivery.timeout.ms` (default 120s) per record.

## What's happening (the mechanism)

`KafkaTemplate.send(...)` returns immediately; the actual produce request is batched by the producer's IO thread. The `CompletableFuture<SendResult>` completes when the broker acks (or fails).

Calling `.get()` (no-arg) on this future blocks the calling thread until completion or interrupt. Calling `.get(long, TimeUnit)` blocks until the timeout. Either way:

- The producer's batching benefit is gone — your throughput is now `1 / round_trip_to_broker` records per second per thread.
- If the broker is unreachable, the future completes with `TimeoutException` only after `delivery.timeout.ms` (default 120_000 ms = 2 minutes). The calling thread is blocked the entire time.
- In a request-scoped controller, this means HTTP clients see 2-minute timeouts.
- In a `@KafkaListener` method, this exceeds `max.poll.interval.ms` and triggers a rebalance.
- In a `@Scheduled` task, the executor thread is held captive.

The docs explicitly document this pattern but as an opt-in: "Sync usage (block)" with a `try { template.send(record).get(10, TimeUnit.SECONDS); }`. The unbounded `.get()` is the smell.

## Operational impact

- p99 latency dominated by broker RTT. Connection blips show up as 2-minute hangs.
- Thread pool exhaustion — Tomcat / undertow worker threads sit on `Object.wait`.
- Cascading rebalances when `.get()` is inside a `@KafkaListener`.
- Memory pressure from blocked threads holding request objects in scope.

## How to fix

```java
// BAD
kafkaTemplate.send("audit", event).get();

// BAD — even worse, no timeout
kafkaTemplate.send("audit", event).join();

// GOOD — fire and observe via callback
kafkaTemplate.send("audit", event)
    .whenComplete((res, ex) -> { if (ex != null) log.error("audit", ex); });

// ACCEPTABLE — when you genuinely need a sync send (e.g. transactional flush
// at the end of an HTTP request), bound the wait
try {
    kafkaTemplate.send("audit", event).get(5, TimeUnit.SECONDS);
} catch (ExecutionException ee) {
    throw new ServiceException("audit publish failed", ee.getCause());
} catch (TimeoutException te) {
    throw new ServiceException("audit publish timed out", te);
}
```

If you really need synchronous behavior across many sends, batch them and wait once on `CompletableFuture.allOf(...)` with a bounded `get(...)`.

## When this might be a false positive

- Startup / one-shot scripts where blocking is the intended behavior. Configure suppression by class or package.
- Tests — exclude `src/test/java`.

## Detection strategy

- Bytecode: locate `INVOKEVIRTUAL java/util/concurrent/CompletableFuture get()Ljava/lang/Object;` (no-arg) immediately following a `KafkaTemplate.send(...)` call (the future is on the stack).
- Also flag `CompletableFuture.join()`, `CompletableFuture.getNow(null)`.
- Bounded `get(long, TimeUnit)` may also be flagged with INFO severity if you want to encourage callback usage; or suppressed entirely.
- Confidence: HIGH for the no-arg pattern; MEDIUM for `join()`.

## References

- Spring Kafka — `KafkaTemplate` sync usage: https://docs.spring.io/spring-kafka/reference/kafka/sending-messages.html
- Apache Kafka — `delivery.timeout.ms`: https://kafka.apache.org/documentation/#producerconfigs_delivery.timeout.ms
