# SPRING_LISTENER_THREAD_SLEEP

**Severity**: WARNING
**Confidence**: MEDIUM
**Detection**: bytecode
**Tagline**: Thread.sleep() in a listener is a self-inflicted rebalance.

## TL;DR

The linter flags `@KafkaListener` methods that call `Thread.sleep(...)` or other obvious long-blocking calls on the container thread. Anything longer than `max.poll.interval.ms` (default 300_000 ms) triggers a rebalance and partition revocation.

## What's happening (the mechanism)

A `@KafkaListener` method runs on the listener container's poll thread. The Kafka consumer client uses a background heartbeat thread, but the broker also tracks `max.poll.interval.ms` — the time between two `poll()` calls. If the listener method blocks for longer than that, the group coordinator considers the consumer dead, revokes its partitions, and triggers a rebalance.

Concrete failure pattern:

1. Listener calls `Thread.sleep(600_000)` (10 minutes).
2. After 5 minutes, the broker rebalances. The previous owner's offsets that weren't committed are reset.
3. Another consumer in the group picks up the partition and reprocesses the record.
4. The original consumer wakes up, tries to commit, and gets `CommitFailedException`.
5. Spring's default error handler logs and the consumer re-joins. Loop.

Beyond `Thread.sleep`, the same pattern applies to any long sync IO: a blocking HTTP call, a `JdbcTemplate.queryForObject` against an overloaded DB, a `Future.get()` without a timeout, etc. `Thread.sleep` is the most static-checkable canary.

For deliberate delays / retry waits, use Spring Kafka's `DefaultErrorHandler` with a `BackOff` (the container pauses the consumer correctly during back-off) or non-blocking retries via `@RetryableTopic`.

## Operational impact

- `kafka.consumer.commit-failed-rate` spikes.
- Broker logs show `Member <id> failed, removing it from the group` for the listener's consumer.
- `RebalanceInProgressException` / `CommitFailedException` in the Spring app logs.
- Throughput drops to ~0 while the group is rebalancing (consumer outage = stop-the-world for the partition).
- Same record processed multiple times — bad for non-idempotent downstreams.

## How to fix

```java
// BAD
@KafkaListener(topics = "orders", groupId = "g")
public void onOrder(Order o) throws InterruptedException {
    Thread.sleep(60_000); // throttle?
    process(o);
}

// GOOD — let the container manage backpressure
// Set max.poll.records lower; use ConcurrentKafkaListenerContainerFactory's
// pause/resume API or a DefaultErrorHandler with FixedBackOff for retry waits

// GOOD — for deliberate retry waits, use DefaultErrorHandler
@Bean
public DefaultErrorHandler errorHandler() {
    return new DefaultErrorHandler(new FixedBackOff(1_000L, 3));
}

// GOOD — for explicit throttling, use container pause
container.pausePartition(new TopicPartition("orders", 0));
// ... later
container.resumePartition(new TopicPartition("orders", 0));
```

If you genuinely need to wait minutes between records (rate-limited external API), use `@RetryableTopic` and let the retry topic provide the delay — the original consumer keeps polling so no rebalance.

## When this might be a false positive

- A short `Thread.sleep` (< a few hundred ms) used as a deliberate jitter for retry-on-conflict patterns. Configure a threshold (e.g., flag only sleeps > 5_000 ms) or downgrade to INFO when the argument is a small literal.
- Test code under `src/test/java` — exclude from scan.

## Detection strategy

- Bytecode: in any method annotated `@KafkaListener` (or transitively called), look for `INVOKESTATIC java/lang/Thread sleep(J)V` and `sleep(JI)V`.
- Bonus signals: `INVOKEVIRTUAL java/util/concurrent/Future get()` (without timeout), `INVOKEVIRTUAL java/util/concurrent/CompletableFuture get()`.
- Extract sleep argument when it's a constant (`LDC2_W`); flag if > 5_000 ms or unknown.
- Trace one level of intra-class calls before giving up.
- Confidence: MEDIUM (sleeps in helper methods are easy to miss; argument may be a variable).

## References

- Spring Kafka — Listener Container Properties (max.poll.interval.ms / rebalances): https://docs.spring.io/spring-kafka/reference/kafka/container-props.html
- Apache Kafka — `max.poll.interval.ms`: https://kafka.apache.org/documentation/#consumerconfigs_max.poll.interval.ms
