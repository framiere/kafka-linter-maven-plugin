# QK_FAILURE_STRATEGY_IGNORE

**Severity**: ERROR
**Confidence**: HIGH
**Detection**: config-file
**Tagline**: failure-strategy=ignore is data loss with a smile.

## TL;DR

`mp.messaging.incoming.<channel>.failure-strategy=ignore` commits the offset of records your handler failed to process — silent, permanent data loss with no DLQ, no retry, no health-probe signal.

## What's happening (the mechanism)

SmallRye Reactive Messaging's Kafka inbound connector applies a `failure-strategy` whenever the downstream pipeline nacks a message:

- `fail` (default): nack stops the channel, offset NOT committed. The next restart will re-read the record. Liveness probe flips to DOWN.
- `ignore`: nack is logged at WARN, offset IS committed, channel keeps moving. The record is gone.
- `dead-letter-queue`: record is forwarded to `dead-letter-topic-<channel>` (configurable), then the offset is committed.
- `delayed-retry-topic` (experimental): record is forwarded to a chain of retry topics.

`ignore` is the only strategy that *advances the consumer past data that was never successfully processed* without leaving any trace recoverable from Kafka itself. It is functionally equivalent to `try { handle(msg); } catch (Throwable t) { log.warn(t); commit(); }` — the failure mode every Kafka best-practice doc warns against.

## Operational impact

- No metric flips. SmallRye's liveness probe stays UP because the `ignore` strategy reports success to the health subsystem.
- The only signal is `WARN` log lines like `SRMSG18230: A message has been nacked, ignoring failure`. If your log aggregator filters WARN, this is invisible.
- Drop rate is unbounded: a bad codepath or schema mismatch can silently discard every record that hits it.
- On-call gets paged only when downstream consumers notice the gap — usually hours or days later.

## How to fix

```properties
# BAD — silent data loss
mp.messaging.incoming.orders.connector=smallrye-kafka
mp.messaging.incoming.orders.failure-strategy=ignore

# GOOD — recoverable: failed records land on a DLQ topic
mp.messaging.incoming.orders.connector=smallrye-kafka
mp.messaging.incoming.orders.failure-strategy=dead-letter-queue
mp.messaging.incoming.orders.dead-letter-queue.topic=orders-dlq
```

For transient failures, prefer `delayed-retry-topic` or wrap the handler in `@Retry` from SmallRye Fault Tolerance and let the default `fail` strategy stop the channel only after retries are exhausted.

## When this might be a false positive

- Pure telemetry / sampling pipelines where any individual record is genuinely disposable (e.g., raw click events fed into a sketch). Even then, prefer `dead-letter-queue` with a short retention.
- Test profiles (`%test`, `%dev`) where you want a flaky integration to not stop the suite.

The rule SHOULD treat a configured `<channel>.dead-letter-queue.topic` alongside `failure-strategy=ignore` as a contradiction (the DLQ topic is dead config).

## Detection strategy

- Config: scan `application.properties`, `application.yml`, `application-<profile>.properties` for `mp.messaging.incoming.*.failure-strategy=ignore`.
- Confidence: HIGH for production profiles (`%prod`, unprefixed), MEDIUM for `%dev`/`%test`.
- Pair with QK_DLQ_TOPIC_UNUSED if a `dead-letter-queue.topic` is set on the same channel.

## References

- https://smallrye.io/smallrye-reactive-messaging/latest/kafka/receiving-kafka-records/
- https://quarkus.io/blog/kafka-commit-strategies/
- https://quarkus.io/guides/kafka (Failure strategies section)
