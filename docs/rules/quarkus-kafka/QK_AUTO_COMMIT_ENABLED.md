# QK_AUTO_COMMIT_ENABLED

**Severity**: WARNING
**Confidence**: HIGH
**Detection**: config-file
**Tagline**: Let SmallRye commit. Don't hand the wheel to Kafka.

## TL;DR

Setting `mp.messaging.incoming.<channel>.enable.auto.commit=true` disables SmallRye's `throttled` strategy and hands offset management to Kafka's background committer — which commits based on wall-clock time, not on whether your reactive pipeline acked the record. At-least-once is no longer guaranteed.

## What's happening (the mechanism)

The SmallRye Kafka connector explicitly disables Kafka auto-commit unless the user re-enables it. When `enable.auto.commit=true`:

- The connector's default `commit-strategy` flips from `throttled` to `ignore`.
- Kafka's own thread commits the current consumer position every `auto.commit.interval.ms` (5000 ms by default).
- The position advances as records are *polled*, not as records are *acked* by your `@Incoming` method.

Net result: if your handler is async (returns `CompletionStage`, `Uni`, or uses `@Blocking`), records can be committed before processing finishes. A crash mid-batch loses the in-flight records. The SmallRye docs note plainly: this configuration "DOES NOT guarantee at-least-once delivery."

## Operational impact

- Looks healthy: `consumer_lag` is low because offsets march forward on schedule.
- Crashes drop records that were polled-but-not-yet-processed.
- Quantifying the loss is hard — there's no DLQ, no nack signal, no log line.
- Race with `failure-strategy=fail`: the channel halts, but the offset was already advanced past the failed record. Restart resumes *after* it.

## How to fix

```properties
# BAD
mp.messaging.incoming.orders.enable.auto.commit=true

# GOOD — let SmallRye drive the commit on ack
mp.messaging.incoming.orders.commit-strategy=throttled
# (enable.auto.commit defaults to false; the connector enforces it)
```

If you genuinely need the throughput characteristics of Kafka auto-commit, pair it with `@Acknowledgment(Acknowledgment.Strategy.NONE)` on the consuming method to make the at-most-once contract explicit:

```java
@Incoming("metrics")
@Acknowledgment(Acknowledgment.Strategy.NONE)
public void consume(Metric m) { /* fire and forget — acceptable for telemetry */ }
```

## When this might be a false positive

- Telemetry / metrics consumers where at-most-once is the design.
- Migrating legacy code intentionally — flag, don't break.

## Detection strategy

- Config: `mp.messaging.incoming.<channel>.enable.auto.commit=true`.
- Bonus signal: if the matching consumer method has `@Acknowledgment(Strategy.NONE)`, lower severity to INFO (intentional).
- Confidence: HIGH otherwise.

## References

- https://smallrye.io/smallrye-reactive-messaging/latest/kafka/receiving-kafka-records/ (commit-strategy section)
- https://quarkus.io/blog/kafka-commit-strategies/
