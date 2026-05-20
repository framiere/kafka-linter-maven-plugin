# QK_FAILURE_STRATEGY_FAIL_NO_DLQ

**Severity**: WARNING
**Confidence**: MEDIUM
**Detection**: config-file
**Tagline**: The default fails loud — make sure you actually want the channel to die.

## TL;DR

`failure-strategy=fail` (the SmallRye default) stops the channel on the first nack and never resumes until the app restarts. If you haven't paired it with a DLQ or retry, one poison pill takes the whole consumer offline.

## What's happening (the mechanism)

The default `failure-strategy=fail` means: nack → channel halts, offset not committed, liveness probe DOWN, no new records polled. The reactive subscription is terminated; nothing within the running JVM recovers it. The only way back is a process restart, and on restart the same record is re-read and re-fails — a crashloop.

This is conservative and correct *if* the operator has a recovery plan (DLQ, retry, manual intervention). It is dangerous as a silent default in services that haven't designed for that.

## Operational impact

- `kafka_consumer_records_consumed_total` flatlines for the affected partition assignment.
- `smallrye-health` liveness probe reports DOWN; Kubernetes restarts the pod; the cycle repeats.
- `consumer_lag` grows monotonically.
- On-call pages immediately (good) but cannot remediate without code/data intervention.

## How to fix

```properties
# Option A — DLQ for non-recoverable failures
mp.messaging.incoming.orders.failure-strategy=dead-letter-queue
mp.messaging.incoming.orders.dead-letter-queue.topic=orders-dlq

# Option B — Retry then DLQ for transient failures
mp.messaging.incoming.orders.failure-strategy=delayed-retry-topic
mp.messaging.incoming.orders.delayed-retry-topic.topics=orders-retry-1,orders-retry-2
mp.messaging.incoming.orders.delayed-retry-topic.timeout=120000
```

Combine with `@Retry` (SmallRye Fault Tolerance) for in-process retries on transient failures before the channel-level strategy kicks in.

## When this might be a false positive

- Critical processing where stopping is the correct outcome (e.g., financial settlement where a malformed record MUST page a human before processing continues).
- Pipelines with strong upstream schema enforcement where reaching `fail` is itself a paging-worthy event.

In these cases the rule should be silenced via `// kafka-lint:disable QK_FAILURE_STRATEGY_FAIL_NO_DLQ` in a properties comment or a project-level allowlist.

## Detection strategy

- Config: if `mp.messaging.incoming.<channel>.connector=smallrye-kafka` is present and neither `failure-strategy` is set to something other than `fail` nor a `dead-letter-queue.topic` is configured, emit WARNING.
- Confidence: MEDIUM — needs operator judgement to confirm intentional.

## References

- https://quarkus.io/guides/kafka (Failure strategies)
- https://smallrye.io/smallrye-reactive-messaging/latest/kafka/receiving-kafka-records/
