# QK_HEALTH_DISABLED

**Severity**: WARNING
**Confidence**: HIGH
**Detection**: config-file
**Tagline**: A channel without health checks is a channel without an alarm.

## TL;DR

Setting both `mp.messaging.incoming.<channel>.health-enabled=false` and `health-readiness-enabled=false` removes the channel from SmallRye's health reporting. A stuck or crashed channel becomes invisible to liveness, readiness, and the load balancer.

## What's happening (the mechanism)

SmallRye Reactive Messaging contributes a `SmallRyeHealthCheck` group for each registered channel:
- `health-enabled` (default `true`): includes the channel in *liveness* aggregation. If the channel can't poll, the pod liveness flips DOWN.
- `health-readiness-enabled` (default `true`): includes the channel in *readiness*. If the channel can't reach the broker, the pod is marked NOT READY and load balancers stop routing to it.

Disabling both turns the channel into a silent dependency. `smallrye-health` reports UP even when the channel is dead.

## Operational impact

- Stuck channel + healthy `/q/health` → traffic continues hitting the pod's HTTP endpoints while Kafka work piles up undetected.
- Probes are the only signal you have for "is this consumer doing its job?" — removing them removes the signal.
- `kafka_consumer_records_consumed_total` flatlines but nothing pages.

## How to fix

```properties
# BAD
mp.messaging.incoming.orders.health-enabled=false
mp.messaging.incoming.orders.health-readiness-enabled=false

# GOOD — defaults; just don't override
# (omit both; both default to true)

# If you must disable readiness (e.g., a startup task channel), keep liveness:
mp.messaging.incoming.startup-tasks.health-readiness-enabled=false
# (leave health-enabled=true so a crash still flips liveness)
```

For pure observability on the broker side, also consider `health-topic-verification-enabled=true` (requires admin permission) for end-to-end checking.

## When this might be a false positive

- Channels intentionally optional (e.g., feature-flagged ingestion) where downtime is acceptable.
- In-memory channels (not Kafka-backed) where the readiness check is pointless.

## Detection strategy

- Config: both `mp.messaging.incoming.<channel>.health-enabled=false` AND `health-readiness-enabled=false` (or all `health-*=false`).
- Confidence: HIGH for outgoing/incoming Kafka channels.

## References

- https://smallrye.io/smallrye-reactive-messaging/latest/kafka/receiving-kafka-records/
- https://quarkus.io/guides/kafka (Health reporting)
