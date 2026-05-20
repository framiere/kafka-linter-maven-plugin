# QK_AUTO_OFFSET_RESET_LATEST

**Severity**: WARNING
**Confidence**: MEDIUM
**Detection**: config-file
**Tagline**: auto.offset.reset=latest skips everything on a fresh consumer group.

## TL;DR

The default `auto.offset.reset=latest` means a new consumer group starts reading from the END of the topic — every message produced before it joined is invisible. For first deployments, replays, or recovered groups, this is often not what the operator intended.

## What's happening (the mechanism)

`auto.offset.reset` controls what Kafka does when a consumer group has no committed offset for a partition (new group, expired offset, or topic added to subscription):

- `latest` (default): start from the latest message. New messages from this point forward.
- `earliest`: start from the beginning of the topic.
- `none`: throw an exception. Forces the operator to make a choice.

`latest` is reasonable for telemetry pipelines (you don't care about history). It's a footgun for event-sourced systems where the consumer group is rebuilt and *must* replay history.

The harder case: a consumer group expires due to inactivity (`offsets.retention.minutes`, default 7 days). On the next startup, `auto.offset.reset=latest` skips everything that arrived during the downtime.

## Operational impact

- Migration / first-deploy: messages produced before consumer startup are silently skipped.
- Long downtime: consumer "catches up" by skipping the gap.
- No log line; the consumer just starts processing from "now."

## How to fix

```properties
# Explicit choice based on use case:

# Event-sourced / replay-required pipeline
mp.messaging.incoming.orders.auto.offset.reset=earliest

# Telemetry / no-history pipeline
mp.messaging.incoming.metrics.auto.offset.reset=latest

# Force operator decision via runbook
mp.messaging.incoming.audit.auto.offset.reset=none
```

For "always replay everything on restart" use a unique group id (`group.id=${quarkus.uuid}`) + `auto.offset.reset=earliest`.

## When this might be a false positive

- Telemetry / live-tailing pipelines where `latest` is genuinely correct.

This rule is best as INFO when not set, WARNING when set to `latest` AND the channel is critical (heuristic on naming).

## Detection strategy

- Config: presence of `mp.messaging.incoming.<channel>.connector=smallrye-kafka` AND `auto.offset.reset` unset OR `latest`.
- Confidence: MEDIUM — choice depends on use case.

## References

- https://kafka.apache.org/documentation/#consumerconfigs_auto.offset.reset
- https://smallrye.io/smallrye-reactive-messaging/latest/kafka/receiving-kafka-records/
