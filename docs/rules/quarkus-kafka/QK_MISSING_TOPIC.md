# QK_MISSING_TOPIC

**Severity**: WARNING
**Confidence**: MEDIUM
**Detection**: config-file
**Tagline**: No topic property? Hope your channel name is the topic name you wanted.

## TL;DR

If `mp.messaging.{incoming|outgoing}.<channel>.topic` is unset, SmallRye defaults the topic to the channel name. This is convenient until your channel name (`orders-consumer-v2`) drifts from your topic name (`orders`) and the consumer subscribes to a topic that doesn't exist.

## What's happening (the mechanism)

SmallRye's Kafka connector resolves the topic in this order:
1. `topic` attribute, if set.
2. `topics` (plural) for incoming, when subscribing to multiple topics.
3. `pattern` for regex subscription.
4. Channel name as fallback.

The fallback is fine for greenfield apps where channel = topic. It bites when:
- The channel is renamed (`orders` → `orders-v2`) without updating the topic.
- The topic is renamed without renaming the channel.
- Multiple environments use the same channel name but different topic names.

Behavior on missing topic:
- Producer: creates the topic if `auto.create.topics.enable=true` on the broker (usually disabled in prod) → otherwise fails on send.
- Consumer: subscribes to a non-existent topic; in modern Kafka, this is allowed and the consumer just sits idle until the topic exists.

## Operational impact

- Silent consumer idleness: looks healthy, no errors, no records.
- Producer fails on first send with `UNKNOWN_TOPIC_OR_PARTITION`.
- Diff between dev (channel = topic) and prod (channel ≠ topic) hides issues until deploy.

## How to fix

```properties
# GOOD — explicit topic
mp.messaging.incoming.orders.connector=smallrye-kafka
mp.messaging.incoming.orders.topic=orders.v2
mp.messaging.incoming.orders.value.deserializer=...
```

## When this might be a false positive

- Channel name intentionally matches topic name.
- Greenfield app where the convention is uniform.

This rule is best as INFO (style enforcement) rather than ERROR.

## Detection strategy

- Config: `mp.messaging.{incoming|outgoing}.<channel>.connector=smallrye-kafka` with no `topic`, `topics`, or `pattern` set.
- Confidence: MEDIUM — heuristic.

## References

- https://smallrye.io/smallrye-reactive-messaging/latest/kafka/receiving-kafka-records/
- https://quarkus.io/guides/kafka (Channel configuration)
