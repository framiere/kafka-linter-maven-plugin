# QK_CHANNEL_NAME_COLLISION

**Severity**: ERROR
**Confidence**: HIGH
**Detection**: bytecode + config-file
**Tagline**: Same channel name for in and out wires your app to itself.

## TL;DR

Using the same channel name for both `mp.messaging.incoming.<X>` and `mp.messaging.outgoing.<X>` creates an in-memory loop within the application — incoming and outgoing on the same name don't reach Kafka; they short-circuit through SmallRye's in-memory channel.

## What's happening (the mechanism)

SmallRye's channel routing:
- A channel name with both incoming and outgoing configured is treated as an in-memory channel; the outgoing produces directly to the incoming subscriber in the same JVM.
- A channel name with `connector=smallrye-kafka` set is routed to Kafka.
- If you set `connector=smallrye-kafka` only on the incoming side and have an `@Outgoing("X")` method in code, SmallRye complains at startup about a missing outgoing connector.
- If the user "fixes" this by adding `connector=smallrye-kafka` to outgoing too, BOTH sides bind to Kafka — fine if intended, often a footgun if the user meant in-memory.

The opposite case is just as common: an `@Outgoing("orders-processed")` writing to Kafka, and an `@Incoming("orders-processed")` in the same app meaning to read from Kafka. With no `connector=smallrye-kafka` set on either side, SmallRye wires them in-memory and skips Kafka entirely. Production "looks" fine until you scale to two pods and the second pod's outgoing is invisible to the first pod's incoming.

## Operational impact

- Producer metrics show records being sent. Consumer metrics show records being received. But Kafka admin tools show no traffic on the topic.
- Horizontal scaling breaks: each pod's incoming/outgoing is bound to its own in-memory channel; cross-pod traffic vanishes.

## How to fix

Use distinct channel names AND explicit connectors:

```properties
# GOOD — explicit Kafka, distinct names
mp.messaging.incoming.orders-in.connector=smallrye-kafka
mp.messaging.incoming.orders-in.topic=orders

mp.messaging.outgoing.orders-out.connector=smallrye-kafka
mp.messaging.outgoing.orders-out.topic=orders-processed
```

```java
@Incoming("orders-in")
@Outgoing("orders-out")
public Order process(Order o) { ... }
```

## When this might be a false positive

- Genuinely in-memory channels for intra-app routing (no Kafka involvement intended). Detector should suppress when `connector=smallrye-kafka` is absent on both sides AND the user has explicitly named them clearly (heuristic).

## Detection strategy

- Config + bytecode: find pairs of `@Incoming("X")` and `@Outgoing("X")` in the same module. If `mp.messaging.incoming.X.connector=smallrye-kafka` AND `mp.messaging.outgoing.X.connector=smallrye-kafka`, that's likely a typo. If neither has the kafka connector, that's an in-memory channel — silent if unintentional.
- Confidence: HIGH for the same-channel-different-direction-different-intent pattern.

## References

- https://smallrye.io/smallrye-reactive-messaging/latest/concepts/channel-decorator/
- https://quarkus.io/guides/kafka
