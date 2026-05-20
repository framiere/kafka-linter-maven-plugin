# QK_PARTITIONS_DEPRECATED

**Severity**: WARNING
**Confidence**: HIGH
**Detection**: config-file
**Tagline**: `partitions` is dead. Long live `concurrency`.

## TL;DR

`mp.messaging.incoming.<channel>.partitions` is deprecated in SmallRye Reactive Messaging. Use `mp.messaging.incoming.<channel>.concurrency` instead. The old property still works but is on the path to removal.

## What's happening (the mechanism)

`partitions` was the original SmallRye knob for controlling how many concurrent consumers (each polling a subset of partitions) the connector would create. It was renamed to `concurrency` to better match MicroProfile Reactive Messaging conventions.

The properties are not 100% equivalent:
- `partitions` was specifically tied to Kafka partition assignment.
- `concurrency` is a generic channel-level concurrency knob with consistent semantics across connectors.

Setting both can cause confusing precedence behavior. Setting only the deprecated one leaves you on a removal trajectory.

## Operational impact

- No immediate runtime breakage.
- Warning in logs at startup: `partitions attribute is deprecated, use concurrency instead`.
- Risk of silent removal in a future SmallRye major version.

## How to fix

```properties
# BAD
mp.messaging.incoming.orders.partitions=4

# GOOD
mp.messaging.incoming.orders.concurrency=4
```

If you also see `throttled.ordered` or `throttled.ordered.max-concurrency`, those are also deprecated → use `ordered` and `ordered.max-concurrency`.

## When this might be a false positive

- Pinned to an older SmallRye version where `concurrency` doesn't exist.

## Detection strategy

- Config: `mp.messaging.incoming.<channel>.partitions=*` OR `mp.messaging.incoming.<channel>.throttled.ordered=*`.
- Confidence: HIGH.

## References

- https://smallrye.io/smallrye-reactive-messaging/latest/kafka/receiving-kafka-records/
