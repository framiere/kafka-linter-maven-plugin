# OBS_QUARKUS_CLIENT_ID_PREFIX

**Severity**: WARNING
**Confidence**: HIGH
**Detection**: config-file
**Tagline**: Quarkus has a `client-id` knob too. SmallRye RM won't write one for you.

## TL;DR

The linter flags Quarkus projects that don't set `kafka.client.id`
(global) or per-channel `mp.messaging.<incoming|outgoing>.<channel>.client.id`.
Same operational gap as the Spring rule
([OBS_SPRING_CLIENT_ID_PREFIX](./OBS_SPRING_CLIENT_ID_PREFIX.md)) but
on the SmallRye Reactive Messaging Kafka connector.

## The setup

Quarkus' Kafka client and the SmallRye RM Kafka connector both pass
through the underlying Apache Kafka client configs. `client.id` is one
of them. Set globally via `kafka.client.id` or per-channel via the
`client.id` attribute under `mp.messaging.*`.

Unlike Spring Boot, Quarkus does NOT have a separate "prefix" config —
the value is the literal `client.id` sent to the broker.

## What's actually happening (the mechanism)

When the connector creates a `KafkaProducer` / `KafkaConsumer`, it
merges:

- `kafka.*` keys (global)
- `mp.messaging.<incoming|outgoing>.<channel>.*` keys (per channel)
- Channel-specific overrides

If `client.id` is absent at every level, the underlying client falls
back to `producer-N` / `consumer-N`.

## How to fix (no → yes)

```properties
# no — no client.id at any level
kafka.bootstrap.servers=...
mp.messaging.incoming.orders.topic=orders

# yes — global prefix-style + per-channel
kafka.client.id=orders-svc
mp.messaging.incoming.orders.client.id=orders-svc-consumer-orders
mp.messaging.outgoing.invoices.client.id=orders-svc-producer-invoices
```

For containerized deployments, fold in the pod name:

```properties
kafka.client.id=orders-svc-${HOSTNAME:local}
mp.messaging.incoming.orders.client.id=orders-svc-consumer-orders-${HOSTNAME:local}
```

## When this might be a false positive

- Dev profile with Dev Services Kafka.
- Tests using `@QuarkusTest` with embedded Kafka.

## Detection strategy

- **Config files:** flag if `kafka.client.id` is absent AND no per-channel
  `mp.messaging.*.client.id` is present.
- Suppress in `%dev.` and `%test.` scoped keys.
- **Confidence: HIGH** — provable from config absence.

## References

- Quarkus — Apache Kafka reference:
  https://quarkus.io/guides/kafka
- SmallRye Reactive Messaging — Kafka connector:
  https://smallrye.io/smallrye-reactive-messaging/latest/kafka/kafka/
- Cross-link: [CLIENT_ID_MISSING](../kafka-clients/CLIENT_ID_MISSING.md)
