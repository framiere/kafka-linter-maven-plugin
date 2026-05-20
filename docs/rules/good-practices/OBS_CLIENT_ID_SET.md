# OBS_CLIENT_ID_SET

**Severity**: WARNING
**Confidence**: MEDIUM
**Detection**: bytecode + config-file
**Tagline**: `client.id` is the difference between `producer-1` and `orders-svc-prod-pod-3` in broker logs.

## TL;DR

The good-practice form of [CLIENT_ID_MISSING](../kafka-clients/CLIENT_ID_MISSING.md).
Every producer, consumer, and Streams application must set
`client.id`, ideally derived from `<app>-<env>-<role>-<instance>`.
The anti-pattern doc handles the why; this one captures the convention.

## Cross-reference

See [CLIENT_ID_MISSING](../kafka-clients/CLIENT_ID_MISSING.md) for the
mechanism — `client.id` appears in broker request logs, JMX metric
tags, audit logs, and is the key for per-application quotas.

## What the good-practice adds

A convention:

- Producers: `<app>-prod-producer-<pod-ordinal>` (e.g.
  `orders-svc-prod-producer-3`).
- Consumers: `<app>-prod-consumer-<group>-<pod-ordinal>`. Pair with
  `group.instance.id` for static membership.
- Streams: Streams sets a `client.id` derived from `application.id`
  automatically; if you want a per-instance suffix, set
  `client.id=<application.id>-<pod-ordinal>` explicitly.

For Spring Boot:

```yaml
spring:
  kafka:
    client-id: orders-svc-${POD_NAME:local}
    consumer:
      client-id: orders-svc-consumer-${POD_NAME:local}
    producer:
      client-id: orders-svc-producer-${POD_NAME:local}
```

For Quarkus, set
`kafka.client.id-prefix=orders-svc-` or use per-channel `client-id`
properties.

## References

See the linked anti-pattern doc, KIP-371, and the Apache Kafka quota
docs.
