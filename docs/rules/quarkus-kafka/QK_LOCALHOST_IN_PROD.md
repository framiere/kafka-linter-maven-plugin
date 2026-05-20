# QK_LOCALHOST_IN_PROD

**Severity**: ERROR
**Confidence**: HIGH
**Detection**: config-file
**Tagline**: localhost:9092 in production: this isn't your laptop.

## TL;DR

Hardcoded `kafka.bootstrap.servers=localhost:9092` in unprofiled `application.properties` (or under `%prod`) almost certainly indicates a config that was never set for the production environment. The SmallRye default is also `localhost:9092` — so an unset key is just as broken.

## What's happening (the mechanism)

`kafka.bootstrap.servers` defaults to `localhost:9092` if unset. In a containerized environment, that targets:
- The pod itself (no Kafka there).
- The localhost interface inside the container's network namespace (still no Kafka).

The app starts, the producer's metadata refresh fails with `Connection to node -1 (localhost/127.0.0.1:9092) could not be established`, and the consumer never receives a record.

## Operational impact

- Producer: `org.apache.kafka.common.errors.TimeoutException: Topic <foo> not present in metadata after 60000 ms`.
- Consumer: `WARN o.a.k.c.NetworkClient - [Consumer ...] Bootstrap broker localhost:9092 (id: -1 rack: null) disconnected`.
- Pod liveness flaps; either crashloops or runs degraded forever, depending on the health setup.

## How to fix

```properties
# BAD
kafka.bootstrap.servers=localhost:9092

# GOOD — env-var-friendly, profile-aware
%dev.kafka.bootstrap.servers=localhost:9092
%test.kafka.bootstrap.servers=localhost:9092
%prod.kafka.bootstrap.servers=${KAFKA_BOOTSTRAP_SERVERS:kafka.prod.svc.cluster.local:9092}
```

Or rely on Dev Services for `%dev`/`%test` (don't set `kafka.bootstrap.servers` at all in those profiles) and require `KAFKA_BOOTSTRAP_SERVERS` env var in production.

## When this might be a false positive

- Local-only sample apps.
- Integration tests that bind to a local broker via a known port.

The rule should only fire on unprofiled or `%prod`-profiled keys.

## Detection strategy

- Config: `kafka.bootstrap.servers=localhost:9092` (or `127.0.0.1:9092`) in unprofiled section or `%prod.` profile.
- Confidence: HIGH.

## References

- https://quarkus.io/guides/kafka (Configuration)
- https://smallrye.io/smallrye-reactive-messaging/latest/kafka/receiving-kafka-records/
