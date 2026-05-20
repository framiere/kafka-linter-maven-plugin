# QK_DEVSERVICES_IN_PROD

**Severity**: ERROR
**Confidence**: HIGH
**Detection**: config-file
**Tagline**: Dev Services in production is a Kafka in a sidecar that nobody told ops about.

## TL;DR

`quarkus.kafka.devservices.enabled=true` set in unprofiled `application.properties` (or in `%prod`) causes Quarkus to start a Kafka container at production launch. The app silently connects to ephemeral storage that vanishes on restart.

## What's happening (the mechanism)

Quarkus Dev Services for Kafka starts a Kafka broker container automatically in dev/test mode when no `kafka.bootstrap.servers` is configured. It is *disabled* by default in production (`%prod`) unless explicitly enabled.

Common ways operators trip this:
- Hardcoded `quarkus.kafka.devservices.enabled=true` in `application.properties` (unprofiled → applies everywhere).
- Forgetting that `application-prod.properties` doesn't override an unprofiled `application.properties` key with `false` unless explicitly set.

The Quarkus container starts, binds to a random port, and the app reads `kafka.bootstrap.servers` from `quarkus.kafka.devservices.discovery` — i.e., points itself at the ephemeral container.

## Operational impact

- App "works" in prod for a while.
- Pod restart → fresh Kafka container → all topics gone → silent data loss.
- Horizontal scaling → each pod has its own private Kafka. Messages don't reach other instances.
- Docker required on the host; in environments without Docker (most prod K8s), startup fails confusingly.

## How to fix

```properties
# BAD — Dev Services active everywhere
quarkus.kafka.devservices.enabled=true

# GOOD — only in dev/test profiles
%dev.quarkus.kafka.devservices.enabled=true
%test.quarkus.kafka.devservices.enabled=true
%prod.kafka.bootstrap.servers=kafka.prod.svc.cluster.local:9092
```

Better: rely on the default. Dev Services is on by default in `%dev` and `%test` when `kafka.bootstrap.servers` isn't set. Just configure `kafka.bootstrap.servers` for `%prod` (or via env var `KAFKA_BOOTSTRAP_SERVERS`) and Dev Services stays off in prod.

## When this might be a false positive

- Truly never. Profile your config.

## Detection strategy

- Config: `quarkus.kafka.devservices.enabled=true` set as an unprofiled key (no `%dev.`/`%test.` prefix).
- Also flag any `quarkus.kafka.devservices.*` key in `%prod.` profile.
- Confidence: HIGH.

## References

- https://quarkus.io/guides/kafka-dev-services
- https://quarkus.io/guides/config-reference (Profiles)
