# INFRA_QUARKUS_DEVSERVICES_PROFILE_SCOPED

**Severity**: WARNING
**Confidence**: HIGH
**Detection**: config-file
**Tagline**: Dev Services live in `%dev.` and `%test.`. Anywhere else is a sidecar Kafka nobody told ops about.

## TL;DR

The good-practice form of
[QK_DEVSERVICES_IN_PROD](../quarkus-kafka/QK_DEVSERVICES_IN_PROD.md).
`quarkus.kafka.devservices.*` keys must be scoped to `%dev.` or
`%test.` profiles, never unprofiled and never `%prod.`.

## Cross-reference

See [QK_DEVSERVICES_IN_PROD](../quarkus-kafka/QK_DEVSERVICES_IN_PROD.md)
for the full mechanism (Quarkus starts a Kafka container at startup
when Dev Services is on, points the app at it, data vanishes on
restart).

## What the good-practice adds

The positive pattern: rely on the default. Quarkus enables Dev Services
in `%dev` / `%test` automatically when `kafka.bootstrap.servers` is
unset. Configure `%prod.kafka.bootstrap.servers` (or set
`KAFKA_BOOTSTRAP_SERVERS` env in your deployment) and Dev Services
stays off in prod without needing an explicit disable.

```properties
# yes — let Dev Services default, configure prod explicitly
%dev.quarkus.kafka.devservices.enabled=true
%test.quarkus.kafka.devservices.enabled=true
%prod.kafka.bootstrap.servers=${KAFKA_BOOTSTRAP_SERVERS}
```

Or even simpler, rely on the defaults:

```properties
# yes — defaults handle dev/test; only prod needs explicit servers
%prod.kafka.bootstrap.servers=${KAFKA_BOOTSTRAP_SERVERS}
```

The good-practice rule fires when:

- `quarkus.kafka.devservices.enabled=true` is set unprofiled OR in
  `%prod.`.
- `quarkus.kafka.devservices.*` keys are set unprofiled.

## References

See the linked anti-pattern doc and
https://quarkus.io/guides/kafka-dev-services.
