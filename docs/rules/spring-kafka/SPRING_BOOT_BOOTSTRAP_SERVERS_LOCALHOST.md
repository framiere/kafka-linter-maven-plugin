# SPRING_BOOT_BOOTSTRAP_SERVERS_LOCALHOST

**Severity**: WARNING
**Confidence**: MEDIUM
**Detection**: config-file
**Tagline**: `bootstrap-servers: localhost:9092` shipped to prod is a Friday-evening pager.

## TL;DR

The linter flags `spring.kafka.bootstrap-servers=localhost:9092` (or `127.0.0.1:9092`) outside of `application-test.yml` / `application-dev.yml` / `application-local.yml`. It's almost always a leftover from local development that needs an override in the active profile.

## What's happening (the mechanism)

`spring.kafka.bootstrap-servers` is the entry point for all Kafka clients in a Spring Boot app. Setting it to `localhost:9092` works only when a broker is running on the same host. In any non-trivial deployment (Kubernetes, EC2, on-prem), the broker is elsewhere — and the application fails with `org.apache.kafka.common.errors.TimeoutException: Topic ... not present in metadata after 60000 ms`.

The dangerous pattern is *partially* overridden:

- `application.yml` has `bootstrap-servers: localhost:9092`.
- `application-prod.yml` has the real broker list — but only in `spring.kafka.properties[bootstrap.servers]`, which doesn't override the typed property (depending on bind order).
- Environment-variable override `SPRING_KAFKA_BOOTSTRAP_SERVERS` is set in the deployment but typo'd → reverts to localhost.

Result: pods come up healthy on the HTTP side, then time out on first send/poll, throw, and look "broken in prod but works locally".

## Operational impact

- `TimeoutException` in producer/consumer logs after 60 s of metadata polling.
- `KafkaHealthIndicator` (if enabled) reports `DOWN`. Otherwise the app is UP-but-not-working.
- Pod readiness probes time out only if a custom probe pokes the Kafka path.
- On-call sees deploy-time outage; root cause is a stale `localhost` line in `application.yml`.

## How to fix

```yaml
# BAD — application.yml shipped with localhost
spring:
  kafka:
    bootstrap-servers: localhost:9092

# GOOD — make it placeholder-only; require explicit value at deploy
spring:
  kafka:
    bootstrap-servers: ${KAFKA_BOOTSTRAP_SERVERS:?must be set}

# GOOD — profile-scoped local default
# application.yml
spring:
  kafka:
    bootstrap-servers: ${KAFKA_BOOTSTRAP_SERVERS}

# application-local.yml
spring:
  kafka:
    bootstrap-servers: localhost:9092
```

The `${VAR:?msg}` syntax makes Spring fail-fast if the variable isn't set — better than a silent localhost fallback.

## When this might be a false positive

- The file is named `application-dev.yml`, `application-local.yml`, `application-test.yml`, or under `src/test/resources/`. Suppress those.
- Single-machine deployments. Rare in modern stacks; mark as INFO if detected.

## Detection strategy

- Config: scan all `application*.properties` / `application*.yml` under `src/main/resources/`.
- Flag values matching `localhost:9092` or `127\.0\.0\.1:9092` for `spring.kafka.bootstrap-servers` AND `spring.kafka.properties[bootstrap.servers]`.
- Suppress for profile names matching `(local|dev|test)`.
- Confidence: MEDIUM — file naming heuristics are imperfect, but the rule catches the most common shipping-from-laptop case.

## References

- Spring Boot — `spring.kafka.bootstrap-servers`: https://docs.spring.io/spring-boot/reference/messaging/kafka.html
- Apache Kafka — `bootstrap.servers`: https://kafka.apache.org/documentation/#producerconfigs_bootstrap.servers
