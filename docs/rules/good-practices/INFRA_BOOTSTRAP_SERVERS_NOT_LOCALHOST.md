# INFRA_BOOTSTRAP_SERVERS_NOT_LOCALHOST

**Severity**: WARNING
**Confidence**: MEDIUM
**Detection**: config-file
**Tagline**: `localhost` in `src/main` is a deploy-time outage waiting for Friday.

## TL;DR

The good-practice form of
[SPRING_BOOT_BOOTSTRAP_SERVERS_LOCALHOST](../spring-kafka/SPRING_BOOT_BOOTSTRAP_SERVERS_LOCALHOST.md)
and [QK_LOCALHOST_IN_PROD](../quarkus-kafka/QK_LOCALHOST_IN_PROD.md).
The positive practice: `bootstrap.servers` outside of dev / test / local
profiles must NOT be a literal `localhost` or `127.0.0.1`. Use a
required env var with fail-fast resolution (`${KAFKA_BOOTSTRAP_SERVERS:?}`)
or a profile-scoped key.

## Cross-reference

See the linked Spring and Quarkus anti-pattern docs for the mechanism
(typed-vs-flat key resolution, env-var bind order).

## What the good-practice adds

A pattern, not just a "don't":

```yaml
# yes — fail fast in main; let dev profile override
spring:
  kafka:
    bootstrap-servers: ${KAFKA_BOOTSTRAP_SERVERS:?KAFKA_BOOTSTRAP_SERVERS must be set}

---
# application-local.yml
spring:
  kafka:
    bootstrap-servers: localhost:9092
```

```properties
# yes — Quarkus, env-var with required-set fallback
kafka.bootstrap.servers=${KAFKA_BOOTSTRAP_SERVERS:none}
%dev.kafka.bootstrap.servers=PLAINTEXT://localhost:9092
%test.kafka.bootstrap.servers=PLAINTEXT://localhost:9092
```

The required-set sentinel (`:?` in Spring,
explicit error key in Quarkus) means the app refuses to start when
nobody set the env var — better than a silent localhost fallback that
goes through readiness checks and only fails on first send.

## References

See the linked anti-pattern docs.
