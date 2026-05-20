# SPRING_BOOT_PRODUCER_ACKS_NOT_ALL

**Severity**: ERROR for acks=0, WARNING for acks=1
**Confidence**: HIGH
**Detection**: config-file
**Tagline**: `spring.kafka.producer.acks=1` ships durability away one record at a time.

## TL;DR

The linter flags `spring.kafka.producer.acks=0` or `=1` in Spring Boot properties. Spring Boot does not override Kafka's `acks` default, so leaving it unset takes Kafka's default — which for modern Kafka (3.0+) is `all`, but Spring Boot's own table shows `1`.

## What's happening (the mechanism)

The producer's `acks` setting decides how many in-sync replicas must acknowledge before the broker responds to the producer:

- `acks=0`: fire-and-forget. The producer never knows whether the broker received the record. No retries because no error to retry.
- `acks=1`: leader-only ack. Survives the IO thread but not a leader failover before replication.
- `acks=all` (Kafka 3.0+ default): leader plus all in-sync replicas. Combined with `min.insync.replicas >= 2` on the broker, durable across single-broker failure.

Spring Boot's own properties table historically lists `1` as the default — but Spring Boot inherits Kafka's default if unset, and the underlying client's default is `all` since Kafka 3.0 (KIP-679). The safe move is to set it explicitly to `all`.

For idempotent producers (`enable.idempotence=true`, the Kafka default since 3.0), `acks` is forced to `all`. Setting it to `0` or `1` while idempotence is enabled raises `ConfigException` at startup.

## Operational impact

- `acks=0`: silent record loss on any network drop. Producer success metrics lie.
- `acks=1`: data loss window equal to the unreplicated tail during leader failover. Often triggered by rolling broker restarts.
- Both: incompatible with idempotent / transactional producers — start-up failure if you also set `enable.idempotence=true` or `transaction-id-prefix`.

## How to fix

```properties
# BAD
spring.kafka.producer.acks=1

# WORSE
spring.kafka.producer.acks=0

# GOOD
spring.kafka.producer.acks=all
# Pair with broker-side topic config: min.insync.replicas=2

# GOOD — rely on Kafka 3.0+ default
# (omit the property entirely)
```

If latency matters more than durability for a specific topic (e.g., metrics ingestion), build a dedicated `KafkaTemplate` bean with `acks=1` and use it only for that topic — don't degrade the application-wide default.

## When this might be a false positive

- Telemetry / metric pipelines on RF=1 dev clusters. `acks=1` and `acks=all` are equivalent there.
- Single-broker local-dev `application-dev.properties`. Tag the file or use profile-scoped suppression.

## Detection strategy

- Config: scan application properties / yml files for `spring.kafka.producer.acks`.
- Flag values `0`, `1`, `'0'`, `'1'` at WARNING.
- Also flag the property in `spring.kafka.producer.properties[acks]` form.
- Cross-check against `spring.kafka.producer.transaction-id-prefix` — if a transaction prefix is set and `acks != all`, escalate to ERROR (will fail at start).
- Confidence: MEDIUM standalone; HIGH when paired with transactions.

## Consult a friend?

> 🤝 **Slow down.** Flipping `spring.kafka.producer.acks=all` is correct, but it also forces the application-wide default — and if there's a `transaction-id-prefix` set or `enable.idempotence=true` anywhere, the producer was *already* using `acks=all` internally; the visible config was lying.
> - For every topic this Spring app produces to: what is the broker-side `min.insync.replicas`? `acks=all` against `min.insync.replicas=1` is just `acks=1` with extra steps.
> - Is there a `transaction-id-prefix` set? If yes, the producer rejected `acks=1` already (`ConfigException`) — which means the application is currently broken, or there's a second `KafkaTemplate` with a different `ProducerFactory` somewhere. Find it before changing the default.
> - If the team wants `acks=1` for a specific telemetry topic, build a dedicated `KafkaTemplate` bean rather than weakening the application-wide property — keep the audit trail of "we explicitly traded durability here".

## References

- Spring Boot — `spring.kafka.producer.acks`: https://docs.spring.io/spring-boot/reference/messaging/kafka.html
- Apache Kafka — `acks`: https://kafka.apache.org/documentation/#producerconfigs_acks
- KIP-679 (acks=all default): https://cwiki.apache.org/confluence/display/KAFKA/KIP-679
- Related core rules: `PRODUCER_ACKS_ZERO`, `PRODUCER_ACKS_ONE`.
