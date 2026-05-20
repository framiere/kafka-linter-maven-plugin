# SPRING_BOOT_AUTO_OFFSET_RESET_LATEST

**Severity**: WARNING
**Confidence**: MEDIUM
**Detection**: config-file
**Tagline**: `auto-offset-reset: latest` means "lose every record produced before the first deploy".

## TL;DR

The linter flags `spring.kafka.consumer.auto-offset-reset=latest` (which is also the default — flag when explicitly set, and warn when not set and a `@KafkaListener` exists). On first deploy or after group deletion, the consumer skips everything currently in the topic and only sees new records.

## What's happening (the mechanism)

`auto.offset.reset` controls what happens when a consumer group has no committed offset for an assigned partition:

- `latest` (Kafka default and Spring Boot default if unset): seek to high-water-mark. Records already in the partition are ignored.
- `earliest`: seek to log-start. Reprocess everything still on disk (subject to retention).
- `none`: throw `NoOffsetForPartitionException` and fail.

The "no committed offset" case happens more often than people think:

- First deploy of a new consumer group.
- Manual offset reset (operator ran `kafka-consumer-groups --reset-offsets`).
- New partitions added to an existing topic (new partition has no committed offset for the group).
- Topic deleted and recreated.
- Consumer-group expiration after `offsets.retention.minutes` (default 7 days, raised to 7 days from 1 day in KIP-186) of inactivity.

For event-sourcing / audit / replay topics, `earliest` is what you want — record loss on first deploy is bad. For ephemeral metric streams or change-data-capture replicas where catch-up is meaningless, `latest` is fine.

The static linter cannot know your intent; the warning is a prompt to make the choice explicit.

## Operational impact

- First deploy: consumer joins, sees the last 0 records, reports caught-up. Records produced before deploy are gone (still in the topic, but never consumed by this group).
- Operator runs offset reset → same data loss.
- Symptoms: downstream system shows gaps right around the deploy timestamp.

## How to fix

```properties
# BAD or DEFAULT — silent for an event-sourced consumer
# spring.kafka.consumer.auto-offset-reset=latest

# GOOD — explicit choice; pick based on semantics
spring.kafka.consumer.auto-offset-reset=earliest

# GOOD — if you genuinely want new-only
spring.kafka.consumer.auto-offset-reset=latest
# (suppression comment for the linter: "intentional - new-only")
```

For per-listener override, set `properties = "auto.offset.reset:earliest"` on the `@KafkaListener` annotation.

## When this might be a false positive

- High-volume telemetry / metric ingestion topics where reprocessing is meaningless. `latest` is the right answer.
- Reporting consumers that should not replay history. Explicit `latest` is intentional.

In both cases, the recommendation is to set the property *explicitly* and add a suppression — the linter is asking for intent, not forbidding `latest`.

## Detection strategy

- Config: scan `application.properties`, `application.yml`, profile variants, and `bootstrap.yml` for `spring.kafka.consumer.auto-offset-reset`.
- Flag value `latest` (explicit) at WARNING.
- Flag absence (relies on default) at INFO when a `@KafkaListener` is present.
- Also flag the property under `spring.kafka.consumer.properties[auto.offset.reset]` form.
- Confidence: MEDIUM — the linter can't infer intent, but the prompt-to-choose is high-value.

## References

- Spring Boot — `spring.kafka.consumer.auto-offset-reset`: https://docs.spring.io/spring-boot/reference/messaging/kafka.html
- Apache Kafka — `auto.offset.reset`: https://kafka.apache.org/documentation/#consumerconfigs_auto.offset.reset
- Related core rule: `CONSUMER_AUTO_OFFSET_RESET_LATEST` in kafka-clients ruleset.
