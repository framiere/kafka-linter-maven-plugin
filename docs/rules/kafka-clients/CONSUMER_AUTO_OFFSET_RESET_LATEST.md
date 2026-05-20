# CONSUMER_AUTO_OFFSET_RESET_LATEST

**Severity**: WARNING
**Confidence**: MEDIUM
**Detection**: both
**Tagline**: auto.offset.reset=latest is "skip the backlog you didn't know you had."

## TL;DR

The linter flags consumers with `auto.offset.reset=latest` in a processing pipeline. On first start, a re-deployed group, or a deleted offsets topic entry, the consumer silently jumps past existing data instead of replaying it.

## What's happening (the mechanism)

`auto.offset.reset` controls behavior when the consumer has no committed offset (new group, expired offsets, deleted topic-partition):

- `latest` (default): start from the next record produced after `poll()`. **Existing data is skipped.**
- `earliest`: start from the partition's `log-start-offset`. Full backfill.
- `none`: throw `NoOffsetForPartitionException` — force the operator to decide.

For analytics, ETL, or any "at-least-once processing of all events" pipeline, `latest` is the wrong default. It hides data loss as "we just started after the gap."

This compounds with offsets expiration: `offsets.retention.minutes` (broker, default 7 days). If a consumer group goes idle for longer, its committed offsets are deleted and the next start re-runs `auto.offset.reset` — silently skipping the gap.

## Operational impact

- New deployment in a fresh environment misses everything produced before it came up — looks like the upstream "wasn't sending" until someone diffs offsets.
- After an offsets-topic incident, all consumers fast-forward to "now" and never reprocess the gap.
- Lag jumps to zero in `kafka-consumer-groups.sh --describe` — looks healthy, is not.
- Common root cause of "we lost a day of events" postmortems.

## How to fix

```java
// BAD (or default — but unspoken)
props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");

// GOOD — for processing pipelines that must not skip data
props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

// GOOD — explicit operator decision required
props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "none");
// then handle NoOffsetForPartitionException explicitly
```

`latest` is right for ephemeral UI tailing (live event view) and the audit/log sidecars that only care about "now".

## When this might be a false positive

- Live dashboards, real-time tail consumers, side-cars showing "latest".
- Pure metrics-emit topics where backlog is meaningless.

## Detection strategy

- Config: `auto.offset.reset=latest` (literal) in any consumer Properties source. MEDIUM (sometimes intentional).
- Bytecode: `Properties.put("auto.offset.reset", "latest")`. MEDIUM.
- Confidence: bump to HIGH if the consumer's `group.id` is unset (anonymous consumer with `latest` is almost certainly a mistake).

## References

- Apache Kafka consumer configs — `auto.offset.reset`: https://kafka.apache.org/documentation/#consumerconfigs_auto.offset.reset
- Apache Kafka — Consumer position: https://kafka.apache.org/documentation/#impl_consumerposition
- Confluent blog — Reset offsets safely: https://docs.confluent.io/platform/current/clients/consumer.html
