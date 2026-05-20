# CONSUMER_MAX_POLL_RECORDS_TOO_HIGH

**Severity**: WARNING
**Confidence**: MEDIUM
**Detection**: config-file
**Tagline**: A batch you can't process in max.poll.interval.ms is a batch you'll process twice.

## TL;DR

The linter flags `max.poll.records` set very high (default-leaning interpretation: > 1000 with default `max.poll.interval.ms`) when the application's processing time per record is unknown or non-trivial. A batch too large to process inside `max.poll.interval.ms` triggers a rebalance and re-delivery.

## What's happening (the mechanism)

`max.poll.records` (default 500) is the cap on records returned by a single `poll()`. `max.poll.interval.ms` (default 300000, 5 minutes) is the maximum time between `poll()` calls before the broker assumes the consumer is dead and kicks it out of the group.

If the application takes `T` ms per record:
- One poll iteration processing time = `max.poll.records * T`.
- If `max.poll.records * T > max.poll.interval.ms` → consumer is evicted mid-batch → uncommitted records re-delivered to the new owner → duplicate processing.
- The evicted consumer also gets `CommitFailedException` when it tries to commit, because its `member.id` is no longer valid.

## Operational impact

- Rebalance loops: each timeout triggers a rebalance, which redistributes partitions, which means the new owner reprocesses uncommitted records, takes even longer (cold cache), and times out itself.
- Duplicate processing — every timeout re-delivers the entire poll batch.
- Symptoms: `kafka.consumer:type=consumer-fetch-manager-metrics:records-lag-max` climbing; `kafka.consumer:type=consumer-coordinator-metrics:last-rebalance-seconds-ago` resetting frequently.
- `CommitFailedException: Commit cannot be completed since the group has already rebalanced` in logs.

## How to fix

```java
// BAD
props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "10000");
// processing takes ~50ms/record → 500s per poll → way over 300s default

// GOOD — keep the batch small enough to fit inside the interval
props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "200");
props.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, "300000");
// 200 * 50ms = 10s, well inside the 5-minute interval

// GOOD — if records really are batchable, raise the interval too
props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "5000");
props.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, "900000");
```

## When this might be a false positive

- True streaming / micro-batch pipelines where processing is bulk and known-fast (vectorized writes to a column store).
- Idempotent processors where occasional double-delivery is fine — but rebalance storms are still bad even when delivery is safe.

## Detection strategy

- Config: `max.poll.records` literal > 1000 without an accompanying raised `max.poll.interval.ms`. MEDIUM.
- Cross-key: ratio `max.poll.records / max.poll.interval.ms > 0.01` (i.e., budget < 10ms per record) is suspicious.
- Bytecode: harder; track the put on the Properties and look for sibling puts in the same method scope.

## References

- Apache Kafka consumer configs — `max.poll.records`, `max.poll.interval.ms`: https://kafka.apache.org/documentation/#consumerconfigs_max.poll.records
- KIP-62 — Allow consumer to send heartbeats from a background thread: https://cwiki.apache.org/confluence/display/KAFKA/KIP-62
- Confluent — Tune Kafka consumers: https://docs.confluent.io/platform/current/clients/consumer.html
