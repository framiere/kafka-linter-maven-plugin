# SPRING_ACK_MODE_RECORD_HIGH_THROUGHPUT

**Severity**: WARNING
**Confidence**: CONTEXT
**Detection**: config-file
**Tagline**: AckMode.RECORD on a hot topic is one offset commit per record — say hi to the brokers.

## TL;DR

The linter flags `spring.kafka.listener.ack-mode=RECORD` (or `setAckMode(AckMode.RECORD)` programmatically) as worth a second look. It commits offsets after every record, which on a high-throughput consumer becomes a commit storm against `__consumer_offsets` and starves the broker.

## What's happening (the mechanism)

AckMode values, in order of commit frequency:

- `RECORD`: commit after each processed record. One commit per record.
- `BATCH` (default): commit after each poll's records are processed. One commit per `max.poll.records` records.
- `TIME`: commit when `ackTime` elapses since last commit.
- `COUNT`: commit when `ackCount` records have been processed since last commit.
- `COUNT_TIME`: whichever first.
- `MANUAL` / `MANUAL_IMMEDIATE`: application controls.

`RECORD` gives you the smallest replay window on crash (at most the last record reprocessed) — but commits hit `__consumer_offsets` once per record. On a consumer running at 10k records/sec:

- 10k commit RPCs/sec to the group coordinator.
- 10k entries appended to `__consumer_offsets` partition for that group.
- Increased GC pressure on the coordinator broker.
- Latency overhead per record (each `acknowledge()` may block briefly).

In practice, `RECORD` is the right choice when:
- Records are expensive and rare (one record every few seconds).
- Replay is genuinely unsafe (non-idempotent side effects, and `BATCH` would replay up to `max.poll.records` of them).

For high throughput, `BATCH` is the right default; `COUNT_TIME` is the explicit tunable when you want a bounded replay window.

The static linter can't measure throughput. This rule is a prompt-for-review.

## Operational impact

- `__consumer_offsets` partition size grows fast → log compaction must keep up.
- Group coordinator broker CPU pegged during peak traffic.
- Consumer-side commit latency adds up — micro-batching efficiency lost.
- Symptom: brokers logs warn `Offset commit failed on partition ... : This is not the correct coordinator` during coordinator transitions.

## How to fix

```properties
# CONTEXT-DEPENDENT — review the trade-off
spring.kafka.listener.ack-mode=RECORD

# DEFAULT — commit per poll, lower commit rate
# (remove the line — ack-mode defaults to BATCH)

# GOOD for bounded replay window without per-record commits
spring.kafka.listener.ack-mode=COUNT_TIME
spring.kafka.listener.ack-count=100
spring.kafka.listener.ack-time=5s
```

If the goal is exactly-once-ish processing of non-idempotent side effects, prefer Kafka transactions (`KafkaTransactionManager`) over `AckMode.RECORD` — transactions atomically tie the side effect to the offset commit, with no per-record commit overhead.

## When this might be a false positive

- Low-volume, high-stakes consumers (one record / minute, each triggering a payment). `RECORD` is appropriate.
- Single-record-per-poll patterns (`max.poll.records=1`) — `RECORD` and `BATCH` are equivalent.
- Demo / test applications.

## Detection strategy

- Config: scan for `spring.kafka.listener.ack-mode=RECORD`.
- Bytecode: detect `INVOKEVIRTUAL` to `ContainerProperties.setAckMode(...)` with `AckMode.RECORD` argument.
- The rule should always be CONTEXT-confidence — flag with a "review the trade-off" prompt rather than a hard recommendation.
- Optionally cross-check against `spring.kafka.consumer.max-poll-records` — if very low (1-10), suppress (per-record is fine).

## References

- Spring Kafka — Manually Committing Offsets (AckMode reference): https://docs.spring.io/spring-kafka/reference/kafka/receiving-messages/ooo-commits.html
- Spring Boot — `spring.kafka.listener.ack-mode`: https://docs.spring.io/spring-boot/reference/messaging/kafka.html
