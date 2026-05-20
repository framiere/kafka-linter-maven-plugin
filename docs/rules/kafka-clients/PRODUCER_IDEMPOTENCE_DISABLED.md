# PRODUCER_IDEMPOTENCE_DISABLED

**Severity**: WARNING
**Confidence**: HIGH
**Detection**: bytecode + config-file
**Tagline**: Turning idempotence off in 2026 is undoing five years of work the client did for you.

## TL;DR

The linter flags producers that explicitly set `enable.idempotence=false`. Since Kafka 3.0 the default is `true`; opting out re-introduces duplicates and re-ordering on retry, which broke many systems before KIP-679 fixed it as a default.

## What's happening (the mechanism)

The idempotent producer attaches a producer ID (PID) and a per-partition monotonic sequence number to each batch. The broker maintains a 5-record sequence-number window per (PID, partition) and rejects duplicates or out-of-order arrivals within that window. This makes retries safe: a transient network error followed by a retry yields one logical write, not two.

KIP-679 (Kafka 3.0, fixed properly in 3.0.1 / 3.1.1 / 3.2.0 — KAFKA-13598) flipped the default to `enable.idempotence=true` and `acks=all`. To make the change safe, the client silently disables idempotence if `max.in.flight.requests.per.connection > 5`, `retries=0`, or `acks != all` is set. So `enable.idempotence=false` is now an explicit choice — almost always the wrong one.

## Operational impact

- Duplicate records on the broker after any transient network blip — observable as duplicate offsets in the downstream consumer (same key, same payload, sequential offsets).
- Out-of-order writes per partition when `max.in.flight.requests.per.connection > 1` and a retry happens: msg2 lands before msg1.
- Compaction does not save you: duplicates with the same key still flow through any non-compacted topic.
- `kafka.producer:type=producer-topic-metrics,client-id=*,topic=*` `record-retry-rate` going non-zero during incidents is the trigger window for the duplicate damage.

## How to fix

```java
// BAD
props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "false");
props.put(ProducerConfig.ACKS_CONFIG, "1");

// GOOD — rely on defaults
// (omit both)

// GOOD — explicit but correct
props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
props.put(ProducerConfig.ACKS_CONFIG, "all");
props.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "5");
props.put(ProducerConfig.RETRIES_CONFIG, Integer.toString(Integer.MAX_VALUE));
```

## When this might be a false positive

- Connecting to brokers older than 2.8 without `IDEMPOTENT_WRITE` ACL granted (idempotence fails on the older broker). Then disabling is correct — but rare in 2026.
- Kafka Connect producers default to `enable.idempotence=false` for broad broker-version compatibility — Connect workers are an explicit exception (KAFKA-13759).

## Detection strategy

- Config: key `enable.idempotence` (or `spring.kafka.producer.properties.enable.idempotence`, `mp.messaging.outgoing.*.enable.idempotence`) literal `false`. HIGH.
- Bytecode: `Properties.put("enable.idempotence", "false")` or `ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG`. HIGH when both arguments are constant.

## Consult a friend?

> 🤝 **Slow down.** Removing `enable.idempotence=false` looks like a one-line fix, but the silent-disable rules in KIP-679 mean the producer's *actual* runtime config depends on three other knobs you may also be setting.
> - Is `max.in.flight.requests.per.connection > 5`, `retries=0`, or `acks != all` set anywhere in the same Properties? If yes, flipping idempotence on without fixing those silently turns it back off (KAFKA-13673) and the diff looks like a win.
> - If this producer also has a `transactional.id`, idempotence is already implicitly required — and going from "silently off" to "on" means the next deploy gets a fresh PID and any in-flight transaction from the previous instance gets fenced. Is your deploy strategy compatible with that fence?
> - Kafka Connect producers and pre-2.8 brokers without `IDEMPOTENT_WRITE` ACL are the legitimate exceptions — confirm you're neither before merging.

## References

- KIP-679: https://cwiki.apache.org/confluence/display/KAFKA/KIP-679
- KAFKA-13598 (default bug fix): https://issues.apache.org/jira/browse/KAFKA-13598
- KAFKA-13673 (silent fallback on conflict): https://issues.apache.org/jira/browse/KAFKA-13673
- Apache Kafka producer configs — `enable.idempotence`: https://kafka.apache.org/documentation/#producerconfigs_enable.idempotence
