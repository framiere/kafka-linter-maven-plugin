# PRODUCER_RETRIES_ZERO

**Severity**: WARNING
**Confidence**: MEDIUM
**Detection**: both
**Tagline**: retries=0 means every transient network blip is a permanent failure.

## TL;DR

The linter flags producers that set `retries=0` (or any value < `Integer.MAX_VALUE` that is suspiciously low) — it disables retry of transient broker errors and forces every `NotLeaderForPartitionException`, `NetworkException`, or `RequestTimedOutException` into a user-visible `send()` failure.

## What's happening (the mechanism)

`retries` caps the number of times the producer's internal `Sender` thread will re-enqueue a batch that failed with a retriable error. Defaults: `retries=Integer.MAX_VALUE` since Kafka 2.1. The overall retry window is bounded by `delivery.timeout.ms` (default 2 minutes, KIP-91), not by retry count — so leaving `retries` at the default is safe.

Setting `retries=0` also conflicts with `enable.idempotence=true`. Since Kafka 3.0:

- Explicit `enable.idempotence=true` + `retries=0` → `ConfigException`.
- Implicit idempotence + `retries=0` → idempotence silently disabled (KAFKA-13673).

So `retries=0` is both a durability regression and a stealth idempotence kill switch.

## Operational impact

- Every leader election / network partition / broker restart raises a `Callback` failure to the application; teams typically catch and log, then lose the record.
- `producer-metrics:record-error-rate` spikes during normal cluster operations (rolling restarts) instead of being absorbed by the client.
- Loss of idempotence guarantee when set alongside `enable.idempotence` default-true (silent).
- High pager noise — a healthy cluster operation surfaces as an application-level "Kafka broken" alert.

## How to fix

```java
// BAD
props.put(ProducerConfig.RETRIES_CONFIG, "0");

// GOOD — rely on default (Integer.MAX_VALUE)
// (omit)

// GOOD — explicit
props.put(ProducerConfig.RETRIES_CONFIG, Integer.toString(Integer.MAX_VALUE));
props.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, "120000");
```

The right knob to bound retry effort is `delivery.timeout.ms`, not `retries`.

## When this might be a false positive

- Pipelines where the application implements its own retry/dead-letter logic and explicitly wants `send()` failures to surface. Document the choice and downgrade the rule for that module.
- Throw-away one-shot CLI tools.

## Detection strategy

- Config: key `retries` literal `0`. MEDIUM confidence.
- Bytecode: `Properties.put("retries", "0")`. MEDIUM confidence.
- HIGH confidence when paired with `enable.idempotence=true` in the same config (this is a `ConfigException` at runtime).

## References

- Apache Kafka producer configs — `retries`, `delivery.timeout.ms`: https://kafka.apache.org/documentation/#producerconfigs_retries
- KIP-91 — delivery.timeout.ms: https://cwiki.apache.org/confluence/display/KAFKA/KIP-91+Provide+Intuitive+User+Timeouts+in+The+Producer
- KAFKA-13673: https://issues.apache.org/jira/browse/KAFKA-13673
