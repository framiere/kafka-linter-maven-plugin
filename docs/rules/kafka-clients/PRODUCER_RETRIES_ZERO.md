# PRODUCER_RETRIES_ZERO

**Severity**: WARNING
**Confidence**: MEDIUM
**Detection**: bytecode + config-file
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

## Consult a friend?

> 🤝 **Slow down.** `retries=0` was almost always set because someone hit "duplicate messages" once and assumed retries were the culprit. Removing it without fixing the actual duplicate-detection knobs trades one bug for another.
> - Was `retries=0` added to "stop duplicates"? If yes: idempotence (default-true since 3.0) is what prevents duplicates — but `retries=0` silently disabled idempotence (KAFKA-13673). Removing `retries=0` re-enables it; the team needs to verify the original duplicate problem is actually gone, not just papered over.
> - Does any caller treat a `send()` failure as "this record is lost, log it and move on"? With `retries=Integer.MAX_VALUE` + `delivery.timeout.ms=120000`, that handler now fires only after 2 minutes of blocking on the callback thread — confirm the calling code can wait that long.
> - If the application has its own DLT / retry logic on top, set `delivery.timeout.ms` short enough that the surfaced failure still arrives within the application's SLO. Don't leave both retry layers at their default — they compound.

## References

- Apache Kafka producer configs — `retries`, `delivery.timeout.ms`: https://kafka.apache.org/documentation/#producerconfigs_retries
- KIP-91 — delivery.timeout.ms: https://cwiki.apache.org/confluence/display/KAFKA/KIP-91+Provide+Intuitive+User+Timeouts+in+The+Producer
- KAFKA-13673: https://issues.apache.org/jira/browse/KAFKA-13673
