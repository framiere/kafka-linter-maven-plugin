# PRODUCER_MAX_IN_FLIGHT_TOO_HIGH

**Severity**: ERROR
**Confidence**: HIGH
**Detection**: bytecode + config-file
**Tagline**: The broker only remembers your last 5 sequences. Sixth one rewrites history.

## TL;DR

The linter flags `max.in.flight.requests.per.connection > 5`. With idempotence on (the default since Kafka 3.0) the broker tracks at most 5 in-flight sequences per partition; exceeding that either silently disables idempotence or throws `ConfigException`.

## What's happening (the mechanism)

The idempotent producer's broker-side de-duplication cache holds the last five sequence numbers per (PID, topic-partition). With six or more in-flight requests, a retry of an old batch can land outside the window and be treated as a fresh write — defeating exactly-once-per-write semantics.

Behavior depends on whether the conflict is explicit:

- `enable.idempotence=true` explicit + `max.in.flight > 5` explicit → `ConfigException` at producer construction (since Kafka 3.0).
- `enable.idempotence` left default (true) + `max.in.flight > 5` explicit → idempotence silently disabled (KAFKA-13673).
- `enable.idempotence=false` + `max.in.flight > 5` → allowed; retries can re-order messages.

librdkafka clamps `max.in.flight` to 5 silently when idempotence is on — different from the Java client.

## Operational impact

- Silent loss of exactly-once semantics: duplicates appear under retry storms without any error log.
- Out-of-order per-partition writes when retries fire (msg2 lands before msg1 in the broker log).
- Hard-to-diagnose because `producer-metrics:record-send-rate` looks fine; the damage is shape-of-data, not throughput.
- `ConfigException` on startup if both keys are explicit — a build-time failure is the best case here.

## How to fix

```java
// BAD
props.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "10");

// GOOD — drop the property and rely on default (5)
// (omit)

// GOOD — explicit cap
props.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "5");
```

If you need strict per-partition ordering without idempotence, set `max.in.flight.requests.per.connection=1` — but you give up most of the parallelism that the idempotent producer provides for free.

## When this might be a false positive

- Legacy code that deliberately disables idempotence and uses `max.in.flight=1` to preserve ordering — but those code paths set in-flight to 1, not >5, so the rule doesn't fire there.
- Almost none in practice for the >5 case.

## Detection strategy

- Config: key `max.in.flight.requests.per.connection` integer literal greater than 5.
- Bytecode: `Properties.put("max.in.flight.requests.per.connection", <int or string>)` where the value resolves to a constant > 5. HIGH confidence.
- If value is non-constant (read from env), MEDIUM and recommend a manifest-level audit.

## Consult a friend?

> 🤝 **Slow down.** Dropping `max.in.flight` from a high value back to 5 changes throughput and ordering shape in ways that the team may have implicitly come to depend on.
> - Why was the value raised above 5 in the first place? If "for throughput", measure first: with idempotence-induced batching, in-flight=5 is usually within single-digit-percent of in-flight=10. The current setting may be cargo-cult.
> - If `enable.idempotence` was left implicit, KAFKA-13673 means idempotence has been silently *off* — which means ordering during retries was already broken. Re-enabling idempotence will surface latent ordering assumptions in downstream consumers. Are there any?
> - librdkafka clients in the same project (non-JVM polyglot) clamp `max.in.flight` to 5 silently when idempotence is on — the Java fix won't visibly change anything for them. Confirm you're not chasing a Java-only ghost.

## References

- Apache Kafka producer configs — `max.in.flight.requests.per.connection`: https://kafka.apache.org/documentation/#producerconfigs_max.in.flight.requests.per.connection
- librdkafka discussion #4070 — the 5 limit explained: https://github.com/confluentinc/librdkafka/discussions/4070
- KAFKA-13673: https://issues.apache.org/jira/browse/KAFKA-13673
