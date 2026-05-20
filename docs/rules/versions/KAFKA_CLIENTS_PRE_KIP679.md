# KAFKA_CLIENTS_PRE_KIP679

**Severity**: WARNING
**Confidence**: HIGH
**Detection**: pom-dependency
**Tagline**: A client from before durable defaults — your producer is fire-and-forget unless you said otherwise.

## TL;DR

Any `kafka-clients` version below `3.0.0` ships pre-KIP-679 producer defaults: `acks=1`, `enable.idempotence=false`. Even if your code looks correct, "the default" is no longer a safe default. The linter flags the dependency and recommends either an upgrade or explicit producer configuration.

## The setup

A team upgraded the broker to 3.x or moved to a managed cluster, but the client pinned in the pom is still 2.5 / 2.7 / 2.8 — perhaps because Spring Boot 2.7 (also EOL) pulled it in via its BOM. The team assumes "Kafka makes the right defaults". Until KIP-679, it didn't.

## What's actually happening

KIP-679 (proposed late 2020, shipped properly in Kafka 3.0.1 / 3.1.1 / 3.2.0 after a default-not-being-applied bug — KAFKA-13598) changed two producer defaults:

| Config | Pre-3.0 default | Post-KIP-679 default |
|--------|-----------------|----------------------|
| `acks` | `1` | `all` |
| `enable.idempotence` | `false` | `true` |

The two changes are coupled: idempotence requires `acks=all` and bounded in-flight. The pre-KIP-679 defaults made the producer fast and lossy; the post-KIP-679 defaults make it durable and deduplicating with negligible throughput cost (as the KIP's own benchmarks showed).

On a 2.8 client you get:
- A leader-only ack — leader crash before replication = lost write.
- No producer ID — retried sends become duplicates on the broker.
- No sequence-number window — out-of-order writes per partition under retry.

## Why this is subtle

The code looks identical between versions:

```java
KafkaProducer<String, String> p = new KafkaProducer<>(props);
p.send(new ProducerRecord<>("topic", key, value));
```

A reviewer reading this in a PR has no way to know the client version. The pom is one file away — and often the pom doesn't show the version because Spring Boot / Quarkus / Confluent Platform BOM picks it. So a project that "looks fine" is actually fire-and-forget because of a transitive dependency three levels deep.

There is also a famous trap inside KIP-679 itself: in Kafka 3.0.0 and 3.1.0 a config-validation bug meant idempotence stayed off even when the user thought the default was on (KAFKA-13598). So "I'm on 3.0" is not enough — the linter must treat 3.0.0 and 3.1.0 specifically as still-buggy and require 3.0.1 / 3.1.1 / 3.2.0+ for the default to actually apply.

## Operational impact

- **Silent data loss** on leader crashes (the canonical `acks=1` failure mode).
- **Duplicates on retry** because no PID/sequence-number deduplication on the broker side.
- **Re-ordering** within a partition when `max.in.flight.requests.per.connection > 1` and a retry happens — message N+1 lands before N.
- **Confusing upgrade behavior**: bumping the client to 3.2+ silently turns on idempotence, which in turn requires `IDEMPOTENT_WRITE` ACL on brokers < 2.8. Production teams have been bitten by upgrade-day ACL failures.

## How to fix

Option A — upgrade the client (preferred):

```xml
<dependency>
    <groupId>org.apache.kafka</groupId>
    <artifactId>kafka-clients</artifactId>
    <version>3.9.2</version>
</dependency>
```

Option B — if you really must stay below 3.0, make the durability explicit in code:

```java
props.put(ProducerConfig.ACKS_CONFIG, "all");
props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
props.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "5");
props.put(ProducerConfig.RETRIES_CONFIG, Integer.toString(Integer.MAX_VALUE));
```

Or via properties:

```properties
acks=all
enable.idempotence=true
max.in.flight.requests.per.connection=5
retries=2147483647
```

## When this might be a false positive

- An audit / replay job that only consumes and never produces — the producer defaults are irrelevant. The linter can scope this rule to projects that have a producer in compiled bytecode.
- Connecting to a broker < 2.8 without `IDEMPOTENT_WRITE` ACL granted; durable defaults will fail on that broker. Document this; it's the legitimate exception.

## Detection strategy

- Resolved `kafka-clients` version `< 3.2.0` (treat 3.0.0 and 3.1.0 as buggy-default per KAFKA-13598, so the bar is 3.2.0 for "defaults definitely apply").
- Optional secondary signal: bytecode pass shows a `KafkaProducer` instance — if there is no producer at all, downgrade to INFO or skip.
- Combine with `PRODUCER_ACKS_ONE` / `PRODUCER_IDEMPOTENCE_DISABLED` outputs so the user sees the chain: "pre-KIP-679 client + no explicit override = lossy by default".

## References

- [KIP-679 — Producer will enable the strongest delivery guarantee by default](https://cwiki.apache.org/confluence/display/KAFKA/KIP-679)
- [KAFKA-13598 — idempotence default not applied in 3.0.0 / 3.1.0](https://issues.apache.org/jira/browse/KAFKA-13598)
- [Kafka 3.0 upgrade notes](https://kafka.apache.org/30/getting-started/upgrade/)
