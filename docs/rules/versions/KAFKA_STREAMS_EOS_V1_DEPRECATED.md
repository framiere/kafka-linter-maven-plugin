# KAFKA_STREAMS_EOS_V1_DEPRECATED

**Severity**: WARNING
**Confidence**: HIGH
**Detection**: combination
**Tagline**: exactly_once is a 2018 setting on a 2026 client — use exactly_once_v2.

## TL;DR

Kafka Streams' `processing.guarantee=exactly_once` (EOS v1) was deprecated in Streams 3.0 and removed in Streams 4.0. The replacement is `exactly_once_v2`, available since 2.6. The linter flags either (a) a `kafka-streams` version that still has the option present but deprecated, paired with `exactly_once` in config, or (b) a `kafka-streams` 4.x build with `exactly_once` in config (which fails to start).

## The setup

A team's Streams config has carried `processing.guarantee=exactly_once` since Streams 2.x. Streams 2.6 introduced `exactly_once_beta` (later renamed `exactly_once_v2`). Streams 3.0 marked v1 as deprecated. Streams 4.0 removed it. Most teams missed the deprecation warning in the logs.

## What's actually happening

EOS v1 (Streams 0.11–2.5) used a producer-per-task model: each Streams task created its own transactional producer. For applications with many partitions this exploded the number of producers, broker-side memory, and recovery time. KIP-447 introduced EOS v2 with a shared producer-per-thread model:

| Property | EOS v1 | EOS v2 |
|----------|--------|--------|
| Producers per Streams instance | One per task | One per thread |
| Broker memory footprint | O(tasks) | O(threads) |
| Recovery time after crash | Long | Short |
| Available since | 0.11 | 2.6 (`exactly_once_beta`), 3.0 (renamed `exactly_once_v2`) |
| Deprecated | 3.0 | — |
| Removed | 4.0 | — |

In Streams 4.0+ the string `exactly_once` is rejected at startup with a `ConfigException`. In 3.x it's accepted but emits a deprecation warning.

EOS v2 also requires broker version `>= 2.5`. Practically any broker in 2026 satisfies this.

## Why this is subtle

- The setting is a single string. Code review on a four-character diff is unlikely.
- The deprecation warning is one log line that gets drowned by Streams' typical startup noise.
- "Exactly once is exactly once" — teams assume v1 vs v2 doesn't change semantics. It doesn't change *correctness*; it changes *cost*. But on 4.0 it's not even a choice — v1 won't run.

## Operational impact

- **Streams 4.0 hard failure at startup** with `ConfigException: 'exactly_once' is not a valid value for configuration processing.guarantee. Valid values are [at_least_once, exactly_once_v2]`.
- **Streams 3.x** keeps running but with the costlier producer-per-task model — higher broker memory, longer rebalance recovery, more file descriptors per worker.
- **Future-proofing failure** — a team upgrading from Streams 3.x to 4.x without changing the config sees a fresh outage.

## How to fix

```properties
# BAD
processing.guarantee=exactly_once

# GOOD
processing.guarantee=exactly_once_v2
```

Or in code:

```java
// BAD
props.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE);

// GOOD
props.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE_V2);
```

## When this might be a false positive

- A read-replay job using `processing.guarantee=at_least_once` is unaffected. The rule fires only when `exactly_once` (the v1 string) appears.
- An application targeted at a broker stuck below 2.5 cannot use v2 — but any 2026 deployment is well past that.

## Detection strategy

- Two signals:
  1. `kafka-streams` resolved version `>= 3.0` AND config file (or bytecode `StreamsConfig.PROCESSING_GUARANTEE_CONFIG`) sets the value `exactly_once` (literal `"exactly_once"`, not `exactly_once_v2`).
  2. Even without the version signal, the config literal is enough to warn.
- Streams 4.0+ ERRORs (won't start). 3.x WARNs (deprecated, will fail on next major).

## References

- [KIP-447 — Producer scalability for exactly once semantics](https://cwiki.apache.org/confluence/display/KAFKA/KIP-447)
- [Kafka Streams upgrade guide](https://kafka.apache.org/documentation/streams/upgrade-guide)
- [Streams configuration — `processing.guarantee`](https://kafka.apache.org/documentation/#streamsconfigs_processing.guarantee)
