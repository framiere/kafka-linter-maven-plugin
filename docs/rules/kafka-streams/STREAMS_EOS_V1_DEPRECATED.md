# STREAMS_EOS_V1_DEPRECATED

**Severity**: ERROR on Streams 4.x (won't start), WARNING on Streams 3.x (deprecated)
**Confidence**: HIGH
**Detection**: config-file + pom-dependency
**Tagline**: `exactly_once` (v1) is a per-task producer. `exactly_once_v2` is the only one you should pick.

## TL;DR

The linter flags `processing.guarantee=exactly_once` and `processing.guarantee=exactly_once_beta` — both deprecated since 3.0 by KIP-732 and slated for removal. The only valid EOS value going forward is `exactly_once_v2`.

## What's happening (the mechanism)

The original EOS-v1 (KIP-129, KIP-130) used one producer per task: every input partition got a dedicated transactional producer, separate buffers, separate threads, separate broker connections. This didn't scale — broker memory and concurrent-transaction count grew linearly with partitions.

KIP-447 introduced a single thread-producer that can handle multiple tasks in one transaction, which dramatically reduces broker load. The new mode was initially called `exactly_once_beta`; KIP-732 (Kafka 3.0) renamed it `exactly_once_v2`, deprecated both `exactly_once` and `exactly_once_beta`, and announced removal in 4.0 (or one year after 3.0, whichever is later).

Today:
- `exactly_once` — deprecated, scheduled for removal. Carries the old per-task-producer overhead.
- `exactly_once_beta` — deprecated alias. Use `exactly_once_v2` instead.
- `exactly_once_v2` — current, required broker version 2.5+.

## Operational impact

- On Kafka 4.x: the application refuses to start (config removed).
- On 3.x: deprecation warning at startup; runtime works but you carry v1's broker load.
- Broker-side: `kafka.server:type=BrokerTopicMetrics,name=TotalProduceRequestsPerSec` and transactional state in `__transaction_state` grows proportional to (tasks × partitions), not just tasks.
- Rebalance time worse than necessary because each task has its own producer to fence and rebuild.

## How to fix

```properties
# BAD
processing.guarantee=exactly_once
# BAD — deprecated alias
processing.guarantee=exactly_once_beta

# GOOD
processing.guarantee=exactly_once_v2
```

```java
// BAD
props.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE);

// GOOD
props.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE_V2);
```

Required broker minimum: 2.5+. Verify before flipping.

## Why this is subtle

- The setting is a single string. Code review on a four-character diff is unlikely to catch it.
- The 3.x deprecation warning is one log line that gets drowned by Streams' typical startup noise.
- "Exactly once is exactly once" — teams assume v1 vs v2 doesn't change semantics. It doesn't change *correctness*; it changes *cost* (broker memory, FD count, rebalance recovery time). But on 4.x it's not even a choice — v1 won't run.
- A team upgrading from Streams 3.x to 4.x without changing the config sees a fresh `ConfigException: 'exactly_once' is not a valid value for configuration processing.guarantee. Valid values are [at_least_once, exactly_once_v2]` at startup.

## When this might be a false positive

- Targeting a broker older than 2.5 (very rare in 2026). If you must, document and suppress per-module.

## Detection strategy

Two-signal rule, severity escalates with the version:

1. **Config-file signal:** `processing.guarantee=exactly_once` or `=exactly_once_beta` in any `application.properties`, `application.yaml`, Spring `spring.kafka.streams.properties.processing.guarantee`, or Quarkus `kafka-streams.processing.guarantee`.
2. **Bytecode signal:** `Properties.put` / `Map.put` where the value matches `LDC "exactly_once"` or `LDC "exactly_once_beta"` paired with key `LDC "processing.guarantee"` or `GETSTATIC StreamsConfig.PROCESSING_GUARANTEE_CONFIG`.
3. **Version-gate (pom-dependency):** resolve `org.apache.kafka:kafka-streams` from `project.getArtifacts()`. Severity:
   - `>= 4.0` → **ERROR** (config rejected at startup, hard failure).
   - `3.0 ≤ x < 4.0` → **WARNING** (deprecated, runs but emits startup warning, will break on next major upgrade).
   - `< 3.0` → not applicable (v1 still primary).
4. Even without the version signal, the config literal alone is enough to emit a warning.
- Confidence: HIGH.

## Consult a friend?

> 🤝 **Slow down.** Changing `exactly_once` → `exactly_once_v2` is a one-line diff, but the broker-side transactional state machine changes (one producer per *thread* instead of one per *task*), and the migration path has a one-way rebalance.
> - Are all brokers in your cluster on 2.5+? `exactly_once_v2` rejects older brokers — a heterogeneous cluster during a rolling broker upgrade will see Streams instances refuse to start. Confirm the broker fleet is uniformly upgraded *first*.
> - On the version flip, Streams writes a new `__consumer_offsets` group state and the existing `__transaction_state` entries from v1 are orphaned. The first rebalance after the change is longer than usual — schedule the deploy during a low-traffic window.
> - If multiple Streams apps share a Kafka cluster, are any of them still on v1? They'll keep paying the per-task-producer broker tax. The fix is per-app, not per-cluster — track the migration as a cluster-wide effort.

## References

- KIP-447 — Producer scalability for exactly once: https://cwiki.apache.org/confluence/display/KAFKA/KIP-447:+Producer+scalability+for+exactly+once+semantics
- KIP-732 — Deprecate eos-alpha and replace eos-beta with eos-v2: https://cwiki.apache.org/confluence/display/KAFKA/KIP-732:+Deprecate+eos-alpha+and+replace+eos-beta+with+eos-v2
- Streams config docs: https://kafka.apache.org/documentation/streams/developer-guide/config-streams.html#processing-guarantee

## Cross-reference

Also recommended by Confluent agent-skills — see kafka-streams-programming/references/topology-patterns.md § EOS decision tree (KIP-732 deprecation of exactly_once / exactly_once_beta).
