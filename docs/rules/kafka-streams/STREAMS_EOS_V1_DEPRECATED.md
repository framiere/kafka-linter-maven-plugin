# STREAMS_EOS_V1_DEPRECATED

**Severity**: ERROR
**Confidence**: HIGH
**Detection**: config-file
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

## When this might be a false positive

- Targeting a broker older than 2.5 (very rare in 2026). If you must, document and suppress per-module.

## Detection strategy

- Config files: `processing.guarantee=exactly_once` or `=exactly_once_beta` in any `application.properties`, `application.yaml`, Spring `spring.kafka.streams.properties.processing.guarantee`, or Quarkus `kafka-streams.processing.guarantee`.
- Bytecode: `Properties.put` / `Map.put` where the value matches `LDC "exactly_once"` or `LDC "exactly_once_beta"` paired with key `LDC "processing.guarantee"` or `GETSTATIC StreamsConfig.PROCESSING_GUARANTEE_CONFIG`.
- Confidence: HIGH.

## References

- KIP-447 — Producer scalability for exactly once: https://cwiki.apache.org/confluence/display/KAFKA/KIP-447:+Producer+scalability+for+exactly+once+semantics
- KIP-732 — Deprecate eos-alpha and replace eos-beta with eos-v2: https://cwiki.apache.org/confluence/display/KAFKA/KIP-732:+Deprecate+eos-alpha+and+replace+eos-beta+with+eos-v2
- Streams config docs: https://kafka.apache.org/documentation/streams/developer-guide/config-streams.html#processing-guarantee

## Cross-reference

Also recommended by Confluent agent-skills — see kafka-streams-programming/references/topology-patterns.md § EOS decision tree (KIP-732 deprecation of exactly_once / exactly_once_beta).
