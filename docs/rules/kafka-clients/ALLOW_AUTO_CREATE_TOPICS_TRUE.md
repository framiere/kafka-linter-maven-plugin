# ALLOW_AUTO_CREATE_TOPICS_TRUE

**Severity**: WARNING
**Confidence**: HIGH
**Detection**: config-file
**Tagline**: Auto-creating topics from the consumer makes typos into infrastructure.

## TL;DR

The linter flags `allow.auto.create.topics=true` on consumers (it is the default in older clients; the default flipped to `false` in Kafka 4.0). With it on, a typo in `subscribe("ordres")` creates the topic `ordres` on the broker with default partitions/RF — and you never notice the typo.

## What's happening (the mechanism)

`allow.auto.create.topics` controls whether the consumer's metadata request can implicitly create the topic if the broker's `auto.create.topics.enable=true` and the topic does not exist. The created topic gets the broker defaults — typically `num.partitions=1` and `default.replication.factor=1` — which is essentially never what production wants.

Defaults:
- Kafka < 4.0: `allow.auto.create.topics=true` (consumer default).
- Kafka ≥ 4.0: `allow.auto.create.topics=false` (default flipped).

Even with the client default true, the broker side `auto.create.topics.enable` (often false in managed clusters) decides whether the topic actually gets created. But many self-hosted clusters leave it on.

## Operational impact

- Typo creates a phantom topic with 1 partition, RF=1 — no HA, no scaling — that silently accumulates real producer traffic.
- Misnamed topics show up in cluster inventory, complicate audit and quota tracking.
- Capacity planning breaks: the "real" topic has its planned partition count, the typo'd shadow has 1 partition and chokes.

## How to fix

```java
// BAD
props.put(ConsumerConfig.ALLOW_AUTO_CREATE_TOPICS_CONFIG, "true");

// GOOD — explicit
props.put(ConsumerConfig.ALLOW_AUTO_CREATE_TOPICS_CONFIG, "false");

// GOOD — let Kafka 4.0+ default apply
// (omit)
```

Pair this with a broker-side `auto.create.topics.enable=false`.

## When this might be a false positive

- Embedded test clusters (Testcontainers) where convenience auto-creation is desired.
- Some Kafka Streams test rigs.

## Detection strategy

- Config: `allow.auto.create.topics=true` literal. HIGH.
- Bytecode: `Properties.put("allow.auto.create.topics", "true")`. HIGH.

## References

- Apache Kafka consumer configs — `allow.auto.create.topics`: https://kafka.apache.org/documentation/#consumerconfigs_allow.auto.create.topics
- KIP-361 — Add Consumer Configuration to Disable Auto Topic Creation: https://cwiki.apache.org/confluence/display/KAFKA/KIP-361
- Apache Kafka 4.0 upgrade notes: https://kafka.apache.org/40/documentation.html#upgrade
