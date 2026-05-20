# CONSUMER_ISOLATION_READ_UNCOMMITTED_WITH_TXN

**Severity**: WARNING
**Confidence**: MEDIUM
**Detection**: config-file
**Tagline**: read_uncommitted from a transactional topic is reading the draft, not the final.

## TL;DR

The linter flags consumers configured with `isolation.level=read_uncommitted` (the default) when the topic is known to be written by a transactional producer. The consumer will see aborted records and pre-commit records that the producer later rolled back.

## What's happening (the mechanism)

`isolation.level=read_uncommitted` (default) returns all records regardless of transaction status. The consumer sees:
- Committed transactional records.
- Aborted transactional records (records the producer rolled back via `abortTransaction()`).
- Records from in-flight transactions (records produced but not yet committed).

`isolation.level=read_committed` skips aborted records and waits for the commit/abort marker before exposing transactional records. The Last Stable Offset (LSO) bounds what a `read_committed` consumer can see.

The mismatch is silent: nothing fails, the consumer just sees ghost records. For exactly-once pipelines this defeats the entire point of transactions.

## Operational impact

- Downstream sees records that were rolled back upstream — phantom side-effects.
- "Exactly-once" pipelines silently downgrade to "at-least-once with extras".
- Pairs nastily with `enable.auto.commit=true` — phantom records get committed, then the upstream aborts, and now the consumer can never roll back its own commit.

## How to fix

```java
// BAD — default is read_uncommitted, fine for non-transactional topics,
//       wrong for downstream of a transactional producer
// (omit isolation.level)

// GOOD
props.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");
```

This rule is hard to apply universally — the linter cannot generally know whether the upstream is transactional. It is most useful when:
1. The same project contains both a transactional producer and a consumer.
2. `enable.idempotence=true` + `transactional.id` is set elsewhere, and the same group reads from the produced topic.

## When this might be a false positive

- Non-transactional producers — `read_uncommitted` is correct.
- Cross-team setups where the producer team uses transactions but the consumer team is unaware (real anti-pattern, hard to detect statically).

## Detection strategy

- Config: detect `transactional.id` set in any producer config in the same project; flag any consumer in the same project that does not explicitly set `isolation.level=read_committed`. MEDIUM.
- Bytecode: locate `KafkaProducer` constructions with `transactional.id` set, then locate `KafkaConsumer` constructions in the same project. MEDIUM.

## Consult a friend?

> 🤝 **Slow down.** Flipping to `read_committed` is the right move for EOS, but it isn't free — the consumer is now gated by the broker's Last Stable Offset (LSO), which lags the log-end by however long upstream transactions stay open.
> - What's the upstream producer's `transaction.timeout.ms`? Whatever it is, that's the worst-case end-to-end latency floor on this consumer once you flip to `read_committed`. If your SLO is tighter than that timeout, you have a problem.
> - If the upstream JVM ever crashes mid-transaction, downstream `read_committed` consumers stall until the broker times out the open transaction. Is the consumer's lag alerting tuned to distinguish "stuck on aborted txn" from "actually behind"?
> - Is `enable.auto.commit=true` set on this consumer? If yes, you've been auto-committing phantom (aborted) offsets — flipping `read_committed` *and* leaving auto-commit on still skips the manual-commit discipline that EOS read-process-write needs.

## References

- Apache Kafka consumer configs — `isolation.level`: https://kafka.apache.org/documentation/#consumerconfigs_isolation.level
- KIP-98 — Transactions: https://cwiki.apache.org/confluence/display/KAFKA/KIP-98
- Confluent — Transactions in Kafka: https://www.confluent.io/blog/transactions-apache-kafka/
