# QK_COMMIT_STRATEGY_IGNORE

**Severity**: WARNING
**Confidence**: HIGH
**Detection**: config-file
**Tagline**: commit-strategy=ignore: you process everything, you remember nothing.

## TL;DR

`commit-strategy=ignore` with the default `enable.auto.commit=false` means the connector never advances the consumer offset. Every restart re-reads from the last externally-committed position — which for a fresh consumer group is `auto.offset.reset`, i.e. usually `latest` (skip everything) or `earliest` (replay everything).

## What's happening (the mechanism)

Three commit strategies exist on the Kafka inbound connector:

- `throttled` (default when `enable.auto.commit` is not true): tracks acked messages, periodically commits the highest *consecutive* acked offset per partition every `auto.commit.interval.ms` (default 5000 ms). At-least-once.
- `latest`: commits on every ack. "Should not be used in high-load environments — offset commit is expensive."
- `ignore`: no commit performed by the connector. Default *only when* `enable.auto.commit=true`, where Kafka's own background committer handles it. When `enable.auto.commit=false` AND `commit-strategy=ignore`, *nothing commits*.

The footgun: setting `commit-strategy=ignore` without explicitly also enabling Kafka auto-commit, and without manually committing via `ConsumerRebalanceListener` or `KafkaConsumer` injection. The connector behaves as documented; the operator's mental model is wrong.

## Operational impact

- `kafka_consumer_committed_offset` flatlines.
- `kafka_consumer_lag` appears to grow indefinitely (because committed offset never advances), even though records are being processed.
- On restart, the consumer rewinds — same records reprocessed. If the handler is non-idempotent: duplicates downstream.
- Common with exactly-once transactional setups where commit is intentionally externalized — but only legitimate if `commit-strategy=ignore` is paired with a transactional producer that commits offsets via `sendOffsetsToTransaction`.

## How to fix

```properties
# BAD — never commits
mp.messaging.incoming.orders.commit-strategy=ignore
# (and no transactional offset commit anywhere in code)

# GOOD — at-least-once with periodic commit
mp.messaging.incoming.orders.commit-strategy=throttled
mp.messaging.incoming.orders.throttled.unprocessed-record-max-age.ms=60000

# OR (intentional exactly-once)
mp.messaging.incoming.orders.commit-strategy=ignore
mp.messaging.incoming.orders.failure-strategy=ignore
# Paired with KafkaTransactions<T> emitter committing offsets in the txn.
```

## When this might be a false positive

- Exactly-once transactional pipelines using `KafkaTransactions<T>` — `commit-strategy=ignore` is *required* there to avoid double commits (one in the txn, one outside).
- Stateless side-effect-free consumers where replay-on-restart is acceptable.

Detector should suppress this when the same module also injects `KafkaTransactions<T>` (bytecode signal) or sets `mp.messaging.outgoing.<channel>.transactional.id`.

## Detection strategy

- Config: `mp.messaging.incoming.<channel>.commit-strategy=ignore` AND `enable.auto.commit` is unset or `false`.
- Bytecode: search for `Lio/smallrye/reactive/messaging/kafka/transactions/KafkaTransactions;` field injections to suppress.
- Confidence: HIGH unless a KafkaTransactions injection is found in the same module.

## Consult a friend?

> 🤝 **Slow down.** `commit-strategy=ignore` is *correct* when the offset commit is happening inside a `KafkaTransactions<T>` transaction — and *catastrophic* when it isn't. The fix depends entirely on which case you're in.
> - Does the same module inject `KafkaTransactions<T>` and call `sendOffsetsToTransaction` (or `withTransaction(...)`) in every code path? If yes, `commit-strategy=ignore` is required — switching to `throttled` causes double commits that race with the transactional one and break EOS.
> - If no transactional emitter is present, the offset has *never* been committed. On the first restart since deploy, the consumer either replays the entire topic (`auto.offset.reset=earliest`) or skips everything since deploy (`latest`). Which is it, and is the handler idempotent enough to survive that replay?
> - `kafka_consumer_lag` has been growing since the misconfiguration shipped — alerts hooked to lag have been firing or have been muted. Either way, the team needs to know lag will *snap back to zero* the moment commits start happening, not gradually.

## References

- https://smallrye.io/smallrye-reactive-messaging/latest/kafka/receiving-kafka-records/
- https://quarkus.io/blog/kafka-commit-strategies/
