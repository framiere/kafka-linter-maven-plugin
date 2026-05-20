# PRODUCER_ACKS_ONE

**Severity**: WARNING
**Confidence**: HIGH
**Detection**: bytecode + config-file
**Tagline**: acks=1 means "the leader saw it" — and then the leader died.

## TL;DR

The linter flags producers configured with `acks=1`. The leader acknowledges before replication completes, so an unclean leader election or a leader crash after the ack costs you the in-flight window.

## What's happening (the mechanism)

`acks=1` waits for the partition leader to write the record to its log, but does not wait for followers in the ISR to catch up. If the leader fails after sending the ack but before any follower has replicated the record, a new leader is elected from the surviving ISR and the record is gone — replaced by whatever offset the new leader had.

Since Kafka 3.0 the default is `acks=all` (KIP-679). `acks=1` is an explicit weakening of that default. With `enable.idempotence=true` (also default since 3.0), the producer requires `acks=all` — setting `acks=1` and explicitly enabling idempotence raises `ConfigException`. If idempotence is left at its implicit default and `acks=1` is set, idempotence is silently disabled (KAFKA-13673).

## Operational impact

- Data loss window equal to the unreplicated tail of the partition during any leader failover (rolling restart, broker JVM crash, network partition).
- Silent: producer-side metrics show success; only downstream missing-record alarms detect the loss, often hours later.
- `kafka.server:type=ReplicaManager,name=UnderReplicatedPartitions` going non-zero is the precursor symptom — every record produced during that window is at risk.
- With `unclean.leader.election.enable=true` on the broker side, the loss window is unbounded because a stale replica can be elected.

## How to fix

```java
// BAD
props.put(ProducerConfig.ACKS_CONFIG, "1");

// GOOD — rely on default
// (omit the property)

// GOOD — explicit
props.put(ProducerConfig.ACKS_CONFIG, "all");
```

If latency is the concern, tune `min.insync.replicas=2` on the topic plus `acks=all`. The marginal latency cost over `acks=1` is typically a few ms on the same AZ, and you keep the durability guarantee.

## When this might be a false positive

- Single-broker dev clusters (RF=1) where `acks=all` and `acks=1` are equivalent.
- Internal high-volume telemetry where the team has deliberately traded durability for latency. Downgrade to INFO for that module.

## Detection strategy

- Config: `acks=1` (string) in any Kafka producer Properties source.
- Bytecode: `Properties.put("acks", "1")` or via `ProducerConfig.ACKS_CONFIG`. MEDIUM confidence because `acks=1` is sometimes a conscious choice.
- Suggest pairing with a broker-side `min.insync.replicas` audit so the rule's recommendation lands cleanly.

## Consult a friend?

> 🤝 **Slow down.** Flipping `acks=1` to `acks=all` only buys durability if the *broker side* has `min.insync.replicas >= 2` on every topic this producer writes to — otherwise `acks=all` collapses to `acks=1` against a single-ISR partition with no error.
> - For every topic this producer writes to: what is `min.insync.replicas` on the topic config (or broker default)? If it's 1, `acks=all` is purely cosmetic.
> - During the ISR shrinking that happens in any rolling broker restart, a partition with `min.insync.replicas=2` and one ISR will *reject* writes (`NotEnoughReplicasException`). Is the producer's `delivery.timeout.ms` long enough to ride out a normal rolling restart, or will sends start failing user-visibly?
> - If you also have `enable.idempotence=true` left implicit, KAFKA-13673 means `acks=1` silently disabled idempotence — so this fix also re-enables duplicate-detection. Was the team relying on the (silently broken) at-most-once behavior anywhere?

## References

- Apache Kafka producer configs — `acks`: https://kafka.apache.org/documentation/#producerconfigs_acks
- Apache Kafka — Data Durability: https://kafka.apache.org/documentation/#design_ha
- KIP-679: https://cwiki.apache.org/confluence/display/KAFKA/KIP-679
