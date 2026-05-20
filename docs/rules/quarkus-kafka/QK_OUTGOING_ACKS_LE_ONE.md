# QK_OUTGOING_ACKS_LE_ONE

**Severity**: ERROR for acks=0, WARNING for acks=1
**Confidence**: HIGH
**Detection**: config-file
**Tagline**: acks=1 trusts one broker. acks=0 trusts nothing.

## TL;DR

`mp.messaging.outgoing.<channel>.acks=0` or `acks=1` weakens producer durability. The SmallRye default is `1` (single-replica ack). For data you care about, set `acks=all` and rely on broker `min.insync.replicas` to define quorum.

## What's happening (the mechanism)

- `acks=0`: producer fires and forgets. Network error = silent loss. Broker death = silent loss.
- `acks=1` (SmallRye default): leader writes to its log and acks. If the leader dies before replicas catch up, the record is lost.
- `acks=all` (a.k.a. `-1`): leader waits for all in-sync replicas to write before acking. Combined with `min.insync.replicas=2` on the broker side, you survive a single-broker failure.

Note that SmallRye's `acks` default is `1`, while plain Kafka clients defaulted to `1` historically and `all` since Kafka 3.0. The Quarkus extension does NOT inherit Kafka 3.0's new default — it stays at `1`.

## Operational impact

- Data loss on broker failover: undetectable from the producer side.
- `kafka_producer_record_send_total` looks identical to a healthy run.
- Only post-incident audits against source-of-truth systems reveal the gap.

## How to fix

```properties
# BAD
mp.messaging.outgoing.orders.acks=1

# GOOD
mp.messaging.outgoing.orders.acks=all
# Broker side (set on the topic or cluster):
#   min.insync.replicas=2
```

Also enable idempotence (default `true` in Kafka client 3.0+, but the SmallRye extension may pin it differently — explicitly set):

```properties
mp.messaging.outgoing.orders.enable.idempotence=true
```

## When this might be a false positive

- Telemetry / log shipping where occasional loss is acceptable in exchange for throughput.
- Pre-production load testing.

## Detection strategy

- Config: `mp.messaging.outgoing.<channel>.acks` set to `0` or `1`. Also flag absence in production profiles (relies on default `1`).
- Confidence: HIGH for `0`, MEDIUM for `1` (default-related, operator may not realize).

## Consult a friend?

> 🤝 **Slow down.** SmallRye's `acks=1` default means most Quarkus apps that "just work" today have been quietly under-durable since v1. Flipping to `acks=all` is correct, but the *broker side* has to be ready or you've just changed the failure mode, not the durability.
> - For each channel: what is `min.insync.replicas` on the target topic? `acks=all` with `min.insync.replicas=1` is still single-leader durability. Confirm the topic config, not just the cluster default.
> - During a rolling broker restart, partitions with `min.insync.replicas=2` and a temporarily-shrunk ISR will reject writes with `NotEnoughReplicasException`. Has Quarkus' default `request.timeout.ms` been tuned, or will the producer surface failures during routine cluster maintenance?
> - SmallRye's `enable.idempotence` default also differs from Apache Kafka's — explicitly set both `acks=all` *and* `enable.idempotence=true` rather than relying on either default. Two visible lines beats two invisible mismatches.

## References

- https://smallrye.io/smallrye-reactive-messaging/latest/kafka/writing-kafka-records/
- https://quarkus.io/guides/kafka
