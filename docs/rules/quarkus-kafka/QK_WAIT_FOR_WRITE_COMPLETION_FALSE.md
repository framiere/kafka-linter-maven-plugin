# QK_WAIT_FOR_WRITE_COMPLETION_FALSE

**Severity**: WARNING
**Confidence**: HIGH
**Detection**: config-file
**Tagline**: waitForWriteCompletion=false un-does acks=all from the Quarkus side.

## TL;DR

`mp.messaging.outgoing.<channel>.waitForWriteCompletion=false` tells the SmallRye connector to ack the incoming Message *as soon as the record is enqueued in the Kafka client* — not after the broker acknowledges it. Combined with `acks=all`, this is contradictory: the Kafka client waits for full replication, but the upstream channel doesn't.

## What's happening (the mechanism)

The connector by default (`waitForWriteCompletion=true`) joins the upstream ack with the Kafka producer's send future. Setting it to `false` decouples them:

- Incoming `Message.ack()` fires when the record enters the producer's send buffer.
- The upstream offset is committed.
- If the producer's actual send fails later (broker down, batch timeout), the failure is logged but the upstream offset is already committed.

Effect: looks like `acks=0` from the upstream consumer's perspective, regardless of what `acks` is set to.

## Operational impact

- Lost records on broker outages during peak load (producer queues fill, sends time out, upstream consumer already moved on).
- Hard to diagnose: `acks=all` is set, so engineers assume durability is solid.
- Common pattern when chasing throughput without understanding the contract.

## How to fix

```properties
# BAD — acks=all is performative if upstream commits before write completion
mp.messaging.outgoing.orders.acks=all
mp.messaging.outgoing.orders.waitForWriteCompletion=false

# GOOD — keep the default true
mp.messaging.outgoing.orders.acks=all
# waitForWriteCompletion defaults to true; omit
```

If you genuinely need fire-and-forget throughput, set `acks=0` *and* document it explicitly — don't hide it behind `waitForWriteCompletion=false`.

## When this might be a false positive

- Pipelines where the upstream is in-memory or replayable, and the only durability guarantee that matters is the producer's own.

## Detection strategy

- Config: `mp.messaging.outgoing.<channel>.waitForWriteCompletion=false`.
- Cross-check: also note when `acks` is set to `all` on the same channel — high confidence the operator misunderstood.
- Confidence: HIGH.

## References

- https://smallrye.io/smallrye-reactive-messaging/latest/kafka/writing-kafka-records/
