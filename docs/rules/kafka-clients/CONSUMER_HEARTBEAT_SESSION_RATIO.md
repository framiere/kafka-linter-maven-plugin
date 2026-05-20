# CONSUMER_HEARTBEAT_SESSION_RATIO

**Severity**: WARNING
**Confidence**: HIGH
**Detection**: config-file
**Tagline**: Heartbeats should be 1/3 of the session — two misses, you're still alive.

## TL;DR

The linter flags consumer configs where `heartbeat.interval.ms` is not less than 1/3 of `session.timeout.ms`. The official guidance is "lower than session.timeout.ms, typically no higher than 1/3" — i.e., three heartbeats per session window.

## What's happening (the mechanism)

The consumer's background thread sends `Heartbeat` requests to the group coordinator every `heartbeat.interval.ms`. If the coordinator goes `session.timeout.ms` without receiving a heartbeat, it declares the member dead and triggers a rebalance.

The 1/3 ratio gives the consumer two retries within the session window — under transient network load, missing one or two heartbeats is normal, and the third is what evicts you. Going closer to 1/1 means a single missed heartbeat evicts you.

Defaults (Kafka 3.0+): `session.timeout.ms=45000`, `heartbeat.interval.ms=3000` — comfortably better than 1/3.

The broker also constrains `session.timeout.ms` to the range `[group.min.session.timeout.ms, group.max.session.timeout.ms]` (broker-side, default 6000 / 1800000). Outside that range, the client cannot join the group.

## Operational impact

- Rebalance storms during normal network jitter: one missed heartbeat = eviction = rebalance = downstream lag spike.
- `kafka.consumer:type=consumer-coordinator-metrics:rebalance-rate-per-hour` consistently > 0 with no deploys / scaling events is the smoking gun.
- Member rejected by broker if `session.timeout.ms` is outside the broker's allowed range — startup fails with `InvalidSessionTimeoutException`.

## How to fix

```properties
# BAD — 8s heartbeat in a 10s session: 1 miss = eviction
session.timeout.ms=10000
heartbeat.interval.ms=8000

# GOOD — 3x ratio
session.timeout.ms=30000
heartbeat.interval.ms=10000

# GOOD — defaults work well
# (omit both)
```

For consumers with long processing time, raise `max.poll.interval.ms` instead — the heartbeat thread keeps sending heartbeats during processing, so the session does not depend on poll latency.

## When this might be a false positive

- Brand new consumer rebalance protocol (KIP-848, `group.protocol=consumer`) — `heartbeat.interval.ms` is unused, the broker controls it via `group.consumer.heartbeat.interval.ms`. Suppress if `group.protocol=consumer` is set.

## Detection strategy

- Config: parse both keys from the same source, flag if `heartbeat.interval.ms * 3 >= session.timeout.ms`. HIGH.
- Bytecode: same logic across `Properties.put` calls in the same method body. HIGH.
- Suppress when `group.protocol=consumer` is present.

## References

- Apache Kafka consumer configs — `heartbeat.interval.ms`, `session.timeout.ms`: https://kafka.apache.org/documentation/#consumerconfigs_heartbeat.interval.ms
- KIP-62 — Background heartbeat thread: https://cwiki.apache.org/confluence/display/KAFKA/KIP-62
- KIP-848 — New consumer rebalance protocol: https://cwiki.apache.org/confluence/display/KAFKA/KIP-848
