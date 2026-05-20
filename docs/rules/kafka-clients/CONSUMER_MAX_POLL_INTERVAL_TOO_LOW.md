# CONSUMER_MAX_POLL_INTERVAL_TOO_LOW

**Severity**: WARNING
**Confidence**: MEDIUM
**Detection**: config-file
**Tagline**: Lower max.poll.interval.ms than your slowest record and you've built a rebalance perpetual motion machine.

## TL;DR

The linter flags `max.poll.interval.ms` set below a reasonable processing budget (e.g. < 30s, much lower than the default 300000). Combined with a non-trivial workload, it guarantees rebalances under load.

## What's happening (the mechanism)

`max.poll.interval.ms` (default 300000) is the maximum time the broker will wait between consecutive `poll()` calls before evicting the consumer. It is separate from `session.timeout.ms` (which is what the heartbeat thread enforces) — `max.poll.interval.ms` enforces "you must come back and ask for more records within this time."

If the application's processing time for a single poll batch exceeds `max.poll.interval.ms`, the consumer is kicked from the group mid-processing. The commit fails with `CommitFailedException`, and the next poll re-delivers the entire batch to the new owner.

Reducing `max.poll.interval.ms` below the default is sometimes done to make rebalances "faster" — but it makes the system fragile under any slow batch (GC pause, downstream timeout, cold cache).

## Operational impact

- Rebalance loop under normal-but-slow conditions (GC, slow downstream).
- Repeated `CommitFailedException` in logs with `"Commit cannot be completed since the group has already rebalanced"`.
- Downstream sees duplicate processing of the unsuccessful batch on every cycle.

## How to fix

```java
// BAD
props.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, "5000");

// GOOD — default
// (omit)

// GOOD — explicit, generous, matched to worst-case batch processing
props.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, "600000");  // 10 minutes
props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "200");         // and small batches
```

## When this might be a false positive

- True low-latency event-loop consumers where processing is always sub-second. But even there, the default is harmless.

## Detection strategy

- Config: `max.poll.interval.ms < 30000`. MEDIUM.
- Cross-key: flag HIGH if `max.poll.interval.ms < session.timeout.ms` (a documented anti-pattern — the heartbeat enforcement would never trigger first).

## References

- Apache Kafka consumer configs — `max.poll.interval.ms`: https://kafka.apache.org/documentation/#consumerconfigs_max.poll.interval.ms
- KIP-62: https://cwiki.apache.org/confluence/display/KAFKA/KIP-62
