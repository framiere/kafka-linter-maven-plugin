# STREAMS_REPLICATION_FACTOR_ONE

**Severity**: ERROR
**Confidence**: HIGH
**Detection**: config-file
**Tagline**: `replication.factor=1` is a state store with an expiration date.

## TL;DR

The linter flags `replication.factor=1` on Kafka Streams configs. Internal topics (changelog, repartition) inherit this value; a single broker failure means permanent state loss.

## What's happening (the mechanism)

`StreamsConfig.REPLICATION_FACTOR_CONFIG` controls the replication factor of *all* internal topics that Streams creates automatically — repartition topics and the changelog topics that back state stores. Default since 2.4 (KIP-464) is `-1`, which means "use broker default" (typically 3 in production).

With `replication.factor=1`, each internal topic has exactly one replica. If the broker hosting a changelog partition dies:
- The data is gone. Permanently.
- When the Streams task restarts and tries to restore from the changelog, the topic is unavailable or the partition is offline; the task hangs in `RESTORING` until manually intervened.
- For repartition topics with retention not yet elapsed, in-flight messages are lost.

Same disaster, faster, with disk failure on the broker.

## Operational impact

- After any broker failure: `kafka.streams:type=stream-task-metrics,task-id=...:restore-records-rate` is 0 forever, state store reads return stale or empty data, downstream computations are wrong without throwing.
- Joins go cold (the foreign-side state store can't be rebuilt) → silent under-counts.
- Interactive queries fail with `InvalidStateStoreException` because the store can't reach RUNNING.
- Recovery requires re-running the entire upstream pipeline from a point of origin you may no longer have.

## How to fix

```properties
# BAD
replication.factor=1

# GOOD — explicit
replication.factor=3

# BEST — defer to broker default (since 2.4, KIP-464)
# omit the property, broker handles it
```

```java
// GOOD
props.put(StreamsConfig.REPLICATION_FACTOR_CONFIG, 3);
// or do not set it — uses broker default (-1 since 2.4)
```

Also configure `min.insync.replicas=2` at the broker / topic level for changelog topics, and set `acks=all` on the internal producer (Streams does this for you with EOS).

## When this might be a false positive

- Single-broker development cluster (only one broker exists). Suppress per-environment, never globally.
- Genuinely ephemeral applications where state loss is acceptable (rare).

## Detection strategy

- Config files: `replication.factor=1` in `application.properties` / yaml / Spring `spring.kafka.streams.replication-factor` / Quarkus `kafka-streams.replication.factor`.
- Bytecode: `Properties.put` / `Map.put` with key matching `replication.factor` or `StreamsConfig.REPLICATION_FACTOR_CONFIG` and value `LDC 1` / `ICONST_1` / `LDC "1"`.
- Confidence: HIGH.

## Consult a friend?

> 🤝 **Slow down.** Changing `replication.factor` from 1 to 3 only takes effect on *newly created* internal topics. The existing changelog and repartition topics keep their RF=1 until you delete or alter them — and you can't ALTER through Streams.
> - What's the current state of existing changelog topics? Use `kafka-topics.sh --describe` to confirm. If they're already at RF=1, the application config change does nothing until you `kafka-reassign-partitions` them by hand, or wipe them and restore from upstream.
> - Wiping a changelog topic means a *full* state restore on next startup — for a multi-GB RocksDB store, that can be hours of restore-only time. Is that downtime acceptable? Can it run during a planned maintenance window?
> - The fix usually pairs with `min.insync.replicas=2` on the topic and `acks=all` on the internal producer — EOS turns these on, but ALOS doesn't. Confirm which mode you're in and whether the broker-side topic config matches.

## References

- Streams config — replication.factor: https://kafka.apache.org/documentation/streams/developer-guide/config-streams.html#replication-factor
- KIP-464 — Allow internal topics to use broker default replication factor: https://cwiki.apache.org/confluence/display/KAFKA/KIP-464%3A+Defaults+for+AdminClient%23createTopic

## Cross-reference

Also recommended by Confluent agent-skills — see kafka-streams-programming/references/config-baseline.md § Production-grade baseline (replication.factor=3 minimum).
