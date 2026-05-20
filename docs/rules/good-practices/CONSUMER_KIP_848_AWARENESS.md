# CONSUMER_KIP_848_AWARENESS

**Severity**: WARNING
**Confidence**: CONTEXT
**Detection**: config-file
**Tagline**: KIP-848 is the future. It's just not your present yet.

## TL;DR

The linter does NOT yet recommend `group.protocol=consumer` (the new
KIP-848 protocol). It only emits an informational note when it detects
either: (a) an explicit `group.protocol=classic` on Kafka 4.0+ (so the
team has thought about it and chosen the old one — fine, just be aware
the deprecation timer is running), or (b) `group.protocol=consumer` on
clients/brokers where it's still production-preview. The new protocol
is meaningfully better, but not yet the default; do not migrate without
evaluating broker compatibility, observability tooling, and edge cases.

## The setup

KIP-848 (Apache Kafka 3.7 preview, 3.8 production preview, 4.0
production-ready, 4.1+ on its way to default) introduces a new consumer
group protocol that moves partition assignment from the client to the
broker. It eliminates entire categories of failure: no more
client-driven rebalance leader, no more group leader's strategy being
the one that "wins", no more stop-the-world rebalances on every join.

The classic protocol (the one everyone has used since 0.9):
- A consumer joins, the group coordinator picks a "leader" consumer.
- The leader runs the assignment strategy locally and tells the
  coordinator the assignments.
- The coordinator distributes assignments to other consumers.
- On any change, all consumers stop, re-sync, and resume.

The new protocol (KIP-848):
- Consumers send heartbeats with their interests.
- The broker computes assignments incrementally.
- No client-side group leader. No global rebalance pause.
- Existing keys like `partition.assignment.strategy` are replaced by
  `group.remote.assignor` (server-side) or `group.local.assignor`.

It's not the default in 4.0 because:
- It's tied to KRaft mode brokers (no ZK support).
- Some client-side tooling (kafka-consumer-groups.sh in older
  versions) needs to be aware.
- Mixed-mode operation during a rolling migration is supported but
  needs care.

## What's actually happening (the mechanism)

With `group.protocol=classic` (still default in 4.0):
- `JoinGroupRequest` / `SyncGroupRequest` flow as before.
- `partition.assignment.strategy` honored on the client.

With `group.protocol=consumer` (KIP-848):
- `ConsumerGroupHeartbeatRequest` replaces the join/sync dance.
- `group.remote.assignor` selects the broker-side assignor (e.g.
  `uniform` or `range`).
- Server controls the rebalance protocol incrementally — partition
  moves happen one at a time.

JMX: a new family of metrics under
`kafka.consumer:type=consumer-coordinator-metrics` reflects heartbeat
state. The classic rebalance metrics still exist for compat but go to
zero on the new protocol.

## Operational impact

- **Cost.** Same.
- **Latency.** Incremental rebalances are nearly invisible. Classic
  protocol can pause the group for seconds.
- **Throughput.** Same in steady state. Much better during scale-up /
  scale-down.
- **Operational toil.** Drops a lot — no more "why did 50 consumers
  rebalance because one joined" investigations.

## When this might be a false positive

- The team has explicitly chosen classic until KIP-848 is the default —
  fine. Don't promote past INFO.
- Clusters older than 3.7 don't support it at all — the rule should
  not even fire.
- Some Kafka cloud providers haven't enabled it yet.

## Detection strategy

- **Config files:** check for `group.protocol`. If absent or
  `classic`, emit INFO (not WARNING) noting KIP-848 exists. If
  `consumer`, emit INFO noting it's a recent feature; verify broker
  version is 4.0+ and `group.coordinator.rebalance.protocols=classic,consumer`
  is set on the broker.
- **Confidence: CONTEXT.** This is an awareness rule, not a
  recommendation. Default to INFO; suppress entirely on `kafka-clients` <
  3.7.

## References

- KIP-848 — Next Generation Consumer Rebalance Protocol:
  https://cwiki.apache.org/confluence/display/KAFKA/KIP-848%3A+The+Next+Generation+of+the+Consumer+Rebalance+Protocol
- Apache Kafka 4.0 release notes:
  https://kafka.apache.org/blog
- Confluent — New Consumer Group Protocol:
  https://www.confluent.io/blog/kip-848-the-next-generation-consumer-rebalance-protocol/
