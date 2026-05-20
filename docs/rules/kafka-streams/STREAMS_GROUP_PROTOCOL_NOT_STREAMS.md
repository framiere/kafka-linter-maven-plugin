# STREAMS_GROUP_PROTOCOL_NOT_STREAMS
**Severity**: WARNING
**Confidence**: CONTEXT
**Detection**: config-file + pom-dependency
**Tagline**: On AK 4.2+ / CP 8.2+, classic rebalancing is 50-80% slower for no reason.
**Source**: Confluent agent-skills — kafka-streams-programming/SKILL.md preamble § Invariant Checklist #3 and kafka-streams-programming/references/topology-patterns.md § Assignment Strategy.

## TL;DR

The linter flags Kafka Streams applications running on Apache Kafka 4.2+ / Confluent Platform 8.2+ / Confluent Cloud where `group.protocol=streams` is not set. KIP-1071 introduces a Streams-specific consumer group protocol that delivers 50-80% faster rebalances. The skill makes it an invariant on supported brokers: enable it unless one of the four documented incompatibilities applies.

## What's happening (the mechanism)

Before KIP-1071, Kafka Streams piggybacked on the generic consumer group protocol (`group.protocol=classic`). Every rebalance went through `JoinGroup` / `SyncGroup` with the entire group, and the broker-side group coordinator had no awareness that Streams tasks carry state — it could shuffle partitions around as if they were stateless.

KIP-1071 introduces a dedicated `streams` protocol at the broker. The broker maintains task assignment, knows about standby tasks, and can do single-rebalance assignments instead of the multi-phase JoinGroup/SyncGroup dance. Confluent measured 50-80% faster rebalances on production workloads.

The protocol ships in:
- Apache Kafka 4.2+
- Confluent Platform 8.2+
- Confluent Cloud (default-on for new clusters)

On older brokers, setting `group.protocol=streams` crashes the client with `UnsupportedVersionException` at startup. So this rule is **CONTEXT** — it only fires when we can prove the broker is on a supported version (best signal: `kafka-streams` dependency version >= 4.2.0, since the client typically matches the broker).

## Operational impact

- Rebalance duration is the single largest contributor to consumer lag spikes during rolling deploys. A 3-instance Streams app on the classic protocol rebalances in 30-90s; the same app on `group.protocol=streams` rebalances in 5-15s.
- During rebalance, **all** stream threads stop processing. Faster rebalance = less lag = less downstream impact.
- Standby task assignment is computed broker-side instead of client-side under the new protocol — handover on instance loss is faster.

## How to fix (bad → good code)

```properties
# BAD — implicit `classic` on a broker that supports `streams`
application.id=orders-aggregator
bootstrap.servers=broker:9092
# (no group.protocol set)

# GOOD — opt into KIP-1071 explicitly
application.id=orders-aggregator
bootstrap.servers=broker:9092
group.protocol=streams
```

CC rollback contingency (per Confluent skill): if you hit `UnsupportedVersionException` after a CC version downgrade, comment the line out to fall back to classic.

## When this might be a false positive

- Broker version is below AK 4.2 / CP 8.2 — flagging here would crash the app. Confidence is **CONTEXT** for this reason.
- App uses one of the features incompatible with KIP-1071: static membership (`group.instance.id`), regex topic subscriptions, standby replicas, warm-up replicas. See `STREAMS_GROUP_PROTOCOL_STREAMS_INCOMPAT` for the negative form.
- Cluster operator has explicitly disabled the protocol broker-side.

## Detection strategy

- Config: `group.protocol` is unset, AND
- Dependency: `org.apache.kafka:kafka-streams` >= 4.2.0 (proxy for broker capability), AND
- Negative interlock: `group.instance.id` is **not** set, no regex `Pattern.compile(...)` argument to `builder.stream(...)`, `num.standby.replicas` is unset or 0, `max.warmup.replicas` is unset.
- If all four hold, emit WARNING with confidence CONTEXT — explain that the rule depends on broker version.

## References

- KIP-1071 — Streams Rebalance Protocol: https://cwiki.apache.org/confluence/display/KAFKA/KIP-1071
- Confluent agent-skills — kafka-streams-programming/SKILL.md, Invariant #3
- Confluent agent-skills — kafka-streams-programming/references/topology-patterns.md § Assignment Strategy
