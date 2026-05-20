# STREAMS_EXPLICIT_INTERNAL_NAMING_DISABLED
**Severity**: WARNING
**Confidence**: HIGH
**Detection**: config-file
**Tagline**: `ensure.explicit.internal.resource.naming=true` is the one-line safety net for state identity.
**Source**: Confluent agent-skills — kafka-streams-programming/SKILL.md § Invariant Checklist #5, kafka-streams-programming/references/config-baseline.md § Core Properties.

## TL;DR

The linter flags Kafka Streams configs where `ensure.explicit.internal.resource.naming` is unset or `false`. With this flag set to `true`, Streams refuses to start if any internal topic (changelog, repartition) or operator falls back to an auto-generated positional name. Without it, a `.filter()` inserted upstream silently renames every downstream store — and you lose the state.

## What's happening (the mechanism)

Kafka Streams generates internal topic and operator names from the topology's positional DAG: `KSTREAM-AGGREGATE-STATE-STORE-0000000003`, etc. Insert one operator earlier in the chain and the suffix shifts; Streams sees a brand-new store and full-restores from an empty changelog. The accumulated state is silently abandoned.

This is the same failure mode that `STREAMS_UNNAMED_MATERIALIZED` catches per operator. The `ensure.explicit.internal.resource.naming` flag (added in Kafka 3.7) is the cluster-level safety net: when `true`, `KafkaStreams.start()` throws if **any** internal resource lacks an explicit name. Either every operator is named, or the app refuses to start. The skill's invariant checklist names this property under Invariant #5.

It's belt-and-suspenders with the per-operator naming rule, but the suspenders are critical: a single oversight on a `count()` years from now will not silently drop state if this flag is enabled.

## Operational impact

- Without this flag: state can vanish on a deploy with no failure signal — only a `restore-records-rate` spike that operators may not connect to the diff.
- With this flag: a missing `Materialized.as(...)` fails the build's smoke test or the first deploy. Cheap to discover, cheap to fix.

## How to fix (bad → good code)

```properties
# BAD — implicit, positional naming for all operators
application.id=orders-aggregator
default.key.serde=...

# GOOD — startup refuses to run with anonymous internal resources
application.id=orders-aggregator
default.key.serde=...
ensure.explicit.internal.resource.naming=true
```

Then every stateful DSL operator must carry a `Named.as(...)` / `Materialized.as(...)` / `StreamJoined.withStoreName(...)`. See `STREAMS_UNNAMED_MATERIALIZED` and `STREAMS_NAMED_OPERATORS` for the per-call-site form.

## When this might be a false positive

- Pre-Kafka-3.7 applications cannot use this flag (key didn't exist yet). For those, drive the same outcome via the per-operator rules.
- Throwaway demo / `TopologyTestDriver` smoke tests where every test creates its own state. Acceptable to leave the flag unset there.

## Detection strategy

- Config: `ensure.explicit.internal.resource.naming` is absent, empty, or set to `false`.
- Dependency: `org.apache.kafka:kafka-streams` >= 3.7.0 — the property was introduced then.
- Confidence: HIGH. The property is plain text; the recommendation is unambiguous.

## References

- KIP-895 — Ensure explicit names for internal resources: https://cwiki.apache.org/confluence/display/KAFKA/KIP-895
- Confluent agent-skills — kafka-streams-programming/SKILL.md § Invariant Checklist #5
- Confluent agent-skills — kafka-streams-programming/references/config-baseline.md
- Related rule: [STREAMS_UNNAMED_MATERIALIZED](STREAMS_UNNAMED_MATERIALIZED.md)
