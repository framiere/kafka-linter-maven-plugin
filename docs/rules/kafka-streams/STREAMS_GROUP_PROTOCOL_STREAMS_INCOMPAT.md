# STREAMS_GROUP_PROTOCOL_STREAMS_INCOMPAT
**Severity**: ERROR
**Confidence**: HIGH
**Detection**: config-file
**Tagline**: `group.protocol=streams` plus static membership is a `ConfigException` you can ship.
**Source**: Confluent agent-skills — kafka-streams-programming/references/topology-patterns.md § Assignment Strategy.

## TL;DR

The linter flags Kafka Streams configs that combine `group.protocol=streams` with any of the four features that KIP-1071 explicitly does not support as of AK 4.2: static membership (`group.instance.id`), regex topic subscriptions, `num.standby.replicas` > 0, or `max.warmup.replicas` > 0. The app crashes at startup with `ConfigException` (static membership) or `UnsupportedOperationException` (regex) — or silently ignores the setting (standby / warm-up). All four are bugs in waiting.

## What's happening (the mechanism)

KIP-1071 redesigns task assignment broker-side. Several pre-existing Streams features depend on assignment behaviors that only the legacy protocol provides:

| Feature | Protocol that supports it | What `group.protocol=streams` does |
|---|---|---|
| `group.instance.id` (static membership, KIP-345) | classic only | `ConfigException` at startup |
| Regex topic subscription (`Pattern.compile(...)`) | classic only | `UnsupportedOperationException` when subscribing |
| `num.standby.replicas` > 0 | classic only | Silently ignored — no standbys created |
| `max.warmup.replicas` > 0 | classic only | Silently ignored — no warm-up |

The skill's table is unambiguous: if you need any of these four, remove `group.protocol=streams` and fall back to the classic protocol.

## Operational impact

- `ConfigException` / `UnsupportedOperationException` on startup: app crash-loops via Kubernetes / systemd until config is fixed. Downstream lag grows linearly.
- Silently ignored standby replicas: stateful failover takes minutes/hours (full restoration) instead of seconds. Operators believe they have warm standbys; they don't.
- Discovered late, often during a P1: the new protocol was enabled in a config refactor weeks earlier; the standby promise has been false the whole time.

## How to fix (bad → good code)

```properties
# BAD — combination crashes at startup
group.protocol=streams
group.instance.id=${HOSTNAME}

# GOOD — pick one
# Option A: keep KIP-1071, drop static membership
group.protocol=streams
# (remove group.instance.id)

# Option B: keep static membership, drop KIP-1071
# (remove group.protocol)
group.instance.id=${HOSTNAME}
session.timeout.ms=60000
heartbeat.interval.ms=10000
```

```java
// BAD — regex source + KIP-1071
props.put("group.protocol", "streams");
builder.stream(Pattern.compile("orders-.*"), Consumed.with(...));

// GOOD — explicit topic list, KIP-1071 stays
builder.stream(List.of("orders-eu", "orders-us"), Consumed.with(...));
```

## When this might be a false positive

- `num.standby.replicas` set to `0` explicitly — the value matches the default; the user has documented intent. Acceptable, but harmless to leave the property unset.
- A team running on AK 4.1 / CP 8.1 may have copied the config from a newer cluster — the `group.protocol=streams` line will be rejected on startup but `group.instance.id` is the actual config in effect. In that case the real fix is to update the cluster, not the config.

## Detection strategy

- Config: `group.protocol=streams` AND any of:
  - `group.instance.id` is set (non-empty),
  - `num.standby.replicas` is set to a positive integer,
  - `max.warmup.replicas` is set to a positive integer.
- Bytecode: `group.protocol=streams` AND a call to `builder.stream(Ljava/util/regex/Pattern;...)` or `KStream.subscribe(Ljava/util/regex/Pattern;)`.
- Confidence: HIGH for the config combinations (mechanical); MEDIUM for the regex bytecode case (Pattern arg may be a constant in a different class).

## References

- KIP-1071 — Streams Rebalance Protocol: https://cwiki.apache.org/confluence/display/KAFKA/KIP-1071
- Confluent agent-skills — kafka-streams-programming/references/topology-patterns.md § Assignment Strategy (the unsupported-features table)
- KIP-345 — Static membership: https://cwiki.apache.org/confluence/display/KAFKA/KIP-345
