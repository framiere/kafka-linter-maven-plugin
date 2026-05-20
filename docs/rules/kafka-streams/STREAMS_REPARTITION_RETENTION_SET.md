# STREAMS_REPARTITION_RETENTION_SET
**Severity**: ERROR
**Confidence**: HIGH
**Detection**: bytecode + config-file
**Tagline**: Set retention on a repartition topic and the records you haven't processed yet vanish.
**Source**: Confluent agent-skills — kafka-streams-programming/references/config-baseline.md § Topic Management Rules, kafka-streams-programming/references/architecture.md § Repartition Topics.

## TL;DR

The linter flags any explicit `retention.ms` or `retention.bytes` configuration on a Kafka Streams repartition topic. Confluent's skill states the rule plainly:

> Repartition topics: Auto-created with infinite retention. Don't set retention — causes data loss.

Repartition topics carry data that is mid-flight — written by an upstream selectKey/groupBy and not yet consumed by the downstream task. If a record exceeds the retention window before the downstream task picks it up, it's deleted from the broker — and the topology's mathematical result is wrong, silently.

## What's happening (the mechanism)

When the DSL detects that an operator changes the key (`selectKey`, `groupBy`, `map` that touches the key), it materializes an internal "repartition" topic so the downstream operator can consume the re-keyed stream. The naming is `<application.id>-<operator-name>-repartition` (positional if not named — see `STREAMS_UNNAMED_MATERIALIZED`).

By design, Streams auto-creates repartition topics with **infinite retention** (`retention.ms=-1`). That's not a bug — it's correctness. If the downstream task is behind, the records must still be there when it catches up. There is no "the message was old" path; a missing record is a wrong result.

The two ways this rule fires:
1. **Operator-level**: explicit retention on the `Repartitioned` builder (`Repartitioned.with(...)` with no — wait, `Repartitioned` does not expose retention, so this path is harmless. The real risks are below.)
2. **Cluster-side `kafka-configs.sh`** invocation that an SRE ran to "clean up" a broker disk full event, applying retention to all internal topics. The lint can only see this when the topic config is in a tracked file (Kubernetes `KafkaTopic` CR, Terraform, Confluent Cloud topic config block).
3. **`kafka-topics.sh --create --config retention.ms=...`** scripts in the project (e.g., `create-topics.sh`) targeting a name that matches the repartition naming pattern.

So this rule fires primarily on topic-management files (YAML, Terraform, shell scripts) where someone is explicitly creating or altering a topic named `*-repartition` and providing retention.

## Operational impact

- Records exist on the repartition topic, downstream task is rebalancing or restoring → retention expires → records deleted → downstream sees a hole.
- Aggregations under-count by exactly the lost batch with no error signal.
- Hard to detect — the lag metric returns to 0 because the deleted records "no longer exist". You only notice when downstream output reconciliation finds a gap.

## How to fix (bad → good code)

```bash
# BAD — create-topics.sh tries to pre-create a Streams repartition topic with retention
kafka-topics --create --topic my-app-rekey-repartition \
  --partitions 6 --replication-factor 3 \
  --config retention.ms=604800000   # WRONG — 7d retention on a repartition topic

# GOOD — do not pre-create repartition topics at all; let Streams create them with infinite retention
# (only pre-create source, output, and DLQ topics)
```

```yaml
# BAD — Strimzi/KafkaTopic CR sets retention on a *-repartition topic
apiVersion: kafka.strimzi.io/v1beta2
kind: KafkaTopic
metadata:
  name: my-app-rekey-repartition
spec:
  partitions: 6
  config:
    retention.ms: "604800000"   # WRONG

# GOOD — don't manage repartition topics in IaC; let Streams handle them
```

## When this might be a false positive

- The topic name happens to end in `-repartition` but is **not** a Streams-internal topic (e.g., a manual repartitioning topic the user maintains themselves). Rare — the suffix is conventional.
- A user has set retention to `-1` (infinite) explicitly, which matches the default. Harmless, lint should accept `-1`.

## Detection strategy

- Topic naming: any topic whose name matches `<application.id>-*-repartition` where `application.id` is read from another config in the same project.
- Lint targets:
  - Shell scripts containing `kafka-topics ... --topic <pattern>-repartition ... --config retention.ms=...`
  - Terraform resources `confluent_kafka_topic` with names matching the pattern
  - Strimzi `KafkaTopic` CRs
  - `confluent-cli` topic-create commands in any tracked file
- Flag values: any explicit `retention.ms` or `retention.bytes` other than `-1` (infinite).
- Confidence: HIGH when application.id is known and the topic name matches the pattern.

## References

- Confluent agent-skills — kafka-streams-programming/references/config-baseline.md § Topic Management Rules
- Confluent agent-skills — kafka-streams-programming/references/architecture.md § Repartition Topics
- Apache Kafka docs — Streams internal topics: https://kafka.apache.org/documentation/streams/architecture
- Related rule: [STREAMS_UNNAMED_MATERIALIZED](STREAMS_UNNAMED_MATERIALIZED.md) — the naming side of the same concern.
