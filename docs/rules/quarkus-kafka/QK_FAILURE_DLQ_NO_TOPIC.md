# QK_FAILURE_DLQ_NO_TOPIC

**Severity**: WARNING
**Confidence**: HIGH
**Detection**: config-file
**Tagline**: `failure-strategy=dead-letter-queue` with no `dead-letter-queue.topic` sends every failure to a topic named `dead-letter-topic-$channel` — and no one will think to look there.

## TL;DR

The linter flags Quarkus / SmallRye Reactive Messaging Kafka channels that set `failure-strategy=dead-letter-queue` without an explicit `dead-letter-queue.topic`. The connector falls back to a synthetic name `dead-letter-topic-$channel` (where `$channel` is the channel name). Two failure modes: (a) on managed clusters with `auto.create.topics.enable=false` the channel fails on first error trying to send to a non-existent topic; (b) on permissive clusters the channel auto-creates a topic with broker defaults (1 partition, RF=1) that nobody knew existed.

## The setup

Quarkus developer reads the docs, picks `failure-strategy=dead-letter-queue` for their incoming channel, and ships. Tests pass on Testcontainers (which auto-creates topics). In staging on a managed cluster, the first poison pill causes the channel to fail attempting to publish to `dead-letter-topic-orders` — which doesn't exist.

## What's actually happening

SmallRye Reactive Messaging's Kafka connector supports three (and a half) failure strategies on incoming channels:

| Strategy | Behavior | Required extra config |
|----------|----------|------------------------|
| `fail` (default) | Channel stops, no offset commit | none |
| `ignore` | Log + commit + continue (silent data loss) | none |
| `dead-letter-queue` | Publish to DLT topic + commit + continue | optional `dead-letter-queue.topic`, `.key.serializer`, `.value.serializer` |
| `delayed-retry-topic` | Publish to retry topics with delay | retry topic config |

When `dead-letter-queue.topic` is omitted, the default destination is `dead-letter-topic-$channel`. The topic must exist (or the broker must allow auto-creation). The serializers default to "deduced from the input key/value deserializers" which works for `String/Integer/Long` but breaks for Avro/JSON without an explicit serializer pair.

## Why this is subtle

- The default name pattern is undocumented in the obvious places; engineers see "dead-letter-queue strategy" and assume sane defaults.
- On Testcontainers / local Kafka, `auto.create.topics.enable=true` is default → the topic just appears. Tests pass.
- On production managed clusters (Confluent Cloud, AWS MSK with disabled auto-create) → first error → channel stops with cryptic "topic not found" → cascading from the failure-handling path itself.
- If the topic is auto-created, it gets broker defaults (1 partition, RF=1) — useless for HA, contradicts capacity planning.
- Naming: `dead-letter-topic-X` (with `dead-letter-topic-` prefix) is unusual; most teams adopt `X.dlt` or `X.dlq`. Search "dlq" or "dlt" in the topic list will not find it.

## Operational impact

- First poison pill: channel stops (managed cluster, no auto-create) — same impact as `failure-strategy=fail`, despite the DLQ config.
- Or: auto-created shadow topic with 1 partition, RF=1, named oddly, never alerted on, accumulates poison records silently.
- DLQ replay procedure breaks: ops finds `orders.dlt` empty (because the real DLT is `dead-letter-topic-orders`).

## Failure scenarios (walkthrough)

1. **The managed-cluster surprise.** Local dev: works. Staging Confluent Cloud: first record with a Jackson deserialization error → channel fails → "topic dead-letter-topic-orders does not exist". Engineer creates the topic by hand. Production: same story, customer impact during the window.

2. **The hidden shadow.** Self-hosted cluster with auto-create on. Poison pills accumulate in `dead-letter-topic-orders` — 1 partition, RF=1, unmonitored. After a year, the partition is at 50 GB on one broker. That broker fills its disk and goes down. Investigation reveals the orphan topic.

## How to fix

```properties
# BAD — implicit default name
mp.messaging.incoming.orders.failure-strategy=dead-letter-queue

# GOOD — explicit, named per your project conventions
mp.messaging.incoming.orders.failure-strategy=dead-letter-queue
mp.messaging.incoming.orders.dead-letter-queue.topic=orders.dlt
mp.messaging.incoming.orders.dead-letter-queue.key.serializer=org.apache.kafka.common.serialization.StringSerializer
mp.messaging.incoming.orders.dead-letter-queue.value.serializer=io.confluent.kafka.serializers.KafkaAvroSerializer
```

Plus: create the DLT topic with the same partition count as the source topic via your topic management tool (Strimzi `KafkaTopic` CR, Terraform, kafka-cli at deploy time). Don't rely on auto-create.

For deserialization failures specifically, also set:

```properties
mp.messaging.incoming.orders.fail-on-deserialization-failure=false
# (then failures go through failure-strategy → DLQ)
```

## When this might be a false positive

- Test profiles where auto-create is expected and the default topic name is fine.
- The DLT topic is provisioned via infra-as-code under the exact name `dead-letter-topic-$channel` deliberately, with the right RF and partitions. Unusual but acceptable.

## Detection strategy

- config-file: read `application.properties` / `application.yml`.
- For each channel `mp.messaging.incoming.<channel>.failure-strategy=dead-letter-queue`, check for presence of `mp.messaging.incoming.<channel>.dead-letter-queue.topic`. If absent, flag.
- Additionally check `dead-letter-queue.key.serializer` and `.value.serializer` if the channel's value deserializer is not a primitive (Avro, JSON, Protobuf).
- Confidence HIGH — the keys are exact.

## References

- Quarkus Kafka Reactive Messaging — failure strategies: https://quarkus.io/guides/kafka
- SmallRye Reactive Messaging Kafka — failure strategies reference: https://smallrye.io/smallrye-reactive-messaging/latest/kafka/receiving-kafka-records/#failure-strategies
- Quarkus Kafka guide: https://quarkus.io/guides/kafka-reactive-getting-started
