# QK_KSTREAMS_NO_APPLICATION_ID

**Severity**: ERROR
**Confidence**: HIGH
**Detection**: config-file
**Tagline**: A Streams app without an application-id is a consumer group named "guess".

## TL;DR

`quarkus.kafka-streams.application-id` defaults to `${quarkus.application.name}`. Two services with the same `quarkus.application.name` (or unset → `app`) collide on the same consumer group, fight over partitions, and silently process each other's records.

## What's happening (the mechanism)

`application.id` in Kafka Streams determines:
- The Kafka consumer group used to read source topics.
- The prefix for internal topics (changelog, repartition, state store).
- The directory under `state.dir` where local RocksDB stores live.

Two Streams apps with the same `application.id`:
- Are members of the same consumer group → partitions get distributed between them, so each app sees only half the input.
- Read/write the same changelog topics → state corruption.
- Try to claim the same RocksDB directories on disk.

Defaulting to `${quarkus.application.name}` is fine if you set that uniquely per service. If you don't (or two pipelines live in the same app), boom.

## Operational impact

- Streams app A processes record X. Streams app B processes record Y from the same partition. Neither processes both.
- `state.dir` lock contention: one of the apps fails on startup with `LockException`.
- Changelog topics get interleaved updates from both apps, breaking state restore.

## How to fix

```properties
# BAD — relies on default
# (no quarkus.kafka-streams.application-id set)

# GOOD — explicit, environment-aware
quarkus.kafka-streams.application-id=orders-aggregator-v1
# Or per-environment
%prod.quarkus.kafka-streams.application-id=orders-aggregator-v1-prod
%dev.quarkus.kafka-streams.application-id=orders-aggregator-v1-dev-${user.name}
```

## When this might be a false positive

- Single-Streams-pipeline service with a stable, unique `quarkus.application.name`.

## Detection strategy

- Config: presence of `quarkus.kafka-streams.*` properties WITHOUT `quarkus.kafka-streams.application-id`.
- Confidence: HIGH for multi-pipeline projects; MEDIUM for single-pipeline.

## References

- https://quarkus.io/guides/kafka-streams
- https://kafka.apache.org/documentation/streams/developer-guide/config-streams.html (application.id)
