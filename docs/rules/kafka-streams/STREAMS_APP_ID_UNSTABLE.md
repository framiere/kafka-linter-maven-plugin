# STREAMS_APP_ID_UNSTABLE

**Severity**: ERROR
**Confidence**: MEDIUM
**Detection**: config-file
**Tagline**: If your `application.id` shifts, your state is gone.

## TL;DR

The linter flags `application.id` values that are dynamic / unstable across deploys — e.g. `"my-app-" + UUID.randomUUID()`, environment-derived without a stable suffix, or read from a non-pinned `${...}` placeholder.

## What's happening (the mechanism)

`application.id` is the foundation of every identity in Kafka Streams: the consumer group, the internal topic prefix (`<application.id>-<store>-changelog`), the client.id prefix, the state directory subpath. A change in `application.id` means:

- A new consumer group → starts from `auto.offset.reset` (`earliest` or `latest`), abandoning all committed offsets.
- New internal topics → all changelog/repartition state is orphaned.
- New state directory subpath → local stores are unreachable, full restore from new (empty) changelog → empty results.

The defect frequently shows up as `application.id = "${HOSTNAME}-streams"` (changes per pod), `"streams-" + version` (changes per deploy), or as a UUID generated at startup. It's also catastrophic if two unrelated apps share the same `application.id` — they fight for the same consumer group and partition assignment.

## Operational impact

- Every deploy: `kafka.streams:type=stream-task-metrics,task-id=...:restore-records-rate` saturates. Aggregations return 0 / wrong values for the restore window.
- Old internal topics linger forever. Cluster storage cost climbs without an obvious owner.
- If the change is per-pod (HOSTNAME-based), each pod creates its *own* set of internal topics. Topology partitioning is meaningless.
- Auditors / SRE see ghost consumer groups proliferating in `__consumer_offsets`.

## How to fix

```java
// BAD
props.put(StreamsConfig.APPLICATION_ID_CONFIG,
          "my-app-" + UUID.randomUUID());

// BAD — changes when the pod hostname does
props.put(StreamsConfig.APPLICATION_ID_CONFIG,
          "my-app-" + System.getenv("HOSTNAME"));

// BAD — changes per release
props.put(StreamsConfig.APPLICATION_ID_CONFIG,
          "my-app-v" + AppVersion.get());

// GOOD — constant, namespaced
props.put(StreamsConfig.APPLICATION_ID_CONFIG, "billing.aggregator.v1");
```

If you genuinely need multiple parallel topologies (e.g. blue/green, A/B), make the suffix part of a deliberate environment promotion (Helm value, config map), reviewed at PR time, not generated at runtime.

## When this might be a false positive

- Per-environment IDs (`my-app.dev`, `my-app.prod`) read from a controlled config — fine.
- Test code using a random UUID to isolate test runs — fine for tests, suppress that path.

## Detection strategy

- Bytecode: `Properties.put(StreamsConfig.APPLICATION_ID_CONFIG, ...)` where the value involves:
  - `UUID.randomUUID().toString()` — HIGH confidence.
  - `System.getenv("HOSTNAME")` / `System.getenv("POD_NAME")` — MEDIUM.
  - `+` concatenation with non-constant arguments → MEDIUM.
- Config files: value contains `${RANDOM}`, `${HOSTNAME}`, or `${random.uuid}` (Spring) — MEDIUM.
- Confidence: MEDIUM — sometimes deliberate.

## References

- Streams config — application.id: https://kafka.apache.org/documentation/streams/developer-guide/config-streams.html#application-id
- Confluent — Streams application lifecycle: https://docs.confluent.io/platform/current/streams/architecture.html
