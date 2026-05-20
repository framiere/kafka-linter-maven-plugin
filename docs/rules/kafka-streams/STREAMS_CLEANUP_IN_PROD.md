# STREAMS_CLEANUP_IN_PROD

**Severity**: ERROR
**Confidence**: HIGH
**Detection**: bytecode
**Tagline**: `cleanUp()` is for tests. In prod it's `rm -rf` on your state.

## TL;DR

The linter flags any call to `KafkaStreams#cleanUp()` outside test code. `cleanUp()` deletes the local state directory — forcing a full restore from changelog on next start. Catastrophic in production.

## What's happening (the mechanism)

`KafkaStreams.cleanUp()` recursively removes everything under `state.dir/<application.id>/`. It's intended for:
- Local development (start fresh between runs).
- `TopologyTestDriver` cleanup.
- Genuine "reset all state" operations done deliberately by an operator.

It can only be called in `CREATED` or `NOT_RUNNING` state; calling it from any other state throws `IllegalStateException`. But the more common failure is calling it as part of startup logic ("clear stale state from a previous run"). On a stateful production app, that turns every deploy into a full restore — minutes to hours of downtime, broker pressure spike, and the app emits empty/incorrect results until restore completes.

## Operational impact

- Every restart triggers a full restore. Same symptom as `state.dir` on /tmp, but self-inflicted.
- Massive spike in `kafka.streams:type=stream-task-metrics,...:restore-records-rate` cluster-wide.
- During restore: aggregations return wrong values, IQ endpoints fail.
- Bandwidth spike to the broker for changelog re-read.

## How to fix

```java
// BAD — runs on every startup
KafkaStreams streams = new KafkaStreams(topology, props);
streams.cleanUp();
streams.start();

// GOOD — only call in test code, or as a deliberate operator action
// In a CLI tool guarded by an explicit flag:
if (args.contains("--reset-state") && env.equals("dev")) {
    streams.cleanUp();
}
streams.start();
```

To actually reset state across the cluster, use the Streams Reset Tool (`kafka-streams-application-reset.sh`) on the broker side — it also deletes changelogs, which `cleanUp()` doesn't.

## When this might be a false positive

- `TopologyTestDriver` tests.
- Operator scripts for resetting an environment, guarded by an explicit confirmation flag.

## Detection strategy

- Bytecode: `INVOKEVIRTUAL org/apache/kafka/streams/KafkaStreams.cleanUp ()V`.
- Suppress when the calling class extends a test base class, is annotated with `@Test`, or lives under `src/test/...` (heuristic, but high-signal).
- Confidence: HIGH for non-test code.

## References

- KafkaStreams.cleanUp javadoc: https://kafka.apache.org/40/javadoc/org/apache/kafka/streams/KafkaStreams.html#cleanUp--
- kafka-streams-application-reset tool: https://docs.confluent.io/platform/current/streams/developer-guide/app-reset-tool.html
