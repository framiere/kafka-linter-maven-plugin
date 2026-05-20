# STREAMS_INTERACTIVE_QUERY_BEFORE_RUNNING

**Severity**: WARNING
**Confidence**: MEDIUM
**Detection**: bytecode
**Tagline**: Querying a store before `RUNNING` is querying a closet that hasn't been built.

## TL;DR

The linter flags code that calls `KafkaStreams#store(StoreQueryParameters)` without first checking that `KafkaStreams.State == RUNNING` (or attaching a `StateListener`). Calling `store(...)` in any other state throws `InvalidStateStoreException` and consumers see 5xx until the app is fully restored.

## What's happening (the mechanism)

A Streams instance moves through these states: `CREATED → REBALANCING → RUNNING → PENDING_SHUTDOWN → NOT_RUNNING`, plus `ERROR`/`PENDING_ERROR` on failure. Local state stores are only safe to query in `RUNNING`. During `REBALANCING` and `RESTORING`, the underlying store may not exist (task is migrating between instances), may be partially restored (data is incomplete), or may be locked for writes.

A common pattern is exposing a REST endpoint immediately on startup that does:
```java
ReadOnlyKeyValueStore<String, Long> store = streams.store(
    StoreQueryParameters.fromNameAndType("counts", QueryableStoreTypes.keyValueStore()));
return store.get(key);
```

If the first request lands before `RUNNING`, it throws `InvalidStateStoreException`. With a load balancer and rolling deploys, every new pod has a window where it's serving traffic but not in `RUNNING`. K8s readiness probes that don't check Streams state will route traffic to a pod that returns 500s.

## Operational impact

- 5xx errors on the IQ endpoint during deploys and rebalances. SLO burn.
- `kafka.streams:type=stream-metrics,client-id=...:state` JMX gauge (KIP-757) shows non-RUNNING; correlated with downstream errors.
- "Empty" results when partition was migrated to another instance and the local store no longer holds the data — different anti-pattern; needs `KafkaStreams.queryMetadataForKey` for cross-instance routing.

## How to fix

```java
// BAD — race with startup
@GetMapping("/counts/{key}")
public Long getCount(@PathVariable String key) {
    return streams.store(...).get(key);
}

// GOOD — gate on RUNNING + readiness probe
public class StreamsHealth {
    private volatile boolean ready = false;
    public StreamsHealth(KafkaStreams streams) {
        streams.setStateListener((newState, oldState) -> {
            ready = newState == KafkaStreams.State.RUNNING;
        });
    }
    public boolean isReady() { return ready; }
}

@GetMapping("/counts/{key}")
public ResponseEntity<Long> getCount(@PathVariable String key) {
    if (!health.isReady()) {
        return ResponseEntity.status(503).build();   // K8s retries; LB does too
    }
    try {
        return ResponseEntity.ok(
            streams.store(StoreQueryParameters.fromNameAndType("counts",
                QueryableStoreTypes.keyValueStore())).get(key));
    } catch (InvalidStateStoreException e) {
        return ResponseEntity.status(503).build();
    }
}
```

Hook the readiness probe in K8s to `streams.state() == RUNNING`.

## When this might be a false positive

- Tests that explicitly wait for `RUNNING` before querying. Suppress for test paths.
- Server frameworks (Spring's `StreamsBuilderFactoryBean`) that wire readiness for you.

## Detection strategy

- Bytecode: any call to `KafkaStreams.store(...)` in code that isn't preceded (in the same method or guarded by a check) by `state() == RUNNING` or by a `StateListener` registration. Detection of "guarded" is approximate — confidence MEDIUM.
- Pattern: a Spring `@RestController` or JAX-RS resource method directly invoking `KafkaStreams.store(...)`. HIGH suspicion.
- Confidence: MEDIUM.

## References

- KafkaStreams.State javadoc: https://kafka.apache.org/40/javadoc/org/apache/kafka/streams/KafkaStreams.State.html
- Confluent — Interactive Queries: https://docs.confluent.io/platform/current/streams/developer-guide/interactive-queries.html
- KIP-757 — Expose state machine state via JMX: https://cwiki.apache.org/confluence/display/KAFKA/KIP-757%3A+expose+state+machine+state+via+JMX

## Cross-reference

Also recommended by Confluent agent-skills — see kafka-streams-programming/references/production-hardening.md § Interactive queries and readiness state.
