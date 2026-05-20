# STREAMS_NO_SHUTDOWN_HOOK

**Severity**: WARNING
**Confidence**: MEDIUM
**Detection**: bytecode
**Tagline**: Without a shutdown hook, you commit offsets via SIGKILL.

## TL;DR

The linter flags `KafkaStreams.start()` invocations in apps that don't register a JVM shutdown hook (`Runtime.getRuntime().addShutdownHook`) or framework equivalent calling `streams.close(Duration)`. On SIGTERM, the JVM dies before Streams can flush state, commit offsets, and gracefully leave the consumer group.

## What's happening (the mechanism)

Closing `KafkaStreams` cleanly:
- Flushes RocksDB and the in-memory cache.
- Commits offsets (under EOS, ends the open transaction).
- Sends `LeaveGroup` to the group coordinator, triggering a fast rebalance.
- Releases the local state directory lock.

Without close, after SIGTERM the JVM exits in `session.timeout.ms` (default 45s); the group coordinator waits for the session timeout to expire before reassigning the partitions to surviving instances. Uncommitted records reprocess on restart. Under EOS, the open transaction is fenced — recoverable, but adds restart latency.

Many frameworks register this for you (Spring `StreamsBuilderFactoryBean`, Quarkus `@KafkaStreams`). Hand-rolled applications usually don't.

## Operational impact

- Long rebalances on deploy (waiting for session timeout instead of immediate `LeaveGroup`).
- Reprocessing of last commit interval's worth of records.
- Under EOS, brief transaction-abort window seen as `kafka.streams:type=stream-thread-metrics,...:transaction-abort-rate` spike.
- Local state directory lock files left behind → "another stream already running" errors on restart.

## How to fix

```java
KafkaStreams streams = new KafkaStreams(topology, props);
CountDownLatch latch = new CountDownLatch(1);

Runtime.getRuntime().addShutdownHook(new Thread("streams-shutdown") {
    @Override public void run() {
        streams.close(Duration.ofSeconds(30));
        latch.countDown();
    }
});

try {
    streams.start();
    latch.await();
} catch (Throwable t) {
    System.exit(1);
}
System.exit(0);
```

In Kubernetes, set `terminationGracePeriodSeconds` larger than `close(Duration)` to actually give the hook time to run.

## When this might be a false positive

- Apps using a framework that already registers a shutdown hook (Spring Boot, Quarkus). Detect framework presence and suppress.
- Tests using `TopologyTestDriver`.

## Detection strategy

- Bytecode: `KafkaStreams.start()` is called in a class whose enclosing module doesn't contain a `Runtime.addShutdownHook(...)` referencing a `KafkaStreams.close` invocation.
- Suppress when the class is in a Spring `@Configuration` returning a `StreamsBuilderFactoryBean`, or annotated `@ApplicationScoped` with Quarkus.
- Confidence: MEDIUM.

## References

- KafkaStreams.close javadoc: https://kafka.apache.org/40/javadoc/org/apache/kafka/streams/KafkaStreams.html#close-java.time.Duration-
- Confluent — Lifecycle of a streams app: https://docs.confluent.io/platform/current/streams/developer-guide/write-streams.html

## Cross-reference

Also recommended by Confluent agent-skills — see kafka-streams-programming/references/production-hardening.md § Graceful shutdown.
