# STREAMS_UNCAUGHT_HANDLER_DEPRECATED_API

**Severity**: WARNING
**Confidence**: HIGH
**Detection**: bytecode
**Tagline**: `setUncaughtExceptionHandler(Thread.UncaughtExceptionHandler)` is a JDK API. Streams gave you a real one in KIP-671 — use it.

## TL;DR

The linter flags calls to the **deprecated** overload `KafkaStreams.setUncaughtExceptionHandler(Thread.UncaughtExceptionHandler)`. Since Kafka 2.8 (KIP-671), the preferred API is `setUncaughtExceptionHandler(StreamsUncaughtExceptionHandler)`, which returns a `StreamsUncaughtExceptionResponse` enum: `REPLACE_THREAD`, `SHUTDOWN_CLIENT`, or `SHUTDOWN_APPLICATION`. The legacy `Thread.UncaughtExceptionHandler` runs **after** the thread is already shut down — your handler can log, but cannot recover. The new API lets you actually decide.

## The setup

Team encounters an uncaught exception in a Streams topology — likely a NullPointerException in user code on a malformed record. The whole client transitions to ERROR and stops. They look up "Kafka Streams uncaught exception handler" and find the old `Thread.UncaughtExceptionHandler` overload. They wire it up, log the exception, and the next time it happens, the client *still* dies — because the legacy handler runs after-the-fact.

## What's actually happening

Pre-Kafka-2.8:

```java
kafkaStreams.setUncaughtExceptionHandler((thread, throwable) -> {
    log.error("Streams thread {} died", thread.getName(), throwable);
});
```

This is the standard JDK `Thread.UncaughtExceptionHandler` interface. It's called *after* the thread has died, by the JVM. The handler cannot prevent shutdown; it can only log. Kafka Streams' default behavior is to shut down the client when any stream thread dies.

Post-KIP-671 (Kafka 2.8+):

```java
kafkaStreams.setUncaughtExceptionHandler(throwable -> {
    if (throwable instanceof MyTransientException) {
        return StreamsUncaughtExceptionResponse.REPLACE_THREAD;
    }
    if (throwable instanceof MyShardingException) {
        return StreamsUncaughtExceptionResponse.SHUTDOWN_APPLICATION;
    }
    return StreamsUncaughtExceptionResponse.SHUTDOWN_CLIENT;
});
```

Three responses:

- **`REPLACE_THREAD`** — current thread is terminated, transitions to DEAD; a new thread is spun up if the client is still RUNNING/REBALANCING. Use for transient, retryable errors (e.g., a known-bad record handled idempotently in your topology).
- **`SHUTDOWN_CLIENT`** (default if no handler set) — this client transitions to ERROR / NOT_RUNNING. Other instances continue.
- **`SHUTDOWN_APPLICATION`** — best-effort shutdown of every client in the application, signaled via the rebalance protocol.

The old API is deprecated; the JavaDoc explicitly says "use the StreamsUncaughtExceptionHandler overload instead."

## Why this is subtle

- The two methods have the same name; only the parameter type differs. Auto-import + IDE completion often picks the deprecated one because it's been around longer.
- Both compile. Both run. The deprecated one *does something* (it logs), so behavior looks correct in tests where the thread genuinely should die.
- The `REPLACE_THREAD` capability is the actual reason teams reach for an uncaught handler — recover transient failures. The deprecated handler cannot do this.
- Spring Cloud Stream Kafka Streams binder and Quarkus extension still accept either, sometimes wrapping the legacy one internally — make sure you know which you're feeding.

## Operational impact

- Client shuts down on every stream-thread death — no recovery from transient bugs.
- Operator has to restart the app manually after every poison-record-like incident.
- The `Streams app is down` alert fires for events that REPLACE_THREAD would silently recover.

## Failure scenarios (walkthrough)

1. **The flaky downstream.** A topology has a stateful aggregation that occasionally hits a NullPointerException due to a known-flaky upstream join. With deprecated handler: app shuts down, on-call restarts it manually. With REPLACE_THREAD: thread dies, new one is created, processing resumes from last committed offset. Same data path is reprocessed (which the topology handles idempotently).

2. **The horizontal shutdown.** A bug affects every instance the same way (e.g., a deserialization regression). With deprecated handler: every client shuts down at the same moment as it hits the bad record, but the orchestrator might restart each one independently and they spin forever. With SHUTDOWN_APPLICATION: rebalance protocol signals every client to stop cleanly, orchestrator sees them all down, alerts, no restart loop.

## How to fix

```java
// BAD — deprecated
kafkaStreams.setUncaughtExceptionHandler((thread, t) -> {
    log.error("died", t);
});

// GOOD — KIP-671
kafkaStreams.setUncaughtExceptionHandler(throwable -> {
    meterRegistry.counter("streams.uncaught",
        "exception", throwable.getClass().getSimpleName()).increment();
    if (throwable instanceof TransientUpstreamException) {
        log.warn("Replacing thread on transient exception", throwable);
        return StreamsUncaughtExceptionResponse.REPLACE_THREAD;
    }
    log.error("Shutting down on uncaught exception", throwable);
    return StreamsUncaughtExceptionResponse.SHUTDOWN_CLIENT;
});
```

For multi-instance applications, prefer `SHUTDOWN_APPLICATION` for shared-fate errors (schema corruption that affects all instances) — it stops everyone cleanly via the rebalance protocol instead of relying on each orchestrator to find its instance down.

## Consult a friend?

> 🤝 **Slow down.** `REPLACE_THREAD` will keep restarting the thread on the same bad record forever if the failure is deterministic. Verify:
> - You have an idempotent processing path (or you've published the offending record to a DLT and skipped past it).
> - You have a rate-limit / max-restarts safeguard (KIP-671 itself does not provide this — you need it in your handler or via a state-machine check).
> - For EOS topologies, `REPLACE_THREAD` interacts with the transactional producer fencing — read KIP-671 and the EOS chapter together.

## When this might be a false positive

- The code uses the lambda form `(thread, t) -> ...` against the deprecated overload because the type `StreamsUncaughtExceptionHandler` overload didn't exist in the Kafka version on the classpath. Verify the `kafka-streams` version is < 2.8 before suppressing.
- Test fixtures using the legacy API for backward compatibility with old harnesses.

## Detection strategy

- bytecode: invocations of `KafkaStreams.setUncaughtExceptionHandler(Ljava/lang/Thread$UncaughtExceptionHandler;)V`.
- Compare to the modern `(Lorg/apache/kafka/streams/errors/StreamsUncaughtExceptionHandler;)V`.
- Confidence HIGH — method descriptor is exact.

## References

- KIP-671 — Streams-specific uncaught exception handler: https://cwiki.apache.org/confluence/display/KAFKA/KIP-671:+Introduce+Kafka+Streams+Specific+Uncaught+Exception+Handler
- Confluent — Handling uncaught exceptions: https://developer.confluent.io/tutorials/error-handling/kstreams.html
- KIP-696 — Streams FSM ERROR state: https://cwiki.apache.org/confluence/display/KAFKA/KIP-696:+Update+Streams+FSM+to+clarify+ERROR+state+meaning
