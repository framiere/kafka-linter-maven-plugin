# STREAMS_UNCAUGHT_HANDLER_MISSING

**Severity**: WARNING
**Confidence**: MEDIUM
**Detection**: bytecode
**Tagline**: No uncaught handler? Then your topology has one shape — `DEAD`.

## TL;DR

The linter flags `KafkaStreams` instances where `setUncaughtExceptionHandler(StreamsUncaughtExceptionHandler)` is never called. The default behavior is `SHUTDOWN_CLIENT`: any non-internally-handled exception kills the instance, which usually means the JVM.

## What's happening (the mechanism)

KIP-671 (Kafka 2.8) introduced `StreamsUncaughtExceptionHandler` with three responses:
- `REPLACE_THREAD` — restart only the failing thread. The instance keeps going.
- `SHUTDOWN_CLIENT` — close just this instance, leave the fleet alive. **Default.**
- `SHUTDOWN_APPLICATION` — propagate shutdown to every instance in the consumer group via the rebalance protocol.

Without setting a handler, the default `SHUTDOWN_CLIENT` activates: when a deserialization error sneaks past the deserialization handler, or a user-code exception bubbles out of a processor, the StreamThread dies; if it's the last alive thread, the `KafkaStreams` instance transitions to `ERROR`. Your supervisor (Kubernetes, systemd) restarts the JVM, which polls the same poison record, and the cycle repeats — `CrashLoopBackOff`.

Even worse: many users still call the deprecated `setUncaughtExceptionHandler(Thread.UncaughtExceptionHandler)` from before KIP-671. That handler fires *after* the thread is already dead and can't restart it — a no-op for recovery.

## Operational impact

- `kafka.streams:type=stream-metrics,client-id=...:alive-stream-threads` falls to 0 on the affected instance.
- Pod restart loop visible in Kubernetes events.
- One bad record DDoSes the entire deployment.
- Fleet-wide outage from a single producer mistake upstream.

## How to fix

```java
KafkaStreams streams = new KafkaStreams(topology, props);

// BAD — handler not set; default SHUTDOWN_CLIENT on any error.

// GOOD — replace thread for transient errors, shut down on repeat failures
streams.setUncaughtExceptionHandler(exception -> {
    log.error("Uncaught exception in stream thread", exception);
    if (isTransient(exception)) {
        return StreamThreadExceptionResponse.REPLACE_THREAD;
    }
    return StreamThreadExceptionResponse.SHUTDOWN_APPLICATION;
});

streams.start();
```

A typical pattern is a max-failures-per-window handler:

```java
public class MaxFailuresHandler implements StreamsUncaughtExceptionHandler {
    private final int maxFailures;
    private final Duration window;
    private final Deque<Instant> failures = new ConcurrentLinkedDeque<>();

    public StreamThreadExceptionResponse handle(Throwable e) {
        Instant now = Instant.now();
        failures.add(now);
        failures.removeIf(t -> Duration.between(t, now).compareTo(window) > 0);
        return failures.size() > maxFailures
            ? StreamThreadExceptionResponse.SHUTDOWN_CLIENT
            : StreamThreadExceptionResponse.REPLACE_THREAD;
    }
}
```

## When this might be a false positive

- Toy app where letting the JVM die is the desired behavior. Acceptable.

## Detection strategy

- Bytecode: a `KafkaStreams` instance is constructed (`new org/apache/kafka/streams/KafkaStreams`) and on the same instance no `setUncaughtExceptionHandler(Lorg/apache/kafka/streams/errors/StreamsUncaughtExceptionHandler;)` call exists in the enclosing class's bytecode. MEDIUM confidence (could be set in a helper).
- Also flag the deprecated overload: `setUncaughtExceptionHandler(Ljava/lang/Thread$UncaughtExceptionHandler;)`. HIGH confidence — that one cannot recover.
- Confidence: MEDIUM (default) / HIGH (deprecated overload).

## References

- KIP-671 — Streams Specific Uncaught Exception Handler: https://cwiki.apache.org/confluence/display/KAFKA/KIP-671:+Introduce+Kafka+Streams+Specific+Uncaught+Exception+Handler
- Confluent — Handling uncaught exceptions: https://developer.confluent.io/tutorials/error-handling/kstreams.html
- Bill Bejeck — Streams uncaught exceptions workshop: https://github.com/bbejeck/streams-uncaught-exceptions-workshop

## Cross-reference

Also recommended by Confluent agent-skills — see kafka-streams-programming/references/production-hardening.md § StreamsUncaughtExceptionHandler (KIP-671 MaxFailures pattern), kafka-streams-programming/SKILL.md § Invariant Checklist #6.
