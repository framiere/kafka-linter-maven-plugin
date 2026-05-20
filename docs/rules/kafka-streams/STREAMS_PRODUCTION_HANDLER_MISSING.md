# STREAMS_PRODUCTION_HANDLER_MISSING

**Severity**: WARNING
**Confidence**: MEDIUM
**Detection**: bytecode + config-file
**Tagline**: `default.production.exception.handler` unset means the first `RecordTooLargeException` kills the topology.

## TL;DR

The linter flags Kafka Streams applications that do not set `default.production.exception.handler` (also: `processing.exception.handler` since Kafka 3.9). The default implementation `DefaultProductionExceptionHandler` returns `FAIL` for every exception — including transient ones like `RecordTooLargeException` that a sensible production app would prefer to log + drop. With the default, a single oversized record on a busy topic shuts the whole stream thread down.

## The setup

A Streams topology is running fine. One day, an upstream producer changes a field that bloats a record above `max.request.size`. The Streams app tries to write this record downstream, the broker rejects with `RecordTooLargeException`, the default handler returns `FAIL`, the stream thread shuts down. The on-call gets paged for "Streams down" and finds one bad record in the logs.

## What's actually happening

`org.apache.kafka.streams.errors.ProductionExceptionHandler` is the interface for handling producer-side failures during topology execution. The default implementation (`DefaultProductionExceptionHandler`):

```java
public class DefaultProductionExceptionHandler implements ProductionExceptionHandler {
    public ProductionExceptionHandlerResponse handle(
            ProducerRecord<byte[], byte[]> record, Exception exception) {
        return ProductionExceptionHandlerResponse.FAIL;
    }
}
```

`FAIL` causes the stream thread to log at ERROR, set the `sendException`, and shut down on the next `commit` cycle. There is no built-in `LogAndContinue` for the production path — you must implement your own.

The framework constraints (KIP-210):
- The handler is only invoked for exceptions returned via the producer callback. Exceptions thrown directly from `send()` (rare — `KafkaException`, authentication failures) are not routed through this handler.
- `ProducerFencedException` short-circuits — never passed to the handler (it's self-healing).
- Some authorization / invalid-host exceptions always fail regardless of the handler.

Companion handlers:
- `default.deserialization.exception.handler` for input (see existing `STREAMS_DEFAULT_DESERIALIZATION_HANDLER`).
- `processing.exception.handler` (Kafka 3.9+, KIP-1033) for in-topology user-code exceptions.

## Why this is subtle

- The default is "FAIL on every production error", which sounds safe ("fail loud!"). In practice it makes transient broker glitches look like topology-level disasters.
- Custom production handler is a one-class implementation that almost no one writes pre-incident.
- "Fail" causes the stream thread to die, but the application keeps running (and the StreamsUncaughtExceptionHandler default is `SHUTDOWN_CLIENT`, so the whole client shuts down). The interaction between these two defaults amplifies "one bad record kills the app."

## Operational impact

- Single oversized record → whole topology shuts down → page on-call.
- No DLT for production-side errors — the user must implement one in the custom handler.
- Stream state is left in whatever state the unfinished commit left it (next start re-processes uncommitted offsets — usually safe with EOS, but a discontinuity).

## Failure scenarios (walkthrough)

1. **The producer-side schema bloat.** An upstream service starts sending records with an extra payload field. Records cross `message.max.bytes` on the downstream topic. The Streams app trying to write these gets `RecordTooLargeException`. Default handler: FAIL. Thread dies, client transitions to ERROR. On-call wakes up.

2. **The authorization rotation.** Service account loses topic-write ACL during a credential rotation. Every produce attempt → `TopicAuthorizationException` (note: this is not routed through the user handler — it's pre-filtered as fatal). But if your custom handler tries to handle it, you find your code path doesn't actually run.

## How to fix

```java
// GOOD — custom handler that drops oversized records, fails on real errors
public class SizeTolerantProductionHandler implements ProductionExceptionHandler {
    private static final Logger log = LoggerFactory.getLogger(...);

    @Override
    public ProductionExceptionHandlerResponse handle(
            ProducerRecord<byte[], byte[]> record, Exception exception) {
        if (exception instanceof RecordTooLargeException) {
            log.error("Dropping oversized record on topic {} key {} size {}",
                record.topic(), record.key(), record.value().length, exception);
            // optional: emit a counter via Micrometer here
            return ProductionExceptionHandlerResponse.CONTINUE;
        }
        return ProductionExceptionHandlerResponse.FAIL;
    }

    @Override public void configure(Map<String, ?> configs) {}
}
```

```properties
# wire it
default.production.exception.handler=com.example.SizeTolerantProductionHandler
# add the deserialization companion while you're at it
default.deserialization.exception.handler=org.apache.kafka.streams.errors.LogAndContinueExceptionHandler
# and the processing one (Kafka 3.9+)
processing.exception.handler=com.example.MyProcessingHandler
```

## Consult a friend?

> 🤝 **Slow down.** A custom `ProductionExceptionHandler` that returns `CONTINUE` silently drops records. Before merging:
> - For every exception type you `CONTINUE` on, emit a metric counter — silent drops break audit chains.
> - For EOS topologies (`processing.guarantee=exactly_once_v2`), confirm that "drop on production failure" is acceptable. EOS guarantees "each record is processed exactly once *if it's processed at all*" — `CONTINUE` means "0 times", which may or may not be what you want.
> - Read KIP-210 for the framework's contract about when your handler runs (and when it doesn't).

## When this might be a false positive

- The application is dev / fixture and "fail loud on any error" is the right behavior.
- Custom handler exists but is registered via `StreamsBuilder` programmatically rather than via config — bytecode scan must check for `streamsConfig.put(StreamsConfig.DEFAULT_PRODUCTION_EXCEPTION_HANDLER_CLASS_CONFIG, ...)`.

## Detection strategy

- config-file: scan `application.properties` / `application.yml` / `streams.properties` for `default.production.exception.handler`.
- bytecode: scan for `StreamsConfig.DEFAULT_PRODUCTION_EXCEPTION_HANDLER_CLASS_CONFIG` (or the literal string) being used as a key in a `Properties.put(...)`/`Map.of(...)` argument.
- Combine: if neither source mentions the handler, flag.
- Same shape rule for `default.deserialization.exception.handler` (existing `STREAMS_DEFAULT_DESERIALIZATION_HANDLER`) and `processing.exception.handler`.
- Confidence MEDIUM — programmatic config from injected properties bypasses the static scan.

## References

- Apache Kafka — `DefaultProductionExceptionHandler` (2.7 javadoc): https://kafka.apache.org/27/javadoc/org/apache/kafka/streams/errors/DefaultProductionExceptionHandler.html
- KIP-210 — Custom error handling for Streams production: https://cwiki.apache.org/confluence/display/KAFKA/KIP-210+-+Provide+for+custom+error+handling++when+Kafka+Streams+fails+to+produce
- KIP-1033 — `processing.exception.handler`: https://cwiki.apache.org/confluence/display/KAFKA/KIP-1033%3A+Add+Kafka+Streams+exception+handler+for+exceptions+occurring+during+processing
- Confluent — Kafka Streams error handling: https://developer.confluent.io/courses/kafka-streams/error-handling/

## Cross-reference

Also recommended by Confluent agent-skills — see kafka-streams-programming/references/production-hardening.md § Production exception handler.
