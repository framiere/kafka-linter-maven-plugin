# SPRING_KSTREAMS_BINDER_DEFAULT_DEH

**Severity**: WARNING
**Confidence**: MEDIUM
**Detection**: config-file
**Tagline**: Spring Cloud Stream Kafka Streams binder defaults to `logAndFail` — first poison pill takes down the function.

## TL;DR

The linter flags Spring Cloud Stream Kafka Streams binder configurations that do not override `spring.cloud.stream.kafka.streams.binder.deserializationExceptionHandler` (or the per-binding equivalent). The default delegates to Kafka Streams' `LogAndFailExceptionHandler`, which kills the stream thread on the first deserialization error. The binder offers two replacements out of the box: `logAndContinue` and `sendToDlq` — the latter wires a `DeadLetterPublishingRecoverer`-equivalent automatically using a `DltSenderContext`.

## The setup

Team uses Spring Cloud Stream's Kafka Streams binder for the convenience: `@Bean Function<KStream<...>, KStream<...>> process()`. They don't dig into the binder docs. First poison pill arrives. The function dies. Spring Boot's auto-config restarts it — the same record is the first one polled — dies again. Restart loop until operator intervention.

## What's actually happening

Spring Cloud Stream's Kafka Streams binder wraps the underlying Kafka Streams `DeserializationExceptionHandler` config. The binder-specific property is:

```
spring.cloud.stream.kafka.streams.binder.deserializationExceptionHandler=logAndFail|logAndContinue|sendToDlq
```

Or per binding:

```
spring.cloud.stream.kafka.streams.bindings.<binding>.consumer.deserializationExceptionHandler=...
```

Three options:

- **`logAndFail`** (default) — delegates to Kafka's `LogAndFailExceptionHandler`. Stream thread dies on first error.
- **`logAndContinue`** — delegates to `LogAndContinueExceptionHandler`. Silent drops, same as `STREAMS_DESER_HANDLER_BLANKET_CONTINUE`.
- **`sendToDlq`** — uses Spring's `RecoveringDeserializationExceptionHandler` with a `DeadLetterPublishingRecoverer` configured via the binder's `DltSenderContext`. Publishes to `<topic>.DLT` (or custom destination resolver).

The default is "fail loud", which for production read-process-write topologies usually translates to "wake up the on-call." `sendToDlq` is the sensible default for most production work, but you have to opt in.

## Why this is subtle

- Spring Cloud Stream is itself a layer over Kafka Streams — engineers don't always realize they need to set this property in the binder namespace, not the underlying Kafka namespace.
- Setting the Kafka Streams native property (`default.deserialization.exception.handler=...`) directly may or may not override the binder default depending on version.
- The `sendToDlq` path requires either auto-creation of the DLT topic or pre-provisioning — the binder doesn't fail at startup if the DLT doesn't exist; it fails on first poison pill.
- Spring Boot 3.x / Spring Cloud Stream 4.x changed property names; verify against your version.

## Operational impact

- First poison pill kills the function. Restart loop or stuck.
- No DLT entries (with default `logAndFail`).
- No metric showing "deserialization error rate" if the binder's metrics aren't separately enabled.

## Failure scenarios (walkthrough)

1. **The schema migration.** Schema Registry is updated with a backwards-incompatible change. New records can't be deserialized by the current consumer. Default `logAndFail` → function dies on first new record → restart loop → no progress until the consumer is upgraded.

2. **The "we have DLQ" misunderstanding.** Team set `spring.cloud.stream.bindings.process-in-0.consumer.dlq-name=orders.dlt` thinking it auto-enables `sendToDlq` mode. It doesn't — `dlq-name` only takes effect when `deserializationExceptionHandler=sendToDlq`. Without the latter, the property is silently ignored.

## How to fix

```properties
# GOOD — global
spring.cloud.stream.kafka.streams.binder.deserialization-exception-handler=sendToDlq

# GOOD — per binding (more precise)
spring.cloud.stream.kafka.streams.bindings.process-in-0.consumer.deserialization-exception-handler=sendToDlq
spring.cloud.stream.kafka.streams.bindings.process-in-0.consumer.dlq-name=orders.dlt

# REQUIRED — provide a DltSenderContext bean for sendToDlq to publish via the right ProducerFactory
# (Spring Cloud Stream 4.x auto-wires this from the binder's producer factory)
```

```java
// Programmatic — register a custom binder-aware handler
@Bean
public DeserializationExceptionHandler dlqDeserializationHandler(
        DltSenderContext dltSenderContext) {
    return new SendToDlqAndContinue(dltSenderContext);
}
```

## Consult a friend?

> Slow down. Spring Cloud Stream layers a programming model over Kafka Streams. The configuration namespace is `spring.cloud.stream.kafka.streams.*` — not `kafka-streams.*`. Mixing the two leads to half-applied configs. Reach for the binder docs for your specific Spring Cloud Stream version before assuming a property works.

## When this might be a false positive

- Project explicitly wants "fail loud" semantics (e.g., a critical financial topology where dropping is worse than stopping).
- The binder is in a niche version that names the property differently — verify against the docs for your version.

## Detection strategy

- pom-dependency: `org.springframework.cloud:spring-cloud-stream-binder-kafka-streams`.
- config-file: scan for `spring.cloud.stream.kafka.streams.binder.deserialization-exception-handler` and per-binding equivalents.
- Flag if absent.
- Confidence MEDIUM — programmatic registration via a `@Bean DeserializationExceptionHandler` bypasses the config check.

## References

- Spring Cloud Stream — Kafka Streams binder error handling: https://docs.spring.io/spring-cloud-stream/reference/kafka/kafka-streams-binder/error-handling.html
- Spring Kafka — `RecoveringDeserializationExceptionHandler`: https://docs.spring.io/spring-kafka/api/org/springframework/kafka/listener/RecoveringDeserializationExceptionHandler.html
- Related: `STREAMS_DEFAULT_DESERIALIZATION_HANDLER` (native Streams equivalent)
