# OBS_NO_PRODUCER_INTERCEPTORS

**Severity**: WARNING
**Confidence**: CONTEXT
**Detection**: config-file
**Tagline**: No `interceptor.classes` on the producer is "I want tracing — just not in this app."

## TL;DR

The linter flags producer configurations where `interceptor.classes` is empty (or absent) on projects that ship a distributed-tracing dependency (OpenTelemetry, Brave, Micrometer Observation). Interceptors are the contract Kafka exposes for stamping outbound records with W3C traceparent headers, custom audit metadata, and per-record metric counters. With nothing set, the rest of the tracing stack has no Kafka spans to attach to and the producer is a blind spot in your end-to-end trace.

## The setup

Your team wired Zipkin, Jaeger, or OpenTelemetry into the HTTP layer. Traces look great until the request hands off to Kafka — and then the chain dies. You spend a sprint hunting "where does my trace go" only to find that the producer has no interceptor, no telemetry hook, and no traceparent header on the record. The consumer downstream creates a new root span.

## What's actually happening

`org.apache.kafka.clients.producer.ProducerInterceptor` is the official extension point Kafka added in KIP-42 specifically so platform code (tracing, metrics, audit, encryption) can hook send-time without modifying the application. The producer reads `interceptor.classes` once at construction, instantiates each listed FQN via reflection, and calls `onSend()` before partitioning and `onAcknowledgement()` after broker ack. Tracing libraries plug in here:

- `brave.kafka.interceptor.TracingProducerInterceptor` (Zipkin Brave) — writes B3 headers, starts a PRODUCER span per record.
- `io.opentelemetry.instrumentation.kafkaclients.v2_6.TracingProducerInterceptor` — same idea for OTel.
- Confluent's audit-log interceptor, custom company SDK interceptors, etc.

With nothing in `interceptor.classes`, none of these are wired. The OTel Java agent will still patch `KafkaProducer.send` reflectively if it's running, but if you're using the library-mode artifacts (no agent), you must declare the interceptor explicitly.

## Why this is subtle

- Spring Boot's `spring-kafka` micrometer integration is *consumer/listener-side* by default — it bills the listener invocation timer (`spring.kafka.listener`), not the produce path. Without an interceptor, the produce hop is unobservable even when the consumer side looks fully wired.
- Adding `opentelemetry-kafka-clients-2.6` to `pom.xml` does **nothing on its own**. You must either run the Java agent or set the interceptor explicitly. Adding the dependency creates the false confidence that "tracing is on."
- The default tracing-via-headers approach silently degrades to "two disconnected traces" rather than a hard error.

## Operational impact

- Distributed traces stop at the `KafkaTemplate.send()` line — no PRODUCER span, no traceparent header, downstream consumer starts a new trace.
- No per-record audit log entries (compliance gap if interceptor was the audit path).
- `kafka.producer.record-send-rate` may still be exported via Micrometer or JMX, but business-level tagging (tenant, message type) added by interceptors is missing.

## Failure scenarios (walkthrough)

1. **The disconnected-trace incident.** SRE asks "where did this order go?" Trace ID found in HTTP logs. Jaeger shows the HTTP span, the service-bus enqueue span, then nothing. The order *did* reach the broker — the consumer service has it processed — but with a different trace ID. Root cause: no `interceptor.classes`, no traceparent header propagated.

2. **The PII audit gap.** Compliance requires every customer-data write to go through an audit log. The audit interceptor was deployed via shared library. A new microservice copied an existing producer config but omitted `interceptor.classes`. Six months later, an audit shows missing entries — silent compliance failure.

## How to fix

```properties
# BAD — no interceptors
spring.kafka.producer.bootstrap-servers=...

# GOOD — OpenTelemetry library mode
spring.kafka.producer.properties.interceptor.classes=\
  io.opentelemetry.instrumentation.kafkaclients.v2_6.TracingProducerInterceptor

# GOOD — Brave/Zipkin
spring.kafka.producer.properties.interceptor.classes=\
  brave.kafka.interceptor.TracingProducerInterceptor

# GOOD — multiple interceptors (audit + tracing)
spring.kafka.producer.properties.interceptor.classes=\
  com.example.AuditProducerInterceptor,\
  io.opentelemetry.instrumentation.kafkaclients.v2_6.TracingProducerInterceptor
```

```java
// GOOD — programmatic
props.put(ProducerConfig.INTERCEPTOR_CLASSES_CONFIG,
    List.of(TracingProducerInterceptor.class.getName()));
```

If you use the OpenTelemetry Java agent (`-javaagent:opentelemetry-javaagent.jar`), the agent installs interceptors via bytecode rewriting and this rule should be suppressed for that project.

## When this might be a false positive

- The OpenTelemetry Java agent is in use — it patches Kafka clients without needing config.
- A wrapping abstraction (Spring Cloud Stream binder with `brave-instrumentation-kafka-clients`) installs the interceptor at bean post-processing time.
- Internal-only producer with no tracing requirements (e.g., test fixture, one-shot CLI).

This is why confidence is **CONTEXT**.

## Detection strategy

- pom-dependency: detect tracing libraries on classpath:
  - `io.opentelemetry.instrumentation:opentelemetry-kafka-clients-*`
  - `io.zipkin.contrib.brave-kafka-interceptor:*`
  - `io.micrometer:micrometer-tracing*` (combined with `spring-kafka` ≥ 3.0)
- config-file: look for `interceptor.classes` key (under `producer.properties.` for Spring, `kafka.producer.` for Quarkus, `default.production.` for Streams).
- Flag when tracing dep present AND `interceptor.classes` absent AND no OTel agent declared (no `-javaagent:` in `<argLine>` of Surefire/Failsafe). MEDIUM confidence with the agent-detection heuristic, CONTEXT without it.

## References

- Kafka `interceptor.classes` config: https://kafka.apache.org/documentation/#producerconfigs_interceptor.classes
- KIP-42 — Add Producer and Consumer Interceptors: https://cwiki.apache.org/confluence/display/KAFKA/KIP-42
- OpenTelemetry Kafka clients instrumentation: https://github.com/open-telemetry/opentelemetry-java-instrumentation/blob/main/instrumentation/kafka/kafka-clients/kafka-clients-2.6/library/README.md
- Brave Kafka interceptor: https://github.com/openzipkin-contrib/brave-kafka-interceptor
- Confluent — Distributed tracing for Kafka: https://www.confluent.io/blog/importance-of-distributed-tracing-for-apache-kafka-based-applications/
