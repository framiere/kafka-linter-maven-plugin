# OBS_NO_CONSUMER_INTERCEPTORS

**Severity**: WARNING
**Confidence**: CONTEXT
**Detection**: config-file
**Tagline**: A consumer with no `interceptor.classes` is the second half of a broken trace.

## TL;DR

The linter flags consumer configurations where `interceptor.classes` is empty on projects that depend on a tracing or audit library. Without a `ConsumerInterceptor` (`TracingConsumerInterceptor` or equivalent), the consumer never reads the W3C `traceparent` / B3 trace headers from incoming records, so it starts a fresh trace instead of continuing the producer's. End-to-end visibility is lost at the partition boundary.

## The setup

Same situation as the producer-side rule, but the failure mode is "consumer always shows up as a new root span." Engineers add the producer-side interceptor first, see spans appear in Jaeger, and assume the wiring is complete. The consumer still creates a fresh trace per `poll()` batch, defeating the point.

## What's actually happening

`ConsumerInterceptor` (KIP-42 again) sits between `KafkaConsumer.poll()` and the application. It receives the `ConsumerRecords` and can read headers, attach metadata to MDC, start a CONSUMER span that resumes the producer's trace context, etc.

Spring's listener container does *not* automatically install a tracing interceptor — it adds Micrometer timers at the listener-invocation level (`spring.kafka.listener` timer) but it does not extract trace headers from the record. The interceptor (or the explicit `spring.kafka.consumer.observation-enabled=true` flag) is what bridges the two halves.

In OpenTelemetry, `KafkaTelemetry.create(openTelemetry).getConsumerInterceptor()` returns the interceptor that reads headers + starts spans.

## Why this is subtle

- Adding the producer interceptor while forgetting the consumer one produces traces that *appear* in Jaeger but never link across services. Engineers see "look, spans!" and stop investigating.
- Spring Kafka 3.0 added `observation-enabled` as a first-class flag — but it must be set on both the listener container *and* the `KafkaTemplate` (rule `SPRING_OBSERVATION_HALF_ENABLED` could cover the partial case).
- The `spring.kafka.listener` Micrometer timer creates spans that look like full tracing but actually have no parent — Jaeger shows them as disconnected roots.

## Operational impact

- Every consumer invocation appears as a new root span.
- "Find all traces touching tenant X" queries miss the consumer side entirely.
- Latency analysis becomes per-hop instead of end-to-end.
- Lag investigations have no causal link from the slow consumer back to the upstream service that produced the record.

## Failure scenarios (walkthrough)

1. **The half-instrumented service.** The order-service producer has `TracingProducerInterceptor`. The fraud-check consumer does not. SRE asks "where did order 12345 spend 4 seconds?" Trace shows 200ms in order-service, then nothing for 3.8s. Reality: it was waiting in the fraud-check consumer queue, but that consumer's spans never re-attached to the producer's trace.

2. **The missing audit chain.** Audit interceptor on the producer logs "produced". The consumer side audit was supposed to log "consumed". Without the consumer interceptor, the audit chain shows orders going in and never coming out — false-positive compliance alerts.

## How to fix

```properties
# GOOD — OpenTelemetry
spring.kafka.consumer.properties.interceptor.classes=\
  io.opentelemetry.instrumentation.kafkaclients.v2_6.TracingConsumerInterceptor

# GOOD — Spring Kafka native observation (3.0+)
spring.kafka.listener.observation-enabled=true
spring.kafka.template.observation-enabled=true
```

```java
// GOOD — programmatic
props.put(ConsumerConfig.INTERCEPTOR_CLASSES_CONFIG,
    List.of(TracingConsumerInterceptor.class.getName()));
```

## When this might be a false positive

- OpenTelemetry Java agent in use.
- Spring Kafka 3.0+ with both `listener.observation-enabled=true` and `template.observation-enabled=true` set — Spring installs its own Micrometer-based instrumentation in the listener container, no interceptor needed.
- Quarkus + SmallRye with `mp.messaging.tracing-enabled` (default `true` from Quarkus 3.x onwards on the `smallrye-reactive-messaging-kafka` connector).

## Detection strategy

- pom-dependency: same as `OBS_NO_PRODUCER_INTERCEPTORS`.
- config-file: look for `interceptor.classes` under consumer scope.
- Special-case Spring Kafka 3.x: presence of `spring.kafka.listener.observation-enabled=true` satisfies this rule.
- Special-case Quarkus: presence of `quarkus.opentelemetry.enabled=true` AND smallrye-reactive-messaging-kafka ≥ 3.x satisfies this rule (OTel autoconfig installs the interceptor).
- Confidence CONTEXT — the agent and bean post-processor cases are common.

## References

- Kafka `interceptor.classes` (consumer): https://kafka.apache.org/documentation/#consumerconfigs_interceptor.classes
- KIP-42 — Add Producer and Consumer Interceptors: https://cwiki.apache.org/confluence/display/KAFKA/KIP-42
- Spring Kafka Micrometer Observation: https://docs.spring.io/spring-kafka/reference/kafka/micrometer.html
- OpenTelemetry — Instrumenting Apache Kafka clients: https://opentelemetry.io/blog/2022/instrument-kafka-clients/
