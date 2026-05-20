# OBS_INTERCEPTORS_CONFIGURED

**Severity**: WARNING
**Confidence**: CONTEXT
**Detection**: combination
**Tagline**: Interceptors are how the trace context survives the partition boundary.

## TL;DR

The good-practice form of
[OBS_NO_PRODUCER_INTERCEPTORS](../observability/OBS_NO_PRODUCER_INTERCEPTORS.md)
and
[OBS_NO_CONSUMER_INTERCEPTORS](../observability/OBS_NO_CONSUMER_INTERCEPTORS.md).
Both producer and consumer should register a `*Interceptor.classes`
(or use Spring 3.x's `observation-enabled=true` on both `template` and
`listener`), so the trace context flows from producer to consumer.

## Cross-reference

See the linked anti-pattern docs for the half-instrumented failure mode
(producer-only interceptor → consumer span has no parent → Jaeger
shows disconnected roots).

## What the good-practice adds

The bundle is "both sides", not "one side". The rule fires if EITHER:

- `interceptor.classes` is set on the producer but not the consumer
  (or vice versa), AND
- `spring.kafka.template.observation-enabled` and
  `spring.kafka.listener.observation-enabled` are not both `true`, AND
- Quarkus `quarkus.opentelemetry.enabled` is not `true` with
  smallrye-reactive-messaging-kafka >= 3.

The positive recommendation:

```properties
# yes — OpenTelemetry on both sides
spring.kafka.producer.properties.interceptor.classes=\
  io.opentelemetry.instrumentation.kafkaclients.v2_6.TracingProducerInterceptor
spring.kafka.consumer.properties.interceptor.classes=\
  io.opentelemetry.instrumentation.kafkaclients.v2_6.TracingConsumerInterceptor
```

or the Spring Kafka 3.x native path:

```yaml
spring:
  kafka:
    template:
      observation-enabled: true
    listener:
      observation-enabled: true
```

## References

See the linked anti-pattern docs, KIP-42, and the OpenTelemetry Kafka
instrumentation docs.
