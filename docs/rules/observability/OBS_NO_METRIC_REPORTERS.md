# OBS_NO_METRIC_REPORTERS

**Severity**: WARNING
**Confidence**: CONTEXT
**Detection**: config-file
**Tagline**: Kafka exposes 100+ metrics by default — and ships exactly zero of them unless you ask.

## TL;DR

The linter flags producer/consumer/Streams configurations where `metric.reporters` is empty AND no JMX→Prometheus exporter is wired up at the JVM level. Kafka clients register over a hundred MBeans under `kafka.producer.*`, `kafka.consumer.*`, and `kafka.streams.*` via JMX by default, but if nothing reads JMX (no jmx_exporter agent, no Micrometer binding, no custom reporter) those metrics never leave the JVM. You'll find out the consumer is broken when the lag dashboard you never built is empty.

## The setup

A team rolls out a new Kafka consumer. It works in staging. They look for the lag metric in Grafana — not there. They search the Prometheus targets — Kafka client metrics are missing. They discover that Kafka clients expose metrics via JMX, not via Prometheus directly, and that either a `MetricsReporter` implementation or an out-of-process JMX exporter is required.

## What's actually happening

Kafka client metrics live behind the `org.apache.kafka.common.metrics.MetricsReporter` interface. The client invokes every registered reporter on metric add/remove/change events. There are several ways to get those metrics out of the JVM:

1. **JMX (default, always on).** The client registers a `JmxReporter` automatically — metrics show up as MBeans, e.g. `kafka.consumer:type=consumer-fetch-manager-metrics,client-id=*,attribute=records-lag-max`. Reaching them requires a JMX scraper (`jmx_exporter` Java agent, Datadog JMX integration, etc.) running alongside the JVM.
2. **Micrometer `KafkaClientMetrics`.** Programmatic binding: `new KafkaClientMetrics(producer).bindTo(meterRegistry)`. Spring Boot's `KafkaMetricsAutoConfiguration` does this automatically for the *default* `ProducerFactory` / `ConsumerFactory` beans but not for user-defined custom factories unless the customizer is wired.
3. **OpenTelemetry `MetricsReporter`.** `KafkaTelemetry.create(openTelemetry).metricConfigProperties()` returns a property map you merge into the producer/consumer config — adds an OTel reporter to `metric.reporters`.
4. **Confluent/custom reporter.** `io.confluent.metrics.reporter.ConfluentMetricsReporter` for self-balancing-style platforms.

If none of these is configured and you don't run a JMX scraper, the metrics are invisible.

## Why this is subtle

- "Kafka has metrics" is true (~100 per client) — the trap is they live behind JMX with no default exporter.
- Spring Boot auto-config covers the **default** factories. The moment you `@Bean ProducerFactory<...> custom(...)` without `addListener(micrometerProducerListener)`, you lose them silently.
- Adding `micrometer-registry-prometheus` to `pom.xml` does not enable Kafka metrics unless `KafkaClientMetrics` is bound to the registry.
- Streams' built-in `client.id`-tagged metrics behave the same way: present via JMX, absent from Prometheus without a bridge.

## Operational impact

- Consumer lag dashboards are empty.
- Producer batch-size, record-error-rate, and retry-count are unobservable.
- Capacity planning becomes guesswork.
- The first detection of broken topology is a customer complaint.

## Failure scenarios (walkthrough)

1. **The custom-factory regression.** Team starts on Spring Boot auto-config — Kafka metrics show in Prometheus. They later customize the consumer factory to add a deserializer trust filter and forget the Micrometer listener. Six months later, during a rebalance storm, on-call has no lag metric. The dashboard ran on a deleted metric name nobody noticed.

2. **The agent-vs-library confusion.** Project depends on `micrometer-core` and `micrometer-registry-prometheus`. Engineer assumes Kafka metrics are auto-bridged. They aren't — `KafkaClientMetrics` must be instantiated and `bindTo()` called. The metric scrape returns zero `kafka_*` series.

## How to fix

```properties
# GOOD — explicit metric reporter (OpenTelemetry)
spring.kafka.producer.properties.metric.reporters=\
  io.opentelemetry.instrumentation.kafkaclients.v2_6.KafkaTelemetryReporter
spring.kafka.consumer.properties.metric.reporters=\
  io.opentelemetry.instrumentation.kafkaclients.v2_6.KafkaTelemetryReporter

# GOOD — Spring Boot Micrometer (default factories — autowired)
# nothing to add; KafkaMetricsAutoConfiguration installs MicrometerProducerListener
# / MicrometerConsumerListener on the default factories
```

```java
// GOOD — custom factory + Micrometer listener
@Bean
public ConsumerFactory<String, Order> consumerFactory(MeterRegistry registry) {
    DefaultKafkaConsumerFactory<String, Order> cf =
        new DefaultKafkaConsumerFactory<>(props);
    cf.addListener(new MicrometerConsumerListener<>(registry,
        List.of(Tag.of("app", "order-service"))));
    return cf;
}
```

```yaml
# GOOD — JMX exporter agent (orthogonal path)
# argLine: -javaagent:/opt/jmx_exporter.jar=9404:/opt/jmx_config.yml
# config exposes kafka.consumer:type=consumer-fetch-manager-metrics,client-id=*
```

## When this might be a false positive

- JMX scraper is running at JVM level (visible in `<argLine>` of Surefire/Failsafe or the deployment manifest, not in the application config).
- Spring Boot defaults + default factories only (no custom `@Bean ProducerFactory`).
- Quarkus with `quarkus.micrometer.binder.kafka.enabled=true` (default true on `quarkus-micrometer` ≥ 2.x).

## Detection strategy

- pom-dependency: detect `micrometer-core`, `micrometer-registry-*`, `opentelemetry-kafka-clients-*`, `io.prometheus.jmx:jmx_prometheus_javaagent`.
- config-file: look for `metric.reporters`, `spring.kafka.{producer,consumer}.properties.metric.reporters`, `kafka-streams.metric.reporters`, `quarkus.kafka.metric.reporters`.
- Spring Boot exception: if the project uses *only* the default Kafka factories and has Micrometer on the classpath, suppress this warning.
- Confidence CONTEXT — JVM-level instrumentation is invisible to a static scan.

## References

- Kafka `metric.reporters` config: https://kafka.apache.org/documentation/#producerconfigs_metric.reporters
- Micrometer Kafka metrics: https://docs.micrometer.io/micrometer/reference/reference/kafka.html
- Spring Boot `KafkaMetricsAutoConfiguration`: https://docs.spring.io/spring-boot/docs/current/api/org/springframework/boot/actuate/autoconfigure/metrics/KafkaMetricsAutoConfiguration.html
- JMX Prometheus exporter — `records-lag-max` config: https://github.com/prometheus/jmx_exporter/issues/609
- Confluent Cloud — Monitoring lag: https://docs.confluent.io/cloud/current/monitoring/monitor-lag.html
