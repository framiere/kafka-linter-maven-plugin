# OBS_SPRING_MICROMETER_NOT_BOUND

**Severity**: WARNING
**Confidence**: MEDIUM
**Detection**: bytecode + pom-dependency
**Tagline**: A custom `ProducerFactory` without `MicrometerProducerListener` is `KafkaMetricsAutoConfiguration` waving goodbye.

## TL;DR

The linter flags Spring Boot projects that (a) have `micrometer-core` (or any `micrometer-registry-*`) on the classpath, (b) define a custom `@Bean ProducerFactory` or `@Bean ConsumerFactory`, and (c) do not call `addListener(new MicrometerProducerListener<>(...))` / `MicrometerConsumerListener<>(...)`. Spring Boot's `KafkaMetricsAutoConfiguration` only attaches the Micrometer listener to the *default* factories; user-defined factories silently lose the binding.

## The setup

Team starts with Spring Boot auto-configured Kafka — `KafkaTemplate` and `@KafkaListener` work, Kafka metrics show in Grafana. They later need a custom JSON deserializer / trust filter / dead-letter producer, so they introduce a `@Bean ConsumerFactory` and `@Bean ProducerFactory`. They keep all other config the same. The dashboard suddenly shows `kafka_*` metrics dropping to zero series.

## What's actually happening

`KafkaMetricsAutoConfiguration` (Spring Boot 2.5+) is:

```java
@AutoConfiguration(before = KafkaAutoConfiguration.class)
@ConditionalOnClass({ KafkaClientMetrics.class, ProducerFactory.class })
@ConditionalOnBean(MeterRegistry.class)
public class KafkaMetricsAutoConfiguration {
    @Bean
    DefaultKafkaProducerFactoryCustomizer micrometerProducerListener(MeterRegistry meterRegistry) {
        return factory -> factory.addListener(new MicrometerProducerListener<>(meterRegistry));
    }
    @Bean
    DefaultKafkaConsumerFactoryCustomizer micrometerConsumerListener(MeterRegistry meterRegistry) {
        return factory -> factory.addListener(new MicrometerConsumerListener<>(meterRegistry));
    }
}
```

It exposes a `DefaultKafkaProducerFactoryCustomizer` and `DefaultKafkaConsumerFactoryCustomizer`. **These customizers are applied by `KafkaAutoConfiguration` only when it builds the default factory bean.** If you define your own `@Bean ProducerFactory`, you bypass `KafkaAutoConfiguration`'s factory bean entirely, so the customizer is never invoked.

The fix: autowire the customizers and apply them yourself, or call `addListener(new MicrometerProducerListener<>(meterRegistry))` directly.

## Why this is subtle

- Nothing about defining a custom factory breaks compilation, runtime, or tests.
- The metric is silently absent. Grafana shows "no data" — which looks indistinguishable from "low traffic" on a sparse panel.
- Migration story: a project starts with default factory → metrics work. Someone refactors to a custom factory in PR #847. The PR diff has nothing about metrics. CI passes. Six months later, an incident shows the missing dashboard.

## Operational impact

- `kafka.producer.*` and `kafka.consumer.*` Micrometer meters: missing.
- Spring Kafka's own meters (`spring.kafka.template`, `spring.kafka.listener`) still work — those are bound by Spring Kafka itself, not by the auto-config. This makes the gap even harder to spot because *some* `kafka_*` metrics still show.
- Lag and rebalance metrics unavailable.

## Failure scenarios (walkthrough)

1. **The trust-filter refactor.** PR #847 adds `@Bean ConsumerFactory<String, Order> orderConsumerFactory(...)` to wire a JsonDeserializer with `trustedPackages("com.example")`. Diff is 30 lines, all about the deserializer. CI green. Two months later: rebalance storm, on-call opens "Kafka Consumer Lag" dashboard — empty. The lag metric was only ever bound to the default factory.

2. **The two-cluster service.** App reads from cluster A and writes to cluster B; each needs a distinct factory bean. Both are custom. Both lose Micrometer binding. Producer and consumer metrics both vanish at once, no migration story to point at.

## How to fix

```java
// BAD — custom factory, no listener
@Bean
public ConsumerFactory<String, Order> consumerFactory(KafkaProperties props) {
    return new DefaultKafkaConsumerFactory<>(props.buildConsumerProperties(),
        new StringDeserializer(),
        new JsonDeserializer<>(Order.class));
}

// GOOD — autowire and apply the Spring Boot customizer
@Bean
public ConsumerFactory<String, Order> consumerFactory(
        KafkaProperties props,
        ObjectProvider<DefaultKafkaConsumerFactoryCustomizer> customizers) {
    DefaultKafkaConsumerFactory<String, Order> cf =
        new DefaultKafkaConsumerFactory<>(props.buildConsumerProperties(),
            new StringDeserializer(),
            new JsonDeserializer<>(Order.class));
    customizers.orderedStream().forEach(c -> c.customize(cf));
    return cf;
}

// ALSO GOOD — direct listener
@Bean
public ConsumerFactory<String, Order> consumerFactory(
        KafkaProperties props, MeterRegistry registry) {
    DefaultKafkaConsumerFactory<String, Order> cf = ...;
    cf.addListener(new MicrometerConsumerListener<>(registry,
        List.of(Tag.of("app", "order-service"))));
    return cf;
}
```

## Consult a friend?

> Slow down. The Micrometer listener tags meters with `spring.id` derived from `factoryBeanName.client-id`. If you wire it on two factories that share the same `client.id`, you get duplicate-meter exceptions at startup. Verify your `client.id` is unique per factory before adding listeners.

## When this might be a false positive

- The custom factory bean replaces a non-Spring-Boot config path (Spring Cloud Stream binder, custom autoconfig) that adds the listener at a different layer.
- Project also uses the OpenTelemetry Java agent — agent-level metrics make this redundant.
- The custom factory is a wrapper that delegates to an inner `DefaultKafkaConsumerFactory` which itself has the listener.

## Detection strategy

- pom-dependency: `org.springframework.boot:spring-boot-starter-actuator` OR `io.micrometer:micrometer-core` AND `org.springframework.kafka:spring-kafka` ≥ 2.5.
- bytecode: scan `@Configuration` classes for `@Bean` methods returning `ProducerFactory` / `ConsumerFactory` (return type or `DefaultKafkaProducerFactory` / `DefaultKafkaConsumerFactory` construction). Check if the method body invokes `addListener(`; if not, flag.
- Confidence MEDIUM — wrapper delegation and external customizer application defeat the static check.

## References

- Spring Boot `KafkaMetricsAutoConfiguration`: https://docs.spring.io/spring-boot/docs/current/api/org/springframework/boot/actuate/autoconfigure/metrics/KafkaMetricsAutoConfiguration.html
- Spring Kafka Micrometer chapter: https://docs.spring.io/spring-kafka/reference/kafka/micrometer.html
- Medium — Enabling Micrometer Metrics with Custom Spring Kafka Factories: https://medium.com/@ruth.kurniawati/enabling-micrometer-metrics-in-spring-boot-applications-containing-custom-spring-kafka-producers-377aa647b730
