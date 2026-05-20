# SPRING_RETRYABLE_TOPIC_NO_KAFKA_TEMPLATE

**Severity**: ERROR
**Confidence**: HIGH
**Detection**: annotation + bytecode
**Tagline**: @RetryableTopic without a template bean fails at startup — every time.

## TL;DR

The linter flags `@RetryableTopic` annotations where the `kafkaTemplate` attribute isn't set AND no bean named `defaultRetryTopicKafkaTemplate` (or otherwise resolvable) is defined. Spring Kafka throws at context init.

## What's happening (the mechanism)

`@RetryableTopic` requires a `KafkaTemplate` to publish records to the retry / DLT topics. The resolution order:

1. If `@RetryableTopic(kafkaTemplate = "myTemplate")` is set, Spring looks up a bean by that name.
2. Otherwise, Spring looks up a bean named `defaultRetryTopicKafkaTemplate`.
3. If neither is found, `BeanCreationException` at startup.

Per the docs: *"If you don't specify a kafkaTemplate name a bean with name `defaultRetryTopicKafkaTemplate` will be looked up. If no bean is found an exception is thrown."*

The common failure case: developer adds `@RetryableTopic` based on a tutorial, expects auto-configuration to wire the template (as it does for many Spring beans). It doesn't — `@RetryableTopic` requires the bean by name.

Spring Boot does auto-configure a `kafkaTemplate` bean (with that name) — but `defaultRetryTopicKafkaTemplate` is *not* an auto-configured alias for it. So even in a Spring Boot app, you need to either:
- Set `kafkaTemplate = "kafkaTemplate"` on the annotation.
- Define a `@Bean(name = "defaultRetryTopicKafkaTemplate")` alias.
- Use the global `@EnableKafkaRetryTopic` configuration with explicit wiring.

## Operational impact

- Application fails to start. `BeanCreationException: Could not find bean named 'defaultRetryTopicKafkaTemplate'`.
- In `lazy-initialization` mode, deferred to first invocation.
- Easy mistake when migrating a listener to use `@RetryableTopic` — the change looks like adding one annotation, but it breaks the entire app.

## How to fix

```java
// BAD — relies on default bean name that isn't auto-wired
@RetryableTopic(attempts = "3")
@KafkaListener(topics = "orders", groupId = "g")
public void onOrder(Order o) { ... }

// GOOD A — explicit reference to Spring Boot's auto-configured template
@RetryableTopic(attempts = "3", kafkaTemplate = "kafkaTemplate")
@KafkaListener(topics = "orders", groupId = "g")
public void onOrder(Order o) { ... }

// GOOD B — define the bean alias once, app-wide
@Configuration
class KafkaRetryConfig {
    @Bean
    KafkaTemplate<String, Object> defaultRetryTopicKafkaTemplate(KafkaTemplate<String, Object> kafkaTemplate) {
        return kafkaTemplate;
    }
}

// GOOD C — enable the global config (since 3.x)
@Configuration
@EnableKafkaRetryTopic
class KafkaConfig { }
```

`@EnableKafkaRetryTopic` is meta-annotated with `@EnableKafka` and bootstraps the retry infrastructure.

## When this might be a false positive

- A bean named `defaultRetryTopicKafkaTemplate` is defined in a `@Configuration` class the linter scans. Suppress when detected.
- A `@RetryTopicConfigurationSupport` subclass is providing the template via override.

## Detection strategy

- Annotation: scan for `@RetryableTopic`.
- Read `kafkaTemplate` attribute; if set to a non-empty string, suppress (assume it's a real bean).
- Otherwise, scan all `@Configuration` / `@Component` classes for a `@Bean` whose name (or method name) is `defaultRetryTopicKafkaTemplate`. If not found, flag.
- Also accept `@EnableKafkaRetryTopic` on a `@Configuration` class as evidence of intentional setup (still useful to flag at INFO so the developer knows to verify).
- Confidence: HIGH when annotation has no `kafkaTemplate` and no candidate bean is found.

## References

- Spring Kafka — `@RetryableTopic` Configuration: https://docs.spring.io/spring-kafka/reference/retrytopic/retry-config.html
- Spring Kafka — `@EnableKafkaRetryTopic`: https://docs.spring.io/spring-kafka/reference/retrytopic/retry-config.html
