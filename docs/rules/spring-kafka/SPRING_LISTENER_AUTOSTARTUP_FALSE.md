# SPRING_LISTENER_AUTOSTARTUP_FALSE

**Severity**: WARNING
**Confidence**: MEDIUM
**Detection**: annotation
**Tagline**: A listener you forgot to start is a feature flag that's always off.

## TL;DR

The linter flags `@KafkaListener(autoStartup = "false")` (or `autoStartup = "${prop:false}"` with a false default) left in production code. Such listeners never consume unless something explicitly calls `KafkaListenerEndpointRegistry.getListenerContainer(id).start()`.

## What's happening (the mechanism)

`autoStartup` overrides the container factory's `autoStartup` flag (introduced in spring-kafka 2.2). When `false`, the container is registered but never started by `SmartLifecycle` at context refresh. The listener exists as a bean, the metric `spring.kafka.listener` is registered, but no `KafkaConsumer.poll()` is ever invoked.

The attribute is a `String` to allow SpEL / placeholder resolution: `autoStartup = "${listen.auto.start:true}"`. The typical foot-gun is one of:

- Hard-coded `"false"` shipped accidentally from a test/debug branch.
- A placeholder with no value and no default — Spring resolves the empty string, which is **not** `"true"`, so the container stays stopped.
- A profile-specific property that is set only on dev (`true`) but not in prod (`false` default), and the listener is "missing" in prod.

## Operational impact

- Topic appears to have no consumer. `kafka-consumer-groups --describe` shows no members in the group.
- Consumer lag grows linearly with producer throughput. PagerDuty fires on `kafka.consumer.records-lag-max` SLO breach.
- The Spring app `/health` is UP. `Lifecycle.isRunning()` on the specific container is `false`, but no out-of-the-box health check surfaces it.
- Common in code where the team intended to enable the container later from an admin endpoint and forgot.

## How to fix

```java
// BAD — never starts
@KafkaListener(topics = "orders", groupId = "g", autoStartup = "false")
public void onOrder(Order o) { ... }

// GOOD — remove the override
@KafkaListener(topics = "orders", groupId = "g")
public void onOrder(Order o) { ... }

// GOOD — gate by profile, but with safe default
@KafkaListener(topics = "orders", groupId = "g",
               autoStartup = "${app.orders.listener.enabled:true}")
public void onOrder(Order o) { ... }
```

If you intentionally want a manually-started listener (e.g. controlled by a feature flag), document it and surface the state via an Actuator endpoint:

```java
@Component
class ListenerHealth implements HealthIndicator {
    private final KafkaListenerEndpointRegistry registry;
    public Health health() {
        boolean running = registry.getListenerContainer("order-listener").isRunning();
        return running ? Health.up().build() : Health.outOfService().build();
    }
}
```

## When this might be a false positive

- Intentional pattern: a manually-controlled listener (e.g., reprocessing, replay, batch backfill) that an operator starts via JMX/Actuator. In that case, mark the listener with a project-specific annotation or comment and configure a suppression.
- Tests that disable autostart to control consumer lifecycle.

## Detection strategy

- Annotation: read `@KafkaListener.autoStartup` from `AnnotationNode.values`. Flag when the literal string is `"false"`.
- For placeholder expressions (`${...:false}`), parse the default segment after the colon — flag when default is `false` or missing and the property isn't set in any scanned application properties.
- Confidence: HIGH for literal `"false"`; MEDIUM for placeholder with default `false`; LOW for unresolved placeholders.

## References

- Spring Kafka — `@KafkaListener` Annotation (`autoStartup` attribute): https://docs.spring.io/spring-kafka/reference/kafka/receiving-messages/listener-annotation.html
- Spring Kafka — Lifecycle Management: https://docs.spring.io/spring-kafka/reference/kafka/receiving-messages/kafka-listener-lifecycle.html
