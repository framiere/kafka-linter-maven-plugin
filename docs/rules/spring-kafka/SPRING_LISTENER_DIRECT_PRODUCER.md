# SPRING_LISTENER_DIRECT_PRODUCER

**Severity**: ERROR
**Confidence**: HIGH
**Detection**: bytecode
**Tagline**: `new KafkaProducer(...)` inside a Spring app is a memory leak with delivery guarantees.

## TL;DR

The linter flags any code that instantiates `org.apache.kafka.clients.producer.KafkaProducer` directly inside a Spring-managed bean — especially inside a `@KafkaListener` method — instead of injecting the auto-configured `KafkaTemplate`.

## What's happening (the mechanism)

In a Spring Boot Kafka application, Spring auto-configures a `KafkaTemplate` bean backed by a singleton `ProducerFactory`. The producer is created once, reused, and closed cleanly on shutdown.

When code constructs `new KafkaProducer<>(props)` directly, especially inside a listener method:

- A brand-new producer is created on every record. Each producer opens TCP connections to all brokers, runs a metadata-fetch round, allocates sender threads, and stays alive until GC'd or explicitly `close()`d.
- If `close()` is never called, the JVM keeps file descriptors / native memory until the next FullGC — which may never come fast enough.
- Connection storm to brokers: every restart with replay creates N producers; the broker sees a thundering herd.
- No participation in Spring transactions: a listener configured with `KafkaTransactionManager` will commit/rollback against the framework producer, but the direct producer is invisible to that transaction — partial writes survive a rollback.
- Configuration drift: properties like `acks`, `enable.idempotence`, `transactional.id` set globally via `spring.kafka.producer.*` are not picked up.

The same pattern in helper services (not just `@KafkaListener`) is the broader anti-pattern — but listeners are the hot path that amplifies the cost.

## Operational impact

- File-descriptor leak. `lsof -p <pid> | wc -l` climbs over hours. JVM eventually fails with `Too many open files`.
- Broker connection count climbs; if the cluster has `max.connections.per.ip` set, new producers from the app start getting rejected.
- `KafkaProducer` thread leaks visible in a thread dump as many `kafka-producer-network-thread | producer-<n>` threads.
- Records sent via the direct producer bypass `ProducerListener`, micrometer observation, transactions, and any interceptor configured globally.

## How to fix

```java
// BAD — direct KafkaProducer in a listener
@KafkaListener(topics = "orders", groupId = "g")
public void onOrder(Order o) {
    Properties p = new Properties();
    p.put("bootstrap.servers", "...");
    try (KafkaProducer<String, String> producer = new KafkaProducer<>(p)) {
        producer.send(new ProducerRecord<>("audit", o.id(), o.toString()));
    }
}

// GOOD — inject KafkaTemplate
@Component
class OrderListener {
    private final KafkaTemplate<String, String> template;

    OrderListener(KafkaTemplate<String, String> template) {
        this.template = template;
    }

    @KafkaListener(topics = "orders", groupId = "g")
    public void onOrder(Order o) {
        template.send("audit", o.id(), o.toString())
                .whenComplete((r, ex) -> { if (ex != null) log.error("audit", ex); });
    }
}
```

If you need multiple producer configurations (e.g., different transactional id prefixes, different serializers), define multiple `KafkaTemplate` beans backed by distinct `ProducerFactory` instances. Don't bypass the container.

## When this might be a false positive

- Application bootstrap code (e.g., a `KafkaProducerFactory` implementation) that legitimately wraps a `KafkaProducer`. Restrict the rule to call sites where the constructor is invoked inside a method of a `@Component` / `@Service` / `@KafkaListener`-carrying class.
- Test code (use Spring's `@EmbeddedKafka` helpers anyway).

## Detection strategy

- Bytecode: scan for `INVOKESPECIAL org/apache/kafka/clients/producer/KafkaProducer <init>` and `NEW org/apache/kafka/clients/producer/KafkaProducer`.
- Higher signal: when the enclosing method is a `@KafkaListener` or the enclosing class is annotated `@Component`/`@Service`/`@Configuration`. Outside those, lower the severity.
- Confidence: HIGH inside `@KafkaListener` methods; MEDIUM elsewhere.

## Consult a friend?

> 🤝 **Slow down.** Replacing a hand-rolled `new KafkaProducer(...)` with `KafkaTemplate` injection is correct, but the two have *different* semantics for transactional containers — and the listener's behavior on rollback changes.
> - Is the listener container configured with a `KafkaTransactionManager`? If yes, `kafkaTemplate.send(...)` inside the listener method joins the same transaction — meaning a downstream rollback now also discards the send the hand-rolled producer used to commit independently. Is that the intended new behavior, or did the team rely on the side-effect surviving the rollback?
> - The hand-rolled producer's config (acks, retries, transactional.id) is being replaced by the Spring-Boot-auto-configured one. Are *those* properties set correctly via `spring.kafka.producer.*`, or did the hand-rolled config diverge for a reason?
> - If the goal of the inline producer was "send to a different cluster", `KafkaTemplate` injection still works — but you need a second `ProducerFactory` bean. Don't fix the leak by accidentally sending audit records to the wrong cluster.

## References

- Spring Kafka — `KafkaTemplate`: https://docs.spring.io/spring-kafka/reference/kafka/sending-messages.html
- Spring Boot — Auto-configured `KafkaTemplate`: https://docs.spring.io/spring-boot/reference/messaging/kafka.html
