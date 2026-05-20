# SPRING_TRANSACTIONAL_WITHOUT_KTM

**Severity**: ERROR
**Confidence**: MEDIUM
**Detection**: annotation + bytecode + config-file
**Tagline**: @Transactional + kafkaTemplate.send() without a transactional producer is decoration.

## TL;DR

The linter flags methods annotated `@Transactional` that call `KafkaTemplate.send(...)`, when no `KafkaTransactionManager` is configured and the `ProducerFactory` does not have a `transactionIdPrefix`. The send is not part of any transaction — rollback won't undo it.

## What's happening (the mechanism)

Spring's `@Transactional` activates a `PlatformTransactionManager`. By default with Spring Boot + JPA, that's `JpaTransactionManager` over a `DataSourceTransactionManager`. The Kafka producer is unaware of that transaction unless one of:

1. `spring.kafka.producer.transaction-id-prefix` is set — Spring Boot creates a `KafkaTransactionManager` and the `KafkaTemplate` synchronizes sends with whatever transaction is active.
2. A `KafkaTransactionManager` bean is manually wired and the `KafkaTemplate` is constructed with a transactional `ProducerFactory`.

Without either, `kafkaTemplate.send(...)` writes directly to Kafka. The DB transaction may roll back, but the Kafka record is already at the broker — it's gone, no compensating action.

Per the docs: *"Normally, when a `KafkaTemplate` is transactional (configured with a transaction-capable producer factory), transactions are required. ... Any attempt to use the template outside the scope of a transaction results in the template throwing an `IllegalStateException`."*

The mirror anti-pattern (transactional template used outside a transaction) is a hard runtime error. The case this rule catches is the silent one: `@Transactional` is there, but the producer is non-transactional, so the developer's expectation that "rollback = no Kafka send" is wrong.

## Operational impact

- Inconsistent state. DB rollback leaves orphan Kafka records — e.g., "order created" event published but the order row was rolled back. Downstream services act on an order that doesn't exist.
- Hard to detect: no log line says "the send was not transactional". Only auditing the bean graph reveals it.
- Manifests during partial-failure scenarios (transient DB error after the send is in flight). On the happy path the bug is invisible.

## How to fix

```java
// BAD — @Transactional but no Kafka TX
@Transactional
public void createOrder(Order o) {
    repo.save(o);
    kafkaTemplate.send("orders", o.id(), o); // not in TX
    if (o.amount() < 0) throw new IllegalArgumentException(); // DB rolls back, Kafka doesn't
}

// GOOD — enable Kafka TX in Spring Boot
// application.yml:
//   spring.kafka.producer.transaction-id-prefix: my-app-${HOSTNAME}-

// GOOD — manual configuration
@Bean
public KafkaTransactionManager<String, Object> ktm(ProducerFactory<String, Object> pf) {
    return new KafkaTransactionManager<>(pf);
}

// With both DB and Kafka transactions, prefer ChainedKafkaTransactionManager
// or use @Transactional with the appropriate manager and let Spring synchronize
```

If you don't want exactly-once across DB and Kafka, accept the inconsistency window explicitly: send Kafka events *after* the DB commit, using `TransactionalEventListener` (or `ApplicationEventPublisher` + `@TransactionalEventListener(phase = AFTER_COMMIT)`). That at least makes the send conditional on commit success.

## When this might be a false positive

- The `@Transactional` is for the DB only, and the team consciously accepts that Kafka sends are best-effort. Configure suppression or downgrade to INFO.
- The send is performed via `executeInTransaction(...)` — a local Kafka transaction independent of the surrounding `@Transactional`. That's an unusual but legitimate pattern.

## Detection strategy

- Annotation: find methods annotated `org.springframework.transaction.annotation.Transactional`.
- Bytecode: within those methods, detect `INVOKEVIRTUAL` to `KafkaTemplate.send(...)`.
- Config: check `spring.kafka.producer.transaction-id-prefix` in application properties.
- Bean-graph: detect any `KafkaTransactionManager` bean defined in `@Configuration` classes.
- Suppress when either is present.
- Confidence: MEDIUM — bean-graph reasoning has gaps; the rule is best-effort but high-signal when both checks fail.

## Consult a friend?

> 🤝 **Slow down.** Wiring `spring.kafka.producer.transaction-id-prefix` looks innocuous but it changes the producer's runtime contract — every send now *requires* an active transaction and throws `IllegalStateException` outside one.
> - Are there *any* `kafkaTemplate.send(...)` call sites in the app outside of `@Transactional` methods (controllers, `@Scheduled`, ad-hoc admin endpoints, healthchecks)? They'll all start throwing on the first deploy after the prefix is set. Audit before merging.
> - Is the `transaction-id-prefix` unique per pod? Two replicas with the same prefix fence each other (`ProducerFencedException`) on epoch bump — same foot-gun as `transactional.id` in plain clients. Spring Boot appends a sequence per producer, but the *prefix* still needs to vary per pod.
> - If the `@Transactional` method also writes to a database, `KafkaTransactionManager` alone won't roll back the DB. You need `ChainedKafkaTransactionManager` (or `TransactionalEventListener(phase=AFTER_COMMIT)`) — confirm which boundary you actually want before merging.
> - Read the Spring Kafka transactions chapter end-to-end — the `KafkaTemplate` / `KafkaTransactionManager` / `ProducerFactory` triangle has subtle rules about which one synchronizes with what.

## References

- Spring Kafka — Transactions: https://docs.spring.io/spring-kafka/reference/kafka/transactions.html
- Spring Boot — `spring.kafka.producer.transaction-id-prefix`: https://docs.spring.io/spring-boot/reference/messaging/kafka.html
- Spring Framework — `@TransactionalEventListener`: https://docs.spring.io/spring-framework/reference/data-access/transaction/event.html
