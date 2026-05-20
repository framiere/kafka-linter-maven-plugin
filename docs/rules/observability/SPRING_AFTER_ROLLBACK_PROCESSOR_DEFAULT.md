# SPRING_AFTER_ROLLBACK_PROCESSOR_DEFAULT

**Severity**: WARNING
**Confidence**: MEDIUM
**Detection**: bytecode
**Tagline**: Transactional listeners don't use `DefaultErrorHandler` — they use `DefaultAfterRollbackProcessor`, with its own equally-bad default backoff.

## TL;DR

The linter flags Spring Kafka projects that combine a transactional listener (`@Transactional`, `KafkaTransactionManager`, or `ContainerProperties.transactionManager`) with no explicit `AfterRollbackProcessor` bean. After a rollback, Spring uses `DefaultAfterRollbackProcessor` with — surprise — `FixedBackOff(0L, 9L)` and no recoverer. Same data-loss-after-10-immediate-retries as the non-transactional case, but on the transactional path it's even harder to see because exceptions get masked by the rollback.

## The setup

Team enables transactions on their Kafka listener (e.g. wants exactly-once semantics for read-process-write). They wire a `KafkaTransactionManager`. They configure a beautiful `DefaultErrorHandler` with exponential backoff and DLT. They test it, and the retries don't happen. They learn that transactional listeners don't route through `CommonErrorHandler` — the exception triggers a rollback, and the *after-rollback* path is governed by `AfterRollbackProcessor`, which has its own defaults.

## What's actually happening

When a transaction is in flight on a listener:

```
@Transactional or KafkaTransactionManager
 ↓ exception
 ↓ transaction rolls back (consumer offset + producer side both undone)
 ↓ AfterRollbackProcessor.process(...) invoked
 ↓ decides: re-seek (retry) or recover (DLT) or skip
```

If no `AfterRollbackProcessor` is configured, Spring uses `DefaultAfterRollbackProcessor` with:
- `BackOff`: `FixedBackOff(0L, 9L)` → 10 immediate retries
- Recoverer: log at ERROR
- After 10 failures: log and skip

Key extra: `DefaultAfterRollbackProcessor` defaults `commitRecovered=false`. This means the recovered offset is **not** committed in a new transaction, so even after "recovery", the next consumer instance may reprocess the same record. The right setting for exactly-once is `commitRecovered=true` *and* providing a `KafkaTemplate` so the DLT publish is part of the rollback-recovery transaction.

## Why this is subtle

- Spring's docs intentionally say: *"If you provide a custom error handler when using transactions, it must throw an exception if you want the transaction rolled back."* Implying the `CommonErrorHandler` is involved — and it *can* be (for pre-rollback exceptions) — but the post-rollback retry is `AfterRollbackProcessor`.
- The mental model "I configured DefaultErrorHandler, retries are handled" is wrong under transactions.
- Failure mode is identical to the non-transactional case: retries are immediate, records are silently dropped after 10 attempts, no DLT.
- Even worse, without `commitRecovered=true`, the "recovered" record can be reprocessed by another consumer, creating duplicates.

## Operational impact

- 10 immediate redeliveries per failed record, log spam, downstream-load amplification.
- No DLT publication.
- "Recovered" records can be reprocessed elsewhere — exactly-once semantic broken.
- Producer-side fences (during long retries with `transactional.id` reused) can cause `ProducerFencedException` that further confuses the picture.

## Failure scenarios (walkthrough)

1. **The exactly-once myth.** Team builds a read-process-write pipeline with EOS turned on. A bug in the processing logic throws on certain inputs. The transaction rolls back, `DefaultAfterRollbackProcessor` retries 10 times immediately, then "recovers" by logging. The record's offset is *not* committed in a new transaction → on the next rebalance, the same record is delivered to another instance → same bug → infinite loop of "exactly-once" duplicates.

2. **The masked retry.** Team sees a record fail and expects to see DLT entries. None appear. They check `kafka.listener.error` metric — zero. They check application logs — find ten identical stack traces. They learn that `DefaultErrorHandler.handleOne` is never invoked in the transactional path.

## How to fix

```java
// BAD — transaction manager + default error handling
@Bean
public KafkaTransactionManager<String, ?> ktm(ProducerFactory<String, ?> pf) {
    return new KafkaTransactionManager<>(pf);
}
@Bean
public DefaultErrorHandler errorHandler() { return new DefaultErrorHandler(); }
// AfterRollbackProcessor not provided → default: 10 retries at 0ms, no DLT, no commitRecovered

// GOOD — explicit, transactional-aware
@Bean
public DefaultAfterRollbackProcessor<String, Object> afterRollback(
        KafkaTemplate<String, Object> template) {
    DeadLetterPublishingRecoverer recoverer = new DeadLetterPublishingRecoverer(template);
    ExponentialBackOffWithMaxRetries bo = new ExponentialBackOffWithMaxRetries(5);
    bo.setInitialInterval(1_000L);
    bo.setMultiplier(2.0);
    bo.setMaxInterval(30_000L);
    DefaultAfterRollbackProcessor<String, Object> p =
        new DefaultAfterRollbackProcessor<>(recoverer, bo, template, true);
    // commitRecovered=true → DLT publish runs in the same new transaction
    p.addNotRetryableException(IllegalArgumentException.class);
    return p;
}

@Bean
public ConcurrentKafkaListenerContainerFactory<String, Object> factory(
        ConsumerFactory<String, Object> cf,
        KafkaTemplate<String, Object> template,
        DefaultAfterRollbackProcessor<String, Object> arp) {
    ConcurrentKafkaListenerContainerFactory<String, Object> f = new ConcurrentKafkaListenerContainerFactory<>();
    f.setConsumerFactory(cf);
    f.getContainerProperties().setTransactionManager(new KafkaTransactionManager<>(template.getProducerFactory()));
    f.setAfterRollbackProcessor(arp);
    return f;
}
```

## Consult a friend?

> 🤝 **Slow down.** This is exactly-once territory. Before applying:
> - Confirm you understand that with `KafkaTransactionManager`, `DefaultErrorHandler` does **not** govern retries on transactional listener exceptions — `AfterRollbackProcessor` does.
> - `commitRecovered=true` requires the `KafkaTemplate` argument to point at the same `ProducerFactory` that the listener container's transaction manager uses. Otherwise the DLT publish is in a different transaction and you lose atomicity.
> - If your listener writes to a database AND a Kafka topic, you need a chained transaction manager — the `KafkaTransactionManager` alone won't roll back the DB.
> - Read the spring-kafka EOS / transactions chapter end-to-end before merging.

## When this might be a false positive

- No transaction manager configured — this rule doesn't apply.
- An explicit `AfterRollbackProcessor` bean is configured (even if it's just `new DefaultAfterRollbackProcessor<>(recoverer, backoff, template, true)`).
- The listener is read-only (no producer side) and a regular `DefaultErrorHandler` actually does suffice — but consider whether you actually need transactions.

## Detection strategy

- bytecode: detect `KafkaTransactionManager` bean OR `ContainerProperties.setTransactionManager` call OR `@Transactional` on a `@KafkaListener` method.
- bytecode: check absence of `AfterRollbackProcessor` bean OR `setAfterRollbackProcessor` call on container/factory.
- bytecode: even if present, flag if the constructor is `new DefaultAfterRollbackProcessor<>()` with no recoverer.
- Confidence MEDIUM — transactional detection has edge cases (JPA `@Transactional` without `KafkaTransactionManager` doesn't change Kafka offset behavior).

## References

- Spring Kafka — `DefaultAfterRollbackProcessor`: https://docs.spring.io/spring-kafka/reference/kafka/annotation-error-handling.html#after-rollback
- Spring Kafka — transactions: https://docs.spring.io/spring-kafka/reference/kafka/transactions.html
- Spring Kafka — exactly-once semantics: https://docs.spring.io/spring-kafka/reference/kafka/exactly-once.html
