# spring-kafka rules

Build-time anti-pattern checks for applications using `org.springframework.kafka:spring-kafka` (typically via Spring Boot's `spring-boot-starter-kafka`). Rules combine ASM bytecode analysis (`@KafkaListener` annotation reads, listener method bodies, `KafkaTemplate` call sites, `CommonErrorHandler` overrides) with parsing of Spring Boot configuration (`application.properties`, `application.yml`, profile variants). Targets spring-kafka 3.x and 4.x; behavior referenced is current as of spring-kafka 4.0.x / Spring Boot 4.0.x unless noted.

## Legend

**Severity**
- `ERROR` — almost always a bug. Will fail context init, throw `IllegalStateException` at runtime, or cause silent data loss.
- `WARNING` — usually a bug; legitimate edge cases exist.

**Confidence**
- `HIGH` — the linter can prove the antipattern from annotation values or a single property.
- `MEDIUM` — heuristic / cross-source inference (annotation + properties + bean graph); some false positives expected.
- `CONTEXT` — depends on environment (broker partition count, runtime profile, intent) that cannot be known statically.

**Detection** — `bytecode`, `annotation`, `config-file`, or a combination (` + `, alphabetical).

A trailing 🤝 in the tagline means the rule's doc carries a `## Consult a friend?` block — read it before changing prod config.

## Catalog (32)

### `@KafkaListener` annotation hygiene

| Rule | Severity | Confidence | Detection | Tagline |
|---|---|---|---|---|
| [SPRING_LISTENER_MISSING_GROUP_ID](./SPRING_LISTENER_MISSING_GROUP_ID.md) | ERROR | HIGH | annotation + bytecode + config-file | A listener with no group is just a private subscription nobody else knows about. |
| [SPRING_LISTENER_AUTOSTARTUP_FALSE](./SPRING_LISTENER_AUTOSTARTUP_FALSE.md) | WARNING | MEDIUM | annotation | A listener you forgot to start is a feature flag that's always off. |
| [SPRING_LISTENER_EMPTY_TOPICS](./SPRING_LISTENER_EMPTY_TOPICS.md) | ERROR | HIGH | annotation | An empty topics array subscribes to nothing, loudly. |
| [SPRING_LISTENER_CONCURRENCY_EXCEEDS_PARTITIONS](./SPRING_LISTENER_CONCURRENCY_EXCEEDS_PARTITIONS.md) | WARNING | CONTEXT | annotation | Extra concurrency past partition count just hires idle threads. |

### Listener method body

| Rule | Severity | Confidence | Detection | Tagline |
|---|---|---|---|---|
| [SPRING_LISTENER_MANUAL_ACK_NEVER_CALLED](./SPRING_LISTENER_MANUAL_ACK_NEVER_CALLED.md) | ERROR | HIGH | bytecode | AckMode.MANUAL without calling acknowledge() is just AckMode.NEVER. |
| [SPRING_LISTENER_THREAD_SLEEP](./SPRING_LISTENER_THREAD_SLEEP.md) | WARNING | MEDIUM | bytecode | Thread.sleep() in a listener is a self-inflicted rebalance. |
| [SPRING_LISTENER_ASYNC_ANNOTATION](./SPRING_LISTENER_ASYNC_ANNOTATION.md) | ERROR | HIGH | annotation | @Async on a listener breaks every offset guarantee Kafka gives you. |
| [SPRING_LISTENER_DIRECT_PRODUCER](./SPRING_LISTENER_DIRECT_PRODUCER.md) | ERROR | HIGH | bytecode | `new KafkaProducer(...)` inside a Spring app is a memory leak with delivery guarantees. 🤝 |
| [SPRING_LISTENER_BATCH_SIGNATURE_MISMATCH](./SPRING_LISTENER_BATCH_SIGNATURE_MISMATCH.md) | ERROR | HIGH | annotation + bytecode + config-file | A batch listener that takes one record will never deploy. |
| [SPRING_LISTENER_SENDTO_NO_REPLY_TEMPLATE](./SPRING_LISTENER_SENDTO_NO_REPLY_TEMPLATE.md) | WARNING | MEDIUM | annotation + bytecode + config-file | @SendTo without a reply template sends your replies into the void. |
| [SPRING_LISTENER_RETURN_VALUE_IGNORED](./SPRING_LISTENER_RETURN_VALUE_IGNORED.md) | WARNING | HIGH | annotation + bytecode | A non-void listener with no @SendTo is a return statement nobody reads. |
| [SPRING_LISTENER_SWALLOWS_EXCEPTION](./SPRING_LISTENER_SWALLOWS_EXCEPTION.md) | WARNING | MEDIUM | bytecode | `try { ... } catch (Exception e) { log.error(e); }` inside a `@KafkaListener` deletes the error handler. 🤝 |

### Container factory & error handling

| Rule | Severity | Confidence | Detection | Tagline |
|---|---|---|---|---|
| [SPRING_DEPRECATED_SEEK_TO_CURRENT_ERROR_HANDLER](./SPRING_DEPRECATED_SEEK_TO_CURRENT_ERROR_HANDLER.md) | WARNING | HIGH | bytecode | SeekToCurrentErrorHandler shipped its last bug fix in 2021. |
| [SPRING_ACK_MODE_RECORD_HIGH_THROUGHPUT](./SPRING_ACK_MODE_RECORD_HIGH_THROUGHPUT.md) | WARNING | CONTEXT | config-file | AckMode.RECORD on a hot topic is one offset commit per record — say hi to the brokers. |
| [SPRING_DEH_DEFAULT_BACKOFF](./SPRING_DEH_DEFAULT_BACKOFF.md) | WARNING | HIGH | bytecode | `new DefaultErrorHandler()` retries ten times with zero delay — that's not a backoff, that's a tantrum. 🤝 |
| [SPRING_DEH_NO_DLT_RECOVERER](./SPRING_DEH_NO_DLT_RECOVERER.md) | WARNING | HIGH | bytecode | `DefaultErrorHandler` with no recoverer logs your data loss and calls it a feature. 🤝 |
| [SPRING_CUSTOM_HANDLER_MISSING_OTHER_EXCEPTION](./SPRING_CUSTOM_HANDLER_MISSING_OTHER_EXCEPTION.md) | WARNING | HIGH | bytecode | A custom `CommonErrorHandler` that skips `handleOtherException` is half an error handler. |
| [SPRING_AFTER_ROLLBACK_PROCESSOR_DEFAULT](./SPRING_AFTER_ROLLBACK_PROCESSOR_DEFAULT.md) | WARNING | MEDIUM | bytecode | Transactional listeners don't use `DefaultErrorHandler` — they use `DefaultAfterRollbackProcessor`, with its own equally-bad default backoff. 🤝 |
| [SPRING_KSTREAMS_BINDER_DEFAULT_DEH](./SPRING_KSTREAMS_BINDER_DEFAULT_DEH.md) | WARNING | MEDIUM | config-file | Spring Cloud Stream Kafka Streams binder defaults to `logAndFail` — first poison pill takes down the function. 🤝 |

### `@RetryableTopic` / non-blocking retries

| Rule | Severity | Confidence | Detection | Tagline |
|---|---|---|---|---|
| [SPRING_RETRYABLE_TOPIC_WITH_BATCH](./SPRING_RETRYABLE_TOPIC_WITH_BATCH.md) | ERROR | HIGH | annotation + bytecode + config-file | @RetryableTopic + batch listeners = unsupported, per the docs. |
| [SPRING_RETRYABLE_TOPIC_NO_KAFKA_TEMPLATE](./SPRING_RETRYABLE_TOPIC_NO_KAFKA_TEMPLATE.md) | ERROR | HIGH | annotation + bytecode | @RetryableTopic without a template bean fails at startup — every time. |

### `KafkaTemplate` & transactions

| Rule | Severity | Confidence | Detection | Tagline |
|---|---|---|---|---|
| [SPRING_KAFKA_TEMPLATE_SEND_NO_CALLBACK](./SPRING_KAFKA_TEMPLATE_SEND_NO_CALLBACK.md) | WARNING | MEDIUM | bytecode | `kafkaTemplate.send(...)` and walk away is fire-and-pray. |
| [SPRING_KAFKA_TEMPLATE_SEND_BLOCKING_GET](./SPRING_KAFKA_TEMPLATE_SEND_BLOCKING_GET.md) | WARNING | HIGH | bytecode | `send(...).get()` turns an async producer back into a synchronous one, one record at a time. 🤝 |
| [SPRING_TRANSACTIONAL_WITHOUT_KTM](./SPRING_TRANSACTIONAL_WITHOUT_KTM.md) | ERROR | MEDIUM | annotation + bytecode + config-file | @Transactional + kafkaTemplate.send() without a transactional producer is decoration. 🤝 |

### Spring Boot configuration

| Rule | Severity | Confidence | Detection | Tagline |
|---|---|---|---|---|
| [SPRING_BOOT_AUTO_OFFSET_RESET_LATEST](./SPRING_BOOT_AUTO_OFFSET_RESET_LATEST.md) | WARNING | MEDIUM | config-file | `auto-offset-reset: latest` means "lose every record produced before the first deploy". |
| [SPRING_BOOT_PRODUCER_ACKS_NOT_ALL](./SPRING_BOOT_PRODUCER_ACKS_NOT_ALL.md) | ERROR for acks=0, WARNING for acks=1 | HIGH | config-file | `spring.kafka.producer.acks=1` ships durability away one record at a time. 🤝 |
| [SPRING_BOOT_BOOTSTRAP_SERVERS_LOCALHOST](./SPRING_BOOT_BOOTSTRAP_SERVERS_LOCALHOST.md) | ERROR (suppress under dev/local/test profile) | HIGH | config-file | `bootstrap-servers: localhost:9092` shipped to prod is a Friday-evening pager. |
| [SPRING_BOOT_ENABLE_AUTO_COMMIT_VS_MANUAL_ACK](./SPRING_BOOT_ENABLE_AUTO_COMMIT_VS_MANUAL_ACK.md) | ERROR | HIGH | annotation + bytecode + config-file | `enable-auto-commit=true` + Acknowledgment in code = two cooks, no kitchen. |
| [SPRING_BOOT_ACK_MODE_MANUAL_WITHOUT_CODE_ACK](./SPRING_BOOT_ACK_MODE_MANUAL_WITHOUT_CODE_ACK.md) | ERROR | MEDIUM | annotation + bytecode + config-file | ack-mode=MANUAL plus no acknowledge() means no commits, ever. |

### Serialization / deserialization

| Rule | Severity | Confidence | Detection | Tagline |
|---|---|---|---|---|
| [SPRING_NO_ERROR_HANDLING_DESERIALIZER](./SPRING_NO_ERROR_HANDLING_DESERIALIZER.md) | WARNING | MEDIUM | config-file | Without ErrorHandlingDeserializer, one bad byte stops every consumer in the group. |
| [SPRING_SERIALIZER_MISMATCH](./SPRING_SERIALIZER_MISMATCH.md) | WARNING | MEDIUM | config-file | JsonSerializer in, StringDeserializer out — the wire becomes a Rorschach test. |
| [SPRING_EHD_LISTENER_NULL_NOT_CHECKED](./SPRING_EHD_LISTENER_NULL_NOT_CHECKED.md) | WARNING | MEDIUM | bytecode + config-file | `ErrorHandlingDeserializer` returns `null` for poison pills — and your listener processes them as legitimate records. 🤝 |

## Cross-cutting notes

- **The Three Magic Tricks Spring does with errors** — `CommonErrorHandler` unifying `ErrorHandler` and `BatchErrorHandler` (KafkaSpring 2.8+), `DefaultErrorHandler` vs `DefaultAfterRollbackProcessor` for transactional vs non-transactional pipelines, and `ErrorHandlingDeserializer` + the `springDeserializerException*` header convention. These are explained in detail in the `observability/` index — many of the rules above (`SPRING_DEH_*`, `SPRING_AFTER_ROLLBACK_PROCESSOR_DEFAULT`, `SPRING_EHD_LISTENER_NULL_NOT_CHECKED`, `SPRING_CUSTOM_HANDLER_MISSING_OTHER_EXCEPTION`) reference those abstractions.
- **Bean-graph reasoning is best-effort.** Rules that depend on detecting a `KafkaTransactionManager`, `KafkaTemplate`, `ProducerListener`, or `setReplyTemplate(...)` call walk the bytecode of `@Configuration` classes. They cannot follow runtime bean factories or conditional `@Bean` methods perfectly. Where a false negative is plausible, the rule's confidence is downgraded to MEDIUM.
- **Property scanning covers Spring Boot 4.x layout.** YAML files use kebab-case keys (`spring.kafka.consumer.group-id`); both kebab-case and camelCase are recognized. Properties files use dotted keys (`spring.kafka.consumer.group-id` or `spring.kafka.consumer.groupId`).
- **Out-of-scope:** Spring Cloud Stream Kafka binder consumer/producer config is not enumerated here, but the Kafka Streams binder gets one rule (`SPRING_KSTREAMS_BINDER_DEFAULT_DEH`) for its `logAndFail` default.
- **Versions referenced.** Where behavior changed across spring-kafka versions, the rule cites the version (most commonly 2.7 for `@RetryableTopic`, 2.8 for `CommonErrorHandler` / `DefaultErrorHandler`, 3.0 for `CompletableFuture` migration in `KafkaTemplate`, 3.2 for class-level `@RetryableTopic` and async return types).

## Related catalogs

- [`../observability/`](../observability/) — `ErrorHandlingDeserializer`, JSON-type-info CVE, lambda anti-patterns, async/reactive bridge, and the long-form Three-Magic-Tricks walk-through.
- [`../versions/`](../versions/) — `SPRING_BOOT_EOL`, `SPRING_KAFKA_EOL`, `SPRING_KAFKA_BOOT_MISMATCH`, `SPRING_BOOT_KAFKA_CLIENT_OVERRIDE`, `SPRING_KAFKA_DUPLICATE_DECLARATION`.
- [`../good-practices/`](../good-practices/) — `OBS_SPRING_CLIENT_ID_PREFIX`, `INFRA_SPRING_EMBEDDED_KAFKA_TEST_ONLY`.
