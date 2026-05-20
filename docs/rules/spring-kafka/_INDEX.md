# spring-kafka rules

Build-time anti-pattern checks for applications using `org.springframework.kafka:spring-kafka` (typically via Spring Boot's `spring-boot-starter-kafka`). Rules combine ASM bytecode analysis (`@KafkaListener` annotation reads, listener method bodies, `KafkaTemplate` call sites) with parsing of Spring Boot configuration (`application.properties`, `application.yml`, profile variants). Targets spring-kafka 3.x and 4.x; behavior referenced is current as of spring-kafka 4.0.x / Spring Boot 4.0.x unless noted.

Severity:
- **ERROR** — almost always a bug. Will fail context init, throw `IllegalStateException` at runtime, or cause silent data loss.
- **WARNING** — usually a bug; legitimate edge cases exist.

Confidence:
- **HIGH** — the linter can prove the antipattern from annotation values or a single property.
- **MEDIUM** — heuristic / cross-source inference (annotation + properties + bean graph); some false positives expected.
- **CONTEXT** — depends on environment (broker partition count, runtime profile, intent) that cannot be known statically.

Detection:
- **bytecode** — ASM scan of `.class` files for annotated methods, parameter types, `KafkaTemplate` call patterns.
- **annotation** — reads `AnnotationNode.values` from `@KafkaListener`, `@RetryableTopic`, `@SendTo`, `@Transactional`, `@Async`, etc.
- **config-file** — text parsing of `application*.properties` / `application*.yml`.
- **combination** — needs more than one source (e.g., annotation + property + bean graph).

## Catalog

### `@KafkaListener` annotation hygiene

| Rule ID | Severity | Conf. | Detection | Tagline |
|---|---|---|---|---|
| [SPRING_LISTENER_MISSING_GROUP_ID](./SPRING_LISTENER_MISSING_GROUP_ID.md) | ERROR | HIGH | combination | A listener with no group is just a private subscription nobody else knows about. |
| [SPRING_LISTENER_AUTOSTARTUP_FALSE](./SPRING_LISTENER_AUTOSTARTUP_FALSE.md) | WARNING | MEDIUM | annotation | A listener you forgot to start is a feature flag that's always off. |
| [SPRING_LISTENER_EMPTY_TOPICS](./SPRING_LISTENER_EMPTY_TOPICS.md) | ERROR | HIGH | annotation | An empty topics array subscribes to nothing, loudly. |
| [SPRING_LISTENER_CONCURRENCY_EXCEEDS_PARTITIONS](./SPRING_LISTENER_CONCURRENCY_EXCEEDS_PARTITIONS.md) | WARNING | CONTEXT | annotation | Extra concurrency past partition count just hires idle threads. |

### Listener method body

| Rule ID | Severity | Conf. | Detection | Tagline |
|---|---|---|---|---|
| [SPRING_LISTENER_MANUAL_ACK_NEVER_CALLED](./SPRING_LISTENER_MANUAL_ACK_NEVER_CALLED.md) | ERROR | HIGH | bytecode | AckMode.MANUAL without calling acknowledge() is just AckMode.NEVER. |
| [SPRING_LISTENER_THREAD_SLEEP](./SPRING_LISTENER_THREAD_SLEEP.md) | WARNING | MEDIUM | bytecode | Thread.sleep() in a listener is a self-inflicted rebalance. |
| [SPRING_LISTENER_ASYNC_ANNOTATION](./SPRING_LISTENER_ASYNC_ANNOTATION.md) | ERROR | HIGH | annotation | @Async on a listener breaks every offset guarantee Kafka gives you. |
| [SPRING_LISTENER_DIRECT_PRODUCER](./SPRING_LISTENER_DIRECT_PRODUCER.md) | WARNING | HIGH | bytecode | `new KafkaProducer(...)` inside a Spring app is a memory leak with delivery guarantees. |
| [SPRING_LISTENER_BATCH_SIGNATURE_MISMATCH](./SPRING_LISTENER_BATCH_SIGNATURE_MISMATCH.md) | ERROR | HIGH | combination | A batch listener that takes one record will never deploy. |
| [SPRING_LISTENER_SENDTO_NO_REPLY_TEMPLATE](./SPRING_LISTENER_SENDTO_NO_REPLY_TEMPLATE.md) | WARNING | MEDIUM | combination | @SendTo without a reply template sends your replies into the void. |
| [SPRING_LISTENER_RETURN_VALUE_IGNORED](./SPRING_LISTENER_RETURN_VALUE_IGNORED.md) | WARNING | HIGH | combination | A non-void listener with no @SendTo is a return statement nobody reads. |

### Container factory & error handling

| Rule ID | Severity | Conf. | Detection | Tagline |
|---|---|---|---|---|
| [SPRING_DEPRECATED_SEEK_TO_CURRENT_ERROR_HANDLER](./SPRING_DEPRECATED_SEEK_TO_CURRENT_ERROR_HANDLER.md) | WARNING | HIGH | bytecode | SeekToCurrentErrorHandler shipped its last bug fix in 2021. |
| [SPRING_ACK_MODE_RECORD_HIGH_THROUGHPUT](./SPRING_ACK_MODE_RECORD_HIGH_THROUGHPUT.md) | WARNING | CONTEXT | config-file | AckMode.RECORD on a hot topic is one offset commit per record — say hi to the brokers. |

### `@RetryableTopic` / non-blocking retries

| Rule ID | Severity | Conf. | Detection | Tagline |
|---|---|---|---|---|
| [SPRING_RETRYABLE_TOPIC_WITH_BATCH](./SPRING_RETRYABLE_TOPIC_WITH_BATCH.md) | ERROR | HIGH | combination | @RetryableTopic + batch listeners = unsupported, per the docs. |
| [SPRING_RETRYABLE_TOPIC_NO_KAFKA_TEMPLATE](./SPRING_RETRYABLE_TOPIC_NO_KAFKA_TEMPLATE.md) | ERROR | HIGH | combination | @RetryableTopic without a template bean fails at startup — every time. |

### `KafkaTemplate` & transactions

| Rule ID | Severity | Conf. | Detection | Tagline |
|---|---|---|---|---|
| [SPRING_KAFKA_TEMPLATE_SEND_NO_CALLBACK](./SPRING_KAFKA_TEMPLATE_SEND_NO_CALLBACK.md) | WARNING | MEDIUM | bytecode | `kafkaTemplate.send(...)` and walk away is fire-and-pray. |
| [SPRING_KAFKA_TEMPLATE_SEND_BLOCKING_GET](./SPRING_KAFKA_TEMPLATE_SEND_BLOCKING_GET.md) | WARNING | HIGH | bytecode | `send(...).get()` turns an async producer back into a synchronous one, one record at a time. |
| [SPRING_TRANSACTIONAL_WITHOUT_KTM](./SPRING_TRANSACTIONAL_WITHOUT_KTM.md) | WARNING | MEDIUM | combination | @Transactional + kafkaTemplate.send() without a transactional producer is decoration. |

### Spring Boot configuration

| Rule ID | Severity | Conf. | Detection | Tagline |
|---|---|---|---|---|
| [SPRING_BOOT_AUTO_OFFSET_RESET_LATEST](./SPRING_BOOT_AUTO_OFFSET_RESET_LATEST.md) | WARNING | MEDIUM | config-file | `auto-offset-reset: latest` means "lose every record produced before the first deploy". |
| [SPRING_BOOT_PRODUCER_ACKS_NOT_ALL](./SPRING_BOOT_PRODUCER_ACKS_NOT_ALL.md) | WARNING | MEDIUM | config-file | `spring.kafka.producer.acks=1` ships durability away one record at a time. |
| [SPRING_BOOT_BOOTSTRAP_SERVERS_LOCALHOST](./SPRING_BOOT_BOOTSTRAP_SERVERS_LOCALHOST.md) | WARNING | MEDIUM | config-file | `bootstrap-servers: localhost:9092` shipped to prod is a Friday-evening pager. |
| [SPRING_BOOT_ENABLE_AUTO_COMMIT_VS_MANUAL_ACK](./SPRING_BOOT_ENABLE_AUTO_COMMIT_VS_MANUAL_ACK.md) | ERROR | HIGH | combination | `enable-auto-commit=true` + Acknowledgment in code = two cooks, no kitchen. |
| [SPRING_BOOT_ACK_MODE_MANUAL_WITHOUT_CODE_ACK](./SPRING_BOOT_ACK_MODE_MANUAL_WITHOUT_CODE_ACK.md) | ERROR | MEDIUM | combination | ack-mode=MANUAL plus no acknowledge() means no commits, ever. |

### Serialization / deserialization

| Rule ID | Severity | Conf. | Detection | Tagline |
|---|---|---|---|---|
| [SPRING_JSON_DESERIALIZER_NO_TRUSTED_PACKAGES](./SPRING_JSON_DESERIALIZER_NO_TRUSTED_PACKAGES.md) | WARNING | MEDIUM | combination | JsonDeserializer without trusted packages will refuse your own classes — until someone widens it to `*`. |
| [SPRING_JSON_DESERIALIZER_TRUST_STAR](./SPRING_JSON_DESERIALIZER_TRUST_STAR.md) | ERROR | HIGH | combination | `spring.json.trusted.packages=*` is `eval()` over the wire. |
| [SPRING_NO_ERROR_HANDLING_DESERIALIZER](./SPRING_NO_ERROR_HANDLING_DESERIALIZER.md) | WARNING | MEDIUM | config-file | Without ErrorHandlingDeserializer, one bad byte stops every consumer in the group. |
| [SPRING_SERIALIZER_MISMATCH](./SPRING_SERIALIZER_MISMATCH.md) | WARNING | MEDIUM | config-file | JsonSerializer in, StringDeserializer out — the wire becomes a Rorschach test. |

## Cross-cutting notes

- **Bean-graph reasoning is best-effort.** Rules that depend on detecting a `KafkaTransactionManager`, `KafkaTemplate`, `ProducerListener`, or `setReplyTemplate(...)` call walk the bytecode of `@Configuration` classes. They cannot follow runtime bean factories or conditional `@Bean` methods perfectly. Where a false negative is plausible, the rule's confidence is downgraded to MEDIUM.
- **Property scanning covers Spring Boot 4.x layout.** YAML files use snake-kebab-case keys (`spring.kafka.consumer.group-id`); both kebab-case and camelCase are recognized. Properties files use dotted keys (`spring.kafka.consumer.group-id` or `spring.kafka.consumer.groupId`).
- **Out-of-scope:** Spring Cloud Stream Kafka binder anti-patterns. Its `application.yml` layout (`spring.cloud.stream.kafka.binder.*`, `spring.cloud.stream.bindings.*`) is similar but the binder injects its own listener container factory. A separate rule set would be needed.
- **Versions referenced.** Where behavior changed across spring-kafka versions, the rule cites the version (most commonly 2.7 for `@RetryableTopic`, 2.8 for `CommonErrorHandler` / `DefaultErrorHandler`, 3.0 for `CompletableFuture` migration in `KafkaTemplate`, 3.2 for class-level `@RetryableTopic` and async return types).

## Total

27 rules in the spring-kafka catalog.
