# SPRING_LISTENER_CONCURRENCY_EXCEEDS_PARTITIONS

**Severity**: WARNING
**Confidence**: CONTEXT
**Detection**: annotation
**Tagline**: Extra concurrency past partition count just hires idle threads.

## TL;DR

The linter flags `@KafkaListener(concurrency = "N")` where N is suspiciously high (configurable threshold, e.g. > 32). The static check cannot read the broker's partition count, so the rule is a prompt-for-review when N looks unreasonable. Each `concurrency` slot above the partition count is an idle Kafka consumer.

## What's happening (the mechanism)

A `ConcurrentMessageListenerContainer` spins up N concurrent child containers, each with its own `KafkaConsumer` and its own poll loop. Kafka's partition assignment protocol distributes partitions across consumers in the group:

- N consumers, P partitions, P >= N: each consumer gets roughly P/N partitions. Optimal.
- P consumers, P partitions: one consumer per partition. Optimal.
- N consumers, P partitions, N > P: N - P consumers get zero partitions. They sit idle, holding broker connections and heartbeats forever.

Each idle consumer:
- Holds an open broker TCP connection.
- Sends regular heartbeats to the group coordinator (`session.timeout.ms` / 3 by default).
- Counts against `max.connections.per.user` and similar broker quotas.
- Wastes app threads — the thread is alive but never invoked.

The static linter cannot inspect topic metadata. The next best thing is a heuristic on the literal value. Concurrency > 32 is almost always a misconfiguration; concurrency > 16 is suspect for most topics.

A second pattern this rule helps with: SpEL-resolved concurrency from an environment variable that drifts above partition count over time as engineers tune the env without re-checking the topic.

## Operational impact

- Apparent capacity does not match real throughput — adding more concurrency doesn't speed anything up because the bottleneck is partition count.
- Broker connection / heartbeat overhead grows linearly with idle consumers.
- `kafka.consumer.assigned-partitions` metric is zero for some consumers — a giveaway.
- Spring's `KafkaListenerEndpointRegistry` shows N child containers; only M < N are actually doing work.

## How to fix

```java
// BAD — N too high
@KafkaListener(topics = "orders", groupId = "g", concurrency = "64")
public void onOrder(Order o) { ... }

// GOOD — match partition count
// Topic 'orders' has 12 partitions
@KafkaListener(topics = "orders", groupId = "g", concurrency = "12")
public void onOrder(Order o) { ... }

// GOOD — externalize and document the contract
@KafkaListener(topics = "orders", groupId = "g",
               concurrency = "${kafka.orders.concurrency:6}")
// in application.yml:
//   kafka.orders.concurrency: 6  # MUST be <= orders.partitions
public void onOrder(Order o) { ... }
```

If you need more concurrency than partitions for processing parallelism (within one consumer), don't add Kafka consumers — process records inside the listener method with a bounded thread pool, and ensure offset commits respect the parallel work (manual ack with careful ordering, or pause/resume).

## When this might be a false positive

- The topic has hundreds or thousands of partitions (common for heavy-keyed streams). High concurrency is fine.
- Migration scenario: app starts with N concurrency, partitions are being increased to match. Transient.

## Detection strategy

- Annotation: read `@KafkaListener.concurrency`. Parse as an integer if literal, else mark unresolved.
- Flag literal values above a threshold (default 32, configurable).
- Bytecode: also flag `ConcurrentKafkaListenerContainerFactory.setConcurrency(int)` calls with high literal values.
- Config: also scan `spring.kafka.listener.concurrency`.
- Confidence: CONTEXT — without broker metadata we can't know the partition count.

## References

- Spring Kafka — `concurrency` attribute: https://docs.spring.io/spring-kafka/reference/kafka/receiving-messages/listener-annotation.html
- Apache Kafka — Consumer group partition assignment: https://kafka.apache.org/documentation/#impl_consumer
