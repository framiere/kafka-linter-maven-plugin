# INFRA_SPRING_EMBEDDED_KAFKA_TEST_ONLY

**Severity**: WARNING
**Confidence**: HIGH
**Detection**: bytecode + pom-dependency
**Tagline**: `EmbeddedKafkaBroker` is a test fixture. It does not belong in `src/main`.

## TL;DR

The linter flags references to `EmbeddedKafkaBroker`,
`@EmbeddedKafka`, or `spring-kafka-test` artifacts from `src/main`
sources. These are test scaffolding; in production code they either
fail to start (no embedded broker JAR on the runtime classpath) or
start one and bind to an ephemeral port that pods don't share.

## The setup

Spring Kafka ships `EmbeddedKafkaBroker` as a JUnit-friendly in-process
Kafka broker (since spring-kafka 2.2). It's intended to live in
`src/test/java`, used via `@EmbeddedKafka` (test-time annotation) or
explicit `EmbeddedKafkaBroker` instantiation in `@Configuration`-marked
test fixtures.

The bug pattern is innocuous: a developer prototypes
`@EmbeddedKafka` to spin up a broker quickly, refactors the
`@Configuration` into `src/main/java` to share it, then forgets to
move it back. In production:

- If the `spring-kafka-test` JAR was added to compile scope (not test
  scope), it ships. The embedded broker starts in every pod.
- If it was kept in test scope, the application fails at startup with
  `ClassNotFoundException`.

Cross-link to the existing anti-pattern:
[KAFKA_TEST_UTILS_RUNTIME_SCOPE](../versions/KAFKA_TEST_UTILS_RUNTIME_SCOPE.md)
covers the related `kafka-streams-test-utils` case at the pom level.

## What's actually happening (the mechanism)

`EmbeddedKafkaBroker.afterPropertiesSet()` starts a real Kafka broker
in-process (originally with ZooKeeper, since spring-kafka 3.0 with
KRaft). It binds to a random port and writes its bootstrap servers
into the `EmbeddedKafkaBroker.SPRING_EMBEDDED_KAFKA_BROKERS` system
property.

In a production pod:

- Each replica has its own private embedded broker.
- The configured `spring.kafka.bootstrap-servers` points to the
  embedded broker → messages never reach other replicas.
- Restart wipes all data.
- Resources (memory, file handles) are wasted starting a broker the
  app immediately ignores.

## How to fix (no → yes)

```java
// no — embedded broker referenced from main
package com.example.kafka;
import org.springframework.kafka.test.EmbeddedKafkaBroker; // imported in src/main!

@Configuration
public class KafkaTopics {
    @Bean
    public EmbeddedKafkaBroker broker() { return new EmbeddedKafkaBroker(1); }
}

// yes — keep in test scope
// src/test/java/com/example/kafka/EmbeddedBrokerTestConfig.java
@TestConfiguration
public class EmbeddedBrokerTestConfig {
    @Bean
    public EmbeddedKafkaBroker broker() { return new EmbeddedKafkaBroker(1); }
}
```

```xml
<!-- yes — pom: spring-kafka-test in test scope only -->
<dependency>
    <groupId>org.springframework.kafka</groupId>
    <artifactId>spring-kafka-test</artifactId>
    <scope>test</scope>
</dependency>
```

## When this might be a false positive

- Genuine in-process broker for a single-machine demo / training app
  (rare, but it does happen).
- Spring `@SpringBootTest` slice configurations that import
  `EmbeddedBroker` — those should be under `src/test/java`, but some
  projects place them under `src/main/test/java` or
  `src/integration-test/java`.

## Detection strategy

- **pom-dependency:** flag `org.springframework.kafka:spring-kafka-test`
  with scope `compile` (or no `scope` element, which defaults to
  compile).
- **Bytecode:** scan `src/main` `.class` files for references to
  `org.springframework.kafka.test.EmbeddedKafkaBroker` or
  `@EmbeddedKafka` (annotation).
- **Confidence: HIGH** on bytecode reference + main-scope dep.

## References

- Spring Kafka — Testing applications:
  https://docs.spring.io/spring-kafka/reference/testing.html
- Cross-link: [KAFKA_TEST_UTILS_RUNTIME_SCOPE](../versions/KAFKA_TEST_UTILS_RUNTIME_SCOPE.md)
