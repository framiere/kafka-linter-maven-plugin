# KAFKA_TEST_UTILS_RUNTIME_SCOPE

**Severity**: WARNING
**Confidence**: HIGH
**Detection**: pom-dependency
**Tagline**: kafka-streams-test-utils in runtime scope ships an embedded broker into production.

## TL;DR

`org.apache.kafka:kafka-streams-test-utils`, `org.springframework.kafka:spring-kafka-test`, and `io.quarkus:quarkus-test-kafka-companion` are **test-only** dependencies that bundle embedded broker bits, large reflective hooks, and assertion frameworks. Declaring them without `<scope>test</scope>` ships them into production deployments — wasting space, expanding attack surface, and occasionally exposing test-only debug endpoints.

## The setup

A developer copy-pasted a test setup from a tutorial. They forgot `<scope>test</scope>`. The build works (the code that uses these classes is in `src/test/java` and compiles fine; runtime classpath now includes the test bits too). The deploy artifact balloons by 50+ MB. Nobody notices until image-size review.

## What's actually happening

The relevant test-only artifacts:

| Artifact | Purpose | Production weight |
|----------|---------|-------------------|
| `kafka-streams-test-utils` | `TopologyTestDriver`, in-memory state stores for tests | ~5 MB + reflective hooks |
| `kafka-clients-test-jar` | Internal classifier for shared test fixtures | ~3 MB |
| `spring-kafka-test` | `@EmbeddedKafka`, `EmbeddedKafkaBroker` (in-JVM broker!) | ~50 MB transitively (full broker) |
| `spring-boot-starter-test` | Includes spring-kafka-test transitively | ~50+ MB |
| `quarkus-test-kafka-companion` | Quarkus test bridges | ~5 MB |
| `io.confluent:kafka-streams-avro-serde` test-jar | Schema Registry fakes | ~10 MB |

`spring-kafka-test` is the worst offender — it transitively brings the **broker** (`org.apache.kafka:kafka_2.13`) so `@EmbeddedKafka` can boot a real broker in-process for tests. In production scope, you've just shipped a Kafka broker inside your microservice.

## Why this is subtle

- Tests run fine either way — scope is a no-op for `mvn test`. The bug is invisible until you check the deploy artifact's size.
- IDEs don't warn. Maven doesn't warn (correct scoping is the developer's responsibility).
- Some Spring Boot starters declare these in `compile` scope as a default; teams that copy from samples inherit the wrong default.

## Operational impact

- **Container image bloat** — 50+ MB extra for spring-kafka-test alone.
- **Cold start regression** — class loading scales with classpath size.
- **Expanded attack surface** — the embedded broker shipping with `spring-kafka-test` carries its own CVE stream that you now inherit in production.
- **Confusing JMX metrics** — embedded broker MBeans register if anything accidentally instantiates `EmbeddedKafkaBroker` at runtime.

## How to fix

```xml
<!-- BAD: missing scope, ships to production -->
<dependency>
    <groupId>org.springframework.kafka</groupId>
    <artifactId>spring-kafka-test</artifactId>
</dependency>

<!-- GOOD -->
<dependency>
    <groupId>org.springframework.kafka</groupId>
    <artifactId>spring-kafka-test</artifactId>
    <scope>test</scope>
</dependency>

<!-- BAD: streams test utils in compile scope -->
<dependency>
    <groupId>org.apache.kafka</groupId>
    <artifactId>kafka-streams-test-utils</artifactId>
    <version>3.9.2</version>
</dependency>

<!-- GOOD -->
<dependency>
    <groupId>org.apache.kafka</groupId>
    <artifactId>kafka-streams-test-utils</artifactId>
    <version>3.9.2</version>
    <scope>test</scope>
</dependency>
```

## When this might be a false positive

- A library that *exposes* test fixtures for its consumers (e.g. a shared internal "test-support" module). Then `compile` scope is intended — but `test-jar` packaging is the cleaner pattern.

## Detection strategy

- Walk `project.getDependencies()` for artifactIds matching:
  - `*-test`, `*-test-utils`, `*-test-jar`, `kafka-streams-test-utils`
  - `spring-kafka-test`, `spring-boot-starter-test`
  - `quarkus-test-*`, `quarkus-junit5-*`
- If scope is not `test`: WARNING.
- Special handling: `spring-boot-starter-test` (which transitively includes other test artifacts) — flag the parent only.

## References

- [Maven dependency scope](https://maven.apache.org/guides/introduction/introduction-to-dependency-mechanism.html#Dependency_Scope)
- [Spring Boot test starter](https://docs.spring.io/spring-boot/docs/current/reference/htmlsingle/#features.testing)
- [Kafka Streams testing guide](https://kafka.apache.org/documentation/streams/developer-guide/testing.html)
