# SPRING_KAFKA_BOOT_MISMATCH

**Severity**: ERROR
**Confidence**: HIGH
**Detection**: pom-dependency + pom-property
**Tagline**: Mixing spring-kafka and Spring Boot major versions is a slow-motion classpath collision.

## TL;DR

`spring-kafka` 2.x is built for Spring Framework 5 / Spring Boot 2.x (`javax.*` namespace). `spring-kafka` 3.x is built for Spring Framework 6 / Spring Boot 3.x (`jakarta.*` namespace). `spring-kafka` 4.x is built for Spring Framework 7 / Spring Boot 4.x. Mixing across majors causes `NoClassDefFoundError` / `NoSuchMethodError` at autoconfiguration time, or worse — silent partial wiring.

## The setup

A team upgraded `spring-kafka` to a new major (read a blog post about a new feature) without realising it pulls a different Spring Framework. Or they upgraded Spring Boot but pinned `spring-kafka` to the previous major in `dependencyManagement` because "we know that version works". Either way, the BOM-managed and explicitly-pinned versions disagree, and Maven's nearest-wins resolution makes the choice silent.

## What's actually happening

The compatibility matrix is rigid:

| spring-kafka | Spring Framework | Spring Boot | Namespace | kafka-clients (compile) |
|--------------|------------------|-------------|-----------|-------------------------|
| 2.9.x | 5.3 | 2.7 | `javax.*` | 3.1.x |
| 3.0.x | 6.0 | 3.0 / 3.1 | `jakarta.*` | 3.3.2 → 3.6.0 |
| 3.1.x | 6.1 | 3.2 | `jakarta.*` | 3.6.x |
| 3.2.x | 6.1 | 3.3 | `jakarta.*` | 3.7.x |
| 3.3.x | 6.1 | 3.4 | `jakarta.*` | 3.8.x → 3.9.x |
| 4.0.x | 7.0 | 4.0 | `jakarta.*` | 4.0.x |

Crossing the 2.x / 3.x boundary changes the **package namespace** for Servlet, JMS, JPA, Bean Validation, et cetera. `spring-kafka` 2.x imports `javax.transaction.Transactional`. `spring-kafka` 3.x imports `jakarta.transaction.Transactional`. Boot 3 ships only `jakarta.*`. Result: a 2.x spring-kafka on a Boot 3.x classpath fails with a class-not-found at the first transactional listener bean.

The cross-major 3.x → 4.x is gentler — both Jakarta — but spring-kafka 4 depends on Spring Framework 7 APIs that don't exist in Spring Framework 6, and vice versa.

## Why this is subtle

- The error message is **`NoClassDefFoundError: javax/transaction/Transactional`** or **`java.lang.NoSuchMethodError: 'void org.springframework.kafka.listener.ContainerProperties.setObservationEnabled(...)'`** — neither says "your spring-kafka is the wrong major".
- Spring Boot's BOM **does** pin `spring-kafka` to the correct version automatically. The problem appears when teams override it in their own `dependencyManagement`, or use a pre-`<dependencyManagement>` `<dependency>` with an explicit version. Maven's nearest-wins then beats Boot's BOM-managed version.
- The override is sometimes added "to get a specific bug fix in spring-kafka". A patch-level bump is fine; a major bump isn't.

## Operational impact

- **Refuse-to-start failures** at Spring context refresh — usually in a Kafka autoconfig class, looking unrelated to a dependency mismatch.
- **Partial wiring** in the rare cases where the classes overlap enough — listener container starts but observability / error-handling features silently noop.
- **Test green, production red** — if your tests don't exercise the specific code path that depends on the missing class, the issue lands only in prod.

## How to fix

```xml
<!-- BAD: Spring Boot 3.x with spring-kafka 2.x explicitly pinned -->
<parent>
    <groupId>org.springframework.boot</groupId>
    <artifactId>spring-boot-starter-parent</artifactId>
    <version>3.5.14</version>
</parent>
<dependencies>
    <dependency>
        <groupId>org.springframework.kafka</groupId>
        <artifactId>spring-kafka</artifactId>
        <version>2.9.13</version>   <!-- explicit downgrade — breaks Boot 3 -->
    </dependency>
</dependencies>

<!-- GOOD: let the Boot BOM manage spring-kafka -->
<parent>
    <groupId>org.springframework.boot</groupId>
    <artifactId>spring-boot-starter-parent</artifactId>
    <version>3.5.14</version>
</parent>
<dependencies>
    <dependency>
        <groupId>org.springframework.kafka</groupId>
        <artifactId>spring-kafka</artifactId>
        <!-- no version: managed by Boot BOM -->
    </dependency>
</dependencies>
```

If you absolutely must override spring-kafka, stay within the major:

```xml
<properties>
    <spring-kafka.version>3.3.5</spring-kafka.version>  <!-- patch override, same major -->
</properties>
```

## When this might be a false positive

- A multi-module reactor where one module pins spring-kafka for tests against an embedded broker, but the runtime module pulls Boot's managed version. Unlikely to cause an issue but worth flagging.

## Detection strategy

Two signals must agree:

1. From `project.getDependencyManagement()` (or resolved parent), determine Spring Boot's `MAJOR.MINOR`.
2. From `project.getArtifacts()`, find the resolved `org.springframework.kafka:spring-kafka` version.
3. Compute the spring-kafka major.
4. Cross-check against the table:
   - Boot 2.x ↔ spring-kafka 2.x.
   - Boot 3.x ↔ spring-kafka 3.x.
   - Boot 4.x ↔ spring-kafka 4.x.
5. Mismatch = ERROR.

Also flag if `spring-kafka` is declared with an explicit version in the project's `<dependencies>` block (any pin is suspicious; only `<properties><spring-kafka.version>` for patch-level overrides is acceptable).

## References

- [Spring for Apache Kafka compatibility matrix](https://spring.io/projects/spring-kafka/)
- [Spring Framework 6 — Jakarta EE 9 baseline](https://github.com/spring-projects/spring-framework/wiki/Upgrading-to-Spring-Framework-6.x)
- [Spring Boot dependency management documentation](https://docs.spring.io/spring-boot/docs/current/reference/htmlsingle/#using.build-systems.maven.parent-pom)
