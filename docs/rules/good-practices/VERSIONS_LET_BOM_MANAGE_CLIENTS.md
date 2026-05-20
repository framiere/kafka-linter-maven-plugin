# VERSIONS_LET_BOM_MANAGE_CLIENTS

**Severity**: WARNING
**Confidence**: HIGH
**Detection**: pom-dependency
**Tagline**: Spring Boot, Quarkus, and Confluent all ship a BOM. Use it, don't fight it.

## TL;DR

The linter flags `pom.xml` files that pin `kafka-clients` /
`kafka-streams` / `spring-kafka` / `smallrye-reactive-messaging-kafka`
to an explicit version when a parent BOM (Spring Boot, Quarkus,
Confluent Platform) already manages them. The BOM picks the
combination that was tested together; overriding piecewise creates an
unsanctioned cocktail.

## The setup

The kafka-clients ecosystem has been BOM-managed for years:

- `spring-boot-dependencies` pins `kafka-clients`, `kafka-streams`,
  `spring-kafka`, and friends to a coherent set.
- `quarkus-bom` does the same for `kafka-clients`,
  `smallrye-reactive-messaging-kafka`, and the GraalVM substitutions.
- `kafka-schema-registry-parent` (Confluent) pins
  `kafka-avro-serializer`, `kafka-json-schema-serializer`, etc., to the
  matching `kafka-clients`.

The well-meaning trap is reading a CVE advisory and adding an explicit
`<dependency>` entry pinning a newer `kafka-clients`. Maven's
nearest-wins beats the BOM. `spring-kafka` was compiled and tested
against the BOM's version; the override creates an unsanctioned
combination — works most of the time, fails in some method-rename or
default-change you didn't expect.

Cross-link to the existing anti-patterns:

- [SPRING_BOOT_KAFKA_CLIENT_OVERRIDE](../versions/SPRING_BOOT_KAFKA_CLIENT_OVERRIDE.md)
- [QUARKUS_KAFKA_CLIENT_OVERRIDE](../versions/QUARKUS_KAFKA_CLIENT_OVERRIDE.md)
- [SMALLRYE_RM_OVERRIDE](../versions/SMALLRYE_RM_OVERRIDE.md)
- [SPRING_KAFKA_DUPLICATE_DECLARATION](../versions/SPRING_KAFKA_DUPLICATE_DECLARATION.md)

The good-practice form, then, is: **omit versions on managed
dependencies**. If you must bump because of a CVE, bump the BOM's
property (`<kafka.version>` in Spring Boot, `<kafka.version>` /
`<quarkus.platform.version>` in Quarkus) rather than adding an override.

## What's actually happening (the mechanism)

When you write:

```xml
<dependency>
    <groupId>org.apache.kafka</groupId>
    <artifactId>kafka-clients</artifactId>
</dependency>
```

without a `<version>` element, Maven resolves through
`<dependencyManagement>`. The Spring Boot parent (or `import` BOM) wins
unless a closer scope overrides. The version that ends up on the
classpath is the one Spring Boot's release team integration-tested.

When you write:

```xml
<dependency>
    <groupId>org.apache.kafka</groupId>
    <artifactId>kafka-clients</artifactId>
    <version>3.9.2</version>
</dependency>
```

you've taken the steering wheel. `spring-kafka` 3.x.x still expects the
3.x.y the BOM pinned. Maven happily resolves the override to 3.9.2 on
the classpath; the test suite runs against 3.9.2; runtime sees 3.9.2.
Usually fine. Sometimes the bump introduces a method signature change
that breaks Spring-Kafka's reflection-based access. By the time the
error surfaces, the override is six months old and "always worked".

The correct way to bump:

```xml
<properties>
    <kafka.version>3.9.2</kafka.version>
</properties>
```

Spring Boot's BOM reads `${kafka.version}` and propagates the bump to
all `kafka-*` artifacts — coherent constellation.

## How to fix (no → yes)

```xml
<!-- no — pin overrides the BOM, decoupled from spring-kafka -->
<dependency>
    <groupId>org.apache.kafka</groupId>
    <artifactId>kafka-clients</artifactId>
    <version>3.9.2</version>
</dependency>

<!-- yes — BOM-managed -->
<dependency>
    <groupId>org.apache.kafka</groupId>
    <artifactId>kafka-clients</artifactId>
</dependency>

<!-- yes — coordinated bump via property -->
<properties>
    <kafka.version>3.9.2</kafka.version>
</properties>
```

```xml
<!-- yes — Quarkus, BOM-managed, no override -->
<dependency>
    <groupId>io.quarkus</groupId>
    <artifactId>quarkus-messaging-kafka</artifactId>
</dependency>
<!-- DO NOT add kafka-clients/<version> separately under Quarkus -->
```

## When this might be a false positive

- Deliberate downgrade to dodge a regression. Document why with an
  XML comment and suppress this rule for that artifact.
- Multi-module reactor projects where one module deliberately overrides
  to test an in-flight bump.
- Library projects (not applications) that intentionally widen their
  supported version range. The override pattern doesn't apply.

## Detection strategy

- **pom-dependency:** detect a `<dependency>` block with
  `groupId=org.apache.kafka, artifactId=kafka-clients|kafka-streams`
  that has an explicit `<version>` AND the project's parent (transitive
  if necessary) is `spring-boot-starter-parent`, imports
  `spring-boot-dependencies`, imports `quarkus-bom`, or imports
  `kafka-schema-registry-parent`.
- Same for `org.springframework.kafka:spring-kafka`,
  `io.quarkus:quarkus-messaging-kafka`, `io.smallrye.reactive:smallrye-reactive-messaging-kafka`,
  `io.confluent:kafka-avro-serializer`,
  `io.apicurio:apicurio-registry-serdes-avro-serde`.
- **Confidence: HIGH** — provable from the resolved Maven model.

## References

See [KAFKA_CLIENTS_DRIFT_IN_MULTIMODULE](../versions/KAFKA_CLIENTS_DRIFT_IN_MULTIMODULE.md)
and the versions/_INDEX.md "BOM trap" section for the full chain of
resolution.

- Spring Boot dependency versions:
  https://docs.spring.io/spring-boot/appendix/dependency-versions/coordinates.html
- Quarkus BOM:
  https://quarkus.io/guides/platform
- Confluent kafka-schema-registry-parent:
  https://docs.confluent.io/platform/current/installation/versions-interoperability.html
