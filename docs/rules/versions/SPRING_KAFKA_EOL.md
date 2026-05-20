# SPRING_KAFKA_EOL

**Severity**: WARNING
**Confidence**: HIGH
**Detection**: pom-dependency
**Tagline**: spring-kafka's OSS life is tied to Spring Boot's — and Boot 2.x stopped getting patches three years ago.

## TL;DR

`spring-kafka` follows Spring Boot's OSS support window. The 2.x line ended OSS support together with Spring Boot 2.7 (June 2023). 3.0–3.4 are also OSS-EOL. As of May 2026 the OSS-supported lines are **3.3.x / 3.4.x for Boot 3.5** and **4.0.x for Boot 4.0**.

## The setup

A team is on `spring-kafka` 2.9.x (the final 2.x patch line) because they're on Spring Boot 2.7. Or on `spring-kafka` 3.0.x because they upgraded Boot once and then froze. The library still releases on the supported branches but **not** on the EOL ones.

## What's actually happening

Spring's OSS support is concentrated on the most recent two minor lines of each library. Practically:

| spring-kafka | Status (May 2026) | Tracks Spring Boot |
|--------------|-------------------|---------------------|
| 4.0.x | Supported | 4.0.x |
| 3.3.x | Supported (current 3.x patches) | 3.4 / 3.5 |
| 3.2.x | Bug fixes only via Boot commercial | 3.3 (OSS-EOL) |
| 3.1.x | OSS-EOL | 3.2 (OSS-EOL) |
| 3.0.x | OSS-EOL | 3.0 / 3.1 (OSS-EOL) |
| 2.9.x | OSS-EOL since Jun 2023 | 2.7 (OSS-EOL) |
| 2.x (other) | OSS-EOL | earlier Boot 2.x lines (all OSS-EOL) |

Stale-version specifics that bite:

- **Listener container API surface** — observation hooks, retry topic configuration, batch error handler signatures have all moved between 2.9 and 3.3. Stack Overflow answers from 2022 don't apply.
- **Retry topics** — the @RetryableTopic API and `DefaultErrorHandler` semantics changed significantly in 3.0; 2.x's `SeekToCurrentErrorHandler` was removed in 3.0.
- **Observation / Micrometer 1.10+** — `spring-kafka` 3.x integrates with `Observation`; 2.x only emits the old metric set. Distributed tracing setup differs.

## Why this is subtle

- "spring-kafka" appears not to need patches because the Kafka client beneath it is what changes most. Teams forget the framework above also accumulates fixes.
- The Boot-EOL → spring-kafka-EOL chain is obvious in retrospect but isn't stated on the spring-kafka project page in big letters.
- 2.x had a long tail of patches (2.9.13 etc) released after its OSS-EOL date because the Spring team was wrapping up. These exist on Maven Central and look "recent", which lulls teams into a false sense of currency.

## Operational impact

- **No backports** for security fixes in `spring-kafka` itself (rare, but they happen — observability injection bugs, deserialization issues in the retry-topic header parser).
- **Frozen API surface** — you cannot pick up newer Kafka features without crossing both `kafka-clients` and `spring-kafka` major boundaries simultaneously.
- **Documentation drift** — the official docs at `docs.spring.io/spring-kafka` redirect older URLs to the current line; troubleshooting old behavior gets harder over time.

## How to fix

```xml
<!-- BEST: bump Spring Boot, let it manage spring-kafka -->
<parent>
    <groupId>org.springframework.boot</groupId>
    <artifactId>spring-boot-starter-parent</artifactId>
    <version>3.5.14</version>
</parent>

<!-- If parented elsewhere, import the BOM:  -->
<dependencyManagement>
    <dependencies>
        <dependency>
            <groupId>org.springframework.boot</groupId>
            <artifactId>spring-boot-dependencies</artifactId>
            <version>3.5.14</version>
            <type>pom</type>
            <scope>import</scope>
        </dependency>
    </dependencies>
</dependencyManagement>
```

## When this might be a false positive

- Tanzu commercial support for the Boot 2.7 line extends through 2029. If you have that contract, the spring-kafka 2.9 line is patched commercially. Flag and let the user suppress.

## Detection strategy

- Resolve `org.springframework.kafka:spring-kafka` from `project.getArtifacts()`.
- Normalize to `MAJOR.MINOR`.
- Compare against the table:
  - 2.x → EOL ERROR.
  - 3.0 / 3.1 → EOL WARNING.
  - 3.2 → OSS-EOL but recent — WARNING (commercial may still cover).
  - 3.3+ → OK.

## References

- [Spring for Apache Kafka project page](https://spring.io/projects/spring-kafka/)
- [Spring Boot support page](https://spring.io/projects/spring-boot#support)
- [spring-kafka release blog](https://spring.io/blog/category/spring-kafka)
