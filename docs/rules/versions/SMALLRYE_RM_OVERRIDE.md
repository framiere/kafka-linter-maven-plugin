# SMALLRYE_RM_OVERRIDE

**Severity**: WARNING
**Confidence**: HIGH
**Detection**: pom-dependency
**Tagline**: Quarkus 3 ships SmallRye Reactive Messaging 4 — pinning your own version breaks the build steps.

## TL;DR

Quarkus 3.x ships SmallRye Reactive Messaging 4.x; Quarkus 2.x shipped 3.x. The Kafka connector class names, configuration prefixes, and CDI bean wiring changed between SmallRye RM 3 and 4. Pinning a different SmallRye RM version than the one Quarkus expects breaks Quarkus's build steps, which generate code against the connector's specific API.

## The setup

A team wanted a fix or feature in a newer SmallRye RM than their Quarkus version shipped. They added an explicit `io.smallrye.reactive:smallrye-reactive-messaging-kafka` dependency with a `<version>` override. Quarkus's build picks it up, but the Quarkus extension code (the build steps that generate CDI beans, register connectors, and emit native-image metadata) was compiled against a different SmallRye RM major.

## What's actually happening

| Quarkus | SmallRye Reactive Messaging | Notable changes |
|---------|----------------------------|-----------------|
| 2.x | 3.x | Old connector API; CDI 2 / `javax.*` |
| 3.0 – 3.6 | 4.0 – 4.6 | New connector API; CDI 4 / `jakarta.*`; Mutiny 2 |
| 3.7+ | 4.x latest | Continued evolution |

What changes across SmallRye RM 3 → 4:

- Connector base class moved from `io.smallrye.reactive.messaging.connectors.*` to `io.smallrye.reactive.messaging.kafka.*` and friends.
- Method signatures on `OutgoingKafkaRecordMetadata` evolved (header API).
- `MessageConverter` SPI changed.
- Mutiny 2 vs Mutiny 1 — `Multi.createFrom().publisher(...)` shape differs.

Quarkus extensions (build steps) reference specific SmallRye RM classes by name. Override SmallRye RM and the build step's generated CDI bean references invalid classes; the deploy artifact fails CDI validation at startup or fails to compile to native.

## Why this is subtle

- The override is one line in dependencyManagement. The breakage surfaces in Quarkus's build phase, which is not where developers expect SmallRye RM errors.
- Native image failures are particularly cryptic: GraalVM reports a class that doesn't exist in the resolved SmallRye RM, but the substitution that referenced it came from Quarkus's extension code.
- Quarkus extension dependencies don't all use the same version property — sometimes the Kafka connector is at one SmallRye version and the JMS one at another. Inconsistent overrides compound the problem.

## Operational impact

- **CDI bean validation failures** at startup, deep in Quarkus internals.
- **Native build failures** — Quarkus extension build steps unhappy.
- **Runtime API mismatches** — listener code compiled against SmallRye RM 4 won't run against 3 and vice versa.

## How to fix

Don't override SmallRye RM. Bump Quarkus itself:

```xml
<dependency>
    <groupId>io.quarkus.platform</groupId>
    <artifactId>quarkus-bom</artifactId>
    <version>3.33.0</version>
    <type>pom</type>
    <scope>import</scope>
</dependency>
```

If you absolutely need a specific SmallRye RM patch, use the property the Quarkus BOM exposes (varies by version):

```xml
<properties>
    <smallrye-reactive-messaging.version>4.27.0</smallrye-reactive-messaging.version>
</properties>
```

This propagates to all SmallRye RM artifacts together. Avoid `<dependency>` overrides on individual SmallRye coordinates.

## When this might be a false positive

- A non-Quarkus project using SmallRye RM directly (e.g. a vanilla CDI / Helidon app). Quarkus-specific extension constraints don't apply.

## Detection strategy

- Detect Quarkus BOM in `getDependencyManagement()`.
- If present, find the SmallRye RM version Quarkus pins.
- Walk `project.getDependencies()` (raw) for any `io.smallrye.reactive:smallrye-reactive-messaging-*` with an explicit `<version>`.
- If the explicit version differs from Quarkus's expectation: WARNING.
- If the major differs (Quarkus pins SmallRye RM 4 but project overrides to 3): ERROR.

## References

- [SmallRye Reactive Messaging docs](https://smallrye.io/smallrye-reactive-messaging/)
- [Quarkus reactive messaging Kafka guide](https://quarkus.io/guides/kafka-reactive-getting-started)
- [Quarkus dependency management](https://quarkus.io/guides/maven-tooling#bom)
