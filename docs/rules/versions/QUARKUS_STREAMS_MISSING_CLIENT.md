# QUARKUS_STREAMS_MISSING_CLIENT

**Severity**: ERROR
**Confidence**: HIGH
**Detection**: pom-dependency
**Tagline**: quarkus-kafka-streams requires quarkus-kafka-client — and won't tell you nicely.

## TL;DR

`io.quarkus:quarkus-kafka-streams` provides the Streams runtime but **deliberately** does not transitively pull `io.quarkus:quarkus-kafka-client`. The two are used together in practice, and missing the client extension causes either a build failure during native compilation or an obscure `MissingResourceException` / `NoClassDefFoundError` at runtime.

## The setup

A developer reads a Quarkus Streams tutorial, copies the `quarkus-kafka-streams` dependency, and assumes the producer/consumer machinery is in there too. Or they're migrating from a plain `kafka-streams` Maven project where adding `kafka-streams` was enough (Streams pulls clients transitively in the vanilla AK case). In Quarkus, the extension partitioning is different.

## What's actually happening

Quarkus splits responsibilities between extensions to enable fine-grained native image optimization:

- `quarkus-kafka-client` — registers all the GraalVM substitutions, reflection configs, and JNI registrations for `org.apache.kafka:kafka-clients`. Provides admin client wiring.
- `quarkus-kafka-streams` — registers Streams-specific substitutions, the Health checks, and the Streams build steps. Does **not** transitively depend on `quarkus-kafka-client` to keep the extension self-contained for users who want to provide their own client configuration.
- `quarkus-messaging-kafka` — the reactive messaging connector. Depends on `quarkus-kafka-client` but does not bring `quarkus-kafka-streams`.

Result: any project using Streams needs both `quarkus-kafka-streams` and `quarkus-kafka-client`. Missing the client extension produces:

- JVM mode: works only because the raw `kafka-clients` jar is on the classpath (pulled transitively by `kafka-streams`); but admin client wiring, health checks, and Dev Services are broken.
- Native mode: missing reflection / substitution configs lead to native compile failures or runtime `NoClassDefFoundError` for kafka-clients internals.

## Why this is subtle

- The JVM-mode error message rarely identifies the missing extension. It surfaces as a generic CDI wiring complaint or a `kafka-clients` reflective access failure.
- Tutorials and Quarkus's own getting-started examples are not always explicit. Earlier docs assumed the developer would add both; recent guides bundle them in starter templates but the pom comments don't explain why.
- Once a developer adds the client extension and the project works, they never revisit. Multi-module reactors where Streams is in one module and clients are in another can have the same trap silently.

## Operational impact

- **Native build failures** during `quarkus build --native`, often with a stack inside SmallRye or kafka-streams referencing a missing class.
- **Dev Services missing** — `quarkus.kafka.devservices` only fires when `quarkus-kafka-client` is present.
- **Health-check incomplete** — the Streams readiness probe relies on `quarkus-kafka-client` wiring.

## How to fix

```xml
<!-- BAD: Streams only -->
<dependency>
    <groupId>io.quarkus</groupId>
    <artifactId>quarkus-kafka-streams</artifactId>
</dependency>

<!-- GOOD: both -->
<dependency>
    <groupId>io.quarkus</groupId>
    <artifactId>quarkus-kafka-client</artifactId>
</dependency>
<dependency>
    <groupId>io.quarkus</groupId>
    <artifactId>quarkus-kafka-streams</artifactId>
</dependency>
```

If you also use the reactive messaging connector, three:

```xml
<dependency>
    <groupId>io.quarkus</groupId>
    <artifactId>quarkus-kafka-client</artifactId>
</dependency>
<dependency>
    <groupId>io.quarkus</groupId>
    <artifactId>quarkus-kafka-streams</artifactId>
</dependency>
<dependency>
    <groupId>io.quarkus</groupId>
    <artifactId>quarkus-messaging-kafka</artifactId>
</dependency>
```

## When this might be a false positive

- A multi-module project where `quarkus-kafka-client` is brought in by a sibling module that depends on the Streams module. The linter should check the *effective* dependency set on the leaf module, not just declared dependencies — `project.getArtifacts()` is the right API.

## Detection strategy

- If `io.quarkus:quarkus-kafka-streams` is present in `project.getArtifacts()`:
  - Check that `io.quarkus:quarkus-kafka-client` is also present.
  - If not: ERROR.
- Cross-module reactors: respect the resolved artifact set (transitive resolution will find a sibling module's client extension).

## References

- [Quarkus Kafka Streams guide](https://quarkus.io/guides/kafka-streams)
- [Quarkus Kafka client guide](https://quarkus.io/guides/kafka)
- [Quarkus extensions catalog](https://quarkus.io/extensions/)
