# JAVA_VERSION_TOO_LOW

**Severity**: ERROR
**Confidence**: HIGH
**Detection**: pom-property
**Tagline**: Kafka 4 needs JDK 17. Spring 6 needs 17. Quarkus 3 needs 17. JDK 11 is not enough anymore.

## TL;DR

A `maven-compiler-plugin` `source`/`target` (or `maven.compiler.source` / `release` property) of 8 or 11 is incompatible with modern Kafka-ecosystem stacks. **Apache Kafka 4.x requires JDK 17. Spring Framework 6 / Boot 3 requires 17. Quarkus 3 requires 17.** Setting your target below 17 either won't compile, or will compile but break at runtime on Jakarta-namespace classes.

## The setup

A team's pom has shipped with `<maven.compiler.source>11</maven.compiler.source>` for years. They picked up Spring Boot 3 or Quarkus 3 and the build proceeds anyway (Maven downloads the jars; classes compile against API stubs). At runtime — or at native-image time — the JDK mismatch surfaces.

## What's actually happening

The JDK minimums in this ecosystem:

| Project | Minimum JDK | Recommended JDK |
|---------|-------------|-----------------|
| `kafka-clients` 3.x | JDK 8 (still supported through 3.9) | 11+ |
| `kafka-clients` 4.x | JDK 11 (server requires 17; client can run on 11 per KIP-1013) | 17+ |
| Spring Boot 2.x | JDK 8 | 11 |
| Spring Boot 3.x | **JDK 17** | 17+ |
| Spring Boot 4.x | **JDK 17** (with 21 recommended) | 21 |
| Quarkus 2.x | JDK 11 | 17 |
| Quarkus 3.x | **JDK 17** | 21 |
| Confluent Platform 7.5+ | **JDK 17** | 17+ |

Compile-time vs runtime: Maven's `release` / `source` / `target` controls the bytecode level. You can have:

- `<source>11</source>` but the project depends on Spring Framework 6 (requires 17) → bytecode emitted is `--release 11`, which uses `javax.*` symbols. But Spring 6 only ships `jakarta.*` — your code references classes that don't exist on the classpath at runtime. NoClassDefFoundError.
- `<source>17</source>` but the JDK running Maven is 11 → the build fails immediately with "release version 17 not supported".

## Why this is subtle

- `maven-compiler-plugin` defaults are version-dependent and have historically been pinned at 1.6 / 1.7. Many old templates carry forward.
- Spring Boot's starter parent sets `java.version` via a property; teams override it without checking what's reasonable.
- CI may run on a different JDK than developers' machines, hiding the issue locally.
- Native Quarkus builds require GraalVM at a JDK level matching the project's `release` — mismatches surface only on native CI.

## Operational impact

- **Compile failure** when a dependency uses Java 17+ language features in its public API (rare but happens with sealed classes).
- **Runtime failure** when a dependency's bytecode is compiled at a higher class-file version than the JVM running the app supports.
- **Jakarta namespace failures** as described above.
- **Module-system surprises** in Java 9+ for projects that opted into JPMS.

## How to fix

```xml
<!-- BAD -->
<properties>
    <maven.compiler.source>11</maven.compiler.source>
    <maven.compiler.target>11</maven.compiler.target>
</properties>

<!-- GOOD -->
<properties>
    <maven.compiler.release>17</maven.compiler.release>
</properties>
```

Use `release` instead of `source` + `target` whenever possible (it also pins the API surface, not just the language level — catches accidental use of newer APIs).

For Spring Boot 4.x or Quarkus latest LTS, prefer 21:

```xml
<properties>
    <maven.compiler.release>21</maven.compiler.release>
</properties>
```

## When this might be a false positive

- A library targeting older consumers — you intentionally keep `release=11` even though you use Kafka. Acceptable if Kafka is exposed via interfaces that don't require Jakarta types.
- A Spring Boot 2.x project on JDK 11 — Boot 2.x supports 11 but the rule should still flag that Boot 2.x itself is EOL (see `SPRING_BOOT_EOL`).

## Detection strategy

- Read `project.getProperties()` for `maven.compiler.source`, `maven.compiler.target`, `maven.compiler.release`, `java.version`.
- Read the `maven-compiler-plugin` configuration (`project.getBuild().getPlugins()`).
- Cross-reference against the resolved Spring Boot / Quarkus / kafka-clients version:
  - Boot 3 or Quarkus 3 with source/target/release `< 17` → ERROR.
  - Boot 4 with release `< 17` → ERROR.
  - kafka-clients 4.x with release `< 11` → ERROR.
- If no compiler version is declared, infer from `${java.version}` or warn that the default is too old.

## References

- [Spring Framework 6 system requirements](https://docs.spring.io/spring-framework/reference/overview.html#overview-getting-started)
- [Quarkus prerequisites](https://quarkus.io/get-started/)
- [Kafka 4.0 release notes — JDK requirements](https://kafka.apache.org/blog)
- [KIP-1013 — Drop broker and tools support for Java 11](https://cwiki.apache.org/confluence/display/KAFKA/KIP-1013)
