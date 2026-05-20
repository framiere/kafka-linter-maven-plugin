# KAFKA_CLIENTS_DRIFT_IN_MULTIMODULE

**Severity**: WARNING
**Confidence**: MEDIUM
**Detection**: pom-dependency
**Tagline**: Two modules in the same reactor on two different kafka-clients is half a fix away from a classloader fight.

## TL;DR

In a multi-module Maven reactor, different modules sometimes resolve `kafka-clients` to different versions because they have different Spring Boot / Quarkus / explicit-pin choices. When those modules are bundled into the same deployable (Spring Boot uber-jar, Quarkus uber-jar, fat-jar) the classloader sees one of them at runtime, chosen by jar order — non-deterministic across machines.

## The setup

A multi-module reactor:
- `core` — pure library, declares `kafka-clients:3.7.1`.
- `service-a` — Spring Boot 3.4 (pins kafka-clients 3.8.1).
- `service-b` — Spring Boot 3.5 (pins kafka-clients 3.9.2).

`service-a` depends on `core`. `service-b` depends on `core` and `service-a`. The final uber-jar contains three kafka-clients versions and only one is on the classloader at runtime.

## What's actually happening

Maven's dependency resolution is per-module. The reactor enforces consistent versions only via `<dependencyManagement>` at the parent level. Without that, each module independently resolves its dependencies and produces its own classpath.

When those modules are aggregated:

- **Spring Boot uber-jar (`spring-boot-maven-plugin` repackage)** — packs all dependencies as nested jars. The Spring Boot launcher loads them in jar order. If `kafka-clients-3.7.1.jar` and `kafka-clients-3.9.2.jar` both exist nested, the launcher uses the first one found.
- **Quarkus über-jar / native image** — Quarkus's build step de-duplicates and picks the highest version, but if your `dependencyManagement` doesn't agree across modules the build emits warnings and may pick a version that none of the modules tested.
- **maven-shade-plugin** — fails loudly on duplicate classes by default. Most teams configure it to "keep first" — same issue.

## Why this is subtle

- Each module's `mvn dependency:tree` shows a single, consistent version. The drift is only visible across modules.
- Local builds and CI typically pull jars in the same alphabetical order, so the "winning" version is stable. Production sometimes differs (different file system, different OS).
- The deploy artifact size is one signal — multiple kafka-clients jars inflate it — but most teams don't check.

## Operational impact

- **Non-deterministic runtime** — same code, different version chosen depending on packaging order.
- **API surface mismatch** — code in `core` compiled against `kafka-clients-3.7.1` may reference a method removed in 3.9.2. Loaded at runtime: `NoSuchMethodError`.
- **Streams / clients version skew** — if `kafka-streams` and `kafka-clients` end up at different versions due to drift, "state store not found" failures during rebalance.

## How to fix

Enforce a single version at the reactor parent:

```xml
<!-- parent pom -->
<dependencyManagement>
    <dependencies>
        <dependency>
            <groupId>org.apache.kafka</groupId>
            <artifactId>kafka-clients</artifactId>
            <version>3.9.2</version>
        </dependency>
        <dependency>
            <groupId>org.apache.kafka</groupId>
            <artifactId>kafka-streams</artifactId>
            <version>3.9.2</version>
        </dependency>
    </dependencies>
</dependencyManagement>
```

If you use Spring Boot's BOM, make sure all modules import it via the same parent, not via per-module `dependencyManagement` imports with potentially-different Boot versions.

For native Quarkus, also enforce via `maven-enforcer-plugin`:

```xml
<plugin>
    <artifactId>maven-enforcer-plugin</artifactId>
    <executions><execution><goals><goal>enforce</goal></goals>
        <configuration><rules>
            <dependencyConvergence/>
        </rules></configuration>
    </execution></executions>
</plugin>
```

## When this might be a false positive

- Modules that are never aggregated into one deployable (true library multi-module without an uber-jar). Drift between independent deployables is mostly fine.
- A deliberate ABI shim where one module bridges between Kafka 3.x and 4.x — rare; document.

## Detection strategy

- This rule operates on the reactor, not a single module. The plugin needs to collect each module's resolved `kafka-clients` (and `kafka-streams`, `connect-api`) version.
- If versions differ across modules: WARNING.
- Bonus: detect whether the project produces an uber-jar (`spring-boot-maven-plugin`, `quarkus-maven-plugin`, `maven-shade-plugin` with no exclusions). If yes, escalate severity.

## References

- [Maven dependency convergence rule](https://maven.apache.org/enforcer/enforcer-rules/dependencyConvergence.html)
- [Spring Boot uber-jar layout](https://docs.spring.io/spring-boot/docs/current/reference/htmlsingle/#appendix.executable-jar)
- [Maven shade plugin](https://maven.apache.org/plugins/maven-shade-plugin/)
