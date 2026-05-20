# QUARKUS_KAFKA_CLIENT_OVERRIDE

**Severity**: WARNING
**Confidence**: HIGH
**Detection**: combination
**Tagline**: The Quarkus BOM pins kafka-clients; overriding it can derail native compilation.

## TL;DR

The Quarkus BOM pins `org.apache.kafka:kafka-clients` to the exact version SmallRye Reactive Messaging Kafka was tested against. Pinning your own version overrides the BOM and risks two things specific to Quarkus: (1) GraalVM substitution mismatches (Quarkus ships substitutions for specific kafka-clients internals), (2) Dev Services and Quarkus extension wiring expecting the BOM-pinned version.

## The setup

A team adds an explicit `<dependency>` for `kafka-clients` because of a CVE notice or a feature they want. The pom now overrides the Quarkus BOM. The build succeeds; tests pass on the JVM. Native compilation fails at `quarkus build --native` with `ClassNotFoundException` deep inside a SmallRye Kafka connector class, or it produces a native image that crashes at startup on `NoSuchMethodError` calling into a kafka-clients class.

## What's actually happening

Quarkus extensions ship `org.graalvm.nativeimage.hosted.Feature` registrations and Substrate substitutions (via Quarkus's `@Substitute` and `@TargetClass`) for the **exact** internal classes of the kafka-clients version the extension was tested against. When you swap the kafka-clients version under the BOM:

- A substitution's `@TargetClass(MetadataCache.class)` may now refer to a method that was renamed.
- A reflection-config entry may name a class that was deleted or moved package.
- The Quarkus build step that scans for kafka-clients metric MBeans assumes a specific class layout.

These problems are **silent on JVM** because GraalVM-only substitutions are no-ops on a normal HotSpot run. Only native builds expose them.

## Why this is subtle

- JVM tests pass, so the override looks safe.
- Native build is often only run in CI on a scheduled job, not on every push — the failure lags the change.
- The native error message is rarely "you overrode kafka-clients"; it's a low-level GraalVM diagnostic that points at SmallRye internals.

## Operational impact

- **Native build failures** that only surface late in the release cycle.
- **Native runtime crashes** in the small set of cases where the substitution silently mismatches but compilation succeeds.
- **Dev Services** breakage: Quarkus's auto-spun-up Kafka container expects a specific client behavior. Mismatched clients can cause `Dev Services` to hang at startup.

## How to fix

The clean fix is to bump Quarkus itself, which moves the BOM and everything underneath together:

```xml
<dependency>
    <groupId>io.quarkus.platform</groupId>
    <artifactId>quarkus-bom</artifactId>
    <version>3.33.0</version>
    <type>pom</type>
    <scope>import</scope>
</dependency>
```

If you must override (e.g. a CVE patch comes faster than the Quarkus release):

```xml
<!-- BAD: explicit dependency, breaks substitutions -->
<dependency>
    <groupId>org.apache.kafka</groupId>
    <artifactId>kafka-clients</artifactId>
    <version>3.9.2</version>
</dependency>

<!-- LESS BAD: managed-dep override, still risky for native -->
<dependencyManagement>
    <dependencies>
        <dependency>
            <groupId>org.apache.kafka</groupId>
            <artifactId>kafka-clients</artifactId>
            <version>3.9.2</version>
        </dependency>
    </dependencies>
</dependencyManagement>
```

If you go this route, **run the native build in CI** and treat any failure as a hard blocker.

## When this might be a false positive

- A non-native Quarkus build that you never plan to GraalVM-compile. The CVE pressure may genuinely outweigh the BOM discipline. Document the trade-off and confirm `quarkus build --native` isn't on the roadmap.
- A small patch-level override (`3.9.1 → 3.9.2`) where the substitutions are unlikely to drift.

## Detection strategy

- Detect Quarkus BOM via `project.getDependencyManagement()`.
- Compute the BOM's pinned `kafka-clients` version.
- Walk `project.getDependencies()` and `project.getDependencyManagement().getDependencies()` for `org.apache.kafka:kafka-clients` with an explicit version.
- If found AND the version differs from the BOM's pin: WARNING.
- Bonus signal: if `quarkus-container-image-*` or any native-build extension is present, escalate severity.

## References

- [Quarkus extension dependency management](https://quarkus.io/guides/maven-tooling#bom)
- [Quarkus native image guide](https://quarkus.io/guides/building-native-image)
- [GraalVM substitution docs](https://www.graalvm.org/latest/reference-manual/native-image/dynamic-features/Substitutions/)
