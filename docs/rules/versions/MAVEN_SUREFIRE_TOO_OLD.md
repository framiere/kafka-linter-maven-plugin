# MAVEN_SUREFIRE_TOO_OLD

**Severity**: WARNING
**Confidence**: HIGH
**Detection**: pom-property
**Tagline**: Surefire below 3.0 can't run JUnit 5 reliably — and Kafka projects depend on JUnit 5 heavily.

## TL;DR

`maven-surefire-plugin` versions below 3.0.0 do not officially support JUnit Platform (JUnit 5). They run JUnit 5 tests via a compatibility shim that misses parallel execution, parametrized test discovery, and `@TestInstance(Lifecycle.PER_CLASS)` edge cases. Kafka integration tests — which lean on JUnit 5, Testcontainers, and `embedded-kafka` — will exhibit flaky failures, dropped tests, or wrong reports.

## The setup

A pom without an explicit Surefire version inherits whatever the Maven super-pom or parent provides. Until Maven 3.9 that was a 2.x Surefire. The team adds JUnit 5 dependencies and writes tests; some run, some don't. The build "passes".

## What's actually happening

JUnit 5 (Jupiter) tests are discovered by a **TestEngine** SPI, not by the legacy JUnit 3/4 `runner` mechanism. Surefire 3.0+ ships JUnit Platform-native test discovery. Surefire 2.x discovers JUnit 5 tests via the `junit-vintage-engine` (which only handles JUnit 4-style tests) or a manual `junit-platform-surefire-provider` (deprecated, missing features).

Specific issues with Surefire < 3:
- `@Nested` test classes silently skipped.
- `@ParameterizedTest` with `@MethodSource` of a non-default method may not enumerate.
- `@TestFactory` (dynamic tests) sometimes missed.
- Parallel test execution (`junit.jupiter.execution.parallel.enabled=true`) ignored.
- `@TestInstance(Lifecycle.PER_CLASS)` works but `@BeforeAll`/`@AfterAll` hook ordering subtly differs.

For Kafka projects this matters because:
- Testcontainers tests rely on `@Testcontainers` and `@Container`, which work but are slow without parallel execution.
- Spring Kafka's `@EmbeddedKafka` uses JUnit 5 extensions; some lifecycle hooks fire in unexpected order on old Surefire.
- Kafka Streams' `TopologyTestDriver` tests often use `@TestInstance(Lifecycle.PER_CLASS)` to share a topology across tests.

## Why this is subtle

- Missing tests in Surefire don't fail the build. The report says "X tests, X passed". You don't notice the ones that were never run.
- The default Surefire version is plumbed through the Maven super-pom — silently outdated for a long time.
- The fix is one line in `<build><plugins>`, but nobody touches it until something obviously breaks.

## Operational impact

- **Tests that "pass" by not running** — entire classes silently dropped from the report.
- **Slower CI** — sequential test execution where parallel was intended.
- **Wrong assertions** — `@BeforeAll` running per-test instead of per-class can change state setup, masking real bugs.

## How to fix

```xml
<build>
    <plugins>
        <plugin>
            <groupId>org.apache.maven.plugins</groupId>
            <artifactId>maven-surefire-plugin</artifactId>
            <version>3.5.2</version>
        </plugin>
        <plugin>
            <groupId>org.apache.maven.plugins</groupId>
            <artifactId>maven-failsafe-plugin</artifactId>
            <version>3.5.2</version>
        </plugin>
    </plugins>
</build>
```

`maven-failsafe-plugin` should be at the same version — same family, same JUnit Platform integration story.

## When this might be a false positive

- A pure JUnit 4 project that hasn't migrated to JUnit 5. Acceptable, but flag it as informational — JUnit 4 has been in maintenance mode for years.
- A project that doesn't run tests at all (rare, but library aggregators sometimes do this).

## Detection strategy

- Walk `project.getBuild().getPlugins()` for `org.apache.maven.plugins:maven-surefire-plugin` and `maven-failsafe-plugin`.
- If the version is missing OR the version is `< 3.0.0`, flag.
- If JUnit Jupiter is on the classpath (`org.junit.jupiter:junit-jupiter` in any scope) and Surefire is `< 3.0`, escalate severity.
- If Spring Boot is the parent, Surefire is usually managed by the BOM — check the effective version, not just the declared one.

## References

- [Maven Surefire 3.0 release notes](https://maven.apache.org/surefire/maven-surefire-plugin/)
- [JUnit Platform Surefire provider migration](https://junit.org/junit5/docs/current/user-guide/#running-tests-build-maven)
- [Surefire JUnit 5 compatibility](https://maven.apache.org/surefire/maven-surefire-plugin/examples/junit-platform.html)
