# KAFKA_CLIENTS_SNAPSHOT

**Severity**: WARNING
**Confidence**: HIGH
**Detection**: pom-dependency
**Tagline**: A SNAPSHOT or milestone client in production is a contract you didn't sign.

## TL;DR

`kafka-clients`, `kafka-streams`, `spring-kafka`, or any messaging-related dependency at a `-SNAPSHOT`, `-RC*`, `-Mn` (milestone), or `-alpha`/`-beta` version is not a stable release. Apache, Spring, and Quarkus publish snapshots for development; running production on them is asking for behavior changes between two builds with the same version string.

## The setup

Someone needed an unreleased fix and used the snapshot from the Apache Snapshot repository. Or a CI job built a fork and named it `3.10.0-SNAPSHOT`. The team merged the change "until the official release", and the official release happened months ago, but the snapshot reference stayed.

## What's actually happening

Maven versions carry semantic suffixes:

- `-SNAPSHOT` — a moving target. Two `mvn install` invocations a day apart can resolve to different bytecode for the same coordinate.
- `-Mn` (milestone, e.g. `4.0.0-M2`) — a development preview. Public API is unstable; methods may be added or removed before final.
- `-RCn` (release candidate, e.g. `4.0.0-RC1`) — feature-complete candidate; bug fixes only before final. Still not final.
- `-alpha` / `-beta` — older convention, similar to milestone.

Production-grade releases have no qualifier: `3.9.2`, `4.0.5`, etc.

For Kafka specifically:

- Apache publishes `kafka-clients-{X}.{Y}.{Z}-SNAPSHOT` on `https://repository.apache.org/snapshots/`. These are nightly builds of trunk.
- Spring publishes `spring-kafka-{X}.{Y}.{Z}-M{n}` and `-RC{n}` for upcoming majors.
- Quarkus publishes `quarkus-bom-{X}.{Y}.{Z}.CR{n}` similarly.

## Why this is subtle

- Once a SNAPSHOT is in the local Maven cache, subsequent builds may not re-fetch. The team's CI may pin to a specific snapshot timestamp by accident, masking the moving-target behavior.
- Milestone and RC suffixes look "almost final" — devs assume the API is locked. It isn't.
- The "wait for the release" path requires someone to revisit the pom; nobody does.

## Operational impact

- **Behavior changes between rebuilds** — `-SNAPSHOT` is the worst case. The same source can produce different runtime behavior on Monday and Tuesday.
- **Surprise API removal** — `-M*` to `-RC*` to GA can drop methods. Code that compiled yesterday doesn't compile today.
- **Stale fixes never adopted** — once the real release ships, your pinned snapshot misses everything that landed after.
- **Snapshot repos can disappear** — Apache occasionally garbage-collects snapshot repos; an old reference may stop resolving entirely.

## How to fix

```xml
<!-- BAD -->
<dependency>
    <groupId>org.apache.kafka</groupId>
    <artifactId>kafka-clients</artifactId>
    <version>3.10.0-SNAPSHOT</version>
</dependency>

<!-- BAD too -->
<dependency>
    <groupId>org.springframework.kafka</groupId>
    <artifactId>spring-kafka</artifactId>
    <version>4.0.0-M2</version>
</dependency>

<!-- GOOD: latest GA -->
<dependency>
    <groupId>org.apache.kafka</groupId>
    <artifactId>kafka-clients</artifactId>
    <version>3.9.2</version>
</dependency>
```

## When this might be a false positive

- A CI job that builds against the upcoming Kafka release to detect API breaks early. Acceptable — but should be isolated to a CI-only profile, not the default build.
- A bug bash where the team is testing an unreleased fix. Should be reverted before merge.

## Detection strategy

- Walk `project.getArtifacts()` for any `org.apache.kafka`, `org.springframework.kafka`, `io.quarkus`, `io.smallrye.reactive`, `io.confluent`, `io.apicurio` artifact.
- For each, check the version qualifier:
  - Contains `-SNAPSHOT` → ERROR.
  - Contains `-M{digit}+` or `-RC{digit}+` or `-CR{digit}+` (Quarkus uses CR) → WARNING.
  - Contains `-alpha` / `-beta` (case-insensitive) → WARNING.
  - `.Final` (JBoss / Hibernate convention) is final, not a snapshot — don't flag.
  - `-redhat-NNNNN` is Red Hat productized — fine, don't flag.
- Severity escalates if `<repositories>` includes the Apache snapshot repo `https://repository.apache.org/snapshots/`.

## References

- [Maven snapshots overview](https://maven.apache.org/guides/getting-started/index.html#what-is-a-snapshot-version)
- [Apache Kafka snapshot repository](https://repository.apache.org/snapshots/)
- [Semantic Versioning 2.0 — pre-release identifiers](https://semver.org/#spec-item-9)
