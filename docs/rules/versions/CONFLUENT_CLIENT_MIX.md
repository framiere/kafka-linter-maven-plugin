# CONFLUENT_CLIENT_MIX

**Severity**: WARNING
**Confidence**: MEDIUM
**Detection**: pom-dependency
**Tagline**: Mixing Confluent Platform clients with Apache Kafka clients is asking two vendors to share one classpath.

## TL;DR

Confluent Platform publishes its own `kafka-clients` build (e.g. `org.apache.kafka:kafka-clients:3.9.0-ccs`) shipped with CP and tested against Confluent's serdes / Schema Registry / Connect distribution. If your pom mixes the Confluent-flavoured `kafka-clients` with the vanilla `org.apache.kafka:kafka-clients` (or pins different patch levels of each), Maven picks one via nearest-wins and the resolution is opaque.

## The setup

A team uses Confluent's serdes (`io.confluent:kafka-avro-serializer`) and also Spring Boot's Kafka starter. Spring Boot's BOM pins vanilla `kafka-clients`. Confluent's serdes BOM may pin a `-ccs`-suffixed Confluent build of the same artifactId. Both end up in the resolution graph; Maven picks one.

## What's actually happening

Confluent ships its own `kafka-clients` distribution for each Confluent Platform release. The artifact has the same coordinates as the Apache release but with the `-ccs` qualifier:

- `org.apache.kafka:kafka-clients:3.9.0` — Apache distribution.
- `org.apache.kafka:kafka-clients:3.9.0-ccs` — Confluent Community Software distribution.

The `-ccs` build is *almost* identical to the Apache build of the same version but may contain Confluent-specific backports and slightly different test selections. The serdes and Connect components were tested against `-ccs`.

When both a vanilla and a `-ccs` version land in `dependencyManagement`, Maven's nearest-wins picks the closer of the two. Outcomes:

1. **Vanilla wins, serdes works** — fine in practice, slightly off from what Confluent tested.
2. **`-ccs` wins, Spring Boot didn't test against it** — fine in practice, Spring's tests didn't cover this combination.
3. **Patch-level mismatch** — `3.9.0-ccs` vs `3.9.1` — different patches; one of them may have a backport the other doesn't.

The mismatch is usually fine, but it's an unstated combination that you've opted into without knowing.

## Why this is subtle

- The two artifacts have the **same** groupId and artifactId. Only the version string differs.
- `mvn dependency:tree` shows the resolved coordinate but not the conflict.
- The `-ccs` suffix is a Maven version qualifier; some semantic-versioning parsers treat it as a pre-release tag, others don't. Spring Boot's `kafka.version` property override may yield surprising results.

## Operational impact

- **Unsupported combinations** — neither Spring nor Confluent tests every cross-flavor pairing. You're the test subject.
- **Patch divergence** — security backports may land in one flavour first.
- **Confusing version output** — `KafkaProducer.LOG_VERSION` reports the picked version, not the conflict. You may genuinely not know which one you're running.

## How to fix

Pick one of the two strategies and stick to it:

**Strategy A — All-Apache**:

```xml
<dependency>
    <groupId>org.apache.kafka</groupId>
    <artifactId>kafka-clients</artifactId>
    <version>3.9.2</version>
</dependency>
<!-- Confluent serdes work fine against vanilla clients -->
<dependency>
    <groupId>io.confluent</groupId>
    <artifactId>kafka-avro-serializer</artifactId>
    <version>7.9.0</version>
    <exclusions>
        <exclusion>
            <groupId>org.apache.kafka</groupId>
            <artifactId>kafka-clients</artifactId>
        </exclusion>
    </exclusions>
</dependency>
```

**Strategy B — All-Confluent**:

```xml
<dependencyManagement>
    <dependencies>
        <dependency>
            <groupId>io.confluent</groupId>
            <artifactId>kafka-clients</artifactId>
            <version>7.9.0-ccs</version>
        </dependency>
    </dependencies>
</dependencyManagement>
```

(Note: there is no `io.confluent:kafka-clients` — the Confluent build still uses `org.apache.kafka` groupId. The `-ccs` qualifier is what makes it Confluent.)

## When this might be a false positive

- Team is intentionally on the `-ccs` line and Spring Boot's BOM is overridden to match — that's a documented pattern. Suppress with a comment.
- Pure consumer of Confluent's serdes without Spring Boot or Quarkus — no conflict to detect.

## Detection strategy

- Walk `project.getArtifacts()` for `kafka-clients`.
- If the version string contains `-ccs`, note it.
- Walk all `kafka-*` artifacts. If some have `-ccs` and some don't, WARNING.
- Compare against the resolved version's siblings: `kafka-streams`, `kafka-clients`, `connect-api` — they should all be the same flavour.

## References

- [Confluent kafka-clients overview](https://docs.confluent.io/kafka-client/overview.html)
- [Confluent Platform supported versions](https://docs.confluent.io/platform/current/installation/versions-interoperability.html)
- [Maven version range syntax](https://maven.apache.org/enforcer/enforcer-rules/versionRanges.html)
