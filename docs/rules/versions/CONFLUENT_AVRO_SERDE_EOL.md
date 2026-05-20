# CONFLUENT_AVRO_SERDE_EOL

**Severity**: WARNING
**Confidence**: HIGH
**Detection**: pom-dependency
**Tagline**: kafka-avro-serializer 5.x belongs to Confluent Platform 5 — twelve releases out of date.

## TL;DR

`io.confluent:kafka-avro-serializer` (and its sibling `kafka-protobuf-serializer`, `kafka-json-schema-serializer`) is versioned to Confluent Platform. CP 5.x is end-of-life since 2022. CP 7.x is current. The linter flags any `io.confluent:kafka-*-serializer` resolved at a 5.x or 6.x version.

## The setup

A team picked Confluent's Avro serializer when their cluster was on Confluent Platform 5 and never bumped. The dependency is `io.confluent:kafka-avro-serializer:5.5.5` (or thereabouts). They may have upgraded their `kafka-clients` since, but the serializer line stays put because "the topic format hasn't changed".

## What's actually happening

Confluent Platform versions and Apache Kafka versions are not the same line, but they're correlated:

| Confluent Platform | Apache Kafka | Schema Registry / serdes line |
|--------------------|--------------|------------------------------|
| 5.x | 2.x | 5.x serdes |
| 6.x | 2.6–2.8 | 6.x serdes |
| 7.0 | 3.0 | 7.0 serdes |
| 7.5 | 3.5 | 7.5 serdes |
| 7.7 | 3.7 | 7.7 serdes |
| 7.9 | 3.9 | 7.9 serdes |
| 8.0 | 4.0 | 8.0 serdes |

CP support is two years standard + one year platinum. CP 5.x reached EOL in 2022, CP 6.x in 2023. Standard support for CP 7.4 ended May 2025.

The serdes artifacts evolve over those releases:

- Schema Registry protocol additions (mutual TLS handshakes, new id-encoding modes for headers vs payload).
- Avro library bump — recent serdes depend on `org.apache.avro:avro` 1.11+, which has its own CVE history.
- Protobuf library bump — older serdes pull `com.google.protobuf:protobuf-java` 3.x; CVE-2024-7254 in protobuf-java was fixed by upgrading to 3.25.5 / 4.27.5.

## Why this is subtle

- The serdes work against any modern Schema Registry — the API hasn't broken. So "it works" is the team's signal that the old version is fine.
- The dependency on `avro` and `protobuf-java` is transitive; nobody bumps the serdes specifically to get a transitive CVE fix.
- The version number gap is big enough to look suspicious in retrospect, but in a 200-line pom it blends in.

## Operational impact

- **Transitive CVEs** in `avro` and `protobuf-java` (regularly disclosed for both).
- **Missing serdes features**: header-based id encoding, latest reference-resolution behavior for protobuf, JSON schema draft-2020-12 support.
- **Schema Registry client features** — auth header support for cloud providers, exponential backoff on transient errors, all evolved in CP 7.x.

## How to fix

```xml
<!-- BAD: CP 5 era -->
<dependency>
    <groupId>io.confluent</groupId>
    <artifactId>kafka-avro-serializer</artifactId>
    <version>5.5.5</version>
</dependency>

<!-- GOOD: CP 7.9 -->
<dependency>
    <groupId>io.confluent</groupId>
    <artifactId>kafka-avro-serializer</artifactId>
    <version>7.9.0</version>
</dependency>
```

Add the Confluent repository if you haven't already:

```xml
<repositories>
    <repository>
        <id>confluent</id>
        <url>https://packages.confluent.io/maven/</url>
    </repository>
</repositories>
```

## When this might be a false positive

- A Schema Registry running on CP 5.x where the wire format expectations match. Rare in 2026; even Confluent Cloud is on CP 7.x+.
- A test fixture pinning an old version intentionally.

## Detection strategy

- Walk `project.getArtifacts()` for `groupId=io.confluent` and any of `kafka-avro-serializer`, `kafka-protobuf-serializer`, `kafka-json-schema-serializer`, `kafka-schema-registry-client`.
- Major version `< 7`: WARNING.
- Major version `< 6`: ERROR.

## References

- [Confluent Platform supported versions](https://docs.confluent.io/platform/current/installation/versions-interoperability.html)
- [Confluent Platform release notes](https://docs.confluent.io/platform/current/release-notes/index.html)
- [Confluent Maven repository](https://packages.confluent.io/maven/)
