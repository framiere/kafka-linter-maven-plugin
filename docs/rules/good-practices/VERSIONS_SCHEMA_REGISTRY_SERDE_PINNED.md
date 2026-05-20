# VERSIONS_SCHEMA_REGISTRY_SERDE_PINNED

**Severity**: WARNING
**Confidence**: MEDIUM
**Detection**: pom-dependency
**Tagline**: Schema Registry serdes must match the broker family, not your random Maven cache.

## TL;DR

The linter flags projects that depend on
`io.confluent:kafka-avro-serializer` (or json-schema /
protobuf variants), or
`io.apicurio:apicurio-registry-serdes-avro-serde`, without pinning to a
version compatible with the broker / Confluent Platform / Apicurio
release in production.

## The setup

Schema Registry client serdes are released in lockstep with the
Schema Registry server:

- Confluent: `io.confluent:kafka-avro-serializer:7.x.y` matches
  Confluent Platform `7.x.y` and Confluent Schema Registry `7.x.y`.
  Cross-version compatibility is best-effort within a major.
- Apicurio: serdes 1.x ≠ serdes 2.x — different group IDs, different
  package paths. The 2.x rename happened in 2021. Cross-link
  [APICURIO_SERDE_V1](../versions/APICURIO_SERDE_V1.md).

Two failure modes:

1. The Confluent Platform parent BOM isn't imported, so the serde
   version drifts from the kafka-clients version. Manifests as
   "`java.lang.NoSuchMethodError`" at first send.
2. The serde version is pinned to whatever was tested against an old
   Schema Registry; production has been upgraded; the new wire format
   field (e.g. CEL-based extensions, custom MIME types) is misread or
   ignored.

## What's actually happening (the mechanism)

Confluent serdes (`KafkaAvroSerializer`, etc.) do three things:

1. Talk to the Schema Registry over HTTP to register/fetch schemas by
   subject.
2. Serialize / deserialize the record body, including the magic-byte
   + 4-byte schema-id prefix.
3. Cache schemas locally.

The wire format evolves slowly but not zero. A version skew between
the Confluent Platform and the serde client most commonly bites on:

- New compatibility modes (`FULL_TRANSITIVE`, etc.) added on the
  registry side; the serde client must understand the response.
- Authentication changes (basic / OAuth / SR-internal). New auth
  modes need a new serde.
- Schema reference resolution (Confluent 6.0+) — old serdes don't
  understand referenced schemas.

The good practice: pin via the Confluent Platform parent BOM, which is
the equivalent of Spring Boot's `spring-boot-dependencies` for the
Confluent constellation. Or import the `kafka-schema-registry-parent`.

## How to fix (no → yes)

```xml
<!-- no — unmanaged version, drifts -->
<dependency>
    <groupId>io.confluent</groupId>
    <artifactId>kafka-avro-serializer</artifactId>
    <version>7.0.1</version>
</dependency>

<!-- yes — BOM-managed -->
<dependencyManagement>
    <dependencies>
        <dependency>
            <groupId>io.confluent</groupId>
            <artifactId>kafka-schema-registry-parent</artifactId>
            <version>7.7.0</version>
            <type>pom</type>
            <scope>import</scope>
        </dependency>
    </dependencies>
</dependencyManagement>

<dependencies>
    <dependency>
        <groupId>io.confluent</groupId>
        <artifactId>kafka-avro-serializer</artifactId>
        <!-- no version, BOM picks 7.7.0 -->
    </dependency>
</dependencies>
```

```xml
<!-- yes — Apicurio 2.x current naming -->
<dependency>
    <groupId>io.apicurio</groupId>
    <artifactId>apicurio-registry-serdes-avro-serde</artifactId>
    <version>2.5.10.Final</version>
</dependency>
```

## When this might be a false positive

- Library projects deliberately keeping a wide compat range.
- Confluent Platform 7.x cluster with a pinned 6.x serde because of a
  known wire compat that works for the project's payload shape.
  Document and suppress.

## Detection strategy

- **pom-dependency:** flag if any of
  - `io.confluent:kafka-avro-serializer`, `kafka-json-schema-serializer`,
    `kafka-protobuf-serializer`, `kafka-schema-registry-client`
  - `io.apicurio:apicurio-registry-serdes-*-serde`
  is declared with a literal `<version>` AND no Confluent /
  Apicurio BOM import is present.
- Cross-link [CONFLUENT_AVRO_SERDE_EOL](../versions/CONFLUENT_AVRO_SERDE_EOL.md)
  for the 5.x EOL case.
- Cross-link [APICURIO_SERDE_V1](../versions/APICURIO_SERDE_V1.md) for
  the 1.x rename case.
- **Confidence: MEDIUM** — many projects pin deliberately.

## References

- Confluent Platform supported versions:
  https://docs.confluent.io/platform/current/installation/versions-interoperability.html
- Apicurio Registry — migrating from 1.x to 2.x:
  https://www.apicur.io/registry/docs/apicurio-registry/2.5.x/getting-started/assembly-migrating-from-1.html
