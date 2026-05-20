# APICURIO_SERDE_V1

**Severity**: WARNING
**Confidence**: HIGH
**Detection**: pom-dependency
**Tagline**: Apicurio Registry Serdes 1.x is on a different package — and its lights went off in 2022.

## TL;DR

`io.apicurio:apicurio-registry-utils-serde` is the Apicurio Registry 1.x serdes line. It was superseded by `io.apicurio:apicurio-registry-serdes-avro-serde` / `apicurio-registry-serdes-protobuf-serde` / `apicurio-registry-serdes-json-schema-serde` in 2.x. The 1.x line is EOL and uses a different Registry REST API (v1) that newer Registry servers may not expose.

## The setup

A team integrated Apicurio Registry serdes when 1.x was the only release. The Registry server has since been upgraded to 2.x or 3.x (compatible REST API), but the client serdes were not bumped. The dependency reads:

```xml
<dependency>
    <groupId>io.apicurio</groupId>
    <artifactId>apicurio-registry-utils-serde</artifactId>
    <version>1.3.2.Final</version>
</dependency>
```

## What's actually happening

Apicurio Registry went through a major API redesign for 2.0:

| Apicurio Registry | Serdes coordinate | API |
|-------------------|-------------------|-----|
| 1.x | `io.apicurio:apicurio-registry-utils-serde` | REST v1 |
| 2.x | `io.apicurio:apicurio-registry-serdes-{avro,protobuf,json}-serde` | REST v2 |
| 3.x | same `serdes-*` artifacts at 3.x | REST v3 (current) |

The 2.x serdes have:
- Distinct artifacts per format (Avro, Protobuf, JSON Schema) instead of one monolithic util artifact.
- Different configuration keys (`apicurio.registry.url`, `apicurio.auth.basic.userId`, etc).
- Support for native compilation (Quarkus integration is meaningful only on 2.x+).
- Confluent Schema Registry compatibility mode for migration paths.

The 1.x line is EOL — Apicurio's 1.x branch on GitHub stopped receiving releases in 2022. The Red Hat Service Registry (the supported commercial variant) is built from 2.x+.

## Why this is subtle

- The 1.x dependency still resolves and the serdes still work against a 2.x/3.x Registry server (the v1 REST endpoints remain for compatibility).
- The migration is more than a version bump — config keys, artifact splits, and class packages all changed. Teams put it off because "it'll work for now".
- 1.x lacks observability hooks that 2.x added; you can be running blind on serdes failures and not know what you're missing.

## Operational impact

- **No CVE backports** for the 1.x line.
- **No Quarkus native image support** in 1.x serdes.
- **Stuck on v1 REST endpoints** — eventual removal from the Registry server is on the roadmap.
- **No reference-resolution improvements** — Protobuf with external references works correctly only on 2.x+.

## How to fix

```xml
<!-- BAD: 1.x -->
<dependency>
    <groupId>io.apicurio</groupId>
    <artifactId>apicurio-registry-utils-serde</artifactId>
    <version>1.3.2.Final</version>
</dependency>

<!-- GOOD: 2.x+ per-format artifacts -->
<dependency>
    <groupId>io.apicurio</groupId>
    <artifactId>apicurio-registry-serdes-avro-serde</artifactId>
    <version>2.6.1.Final</version>
</dependency>
```

The migration also requires updating Properties keys:

```properties
# 1.x
apicurio.registry.url=...

# 2.x
apicurio.registry.url=...
# (key name happens to be the same, but many sibling keys renamed)
```

Consult the migration guide for the full key map.

## When this might be a false positive

- A frozen legacy app talking to a frozen legacy Registry — no upgrade path planned. Document and suppress.

## Detection strategy

- Walk `project.getArtifacts()` for `io.apicurio:apicurio-registry-utils-serde` (or any 1.x-era coordinate).
- Major version `< 2`: WARNING.
- Also flag `io.apicurio:apicurio-registry-rest-client` 1.x.

## References

- [Apicurio Registry 2.x migration guide](https://www.apicur.io/registry/docs/apicurio-registry/2.5.x/getting-started/assembly-migrate-from-version-1.html)
- [Apicurio Registry releases](https://github.com/Apicurio/apicurio-registry/releases)
- [Red Hat Service Registry support](https://access.redhat.com/products/red-hat-service-registry)
