# SR_USE_LATEST_VERSION_MISSING
**Severity**: WARNING
**Confidence**: MEDIUM
**Detection**: config-file
**Tagline**: `auto.register.schemas=false` without `use.latest.version=true` — your producer is stuck on schema ID v1 forever.
**Source**: Confluent agent-skills — kafka-schema-registry/references/categorization.md § Category C (Auto-register) — Producers First, kafka-schema-registry/references/detection-patterns.md § use.latest.version.

## TL;DR

The linter flags producer configurations that set `auto.register.schemas=false` (the correct hardening choice) but omit `use.latest.version=true`. The combination matters: without auto-register, the producer can no longer push a new schema to SR; without `use.latest.version`, it also won't look one up. It pins to whatever schema ID was first cached and silently fails any record whose POJO has fields the cached schema doesn't have. This is a footgun specific to the Category-C → A migration described in Confluent's categorization workflow.

## What's happening (the mechanism)

The Confluent producer's schema-resolution flow has three modes:

1. **`auto.register.schemas=true` (default, dangerous in prod)**: producer compares the in-code schema against the latest in SR; if different, registers a new version. Drift, schema sprawl, no review.
2. **`auto.register.schemas=false` + `use.latest.version=false` (default for #2)**: producer reads the schema from its compiled class / annotated POJO, then asks SR "give me the ID for *this* schema". If the schema isn't registered, the produce fails. If it is, the ID is cached. **The schema never updates**, even if a newer compatible version exists in SR.
3. **`auto.register.schemas=false` + `use.latest.version=true`**: producer ignores the in-code schema for ID lookup, fetches the latest version from SR for the subject, and writes records with that ID. This is the production-ready mode.

The Confluent categorization guide spells this out: when migrating a Category C app (auto-register turned on) into compliance, both knobs must change together. Setting only `auto.register.schemas=false` leaves the producer brittle.

## Operational impact

- Producer's POJO compiled with version N of the schema; SR has version N+1 (added a field, registered via Terraform).
- Without `use.latest.version=true`, the producer keeps writing records with the v1 schema ID. Consumers using the latest schema decode fine (Avro projects missing fields to null). No alarm.
- Reverse drift: a field is added to the POJO but never registered. Without auto-register, the producer fails on first record. Loud failure — fine.
- The dangerous case is partial migration: Category C → "we disabled auto-register, we're good" → silent stale schemas.

## How to fix (bad → good code)

```properties
# BAD — auto-register off, use.latest.version not set
schema.registry.url=http://sr:8081
auto.register.schemas=false
# (no use.latest.version)

# GOOD
schema.registry.url=http://sr:8081
auto.register.schemas=false
use.latest.version=true
```

Producer-Java equivalent:

```java
props.put("auto.register.schemas", false);
props.put("use.latest.version", true);
```

Operational note (from Confluent's categorization guide): the migration order for Category C is producers first — disable auto-register, register the schema via Terraform, set `use.latest.version=true`, then verify consumers.

## When this might be a false positive

- The application explicitly wants to pin to a specific schema version via `latest.compatibility.strict=true` and `use.schema.id=<int>`. Rare, and usually accompanied by very deliberate documentation.
- The producer manages schema IDs entirely outside Confluent's SerDe — e.g., raw `bytes` producer that pre-pends its own ID. Not really an SR consumer at that point.

## Detection strategy

- Config-file only (the values are usually in `application.properties` / `application.yml` / `kafka.properties`):
  - Find a config that sets `auto.register.schemas=false`.
  - Check the same config (or properties bundle reachable to the producer) for `use.latest.version=true`. If absent or false, flag.
- Java code equivalent: a `props.put("auto.register.schemas", false)` without a matching `props.put("use.latest.version", true)` in any code path that reaches the same `Properties`.
- Confidence: MEDIUM — the rule is correct for the standard Confluent serdes; framework-managed configs (Spring Cloud Stream, Quarkus) can override these in non-obvious ways.

## References

- Confluent agent-skills — kafka-schema-registry/references/categorization.md § Category C (Auto-register) — Producers First
- Confluent agent-skills — kafka-schema-registry/references/detection-patterns.md § use.latest.version
- Confluent docs — Avro serializer configs: https://docs.confluent.io/platform/current/schema-registry/connect.html#configuration-options
