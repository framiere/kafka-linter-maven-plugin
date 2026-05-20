# Schema Registry & Schematized Serialization Anti-Patterns

This catalog covers anti-patterns specific to schema-aware producers, consumers, and Streams apps that integrate with Confluent Schema Registry (or any schema-registry-compatible service: Apicurio, AWS Glue, WarpStream BYOC SR).

Rules here are informed by Confluent's published `kafka-schema-registry` and `kafka-streams-programming` agent skills. The categories used by Confluent for risk grouping (A: compliant, B: schema-in-code/no-SR, C: auto-register, D: no schema, E: custom serializer) map directly to the lint signals below.

## Legend

**Severity**
- `ERROR` — production bug or unrecoverable failure mode.
- `WARNING` — silent failure mode, observability gap, or foot-gun.

**Confidence**
- `HIGH` — descriptor-level / mechanical detection. Few false positives.
- `MEDIUM` — heuristic detection (class hierarchy walk, lambda body analysis).
- `CONTEXT` — depends on project intent or runtime — print-only by default.

**Detection** — `bytecode`, `config-file`, or a combination (` + `, alphabetical).

A trailing 🤝 in the tagline means the rule's doc carries a `## Consult a friend?` block — read it before changing prod config.

## Catalog (7)

### Avro logical type mappings — Avro 1.12+ generated setters expect `java.time` / `BigDecimal`

| Rule | Severity | Confidence | Detection | Tagline |
|---|---|---|---|---|
| [AVRO_LOGICAL_TYPE_INSTANT_PRIMITIVE](./AVRO_LOGICAL_TYPE_INSTANT_PRIMITIVE.md) | ERROR | HIGH | bytecode | `Avro 1.12+` generated setters expect `java.time.Instant` — `Long.valueOf(...)` and `Math.max(...)` are compile errors with a paper trail. |
| [AVRO_LOGICAL_TYPE_LOCALDATE_PRIMITIVE](./AVRO_LOGICAL_TYPE_LOCALDATE_PRIMITIVE.md) | ERROR | HIGH | bytecode | An `int` to a `LocalDate` setter is a 1970-01-01 record waiting to be produced. |
| [AVRO_LOGICAL_TYPE_BIGDECIMAL_PRIMITIVE](./AVRO_LOGICAL_TYPE_BIGDECIMAL_PRIMITIVE.md) | ERROR | HIGH | bytecode | `setAmount(0.0)` writes a `double` to a `BigDecimal` field — money has lost its precision. |

### Schema Registry serde configuration

| Rule | Severity | Confidence | Detection | Tagline |
|---|---|---|---|---|
| [SR_JSON_VALUE_TYPE_MISSING](./SR_JSON_VALUE_TYPE_MISSING.md) | ERROR | HIGH | bytecode + config-file | Without `json.value.type`, your consumer gets `LinkedHashMap` and `getCustomerId()` throws `ClassCastException`. |
| [SR_PROTOBUF_VALUE_TYPE_MISSING](./SR_PROTOBUF_VALUE_TYPE_MISSING.md) | WARNING | HIGH | bytecode + config-file | Without `specific.protobuf.value.type`, your consumer gets `DynamicMessage` instead of the generated class — every typed access goes through `getField()`. |
| [SR_USE_LATEST_VERSION_MISSING](./SR_USE_LATEST_VERSION_MISSING.md) | WARNING | MEDIUM | config-file | `auto.register.schemas=false` without `use.latest.version=true` — your producer is stuck on schema ID v1 forever. |

### Schema Registry bypass

| Rule | Severity | Confidence | Detection | Tagline |
|---|---|---|---|---|
| [SR_CUSTOM_SERIALIZER_BYPASS](./SR_CUSTOM_SERIALIZER_BYPASS.md) | WARNING | MEDIUM | bytecode | A hand-rolled `Serializer<T>` that does `JSON.stringify` is Confluent's Category E — schemas you can't evolve and consumers you can't migrate. |

## Cross-cutting themes

- **Avro 1.12+ logical types** are the most common silent build-breaker we see in 2026 — older code used `long` for `timestamp-millis`; the generated setter now expects `java.time.Instant`. The bytecode signature is unambiguous and the failure mode is a loud compile error in some places, silent wrong-value in others.
- **JSON Schema is doubly fragile** without `json.value.type`: the deserializer silently returns `LinkedHashMap`, the listener calls `.getCustomerId()`, NPE. There is no warning on the produce side because the serializer accepts any POJO Jackson can write.
- **Custom serializers** are not just "less standard" — they cannot evolve. Every change to the wire format breaks producers and consumers at once.

## References

- Confluent agent-skills — `kafka-schema-registry/references/categorization.md`
- Confluent agent-skills — `kafka-streams-programming/references/schema-patterns.md`
- Schema Registry docs: <https://docs.confluent.io/platform/current/schema-registry/index.html>
