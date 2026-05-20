# Schema Registry & Schematized Serialization Anti-Patterns

This catalog covers anti-patterns specific to schema-aware producers, consumers, and Streams apps that integrate with Confluent Schema Registry (or any schema-registry-compatible service: Apicurio, AWS Glue, WarpStream BYOC SR).

Rules here are derived from Confluent's published `kafka-schema-registry` and `kafka-streams-programming` agent skills. The categories used by Confluent for risk grouping (A: compliant, B: schema-in-code/no-SR, C: auto-register, D: no schema, E: custom serializer) map directly to the lint signals below.

## Severity legend

- **ERROR**: production bug or unrecoverable failure mode.
- **WARNING**: silent failure mode, observability gap, or foot-gun.

## Confidence legend

- **HIGH**: descriptor-level / mechanical detection. Few false positives.
- **MEDIUM**: heuristic detection (class hierarchy walk, lambda body analysis).
- **CONTEXT**: depends on project intent or runtime — print-only by default.

## Catalog

### Avro logical type mappings (3) — Avro 1.12+ generated setters expect `java.time` / `BigDecimal`

| Rule | Severity | Confidence | Detection | Tagline |
|---|---|---|---|---|
| [AVRO_LOGICAL_TYPE_INSTANT_PRIMITIVE](AVRO_LOGICAL_TYPE_INSTANT_PRIMITIVE.md) | ERROR | HIGH | bytecode | Passing `long` / `Long.valueOf(...)` / `Math.max(...)` to an Avro setter expecting `java.time.Instant`. |
| [AVRO_LOGICAL_TYPE_LOCALDATE_PRIMITIVE](AVRO_LOGICAL_TYPE_LOCALDATE_PRIMITIVE.md) | ERROR | HIGH | bytecode | Passing `int` days-since-epoch to an Avro setter expecting `java.time.LocalDate`. |
| [AVRO_LOGICAL_TYPE_BIGDECIMAL_PRIMITIVE](AVRO_LOGICAL_TYPE_BIGDECIMAL_PRIMITIVE.md) | ERROR | HIGH | bytecode | Passing `double` / `long` to an Avro setter expecting `java.math.BigDecimal`. |

### Schema Registry serde configuration (3)

| Rule | Severity | Confidence | Detection | Tagline |
|---|---|---|---|---|
| [SR_JSON_VALUE_TYPE_MISSING](SR_JSON_VALUE_TYPE_MISSING.md) | ERROR | HIGH | combination | `KafkaJsonSchemaSerializer/Deserializer` without `json.value.type` — runtime `ClassCastException` on `LinkedHashMap`. |
| [SR_PROTOBUF_VALUE_TYPE_MISSING](SR_PROTOBUF_VALUE_TYPE_MISSING.md) | WARNING | HIGH | combination | `KafkaProtobufDeserializer` without `specific.protobuf.value.type` — deserializes to `DynamicMessage`. |
| [SR_USE_LATEST_VERSION_MISSING](SR_USE_LATEST_VERSION_MISSING.md) | WARNING | MEDIUM | config-file | `auto.register.schemas=false` without `use.latest.version=true` — producers stuck on schema ID v1. |

### Schema Registry bypass (1)

| Rule | Severity | Confidence | Detection | Tagline |
|---|---|---|---|---|
| [SR_CUSTOM_SERIALIZER_BYPASS](SR_CUSTOM_SERIALIZER_BYPASS.md) | WARNING | MEDIUM | bytecode | Custom `Serializer<T>` writing JSON/Avro/Proto bytes without Schema Registry — Confluent Category E. |

## Cross-cutting themes

- **Avro 1.12+ logical types** are the most common silent-build-breaker we see in 2026 — older code used `long` for `timestamp-millis`; the generated setter now expects `java.time.Instant`. The bytecode signature is unambiguous and the failure mode is loud compile error in some places, silent wrong-value in others.
- **JSON Schema is doubly fragile** without `json.value.type`: the deserializer silently returns `LinkedHashMap`, the listener calls `.getCustomerId()`, NPE. There is no warning on the produce side because the serializer accepts any POJO Jackson can write.
- **Custom serializers** are not just "less standard" — they cannot evolve. Every change to the wire format breaks producers and consumers at once.

## References

- Confluent agent-skills — kafka-schema-registry/references/categorization.md
- Confluent agent-skills — kafka-streams-programming/references/schema-patterns.md
- Schema Registry docs: https://docs.confluent.io/platform/current/schema-registry/index.html
