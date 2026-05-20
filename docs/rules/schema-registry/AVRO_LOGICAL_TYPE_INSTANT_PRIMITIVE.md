# AVRO_LOGICAL_TYPE_INSTANT_PRIMITIVE
**Severity**: ERROR
**Confidence**: HIGH
**Detection**: bytecode
**Tagline**: `Avro 1.12+` generated setters expect `java.time.Instant` — `Long.valueOf(...)` and `Math.max(...)` are compile errors with a paper trail.
**Source**: Confluent agent-skills — kafka-streams-programming/references/schema-patterns.md § Java type mapping with logical types, kafka-streams-programming/SKILL.md § Invariant Checklist #12.

## TL;DR

The linter flags any call to an Avro-generated setter method (`setXxx(java.time.Instant)`) that receives a primitive `long`, `Long`, the result of `Math.max(...)` / `Math.min(...)` over `Instant` getters, or `Instant.toEpochMilli()`. Avro 1.12+ with the Gradle / Maven Avro plugin generates `java.time.Instant` (not `long`) for fields with `logicalType=timestamp-millis` or `timestamp-micros`. Code written against the pre-1.12 mapping causes compile errors in some paths and subtle runtime bugs in others.

## What's happening (the mechanism)

Avro's `timestamp-millis` logical type wraps an underlying `long` (milliseconds since epoch). Earlier Avro versions generated `long` Java setters; the generated class had `setTimestamp(long ts)`. Avro 1.12+ changed the mapping: the default conversion now generates `setTimestamp(java.time.Instant ts)`.

The skill's invariant #12 names this explicitly:

> Avro logical type Java mappings: Avro 1.12+ generates `java.time.Instant` for `timestamp-millis`/`timestamp-micros`, `LocalDate` for `date`, `BigDecimal` for `decimal`, etc. **Never use raw `long`/`int` literals** with generated setter methods — use `Instant.EPOCH`, `Instant.now()`, `Instant.ofEpochMilli(...)`. Use `Instant.isAfter()`/`isBefore()` instead of `Math.max()`/`Math.min()` for timestamp comparisons. Applies to topology code, aggregation initializers, producers, AND test helpers.

Common bug shapes:

```java
// (1) Long literal passed to Instant setter — compile error in modern Avro, silent boxing-to-Long-then-wrong-conversion in some plugin configs
stats.setLastOrderTime(0L);

// (2) Math.max on Instant getters — Math.max doesn't accept Instant; compile error
updated.setTimestamp(Math.max(a.getTimestamp(), b.getTimestamp()));

// (3) Producer using toEpochMilli() — accidentally truncates to a Long then can't be passed to setTimestamp(Instant)
order.setTimestamp(Instant.now().toEpochMilli());

// (4) Test helper hand-coded with a long literal
event.setTimestamp(1000L);
```

Even in projects where the plugin is configured to keep the legacy `long` mapping (`-DenableDecimalLogicalType=false`, etc.), this is fragile — the next plugin upgrade flips behavior.

## Operational impact

- **Compile errors** in topology / aggregation / producer code that mixes pre-1.12 idioms with new generated classes. Build breaks loudly, sometimes only after a transitive Avro upgrade.
- **Silent wrong values** when the project uses `Object` typed maps (e.g., `GenericRecord.put("timestamp", 1000L)`) — the producer writes a `long`, the schema expects `timestamp-millis`, downstream serdes interpret epoch-millis-of-the-write-time as the field value (often near-zero or absurd future dates).
- **Test brittleness**: tests using `event.setTimestamp(1000L)` pass against the old plugin and fail against the new one — code-review delta is small, blast radius is the entire pipeline.

## How to fix (bad → good code)

```java
// BAD — Long literal to Instant setter
stats.setLastOrderTime(0L);

// GOOD
stats.setLastOrderTime(Instant.EPOCH);

// BAD — Math.max over Instant getters
updated.setTimestamp(Math.max(a.getTimestamp(), b.getTimestamp()));

// GOOD — use Instant comparison
updated.setTimestamp(a.getTimestamp().isAfter(b.getTimestamp()) ? a.getTimestamp() : b.getTimestamp());

// BAD — toEpochMilli() returns long
order.setTimestamp(Instant.now().toEpochMilli());

// GOOD
order.setTimestamp(Instant.now());

// BAD — long literal in a test helper
event.setTimestamp(1000L);

// GOOD
event.setTimestamp(Instant.ofEpochMilli(1000L));
```

## When this might be a false positive

- The project explicitly opts out of logical-type generation (Avro plugin config `enableDecimalLogicalType=false`, `dateTimeLogicalTypeImplementation=joda`, etc.) — the generated setter is `setTimestamp(long)` and `Long.valueOf(...)` is correct. Detect via the Avro plugin configuration block in `build.gradle` / `pom.xml`.
- The setter accepts a primitive because the field is **not** marked with a logical type — the field is just a plain `long`. The lint must inspect the descriptor of the called setter method, not assume.

## Detection strategy

- Bytecode: identify Avro-generated classes by their `org.apache.avro.specific.SpecificRecord` superinterface or `@org.apache.avro.specific.AvroGenerated` annotation. For each call to a method `setXxx(Ljava/time/Instant;)V` on such a class:
  - If the argument stack slot was produced by `INVOKESTATIC java/lang/Long.valueOf(J)Ljava/lang/Long;` followed by an implicit unboxing, flag.
  - If the argument was produced by `INVOKESTATIC java/lang/Math.max(JJ)J` / `Math.min`, flag.
  - If the argument was produced by `INVOKEVIRTUAL java/time/Instant.toEpochMilli()J` followed by a conversion back to Instant via `Instant.ofEpochMilli`, the round-trip is suspicious — flag with lower confidence.
- For `GenericRecord.put(String, Object)` patterns: cannot detect without symbol-resolution on the field's schema. Skip.
- Confidence: HIGH for the direct setter-call cases; MEDIUM for the round-trip.

## References

- Confluent agent-skills — kafka-streams-programming/references/schema-patterns.md § Java type mapping with logical types
- Confluent agent-skills — kafka-streams-programming/SKILL.md § Invariant Checklist #12
- Avro Java Type Mappings: https://avro.apache.org/docs/1.12.0/specification/#logical-types
- Apache Avro AVRO-3819: Type changes for Java 8 date/time logical types
