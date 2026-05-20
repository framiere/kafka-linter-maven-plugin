# AVRO_LOGICAL_TYPE_LOCALDATE_PRIMITIVE
**Severity**: ERROR
**Confidence**: HIGH
**Detection**: bytecode
**Tagline**: An `int` to a `LocalDate` setter is a 1970-01-01 record waiting to be produced.
**Source**: Confluent agent-skills — kafka-streams-programming/references/schema-patterns.md § Java type mapping with logical types, kafka-streams-programming/SKILL.md § Invariant Checklist #12.

## TL;DR

The linter flags Avro-generated setter calls of the form `setXxx(java.time.LocalDate)` that receive a primitive `int`, `Integer`, or any expression intended to be days-since-epoch. Avro 1.12+ generates `java.time.LocalDate` for fields with `logicalType=date`. Code that still passes `int` days-since-epoch — either as a literal `0`, the result of `(int)(System.currentTimeMillis() / 86400000)`, or an `Integer.valueOf` — either fails to compile or boxes through a wrong code path.

## What's happening (the mechanism)

The Avro `date` logical type wraps an underlying `int` (days since epoch, 1970-01-01). Before Avro 1.12, the generated setter signature was `setBirthDate(int days)`. From Avro 1.12 with the default `dateTimeLogicalTypeImplementation=jsr310` (now the default), the setter is `setBirthDate(java.time.LocalDate)`.

The skill names this in invariant #12 together with the `Instant`/`BigDecimal` cases. Common bug shapes:

```java
// (1) Literal int
person.setBirthDate(0);   // intent: 1970-01-01, but setter expects LocalDate

// (2) Days-since-epoch computation passed in
person.setBirthDate((int)(System.currentTimeMillis() / 86_400_000));

// (3) Cross-type assignment between Avro records
to.setBirthDate(from.getBirthDate().toEpochDay());  // toEpochDay() returns long
```

## Operational impact

- Compile errors on the project itself once Avro is upgraded — fast feedback.
- For code paths using `GenericRecord.put("date", 0)` (Object-typed), the producer writes `0` which decodes as 1970-01-01 — every record in the topic has the same wrong date.
- Backward-compatibility break with consumers that expected the old `int` semantics: a downstream Spark job reading the topic may see `null` (if the field is nullable) or wrong dates.

## How to fix (bad → good code)

```java
// BAD
person.setBirthDate(0);
person.setBirthDate((int)(System.currentTimeMillis() / 86_400_000));

// GOOD
person.setBirthDate(LocalDate.EPOCH);              // 1970-01-01
person.setBirthDate(LocalDate.now());
person.setBirthDate(LocalDate.parse("2026-05-20"));
```

For Avro records constructed dynamically via `GenericRecord.put(...)`, switch to passing `LocalDate` values directly — the SR Avro serde handles the conversion to wire bytes per the schema's logical type.

## When this might be a false positive

- Project pinned to pre-1.12 Avro generation or `dateTimeLogicalTypeImplementation=joda` — generated setter is `setBirthDate(int)` and the primitive is correct. Detect via the Avro plugin block.
- Field is NOT a logical type — a plain `int` field with `logicalType` absent. The generated setter is `setXxx(int)`; the primitive is correct.

## Detection strategy

- Bytecode: same approach as `AVRO_LOGICAL_TYPE_INSTANT_PRIMITIVE`. For each `setXxx(Ljava/time/LocalDate;)V` call on a class implementing `SpecificRecord`:
  - If the argument was produced by `INVOKESTATIC java/lang/Integer.valueOf(I)Ljava/lang/Integer;` followed by unboxing, flag.
  - If the argument was an integer literal pushed via `ICONST_*` / `BIPUSH` / `SIPUSH` / `LDC` and converted via `Integer.valueOf`, flag.
- HIGH confidence: setter signature is explicit and unambiguous.

## References

- Confluent agent-skills — kafka-streams-programming/references/schema-patterns.md § Java type mapping with logical types
- Confluent agent-skills — kafka-streams-programming/SKILL.md § Invariant Checklist #12
- Avro logical types — `date`: https://avro.apache.org/docs/1.12.0/specification/#date
