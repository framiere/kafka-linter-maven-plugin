# AVRO_LOGICAL_TYPE_BIGDECIMAL_PRIMITIVE
**Severity**: ERROR
**Confidence**: HIGH
**Detection**: bytecode
**Tagline**: `setAmount(0.0)` writes a `double` to a `BigDecimal` field — money has lost its precision.
**Source**: Confluent agent-skills — kafka-streams-programming/references/schema-patterns.md § Java type mapping with logical types, kafka-streams-programming/SKILL.md § Invariant Checklist #12.

## TL;DR

The linter flags Avro-generated setter calls of the form `setXxx(java.math.BigDecimal)` that receive a primitive `double`, `float`, `long`, `int`, or their wrappers. Avro 1.12+ generates `java.math.BigDecimal` for fields with `logicalType=decimal`. Passing a primitive — especially `double` — loses the precision and scale that the `decimal` logical type exists to guarantee. For money fields, that is the rule's reason to exist.

## What's happening (the mechanism)

Avro's `decimal` logical type wraps an underlying `bytes` (or `fixed`) representation with metadata `{precision, scale}`. The serde turns the BigDecimal value into bytes preserving both precision and scale; the consumer's serde recovers the exact BigDecimal value.

If the Java application uses `double` instead, two things go wrong:
1. **Lossy conversion**: `BigDecimal.valueOf(0.1)` does *not* equal `new BigDecimal("0.1")` — the double `0.1` is the closest double approximation, which is `0.1000000000000000055511151231257827021181583404541015625`. Writing it via the decimal serde will round to the configured scale; downstream sees that rounded value, not your intended `0.1`.
2. **Compile error**: the generated setter signature is `setAmount(BigDecimal)`. Passing a `double` doesn't compile. Many projects "fix" this by writing `setAmount(BigDecimal.valueOf(amount))` where `amount` is a double — re-introducing problem #1.

The skill calls this out in invariant #12 with the same prescription as the other logical types: use the typed Java value, not the primitive.

## Operational impact

- Money fields off by sub-penny amounts that compound over millions of records. Reconciliation against the source-of-truth ledger reveals the drift weeks later.
- Sometimes the field is correct in production but wrong in tests because the test helper used a double literal.
- Schema Registry compatibility checks pass because the schema is unchanged — there is no breaking change that any of the safety nets would catch.

## How to fix (bad → good code)

```java
// BAD — double literal lost precision
order.setAmount(99.99);   // doesn't compile, or compiles via auto-conversion that loses precision

// BAD — BigDecimal.valueOf(double) — same loss
order.setAmount(BigDecimal.valueOf(99.99));

// BAD — money-as-cents long passed in
order.setAmount(BigDecimal.valueOf(9999, 2));   // OK if scale matches the schema, fragile otherwise

// GOOD — string constructor preserves the exact value
order.setAmount(new BigDecimal("99.99"));

// GOOD — defined elsewhere as a typed constant
order.setAmount(MoneyConstants.NINETY_NINE_NINETY_NINE);

// GOOD — for aggregation initializers
stats.setTotalAmount(BigDecimal.ZERO);
```

For arithmetic in aggregators: do all math in BigDecimal-space (`BigDecimal.add`, `BigDecimal.multiply`), never round-trip through double.

## When this might be a false positive

- Field has no logical type — plain `bytes` or `double` or `long`. Then the setter is not `(BigDecimal)` and the rule doesn't apply.
- Project pinned to a pre-1.12 Avro generation config where the setter is still `setAmount(ByteBuffer)`. Rare in 2026.
- Test code where exact precision isn't asserted — still worth fixing but lower priority.

## Detection strategy

- Bytecode: for each `setXxx(Ljava/math/BigDecimal;)V` call on a class implementing `SpecificRecord`:
  - Flag if the argument was produced by `INVOKESTATIC java/math/BigDecimal.valueOf(D)Ljava/math/BigDecimal;` (the `double` overload).
  - Flag if the argument was produced by `INVOKESTATIC java/math/BigDecimal.valueOf(J)Ljava/math/BigDecimal;` (the `long` overload) WITHOUT a preceding `INVOKEVIRTUAL java/math/BigDecimal.setScale(I)Ljava/math/BigDecimal;` — the long overload is sometimes correct for cents-as-long patterns, but only with explicit scale handling.
- HIGH confidence on the `double` overload. MEDIUM on the `long` overload.

## References

- Confluent agent-skills — kafka-streams-programming/references/schema-patterns.md § Java type mapping with logical types
- Confluent agent-skills — kafka-streams-programming/SKILL.md § Invariant Checklist #12
- Avro `decimal` logical type: https://avro.apache.org/docs/1.12.0/specification/#decimal
- BigDecimal precision pitfalls: https://docs.oracle.com/en/java/javase/17/docs/api/java.base/java/math/BigDecimal.html
