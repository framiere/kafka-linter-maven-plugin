# SR_CUSTOM_SERIALIZER_BYPASS
**Severity**: WARNING
**Confidence**: MEDIUM
**Detection**: bytecode
**Tagline**: A hand-rolled `Serializer<T>` that does `JSON.stringify` is Confluent's Category E — schemas you can't evolve and consumers you can't migrate.
**Source**: Confluent agent-skills — kafka-schema-registry/references/categorization.md § Category E (Custom serializers) — Consumers First, kafka-schema-registry/references/code-migration.md § Java composite deserializer.

## TL;DR

The linter flags classes implementing `org.apache.kafka.common.serialization.Serializer<T>` or `Deserializer<T>` whose body invokes ad-hoc JSON / Avro / Protobuf encoders (`ObjectMapper.writeValueAsBytes`, `GenericDatumWriter`, `JSON.stringify`-equivalents) without going through a Confluent SR client. These are Confluent's **Category E**: payload format is locked into the codebase, evolution requires synchronized binary releases of every producer and consumer, and there is no central schema to reason about.

## What's happening (the mechanism)

A `Serializer<T>` interface is just `byte[] serialize(String topic, T data)`. Anything goes inside. Common shapes the lint catches:

```java
// Shape 1 — Jackson direct write, no schema ID
public class TransactionSerializer implements Serializer<Transaction> {
    private final ObjectMapper om = new ObjectMapper();
    @Override public byte[] serialize(String topic, Transaction txn) {
        try { return om.writeValueAsBytes(txn); }
        catch (Exception e) { throw new SerializationException(e); }
    }
}

// Shape 2 — GenericDatumWriter without SR
public class AvroSerializer implements Serializer<GenericRecord> {
    @Override public byte[] serialize(String topic, GenericRecord rec) {
        DatumWriter<GenericRecord> w = new GenericDatumWriter<>(rec.getSchema());
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        Encoder e = EncoderFactory.get().binaryEncoder(out, null);
        w.write(rec, e); e.flush();
        return out.toByteArray();   // no magic byte, no schema ID
    }
}

// Shape 3 — Proto toByteArray without SR header
public class ProtoSerializer implements Serializer<MyMessage> {
    @Override public byte[] serialize(String topic, MyMessage m) {
        return m.toByteArray();
    }
}
```

All three skip the Confluent wire format (magic byte `0x00` + 4-byte schema ID + payload, or the equivalent header-based format from 2024+). That means:

- No SR ID → no central schema → no compatibility check on registry → no audit trail of schema versions.
- Consumers cannot deserialize without the matching custom code, hard-linked by a JAR coordinate.
- Schema evolution requires a flag-day deploy across all producers and consumers.

Confluent's categorization defines this as Category E and prescribes a **consumers-first** rollout (a composite deserializer that accepts both the old custom format and the new SR-wrapped format until old data expires).

## Operational impact

- One forgotten field rename ships in producer v3.4 with no consumer counterpart → all consumers crash on the first record with the new field.
- Cross-team integration: every new consumer team must be given the JAR with the right `Serializer<T>` implementation. There is no `schema.registry.url` they can point at to discover the contract.
- Audit and compliance: the data contract lives only in source; SR-based tooling (e.g., contract testing, schema linters, dataset catalogs) cannot see this stream.

## How to fix (bad → good code)

The full migration is captured in `kafka-schema-registry/references/code-migration.md`. The short form:

```java
// BEFORE (Category E)
props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, MyJsonSerializer.class.getName());

// AFTER — replace with Confluent serializer + Schema Registry
props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaJsonSchemaSerializer.class.getName());
props.put("schema.registry.url", "http://sr:8081");
props.put("auto.register.schemas", false);
props.put("use.latest.version", true);
props.put("json.value.type", Transaction.class.getName());   // consumers
```

Consumer-side, during the rollout window, use a **composite deserializer** that tries the Confluent format first and falls back to the legacy custom format. Once retention expires the old payloads, remove the fallback.

## When this might be a false positive

- The custom serializer is a thin wrapper around a Confluent serializer (e.g., adding a Spring-aware logger). Detect by checking whether the body delegates to `KafkaAvroSerializer` / `KafkaJsonSchemaSerializer` / `KafkaProtobufSerializer`.
- The serializer is for an internal-only topic where producer and consumer ship as a single binary (e.g., embedded compaction store). No external contract — Category E is overkill.
- Test-only serializers in `src/test/java`. Skip.

## Detection strategy

- Bytecode: any class implementing `org.apache.kafka.common.serialization.Serializer` or `Deserializer` whose method body does NOT invoke any Confluent serializer class (`io.confluent.kafka.serializers.*`).
- Also catch the inline pattern: `Properties.put("value.serializer", "com.app.MyJsonSerializer")` where the named class is in the same project and matches the above shape.
- Confidence: MEDIUM — false-positive surface includes legitimate wrappers and internal-only serdes. Print the matched class and method body in the lint output so reviewers can decide.

## References

- Confluent agent-skills — kafka-schema-registry/references/categorization.md § Category E (Custom serializers) — Consumers First
- Confluent agent-skills — kafka-schema-registry/references/code-migration.md § Java composite deserializer
- Confluent docs — Wire format: https://docs.confluent.io/platform/current/schema-registry/fundamentals/serdes-develop/index.html#wire-format
