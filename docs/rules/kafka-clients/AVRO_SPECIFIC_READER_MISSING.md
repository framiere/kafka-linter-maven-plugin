# AVRO_SPECIFIC_READER_MISSING

**Severity**: WARNING
**Confidence**: HIGH
**Detection**: bytecode + config-file
**Tagline**: Generic Avro on a specific topic gives you back GenericRecord — and a ClassCastException later.

## TL;DR

The linter flags consumers configured with `KafkaAvroDeserializer` whose code casts the resulting value to a generated `SpecificRecord` type, but where `specific.avro.reader=true` is not set.

## What's happening (the mechanism)

Confluent's `io.confluent.kafka.serializers.KafkaAvroDeserializer` defaults to returning `GenericRecord` regardless of the topic's Avro schema. To return the generated Java class (the `SpecificRecord` subtype), the consumer must set:

```
specific.avro.reader=true
```

Without this, the deserializer returns `GenericRecord` and any downstream `(MyEvent) record.value()` cast throws `ClassCastException` at runtime.

This is one of the top-3 "first day with Schema Registry" gotchas. Documentation buries it under serializer config.

## Operational impact

- `ClassCastException: GenericData$Record cannot be cast to com.example.MyEvent` at the first record processed.
- Consumer crashloop until the deploy is rolled back.
- Lost time during the first deploy of a new Avro consumer.

## How to fix

```java
// BAD
props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class);
props.put("schema.registry.url", "https://schema-registry:8081");
// no specific.avro.reader
KafkaConsumer<String, MyEvent> c = new KafkaConsumer<>(props); // generic deser, specific generic
ConsumerRecord<String, MyEvent> r = ...;
MyEvent e = r.value(); // ClassCastException

// GOOD
props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class);
props.put("schema.registry.url", "https://schema-registry:8081");
props.put("specific.avro.reader", "true");
KafkaConsumer<String, MyEvent> c = new KafkaConsumer<>(props);
```

## When this might be a false positive

- Code that genuinely wants `GenericRecord` (schema-agnostic consumers, audit tools).

## Detection strategy

- Config: `key.deserializer` or `value.deserializer` equals `io.confluent.kafka.serializers.KafkaAvroDeserializer` AND no `specific.avro.reader=true` set in the same source. HIGH if the consumer's generic type parameter is a non-`GenericRecord` class.
- Bytecode: detect `Properties.put` for the Avro deserializer class; check sibling puts for `specific.avro.reader`. HIGH.
- Strongest signal: the consumer's generic type parameter is a class that implements `org.apache.avro.specific.SpecificRecord`.

## References

- Confluent KafkaAvroDeserializer docs: https://docs.confluent.io/platform/current/schema-registry/serdes-develop/serdes-avro.html
- Confluent — Specific vs Generic: https://docs.confluent.io/platform/current/schema-registry/serdes-develop/serdes-avro.html#avro-deserializer
