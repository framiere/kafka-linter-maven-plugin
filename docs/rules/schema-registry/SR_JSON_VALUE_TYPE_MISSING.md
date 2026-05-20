# SR_JSON_VALUE_TYPE_MISSING
**Severity**: ERROR
**Confidence**: HIGH
**Detection**: combination (bytecode + config-file)
**Tagline**: Without `json.value.type`, your consumer gets `LinkedHashMap` and `getCustomerId()` throws `ClassCastException`.
**Source**: Confluent agent-skills — kafka-streams-programming/references/schema-patterns.md § Critical: always set json.value.type, kafka-schema-registry/references/code-migration.md § Java JSON Schema consumer.

## TL;DR

The linter flags Kafka consumers (or Streams apps) configured with `KafkaJsonSchemaDeserializer` (or `KafkaJsonSchemaSerde` as default value serde) when the matching `json.value.type` / `json.key.type` property is not set. Without it, the deserializer returns a `LinkedHashMap<String, Object>` — not your POJO — and the first typed access (`((Transaction) record.value()).getAccountId()`) throws `ClassCastException` at runtime.

## What's happening (the mechanism)

`KafkaJsonSchemaDeserializer<T>` works in two modes:

- **Untyped (default)**: returns a `LinkedHashMap` of the JSON document. No reflection, no POJO instantiation.
- **Typed**: returns an instance of the class named in `json.value.type` (or `json.key.type` for keys). The deserializer uses Jackson to materialize the POJO.

The deserializer's generic type `<T>` is **erased at runtime** — there is no way for the deserializer to know "you wrote `new KafkaJsonSchemaDeserializer<Transaction>()`". It needs the FQCN as a config string.

In Kafka Streams, the same applies to `KafkaJsonSchemaSerde` and the `default.value.serde`. If multiple value types flow through the topology, a single `json.value.type` only covers one of them — the others must use explicit serdes (see false-positive notes).

## Operational impact

- Producer-side: works perfectly. The serializer just writes the JSON document; no type info is needed on the write path.
- Consumer-side: deserialization "succeeds" (returns a `LinkedHashMap`). The exception lands in the listener's first cast, often deep inside a Streams DSL `mapValues` or a Spring `@KafkaListener`.
- The error message — `class java.util.LinkedHashMap cannot be cast to class com.example.Transaction` — is unambiguous but only surfaces under traffic. Unit tests using `MockSchemaRegistryClient` often miss it because mocks pass POJOs directly.
- For Streams: the failure typically routes through `default.deserialization.exception.handler`. If that's `LogAndContinue`, you get silent data loss. If `LogAndFail`, every record kills the thread.

## How to fix (bad → good code)

```java
// BAD — typed deserializer without json.value.type
Properties props = new Properties();
props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaJsonSchemaDeserializer.class.getName());
props.put("schema.registry.url", "http://sr:8081");
// MISSING: json.value.type

// At consume time:
ConsumerRecord<String, Transaction> rec = ...;
String acc = rec.value().getAccountId();   // ClassCastException

// GOOD — class FQCN registered
props.put(KafkaJsonSchemaDeserializerConfig.JSON_VALUE_TYPE, Transaction.class.getName());
// or via the raw key:
props.put("json.value.type", "com.example.model.Transaction");
```

For Streams with mixed value types in the topology, use **explicit serdes** for the non-default types:

```java
KafkaJsonSchemaSerde<Transaction> txnSerde = new KafkaJsonSchemaSerde<>();
txnSerde.configure(Map.of(
    "schema.registry.url", srUrl,
    "json.value.type", Transaction.class.getName()
), false);

builder.stream("transactions", Consumed.with(Serdes.String(), txnSerde));
```

For keys: `json.key.type` is the symmetrical knob.

## When this might be a false positive

- The application explicitly wants the untyped `LinkedHashMap` (rare — usually for generic schema-discovery tooling).
- The deserializer is wired through Spring Cloud Stream's `MessageConverter` pipeline, where the type is supplied by the `@KafkaListener` method signature and `JsonMessageConverter` rather than the deserializer config. In that case the property doesn't apply.

## Detection strategy

- Bytecode: any class referencing `io.confluent.kafka.serializers.json.KafkaJsonSchemaDeserializer` or `io.confluent.kafka.streams.serdes.json.KafkaJsonSchemaSerde` AS a class literal in a `props.put(..., ...)` call OR as a constructor.
- Config-file: properties file or YAML containing `value.deserializer=...KafkaJsonSchemaDeserializer` or `default.value.serde=...KafkaJsonSchemaSerde` without a matching `json.value.type=` entry.
- Combination: if bytecode shows the class is in use AND no `json.value.type` is set in any reachable `Properties.put` call OR in the externalized config, flag.
- Confidence: HIGH — the failure is mechanical, not heuristic.

## References

- Confluent agent-skills — kafka-streams-programming/references/schema-patterns.md § JSON Schema, § Critical: always set json.value.type
- Confluent agent-skills — kafka-schema-registry/references/code-migration.md § Java JSON Schema consumer
- Confluent docs — JSON Schema Serializer and Deserializer: https://docs.confluent.io/platform/current/schema-registry/serdes-develop/serdes-json.html
