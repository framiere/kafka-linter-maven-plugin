# SR_PROTOBUF_VALUE_TYPE_MISSING
**Severity**: WARNING
**Confidence**: HIGH
**Detection**: bytecode + config-file
**Tagline**: Without `specific.protobuf.value.type`, your consumer gets `DynamicMessage` instead of the generated class — every typed access goes through `getField()`.
**Source**: Confluent agent-skills — kafka-streams-programming/references/schema-patterns.md § Protobuf serde configuration, kafka-schema-registry/references/code-migration.md § Java Protobuf consumer.

## TL;DR

The linter flags `KafkaProtobufDeserializer` / `KafkaProtobufSerde` configurations that omit `specific.protobuf.value.type` (and `specific.protobuf.key.type` for keys). Without the property, the deserializer returns a `com.google.protobuf.DynamicMessage`, not your generated `Transaction.class`. Code written against the generated class fails to compile against the result, OR — worse — uses a `(DynamicMessage)` cast and reads fields by name string, losing all type safety.

## What's happening (the mechanism)

`KafkaProtobufDeserializer<T extends Message>` defaults to returning `DynamicMessage`. `DynamicMessage` is a generic reflective representation of a Protobuf wire payload; it carries the same data but you access fields with:

```java
((DynamicMessage) rec.value()).getField(descriptor.findFieldByName("account_id"))
```

instead of `rec.value().getAccountId()`. When you set `specific.protobuf.value.type=com.example.Transaction`, the deserializer materializes the actual generated class.

This is the Protobuf analogue of `json.value.type` (see `SR_JSON_VALUE_TYPE_MISSING`). The mechanism is the same: generic erasure means the property is the only signal.

## Operational impact

- Less catastrophic than the JSON case: `DynamicMessage` doesn't throw `ClassCastException` on first access — `Object` references survive. The failure mode is downstream: a method expecting `Transaction` cannot accept `DynamicMessage`. Devs work around it with the reflective accessor, and the code rots.
- Streams topologies that join two Protobuf topics will end up using `DynamicMessage` on both sides, making the topology unable to access typed fields without casts.
- The default behavior is intentional in Confluent's design — `DynamicMessage` works for any schema, which is useful for generic tooling. But for application code, you want the typed class.

## How to fix (bad → good code)

```java
// BAD — typed deserializer without specific.protobuf.value.type
props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaProtobufDeserializer.class.getName());
props.put("schema.registry.url", "http://sr:8081");

// At consume time:
DynamicMessage msg = (DynamicMessage) rec.value();
String acc = (String) msg.getField(msg.getDescriptorForType().findFieldByName("account_id"));

// GOOD
props.put(KafkaProtobufDeserializerConfig.SPECIFIC_PROTOBUF_VALUE_TYPE, Transaction.class.getName());

// Now this works:
Transaction txn = rec.value();
String acc = txn.getAccountId();
```

For Streams:

```java
Map<String, Object> serdeConfig = Map.of(
    "schema.registry.url", srUrl,
    "specific.protobuf.value.type", Transaction.class.getName()
);
KafkaProtobufSerde<Transaction> serde = new KafkaProtobufSerde<>();
serde.configure(serdeConfig, false);
```

For keys: `specific.protobuf.key.type`.

## When this might be a false positive

- The application uses `DynamicMessage` intentionally — generic schema-inspection tools, federation gateways, log forwarders that don't care about field semantics.
- Reflection-based dispatch frameworks (rare) where multiple Protobuf types arrive on one topic.

## Detection strategy

- Bytecode: any class referencing `io.confluent.kafka.serializers.protobuf.KafkaProtobufDeserializer` or `io.confluent.kafka.streams.serdes.protobuf.KafkaProtobufSerde` in producer/consumer/serde construction.
- Config-file: properties or YAML with `value.deserializer=...KafkaProtobufDeserializer` or `default.value.serde=...KafkaProtobufSerde` without a matching `specific.protobuf.value.type=...` entry.
- Combination: bytecode confirms the class is used AND no `specific.protobuf.value.type` is found in any reachable config. Flag.
- Confidence: HIGH on the property absence, WARNING (not ERROR) on severity — the `DynamicMessage` default doesn't crash, it just degrades the dev experience.

## References

- Confluent agent-skills — kafka-streams-programming/references/schema-patterns.md § Protobuf serde configuration
- Confluent agent-skills — kafka-schema-registry/references/code-migration.md § Java Protobuf consumer
- Confluent docs — Protobuf Serializer and Deserializer: https://docs.confluent.io/platform/current/schema-registry/serdes-develop/serdes-protobuf.html
