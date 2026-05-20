# STRING_SERIALIZER_NON_STRING

**Severity**: WARNING
**Confidence**: MEDIUM
**Detection**: bytecode
**Tagline**: StringSerializer with a non-String value is a UTF-8 encoder pretending to be a serializer.

## TL;DR

The linter flags producers/consumers configured with `StringSerializer` / `StringDeserializer` whose generic type parameter (or actual record value at the call site) is not `String`. The serializer calls `toString()` on the value — turning a `byte[]`, a domain object, or a `Map` into its accidental `Object.toString()` representation.

## What's happening (the mechanism)

`StringSerializer.serialize(topic, T data)` returns `data.toString().getBytes(charset)`. The generic `T` is erased to `Object` at runtime, so the compiler does not enforce the contract.

Common screwups:
- `new ProducerRecord<>(topic, key, someBytes)` with `value.serializer=StringSerializer` → ships `"[B@5e9f23b4"` (the default `byte[].toString()`).
- `new ProducerRecord<>(topic, key, domainObject)` with the same → ships `"DomainObject{id=...}"` (the default `Object.toString()`).

This often passes tests where the value happens to be a `String` and only breaks when someone wires up a typed producer.

## Operational impact

- Garbage payloads downstream — debugging is hard because the type is wrong but the producer reports success.
- Consumers tooled with `StringDeserializer` read back the garbage as-is and store it in their state.

## How to fix

```java
// BAD
props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
producer.send(new ProducerRecord<>(topic, "key", myBytes));

// GOOD — match the serializer to the type
props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
producer.send(new ProducerRecord<>(topic, "key", myBytes));

// GOOD — serialize at the app layer with a proper format
String json = mapper.writeValueAsString(domainObject);
producer.send(new ProducerRecord<>(topic, "key", json));
```

## When this might be a false positive

- Producers genuinely shipping `String` payloads. The rule only fires when the actual value type at the send-site is not `String`.

## Detection strategy

- Bytecode: detect `Properties.put("value.serializer", LDC "org.apache.kafka.common.serialization.StringSerializer")`. Then locate calls to `producer.send(new ProducerRecord<>(...))` and check the static type of the value argument. If it is not `java.lang.String`, flag. MEDIUM.
- Mirror for `key.serializer` and the deserializer pair.

## References

- StringSerializer source: https://github.com/apache/kafka/blob/trunk/clients/src/main/java/org/apache/kafka/common/serialization/StringSerializer.java
- Apache Kafka serializers — ByteArraySerializer / StringSerializer: https://kafka.apache.org/documentation/#producerapi
