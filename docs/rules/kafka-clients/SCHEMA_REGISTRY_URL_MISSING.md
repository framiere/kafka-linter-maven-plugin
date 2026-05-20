# SCHEMA_REGISTRY_URL_MISSING

**Severity**: ERROR
**Confidence**: HIGH
**Detection**: bytecode + config-file
**Tagline**: A Schema Registry serializer without a URL is just an exception generator.

## TL;DR

The linter flags producers/consumers configured with Confluent's Schema Registry-aware serializers (`KafkaAvroSerializer`, `KafkaProtobufSerializer`, `KafkaJsonSchemaSerializer` and their `Deserializer` counterparts) where `schema.registry.url` is not set.

## What's happening (the mechanism)

Confluent's schema-registry-aware (de)serializers need a registry endpoint to:
- Register the schema on serialize and obtain a schema ID.
- Look up the schema by ID on deserialize.

Without `schema.registry.url`, every (de)serialization call fails with:

```
java.lang.NullPointerException: ...schemaRegistry
```

or in newer versions a `ConfigException`. Either way, every record is poison.

This is sometimes set at the framework level (Spring `properties.schema.registry.url`, Quarkus `apicurio.registry.url`) — the linter should check the framework-aware key when present.

## Operational impact

- All produced records throw on `send()`; downstream sees no data.
- All consumed records throw inside the deserializer; the consumer enters a crashloop unless an `ErrorHandlingDeserializer` is wrapping it.
- Easy to catch in IT but easy to forget when promoting from local Docker (`localhost:8081`) to prod.

## How to fix

```java
// BAD
props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class);
// no schema.registry.url

// GOOD
props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class);
props.put("schema.registry.url", "https://schema-registry.example.com");
// also: basic auth (if applicable)
props.put("basic.auth.credentials.source", "USER_INFO");
props.put("basic.auth.user.info", "<user>:<password>");
```

## When this might be a false positive

- Test fixtures using `MockSchemaRegistryClient` — they still take a URL like `mock://...`. If unset entirely, flag.

## Detection strategy

- Config: `*.serializer` / `*.deserializer` class FQCN starts with `io.confluent.kafka.serializers.` or `io.apicurio.registry.serde.` AND no `schema.registry.url` / `apicurio.registry.url` set in the same source. HIGH.
- Bytecode: same logic over `Properties.put` calls.

## References

- Confluent Schema Registry — client config: https://docs.confluent.io/platform/current/schema-registry/installation/config.html
- Confluent — Avro Serializer: https://docs.confluent.io/platform/current/schema-registry/serdes-develop/serdes-avro.html
- Apicurio — Kafka serdes: https://www.apicur.io/registry/docs/apicurio-registry/2.5.x/getting-started/assembly-using-kafka-client-serdes.html
