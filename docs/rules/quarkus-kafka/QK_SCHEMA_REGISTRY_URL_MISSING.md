# QK_SCHEMA_REGISTRY_URL_MISSING

**Severity**: ERROR
**Confidence**: HIGH
**Detection**: config-file
**Tagline**: Avro without a schema registry URL is just bytes pretending to be schema'd.

## TL;DR

Apicurio or Confluent Avro/Protobuf deserializers require a registry URL property (`apicurio.registry.url` or `schema.registry.url`). If unset, the deserializer either fails at startup or falls back to local-only behavior, silently producing wrong objects.

## What's happening (the mechanism)

Avro/Protobuf deserialization in Kafka:
- The record carries a magic byte + schema ID, not the schema itself.
- The deserializer looks up the schema in a registry by ID.
- Without a registry URL, the ID can't be resolved.

Apicurio `AvroKafkaDeserializer`:
- Requires `apicurio.registry.url` (Apicurio v2) or `apicurio.registry.url` + `apicurio.registry.headers.X-Registry-Tenant` (multitenant).
- Missing config → `NullPointerException` deep inside the deserializer's lazy init.

Confluent `KafkaAvroDeserializer`:
- Requires `schema.registry.url`.
- Missing config → throws `ConfigException: Missing required configuration "schema.registry.url"` at startup.

The trickier mistake: setting `schema.registry.url` for a Confluent codec but using the Apicurio deserializer class. Apicurio ignores the Confluent key.

## Operational impact

- Apicurio: NPE at first record, looks like a code bug.
- Confluent: startup fails with ConfigException — at least it fails fast.
- Mismatched config (wrong property name for wrong deserializer): looks healthy until first record, then NPE/ClassCastException.

## How to fix

```properties
# GOOD — Apicurio v2
mp.messaging.incoming.orders.value.deserializer=io.apicurio.registry.serde.avro.AvroKafkaDeserializer
mp.messaging.incoming.orders.apicurio.registry.url=http://apicurio.svc.cluster.local:8080/apis/registry/v2

# GOOD — Confluent
mp.messaging.incoming.orders.value.deserializer=io.confluent.kafka.serializers.KafkaAvroDeserializer
mp.messaging.incoming.orders.schema.registry.url=http://schema-registry.svc.cluster.local:8081

# For producers using auto-registration (use sparingly in prod):
mp.messaging.outgoing.orders-out.auto.register.schemas=false
mp.messaging.outgoing.orders-out.use.latest.version=true
```

## When this might be a false positive

- Non-registry-based codecs (vanilla JSON, byte arrays, Strings).
- Custom serdes that embed the schema in the record.

## Detection strategy

- Config: `mp.messaging.*.value.deserializer` matching known registry-backed classes (`io.apicurio.*`, `io.confluent.kafka.serializers.KafkaAvroDeserializer`, `io.confluent.kafka.serializers.protobuf.KafkaProtobufDeserializer`) AND the corresponding registry URL property unset.
- Confidence: HIGH.

## References

- https://www.apicur.io/registry/docs/apicurio-registry/2.5.x/getting-started/assembly-using-kafka-client-serdes.html
- https://docs.confluent.io/platform/current/schema-registry/serdes-develop/index.html
- https://quarkus.io/guides/kafka-schema-registry-apicurio
