package sample;

import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * RULE: DESER_JSON_TYPE_INFO_NO_ALLOWLIST — good shape, plain monomorphic mapper.
 *
 * The most common Kafka-consumer shape: deserialize a known
 * concrete class (`MyRecord`) from a topic where the schema is
 * managed externally (Avro / Protobuf / Schema Registry — or
 * just "every payload on `orders.created` is an
 * `OrderCreatedEvent`"). There is no polymorphism at the Java
 * type-system level, therefore no `activateDefaultTyping` call,
 * therefore no rule fire.
 *
 * Monomorphic deserialization is immune to the
 * polymorphic-typing CVE family by construction: there is no
 * type-name in the payload to influence which class Jackson
 * instantiates. The class is hard-coded at the
 * `readValue(bytes, MyRecord.class)` call site.
 *
 * The lint is silent on this file.
 */
public final class GoodPlainMapperNoDefaultTyping {

    public static final class MyRecord {
        public String topic;
        public long offset;
        public String payload;
    }

    public MyRecord deserialize(byte[] bytes) throws Exception {
        ObjectMapper mapper = new ObjectMapper();
        return mapper.readValue(bytes, MyRecord.class);
    }
}
