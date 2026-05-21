package sample;

import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroSerializer;

import java.util.Properties;

/**
 * RULE: SCHEMA_REGISTRY_URL_MISSING.
 *
 * The rule fires when a class constructs a Confluent schema-registry
 * (de)serializer ({@code KafkaAvroSerializer}, {@code KafkaAvroDeserializer},
 * etc.) but never sets {@code schema.registry.url} via a {@code Properties.put(...)}
 * call in the same class.
 *
 * Without a registry URL, the serializer either fails at construction or on
 * the first record — and the resulting stack trace is a deep
 * {@code ClassCastException} or null-pointer, not an obvious 'missing config'.
 *
 * Each anti-pattern method below builds an SR serializer with NO call to
 * {@code put("schema.registry.url", ...)} in the class — the rule must fire
 * at the {@code NEW} site in each method.
 */
public final class BadSchemaRegistryUrlMissing {

    /** Anti-pattern: KafkaAvroSerializer constructed, no schema.registry.url set. */
    public KafkaAvroSerializer makeAvroSerializer() {
        Properties p = new Properties();
        p.put("acks", "all"); // unrelated config, no SR URL
        return new KafkaAvroSerializer(); // FIRES
    }

    /** Anti-pattern: KafkaAvroDeserializer in a method that touches no config at all. */
    public KafkaAvroDeserializer makeAvroDeserializer() {
        return new KafkaAvroDeserializer(); // FIRES
    }

    /** Anti-pattern: two SR sites in one method body, neither paired with a URL config. */
    public void makeBoth() {
        KafkaAvroSerializer s = new KafkaAvroSerializer();   // FIRES
        KafkaAvroDeserializer d = new KafkaAvroDeserializer(); // FIRES
        s.toString();
        d.toString();
    }
}
