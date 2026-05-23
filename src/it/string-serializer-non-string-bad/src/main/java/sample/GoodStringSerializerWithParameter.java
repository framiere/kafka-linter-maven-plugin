package sample;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

/**
 * Silent: documented FALSE-NEGATIVE — class-level gate is satisfied
 * (value.serializer = StringSerializer is configured), but the
 * ProducerRecord value argument comes from an ALOAD of a method
 * parameter. The rule walks back from the ProducerRecord constructor
 * and lands on an ALOAD instruction — the rule's provablyNonString
 * check does NOT recognize ALOAD (it would require a full type-tracking
 * data-flow analyzer to infer the local-variable type), so the
 * default-fallback `return false` triggers: the rule abstains.
 *
 * This is a deliberate trade-off:
 *   - Pro: zero false positives from local-variable type confusion.
 *   - Con: misses the case where a non-String parameter is silently
 *          shipped through StringSerializer.
 *
 * In practice this false-negative is acceptable because:
 *   (a) the in-line, statically-known shapes (NEW <DomainEvent>,
 *       method return, byte[] field, primitive array) capture the
 *       overwhelming majority of real-world incidence; teams that
 *       set up StringSerializer typically use String literals or
 *       String-returning methods, so a non-String parameter is the
 *       outlier.
 *   (b) the rule's sibling — the per-instance ProducerRecord type
 *       parameter inference at the Kafka call site itself (handled
 *       by javac generics) — catches this at compile time when the
 *       producer is declared with explicit type parameters (a
 *       `KafkaProducer<String, String>` will get a Java compile
 *       error if you try to pass a non-String value).
 *
 * This class declares the producer as parameterized
 * `KafkaProducer<String, String>` so the javac generics check would
 * actually catch a mistyped value at compile time — meaning the
 * runtime mistake the rule guards against is the
 * raw-type / Object-erasure shape, not the parameterized shape.
 * Including this fixture documents the boundary explicitly.
 */
public class GoodStringSerializerWithParameter {

    private final KafkaProducer<String, String> producer;

    public GoodStringSerializerWithParameter() {
        Properties props = new Properties();
        props.put("bootstrap.servers", "kafka-1:9092,kafka-2:9092,kafka-3:9092");
        props.put("client.id", "good-string-serializer-parameter");
        props.put("acks", "all");
        props.put("enable.idempotence", "true");
        props.put("compression.type", "zstd");
        props.put("key.serializer", StringSerializer.class.getName());
        props.put("value.serializer", StringSerializer.class);
        this.producer = new KafkaProducer<>(props);
    }

    /**
     * Value arg is ALOAD — the rule abstains. Documented false-negative.
     */
    public void sendFromParameter(String payload) {
        producer.send(new ProducerRecord<>("orders", "k", payload));
    }
}
