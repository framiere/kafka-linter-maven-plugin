package sample;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

/**
 * Silent: class configures value.serializer = StringSerializer
 * (class-level gate satisfied) and ONLY emits ProducerRecord with
 * String-typed values — the rule's per-call check finds a
 * String-source producer instruction and does not fire.
 *
 * Each method exercises one of the value-source shapes the rule
 * recognizes as String:
 *   - LDC of a String literal — `ldc.cst instanceof String` → silent.
 *   - INVOKE returning java/lang/String — return type descriptor is
 *     "Ljava/lang/String;" → silent.
 *   - GETFIELD whose desc is "Ljava/lang/String;" → silent.
 *
 * The class-level gate is the same as the Bad fixture — same shape of
 * `props.put("value.serializer", StringSerializer.class)`.
 */
public class GoodStringSerializerWithStringValues {

    private final String cachedString = "cached-value";
    private final KafkaProducer<String, String> producer;

    public GoodStringSerializerWithStringValues() {
        Properties props = new Properties();
        props.put("bootstrap.servers", "kafka-1:9092,kafka-2:9092,kafka-3:9092");
        props.put("client.id", "good-string-serializer-string-values");
        props.put("acks", "all");
        props.put("enable.idempotence", "true");
        props.put("compression.type", "zstd");
        props.put("key.serializer", StringSerializer.class.getName());
        props.put("value.serializer", StringSerializer.class);
        this.producer = new KafkaProducer<>(props);
    }

    /** Shape A: LDC of a String literal — the canonical happy path. */
    public void sendStringLiteral() {
        producer.send(new ProducerRecord<>("orders", "k", "checkout-event-payload"));
    }

    /** Shape B: method whose return type IS java/lang/String. */
    public void sendStringMethodReturn() {
        producer.send(new ProducerRecord<>("orders", "k", buildPayloadString()));
    }

    /** Shape C: GETFIELD of a String-typed field. */
    public void sendStringField() {
        producer.send(new ProducerRecord<>("orders", "k", cachedString));
    }

    private String buildPayloadString() {
        return "built-payload";
    }
}
