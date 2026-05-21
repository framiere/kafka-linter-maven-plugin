package sample;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

/**
 * RULE: STRING_SERIALIZER_NON_STRING.
 *
 * value.serializer = StringSerializer but ProducerRecord values are statically
 * NOT String. StringSerializer's serialize() returns data.toString().getBytes(),
 * so the wire payload becomes the accidental Object.toString() — `[B@…` for
 * byte[], `OrderEvent[id=…]` for a record, etc.
 *
 * The value-producer at the ProducerRecord constructor call site must be a
 * statically-typed non-String for the rule to fire:
 *  - inline NEWARRAY / ANEWARRAY (byte[] / Object[] literal)
 *  - inline NEW (new SomeType())
 *  - GETSTATIC of a non-String field
 *  - INVOKE returning a non-String type
 */
public final class BadStringSerializerNonString {

    public record OrderEvent(int id, String name) {}

    private static final OrderEvent CONSTANT_EVENT = new OrderEvent(42, "Alice");

    private Properties buildProps() {
        Properties props = new Properties();
        props.put("bootstrap.servers", "kafka-1.prod.example.com:9092");
        props.put("client.id", "bad-string-serializer");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        return props;
    }

    public void sendInlineByteArray() {
        try (KafkaProducer<String, byte[]> producer = new KafkaProducer<>(buildProps())) {
            producer.send(new ProducerRecord<>("orders", "k1", new byte[]{0x00, 0x01, 0x02, 0x03}));
        }
    }

    public void sendInlineDomainRecord() {
        try (KafkaProducer<String, OrderEvent> producer = new KafkaProducer<>(buildProps())) {
            producer.send(new ProducerRecord<>("orders", "k2", new OrderEvent(7, "Bob")));
        }
    }

    public void sendStaticFieldPayload() {
        try (KafkaProducer<String, OrderEvent> producer = new KafkaProducer<>(buildProps())) {
            producer.send(new ProducerRecord<>("orders", "k3", CONSTANT_EVENT));
        }
    }

    public void sendMethodReturnPayload() {
        try (KafkaProducer<String, OrderEvent> producer = new KafkaProducer<>(buildProps())) {
            producer.send(new ProducerRecord<>("orders", "k4", buildEvent()));
        }
    }

    private OrderEvent buildEvent() {
        return new OrderEvent(99, "Carol");
    }
}
