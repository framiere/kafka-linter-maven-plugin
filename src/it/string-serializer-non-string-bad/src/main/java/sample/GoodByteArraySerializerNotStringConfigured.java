package sample;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

/**
 * Silent: the class-level gate is NOT satisfied — this class
 * configures value.serializer = ByteArraySerializer (NOT
 * StringSerializer), so the rule's classConfiguresStringValueSerializer
 * check returns false and NO method in this class is scanned for
 * the per-call value-type check.
 *
 * The rule's gate is: "does THIS class declare it serializes values
 * as String?". If not, the value-type check is silent regardless of
 * what value the ProducerRecord constructor receives — because the
 * accidental-Object.toString hazard simply doesn't exist when the
 * configured serializer is one that handles bytes correctly.
 *
 * This fixture exercises the gate's negative branch: emitting a
 * `byte[]` payload via a ByteArraySerializer-configured producer is
 * the CORRECT shape. The rule must not fire just because a non-String
 * value is being sent — only when StringSerializer is configured AND
 * the value is non-String do we have the .toString() hazard.
 */
public class GoodByteArraySerializerNotStringConfigured {

    private final KafkaProducer<String, byte[]> producer;

    public GoodByteArraySerializerNotStringConfigured() {
        Properties props = new Properties();
        props.put("bootstrap.servers", "kafka-1:9092,kafka-2:9092,kafka-3:9092");
        props.put("client.id", "good-bytes-serializer");
        props.put("acks", "all");
        props.put("enable.idempotence", "true");
        props.put("compression.type", "zstd");
        props.put("key.serializer", StringSerializer.class.getName());
        // CLASS-LEVEL GATE: value.serializer is ByteArraySerializer, not
        // StringSerializer — the rule does not scan this class.
        props.put("value.serializer", ByteArraySerializer.class);
        this.producer = new KafkaProducer<>(props);
    }

    /**
     * Sends a byte[] value through a ByteArraySerializer-configured
     * producer. This is the correctly-paired shape — the value-type
     * matches the serializer.
     */
    public void sendBytePayload() {
        producer.send(new ProducerRecord<>("orders", "k", new byte[]{1, 2, 3, 4}));
    }
}
