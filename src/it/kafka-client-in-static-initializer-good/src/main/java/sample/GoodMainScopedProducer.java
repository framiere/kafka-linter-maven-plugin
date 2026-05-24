package sample;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

/**
 * Good shape #2 — producer constructed in {@code main} as a try-with-resources.
 * Lifecycle is explicit and bound to the application entry point.
 */
public final class GoodMainScopedProducer {

    private GoodMainScopedProducer() {}

    public static void main(String[] args) {
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
            producer.send(new ProducerRecord<>("topic", "key", "value"));
        }
    }
}
