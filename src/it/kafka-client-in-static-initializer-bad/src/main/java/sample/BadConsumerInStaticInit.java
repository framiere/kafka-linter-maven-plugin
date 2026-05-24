package sample;

import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Properties;

/**
 * Bad shape #2 — KafkaConsumer constructed in a static-initializer block.
 */
public final class BadConsumerInStaticInit {

    /** Violation #2: NEW KafkaConsumer inside &lt;clinit&gt;. */
    static final KafkaConsumer<String, String> CONSUMER;

    static {
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("group.id", "bad-static-init-group");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        CONSUMER = new KafkaConsumer<>(props);
    }

    private BadConsumerInStaticInit() {}
}
