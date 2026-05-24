package sample;

import org.apache.kafka.clients.producer.KafkaProducer;

import java.util.Properties;

/**
 * Bad shape #1 — static-final KafkaProducer constructed inline in a static
 * initializer block. javac emits NEW KafkaProducer directly into &lt;clinit&gt;.
 */
public final class BadProducerInStaticInit {

    private static final Properties PROPS;
    /** Violation #1: NEW KafkaProducer inside &lt;clinit&gt; (inline initializer). */
    private static final KafkaProducer<String, String> PRODUCER;

    static {
        PROPS = new Properties();
        PROPS.put("bootstrap.servers", "localhost:9092");
        PROPS.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        PROPS.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        PRODUCER = new KafkaProducer<>(PROPS);
    }

    private BadProducerInStaticInit() {}

    public static KafkaProducer<String, String> get() {
        return PRODUCER;
    }
}
