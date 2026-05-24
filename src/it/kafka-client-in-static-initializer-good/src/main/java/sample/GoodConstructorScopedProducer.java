package sample;

import org.apache.kafka.clients.producer.KafkaProducer;

import java.util.Properties;

/**
 * Good shape #1 — producer constructed in the instance constructor.
 * No NEW inside &lt;clinit&gt;, lifecycle owned by the instance.
 */
public final class GoodConstructorScopedProducer implements AutoCloseable {

    private final KafkaProducer<String, String> producer;

    public GoodConstructorScopedProducer(Properties props) {
        this.producer = new KafkaProducer<>(props);
    }

    @Override
    public void close() {
        producer.close();
    }
}
