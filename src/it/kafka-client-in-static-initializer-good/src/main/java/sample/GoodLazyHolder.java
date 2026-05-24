package sample;

import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Properties;

/**
 * Good shape #3 — lazy initialization via a method, not &lt;clinit&gt;.
 * The {@code NEW KafkaConsumer} lives in {@link #getOrCreate()}, not in the
 * synthesized class initializer.
 */
public final class GoodLazyHolder implements AutoCloseable {

    private volatile KafkaConsumer<String, String> consumer;

    public synchronized KafkaConsumer<String, String> getOrCreate(Properties props) {
        if (consumer == null) {
            consumer = new KafkaConsumer<>(props);
        }
        return consumer;
    }

    @Override
    public synchronized void close() {
        if (consumer != null) {
            consumer.close();
        }
    }
}
