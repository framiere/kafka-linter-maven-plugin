package sample;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;

import java.time.Duration;
import java.util.List;
import java.util.Properties;

/**
 * RULE: CONSUMER_SEEK_BEFORE_POLL.
 *
 * subscribe() is non-blocking; the group join only happens during the first poll().
 * Calling seek() between subscribe() and poll() throws IllegalStateException because
 * the consumer's assignment() is still empty.
 */
public final class BadSeekBeforePoll {

    public void run() {
        Properties p = new Properties();
        p.put("bootstrap.servers", "kafka-1.prod.example.com:9092");
        p.put("client.id", "bad-seek-consumer");
        p.put("group.id", "bad-seek-group");
        p.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        p.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(p);

        consumer.subscribe(List.of("orders"));
        consumer.seek(new TopicPartition("orders", 0), 100L);
        consumer.poll(Duration.ofSeconds(1));
    }
}
