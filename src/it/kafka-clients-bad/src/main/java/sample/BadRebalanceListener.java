package sample;

import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;

import java.time.Duration;
import java.util.Collection;
import java.util.List;
import java.util.Properties;

/**
 * RULE: POLL_IN_REBALANCE_CALLBACK.
 *
 * The rebalance listener calls consumer.poll() inside onPartitionsRevoked — the callback
 * already runs on the poll thread mid-rebalance, so the recursive poll throws
 * IllegalStateException in modern Kafka.
 */
public final class BadRebalanceListener {

    private final KafkaConsumer<String, String> consumer;

    public BadRebalanceListener() {
        Properties p = new Properties();
        p.put("bootstrap.servers", "kafka-1.prod.example.com:9092,kafka-2.prod.example.com:9092,kafka-3.prod.example.com:9092");
        p.put("client.id", "bad-rebalance-consumer");
        p.put("group.id", "bad-rebalance-group");
        p.put("enable.auto.commit", "false");
        this.consumer = new KafkaConsumer<>(p);
    }

    public void subscribe() {
        consumer.subscribe(List.of("t"), new ConsumerRebalanceListener() {
            @Override
            public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
                consumer.poll(Duration.ofMillis(10));
            }

            @Override
            public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
                consumer.poll(Duration.ofMillis(10));
            }
        });
    }
}
