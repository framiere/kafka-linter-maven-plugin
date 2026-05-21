package sample;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.time.Duration;
import java.util.List;
import java.util.Properties;

/**
 * RULE: COMMIT_ASYNC_NO_FINAL_SYNC.
 *
 * commitAsync is called inside the poll loop for throughput, but the class never calls
 * commitSync anywhere — so the last async commit can be lost on shutdown and the next
 * consumer in the group re-processes records.
 */
public final class BadCommitAsync {

    private final KafkaConsumer<String, String> consumer;

    public BadCommitAsync() {
        Properties p = new Properties();
        p.put("bootstrap.servers", "kafka-1.prod.example.com:9092,kafka-2.prod.example.com:9092,kafka-3.prod.example.com:9092");
        p.put("client.id", "bad-commit-async-consumer");
        p.put("group.id", "bad-commit-async-group");
        p.put("enable.auto.commit", "false");
        this.consumer = new KafkaConsumer<>(p);
    }

    public void run() {
        consumer.subscribe(List.of("t"));
        while (running()) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(500));
            for (ConsumerRecord<String, String> r : records) {
                handle(r);
            }
            consumer.commitAsync();
        }
    }

    public void shutdown() {
        consumer.close(Duration.ofSeconds(20));
    }

    private boolean running() { return true; }
    private void handle(ConsumerRecord<String, String> r) {}
}
