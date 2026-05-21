package sample;

import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;

import java.time.Duration;
import java.util.List;
import java.util.Properties;

/**
 * Correct Kafka usage — the linter must report zero violations.
 *  - Producer/Consumer instantiated once, in the constructor.
 *  - compression.type set explicitly.
 *  - send() uses a callback.
 *  - poll() uses a non-zero Duration.
 *  - commitSync is called per BATCH (after the inner loop), still inside the outer poll loop, and uses a bounded Duration.
 *  - enable.auto.commit explicitly disabled.
 */
public final class GoodKafkaUsage {

    private final KafkaProducer<String, String> producer;
    private final KafkaConsumer<String, String> consumer;

    public GoodKafkaUsage() {
        Properties producerProps = new Properties();
        producerProps.put("bootstrap.servers", "kafka-1.prod.example.com:9092,kafka-2.prod.example.com:9092,kafka-3.prod.example.com:9092");
        producerProps.put("client.id", "orders-svc-producer-i07");
        producerProps.put("compression.type", "snappy");
        producerProps.put("linger.ms", "20");
        this.producer = new KafkaProducer<>(producerProps);

        Properties consumerProps = new Properties();
        consumerProps.put("bootstrap.servers", "kafka-1.prod.example.com:9092,kafka-2.prod.example.com:9092,kafka-3.prod.example.com:9092");
        consumerProps.put("client.id", "orders-svc-consumer-i07");
        consumerProps.put("group.id", "my-group");
        consumerProps.put("enable.auto.commit", "false");
        this.consumer = new KafkaConsumer<>(consumerProps);
    }

    public void publish(String topic, String key, String message) {
        producer.send(new ProducerRecord<>(topic, key, message), (md, ex) -> {
            if (ex != null) {
                System.err.println("send failed: " + ex.getMessage());
            }
        });
    }

    public void publishBatch(List<String> keys, List<String> messages) {
        for (int i = 0; i < messages.size(); i++) {
            producer.send(new ProducerRecord<>("topic", keys.get(i), messages.get(i)), (md, ex) -> {});
        }
    }

    public void consume() {
        consumer.subscribe(List.of("my-topic"), new ConsumerRebalanceListener() {
            @Override public void onPartitionsRevoked(java.util.Collection<TopicPartition> partitions) {
                consumer.commitSync(Duration.ofSeconds(10));
            }
            @Override public void onPartitionsAssigned(java.util.Collection<TopicPartition> partitions) {}
        });
        while (running()) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(500));
            for (ConsumerRecord<String, String> r : records) {
                handle(r);
            }
            consumer.commitSync(Duration.ofSeconds(10));
        }
    }

    public void shutdown() {
        producer.flush();
        producer.close(Duration.ofSeconds(20));
        consumer.close(Duration.ofSeconds(20));
    }

    private boolean running() { return true; }
    private void handle(ConsumerRecord<String, String> r) {}
}
