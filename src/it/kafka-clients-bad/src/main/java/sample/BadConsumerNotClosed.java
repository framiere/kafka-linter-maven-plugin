package sample;

import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.time.Duration;
import java.util.List;
import java.util.Properties;

/**
 * RULE: CONSUMER_NOT_CLOSED.
 *
 * KafkaConsumer is constructed and used (subscribe + poll) inside a method but
 * never closed on the same local slot. close() is the only call that issues
 * LeaveGroup to the coordinator; without it the partitions stall behind the
 * dead member for session.timeout.ms on every restart.
 */
public final class BadConsumerNotClosed {

    public void subscribeAndPollWithoutClose() {
        Properties props = new Properties();
        props.put("bootstrap.servers", "kafka-1.prod.example.com:9092");
        props.put("group.id", "orders-readers");
        props.put("client.id", "bad-not-closed-consumer");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(List.of("orders"));
        ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(500));
        records.count();
        // No close() — LeaveGroup never sent, coordinator waits session.timeout.ms.
    }
}
