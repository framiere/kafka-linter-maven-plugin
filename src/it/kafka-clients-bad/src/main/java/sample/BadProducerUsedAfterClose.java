package sample;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

/**
 * RULE: PRODUCER_USED_AFTER_CLOSE.
 *
 * KafkaProducer has a one-way lifecycle: OPEN → CLOSED. After close() the
 * Sender thread is stopped, the accumulator is drained, and the internal
 * closed flag is set. Every subsequent producer-facing call throws
 * IllegalStateException — silently in async callbacks, fatally on the
 * main thread.
 */
public final class BadProducerUsedAfterClose {

    public void shutdownThenSend(ProducerRecord<String, String> record) {
        Properties props = new Properties();
        props.put("bootstrap.servers", "kafka-1.prod.example.com:9092");
        props.put("client.id", "bad-used-after-close");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        KafkaProducer<String, String> producer = new KafkaProducer<>(props);
        producer.close();
        producer.send(record);
        producer.flush();
    }
}
