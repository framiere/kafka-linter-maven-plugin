package sample;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

/**
 * RULE: PRODUCER_NOT_CLOSED.
 *
 * KafkaProducer is constructed and used (send) inside a method but never
 * closed on the same local slot. On JVM exit the Sender thread is killed
 * mid-batch and records still in the accumulator (default 32 MiB) are
 * silently lost.
 */
public final class BadProducerNotClosed {

    public void sendOneRecordWithoutClose(ProducerRecord<String, String> record) {
        Properties props = new Properties();
        props.put("bootstrap.servers", "kafka-1.prod.example.com:9092");
        props.put("client.id", "bad-not-closed-producer");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        KafkaProducer<String, String> producer = new KafkaProducer<>(props);
        producer.send(record);
        // No close() — Sender thread killed mid-batch, in-accumulator records lost on JVM exit.
    }
}
