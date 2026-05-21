package sample;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;

import java.util.Properties;

/**
 * RULE: PRODUCER_PER_RECORD_ALLOCATION.
 *
 * A Spring @PostMapping handler constructs a fresh KafkaProducer per HTTP request, paying
 * the full network-thread + Sender-thread + accumulator (32 MiB) + metadata-fetch +
 * TLS-handshake cost on every call. KafkaProducer should be a long-lived shared singleton.
 */
public final class BadProducerPerRequest {

    @PostMapping("/event")
    public void publish(@RequestBody String body) {
        Properties props = new Properties();
        props.put("bootstrap.servers", "kafka-1.prod.example.com:9092");
        props.put("client.id", "bad-per-request-producer");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
            producer.send(new ProducerRecord<>("events", "k", body));
        }
    }
}
