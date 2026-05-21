package sample;

import org.apache.kafka.clients.producer.KafkaProducer;

import java.util.Properties;

/**
 * RULE: PROPERTIES_MUTATED_AFTER_CTOR.
 *
 * The KafkaProducer constructor copies the Properties into an internal ProducerConfig.
 * Any put() after construction is silently dropped — the live client never observes the
 * late linger.ms change.
 */
public final class BadPropertiesMutatedAfterCtor {

    public KafkaProducer<String, String> build() {
        Properties props = new Properties();
        props.put("bootstrap.servers", "kafka-1.prod.example.com:9092");
        props.put("client.id", "bad-mutated-producer");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        KafkaProducer<String, String> producer = new KafkaProducer<>(props);
        props.put("linger.ms", "100");
        props.setProperty("batch.size", "32768");
        return producer;
    }
}
