package sample;

import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;

import java.util.Map;
import java.util.Properties;

/**
 * RULE: PRODUCER_INIT_TRANSACTIONS_NOT_CALLED.
 *
 * Constructs a transactional producer (transactional.id set in props) and exercises
 * every transactional lifecycle method WITHOUT ever calling producer.initTransactions().
 *
 * At runtime, the first transactional call would throw:
 *   org.apache.kafka.common.KafkaException:
 *   Cannot perform 'beginTransaction' before transactions have been initialized.
 *
 * The rule fires once per call site on the four lifecycle methods. The owning
 * module (kafka-clients-bad) has no initTransactions() anywhere — the
 * project-scoped check correctly identifies the missing init across the module.
 */
public final class BadProducerInitTransactionsNotCalled {

    private static Properties txnProps() {
        Properties p = new Properties();
        p.put("bootstrap.servers", "broker:9092");
        p.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        p.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        p.put("transactional.id", "tx-app-no-init");
        p.put("enable.idempotence", "true");
        return p;
    }

    public void beginCommitNoInit() {
        KafkaProducer<String, String> producer = new KafkaProducer<>(txnProps());
        producer.beginTransaction();  // FIRES
        producer.send(new ProducerRecord<>("orders", "k", "v"));
        producer.commitTransaction();  // FIRES
        producer.close();
    }

    public void beginAbortNoInit() {
        KafkaProducer<String, String> producer = new KafkaProducer<>(txnProps());
        producer.beginTransaction();  // FIRES
        try {
            producer.send(new ProducerRecord<>("orders", "k", "v"));
            throw new IllegalStateException("simulated failure");
        } catch (RuntimeException e) {
            producer.abortTransaction();  // FIRES
        }
        producer.close();
    }

    public void sendOffsetsToTxnNoInit() {
        KafkaProducer<String, String> producer = new KafkaProducer<>(txnProps());
        producer.beginTransaction();  // FIRES
        Map<TopicPartition, OffsetAndMetadata> offsets =
                Map.of(new TopicPartition("in", 0), new OffsetAndMetadata(42L));
        producer.sendOffsetsToTransaction(offsets,
                new org.apache.kafka.clients.consumer.ConsumerGroupMetadata("g"));  // FIRES
        producer.commitTransaction();  // FIRES
        producer.close();
    }
}
