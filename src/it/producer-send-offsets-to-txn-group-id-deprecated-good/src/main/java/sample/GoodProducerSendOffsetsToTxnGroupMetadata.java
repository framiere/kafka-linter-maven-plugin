package sample;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerGroupMetadata;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.function.BiConsumer;

/**
 * RULE: PRODUCER_SEND_OFFSETS_TO_TXN_GROUP_ID_DEPRECATED — must NOT fire.
 *
 * <p>The two methods below exercise the supported
 * {@code sendOffsetsToTransaction(Map, ConsumerGroupMetadata)} overload — the
 * one introduced by KIP-447 and recommended since Kafka 3.0.
 *
 * <ol>
 *   <li>Direct {@code INVOKEVIRTUAL} on
 *       {@code sendOffsetsToTransaction(Map, ConsumerGroupMetadata)} — descriptor
 *       {@code (Ljava/util/Map;Lorg/apache/kafka/clients/consumer/ConsumerGroupMetadata;)V},
 *       NOT the deprecated {@code (Map, String)} shape.</li>
 *   <li>{@code INVOKEDYNAMIC} method-reference capture
 *       {@code producer::sendOffsetsToTransaction} stored into a
 *       {@code BiConsumer<Map<TopicPartition, OffsetAndMetadata>, ConsumerGroupMetadata>}
 *       — SAM signature {@code void accept(Map, ConsumerGroupMetadata)} forces
 *       overload resolution to the SUPPORTED overload. The rule's
 *       {@code INVOKEDYNAMIC} walk resolves the bsmArg handle's descriptor and
 *       sees {@code (Ljava/util/Map;Lorg/apache/kafka/clients/consumer/ConsumerGroupMetadata;)V},
 *       NOT {@code (Ljava/util/Map;Ljava/lang/String;)V}, so it does not fire.</li>
 * </ol>
 *
 * <h2>Why this is the safe shape</h2>
 *
 * <p>{@code consumer.groupMetadata()} returns a {@link ConsumerGroupMetadata}
 * snapshot that bundles together: the consumer group name, the consumer's
 * member ID, and the consumer's current generation ID. The broker checks all
 * three when accepting the offset commit appended to the producer's
 * transaction. If a rebalance has moved the partition to a different consumer
 * instance — bumping the generation ID — a stale producer that still holds an
 * open transaction with the previous generation's metadata will have its
 * commit REJECTED by the broker. The transaction aborts; the new owner's
 * processing remains the source of truth for those offsets.
 *
 * <p>This is the zombie-fencing guarantee that closes the EOS correctness
 * gap of the deprecated {@code (Map, String)} overload.
 */
public final class GoodProducerSendOffsetsToTxnGroupMetadata {

    private static Properties producerProps() {
        Properties p = new Properties();
        p.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "kafka-1:9092,kafka-2:9092,kafka-3:9092");
        p.put(ProducerConfig.CLIENT_ID_CONFIG, "good-send-offsets-groupmeta");
        p.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, "txn-good-send-offsets-groupmeta");
        p.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
        p.put(ProducerConfig.ACKS_CONFIG, "all");
        p.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        p.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        return p;
    }

    private static Properties consumerProps() {
        Properties p = new Properties();
        p.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "kafka-1:9092,kafka-2:9092,kafka-3:9092");
        p.put(ConsumerConfig.CLIENT_ID_CONFIG, "good-send-offsets-groupmeta-consumer");
        p.put(ConsumerConfig.GROUP_ID_CONFIG, "txn-input-group");
        p.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        p.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        p.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, "60000");
        p.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "100");
        p.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        return p;
    }

    public void sendOffsetsWithGroupMetadata() {
        Map<TopicPartition, OffsetAndMetadata> offsets =
                Map.of(new TopicPartition("input", 0), new OffsetAndMetadata(42L));
        try (KafkaProducer<String, String> producer = new KafkaProducer<>(producerProps());
             KafkaConsumer<String, String> consumer = new KafkaConsumer<>(consumerProps())) {
            consumer.subscribe(List.of("input"));
            producer.initTransactions();
            producer.beginTransaction();
            producer.send(new ProducerRecord<>("output", "k", "v"));
            // DOES NOT FIRE — direct INVOKEVIRTUAL on the SUPPORTED
            // (Map, ConsumerGroupMetadata) overload. The metadata snapshot
            // bundles group name + member ID + generation ID; the broker
            // fences the commit against the consumer's CURRENT generation,
            // so a zombie producer's stale commit will be rejected.
            producer.sendOffsetsToTransaction(offsets, consumer.groupMetadata());
            producer.commitTransaction();
            producer.close(Duration.ofSeconds(5));
            consumer.close(Duration.ofSeconds(5));
        }
    }

    public void sendOffsetsAsGroupMetadataMethodReferenceCapture() {
        Map<TopicPartition, OffsetAndMetadata> offsets =
                Map.of(new TopicPartition("input", 0), new OffsetAndMetadata(42L));
        try (KafkaProducer<String, String> producer = new KafkaProducer<>(producerProps());
             KafkaConsumer<String, String> consumer = new KafkaConsumer<>(consumerProps())) {
            consumer.subscribe(List.of("input"));
            producer.initTransactions();
            producer.beginTransaction();
            producer.send(new ProducerRecord<>("output", "k", "v"));
            // DOES NOT FIRE — INVOKEDYNAMIC method-ref capture into a
            // BiConsumer<Map, ConsumerGroupMetadata>. SAM signature
            // void accept(Map, ConsumerGroupMetadata) forces overload
            // resolution to the SUPPORTED overload. The user-class bytecode
            // contains zero INVOKE* targeting sendOffsetsToTransaction; the
            // rule's INVOKEDYNAMIC walk inspects the resolved handle's
            // descriptor and sees (Ljava/util/Map;Lorg/apache/kafka/clients/
            // consumer/ConsumerGroupMetadata;)V — distinct from the
            // deprecated (Ljava/util/Map;Ljava/lang/String;)V — so the
            // descriptor post-filter rejects it.
            BiConsumer<Map<TopicPartition, OffsetAndMetadata>, ConsumerGroupMetadata> deferred =
                    producer::sendOffsetsToTransaction;
            deferred.accept(offsets, consumer.groupMetadata());
            producer.commitTransaction();
            producer.close(Duration.ofSeconds(5));
            consumer.close(Duration.ofSeconds(5));
        }
    }
}
