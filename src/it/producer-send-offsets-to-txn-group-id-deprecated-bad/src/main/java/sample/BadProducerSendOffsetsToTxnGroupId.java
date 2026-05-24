package sample;

import org.apache.kafka.clients.consumer.ConsumerConfig;
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
 * RULE: PRODUCER_SEND_OFFSETS_TO_TXN_GROUP_ID_DEPRECATED.
 *
 * <p>Exercises two shapes the rule must catch:
 *
 * <ol>
 *   <li>Direct {@code INVOKEVIRTUAL} on
 *       {@code sendOffsetsToTransaction(Map, String groupId)} — descriptor
 *       {@code (Ljava/util/Map;Ljava/lang/String;)V}, the deprecated overload.</li>
 *   <li>{@code INVOKEDYNAMIC} method-reference capture
 *       {@code producer::sendOffsetsToTransaction} stored into a
 *       {@code BiConsumer<Map<TopicPartition, OffsetAndMetadata>, String>} —
 *       SAM signature {@code void accept(Map, String)} forces overload
 *       resolution to the deprecated (Map, String) overload. The user-class
 *       bytecode contains zero {@code INVOKE*} targeting
 *       {@code sendOffsetsToTransaction}; the rule's {@code INVOKEDYNAMIC}
 *       walk detects the {@code REF_invokeVirtual} capture and matches its
 *       descriptor exactly against
 *       {@code (Ljava/util/Map;Ljava/lang/String;)V}.</li>
 * </ol>
 *
 * <h2>Why this overload corrupts EOS pipelines</h2>
 *
 * <p>The classic read-process-write loop uses a transactional producer that
 * carries the consumer's committed offset alongside the produced records,
 * atomically. {@code sendOffsetsToTransaction} appends an offset commit to
 * the producer's in-flight transaction. The deprecated
 * {@code (Map, String groupId)} overload identifies the consumer group by
 * NAME alone — the broker has no proof that this particular producer is
 * the legitimate owner of those partitions in the current generation.
 *
 * <p>The failure mode: a network-partitioned 'zombie' producer that still
 * holds an open transaction can therefore commit offsets on behalf of a
 * consumer that has long since been rebalanced away from those partitions.
 * After KIP-447 (Kafka 2.5 broker / 3.0 client) the {@code (Map,
 * ConsumerGroupMetadata)} overload carries the consumer's generation
 * and member ID; brokers reject commits from stale producers, closing the
 * zombie-overwrite window that breaks Exactly-Once Semantics. The
 * deprecation is not a stylistic preference — it is a correctness gap.
 *
 * <h2>Why so many sibling rules are OFF</h2>
 *
 * <p>Transactional EOS code touches a lot of surface area
 * ({@code initTransactions}, {@code beginTransaction},
 * {@code sendOffsetsToTransaction}, {@code commitTransaction}, plus the
 * supporting Producer / Consumer constructors). To keep the build-failure
 * cause attributable to
 * {@code PRODUCER_SEND_OFFSETS_TO_TXN_GROUP_ID_DEPRECATED} alone, the
 * fixture's pom OFFs the unrelated sibling rules.
 */
public final class BadProducerSendOffsetsToTxnGroupId {

    private static Properties producerProps() {
        Properties p = new Properties();
        p.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "kafka-1:9092,kafka-2:9092,kafka-3:9092");
        p.put(ProducerConfig.CLIENT_ID_CONFIG, "bad-send-offsets-groupid");
        p.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, "txn-bad-send-offsets-groupid");
        p.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
        p.put(ProducerConfig.ACKS_CONFIG, "all");
        p.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        p.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        return p;
    }

    private static Properties consumerProps() {
        Properties p = new Properties();
        p.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "kafka-1:9092,kafka-2:9092,kafka-3:9092");
        p.put(ConsumerConfig.CLIENT_ID_CONFIG, "bad-send-offsets-groupid-consumer");
        p.put(ConsumerConfig.GROUP_ID_CONFIG, "txn-input-group");
        p.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        p.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        p.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, "60000");
        p.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "100");
        p.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        return p;
    }

    public void sendOffsetsWithGroupIdString() {
        Map<TopicPartition, OffsetAndMetadata> offsets =
                Map.of(new TopicPartition("input", 0), new OffsetAndMetadata(42L));
        try (KafkaProducer<String, String> producer = new KafkaProducer<>(producerProps());
             KafkaConsumer<String, String> consumer = new KafkaConsumer<>(consumerProps())) {
            consumer.subscribe(List.of("input"));
            producer.initTransactions();
            producer.beginTransaction();
            producer.send(new ProducerRecord<>("output", "k", "v"));
            // FIRES — direct INVOKEVIRTUAL on the deprecated (Map, String) overload.
            // The String "txn-input-group" identifies only the group name; the broker has
            // no proof this producer is the legitimate owner of partition 0 in the current
            // generation, so a rebalanced consumer's offsets can be silently overwritten by
            // a stale producer that still holds an open transaction.
            producer.sendOffsetsToTransaction(offsets, "txn-input-group");
            producer.commitTransaction();
            producer.close(Duration.ofSeconds(5));
            consumer.close(Duration.ofSeconds(5));
        }
    }

    public void sendOffsetsAsMethodReferenceCapture() {
        Map<TopicPartition, OffsetAndMetadata> offsets =
                Map.of(new TopicPartition("input", 0), new OffsetAndMetadata(42L));
        try (KafkaProducer<String, String> producer = new KafkaProducer<>(producerProps())) {
            producer.initTransactions();
            producer.beginTransaction();
            producer.send(new ProducerRecord<>("output", "k", "v"));
            // FIRES — INVOKEDYNAMIC method-ref capture. SAM signature
            // void accept(Map, String) forces overload resolution to the
            // deprecated (Map, String) overload. The user-class bytecode contains
            // ZERO INVOKE* instructions targeting sendOffsetsToTransaction.
            BiConsumer<Map<TopicPartition, OffsetAndMetadata>, String> deferred =
                    producer::sendOffsetsToTransaction;
            deferred.accept(offsets, "txn-input-group");
            producer.commitTransaction();
            producer.close(Duration.ofSeconds(5));
        }
    }
}
