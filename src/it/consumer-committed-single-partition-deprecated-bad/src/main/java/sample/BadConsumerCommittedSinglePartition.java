package sample;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.List;
import java.util.Properties;
import java.util.function.Function;

/**
 * RULE: CONSUMER_COMMITTED_SINGLE_PARTITION_DEPRECATED.
 *
 * <p>Exercises three shapes the rule must catch:
 *
 * <ol>
 *   <li>Direct {@code INVOKEVIRTUAL} on {@code committed(TopicPartition)} —
 *       the no-timeout single-partition overload. Descriptor
 *       {@code (Lorg/apache/kafka/common/TopicPartition;)Lorg/apache/kafka/clients/consumer/OffsetAndMetadata;}.</li>
 *   <li>Direct {@code INVOKEVIRTUAL} on
 *       {@code committed(TopicPartition, Duration)} — the timeout-bounded
 *       single-partition overload. Same deprecation status; same one-fetch-per-partition
 *       cost. Descriptor
 *       {@code (Lorg/apache/kafka/common/TopicPartition;Ljava/time/Duration;)Lorg/apache/kafka/clients/consumer/OffsetAndMetadata;}.</li>
 *   <li>{@code INVOKEDYNAMIC} method-reference capture
 *       {@code consumer::committed} stored into a
 *       {@code Function<TopicPartition, OffsetAndMetadata>} — Java picks the
 *       deprecated single-partition overload because the SAM signature
 *       {@code R apply(TopicPartition)} matches it. The user-class bytecode
 *       contains zero {@code INVOKE*} instructions targeting
 *       {@code committed}; the rule's INVOKEDYNAMIC walk detects the
 *       {@code REF_invokeVirtual} capture and checks the resolved handle's
 *       descriptor prefix.</li>
 * </ol>
 *
 * <h2>Why the loop shape is what gets shipped to production</h2>
 *
 * <p>The pattern below — iterate {@code TopicPartition}s, call
 * {@code committed(tp)} per iteration — is exactly the loop the rule is
 * designed to catch. It looks innocent at code-review time (one call site,
 * a tight for-each), but at runtime each iteration is one full broker
 * round trip to the group coordinator for an {@code OFFSET_FETCH} request.
 * For a 32-partition assignment in-DC (5 ms RTT) that's a 160 ms latency
 * floor on the endpoint; cross-region (80 ms RTT) it's 2.5 s. The hazard
 * surface: IQ endpoints exposing per-partition lag, lag-monitoring
 * sidecars, and {@code ConsumerRebalanceListener.onPartitionsAssigned}
 * implementations that fetch committed offsets per newly-assigned
 * partition. Slow rebalance callbacks risk blowing past
 * {@code max.poll.interval.ms} and triggering another rebalance,
 * compounding the problem.
 */
public final class BadConsumerCommittedSinglePartition {

    private static Properties consumerProps() {
        Properties p = new Properties();
        p.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "kafka-1:9092,kafka-2:9092,kafka-3:9092");
        p.put(ConsumerConfig.CLIENT_ID_CONFIG, "bad-committed-single-partition");
        p.put(ConsumerConfig.GROUP_ID_CONFIG, "bad-committed-single-partition");
        p.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        p.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        p.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, "60000");
        p.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "100");
        p.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        return p;
    }

    public void scrapePerPartitionLagOnePartitionAtATime() {
        List<TopicPartition> assignment = List.of(
                new TopicPartition("topic", 0),
                new TopicPartition("topic", 1),
                new TopicPartition("topic", 2));
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(consumerProps())) {
            consumer.assign(assignment);
            for (TopicPartition tp : assignment) {
                // FIRES — direct INVOKEVIRTUAL on the deprecated (TopicPartition) overload.
                // ONE OFFSET_FETCH per partition — N × broker-RTT of sequential round trips.
                OffsetAndMetadata committed = consumer.committed(tp);
            }
            consumer.close(Duration.ofSeconds(5));
        }
    }

    public void scrapePerPartitionLagWithTimeout() {
        TopicPartition tp = new TopicPartition("topic", 0);
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(consumerProps())) {
            consumer.assign(List.of(tp));
            // FIRES — direct INVOKEVIRTUAL on the deprecated (TopicPartition, Duration) overload.
            // The Duration bounds the wait but the cost shape (one OFFSET_FETCH per partition) is
            // unchanged from the no-timeout overload.
            OffsetAndMetadata committed = consumer.committed(tp, Duration.ofSeconds(2));
            consumer.close(Duration.ofSeconds(5));
        }
    }

    public void captureCommittedAsMethodReference() {
        TopicPartition tp = new TopicPartition("topic", 0);
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(consumerProps())) {
            consumer.assign(List.of(tp));
            // FIRES — INVOKEDYNAMIC method-ref capture. SAM signature
            // R apply(TopicPartition) forces overload resolution to the
            // deprecated (TopicPartition) overload. Bytecode of this method contains
            // ZERO INVOKE* targeting committed; the rule detects the
            // REF_invokeVirtual handle inside the indy's bsmArgs and matches its
            // descriptor prefix.
            Function<TopicPartition, OffsetAndMetadata> deferred = consumer::committed;
            OffsetAndMetadata committed = deferred.apply(tp);
            consumer.close(Duration.ofSeconds(5));
        }
    }
}
