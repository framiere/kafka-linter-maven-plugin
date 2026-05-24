package sample;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.function.Function;

/**
 * Silent: this fixture only calls
 * {@code committed(Set<TopicPartition>)} and
 * {@code committed(Set<TopicPartition>, Duration)} — the KIP-520 batched
 * replacements for the deprecated single-partition overloads — and never
 * the deprecated overloads, neither directly nor as method-reference
 * captures.
 *
 * <p>The rule discriminates by descriptor: only descriptors starting with
 * {@code (Lorg/apache/kafka/common/TopicPartition;} fire.
 * {@code committed(Set<TopicPartition>)} has descriptor
 * {@code (Ljava/util/Set;)Ljava/util/Map;} and
 * {@code committed(Set<TopicPartition>, Duration)} has descriptor
 * {@code (Ljava/util/Set;Ljava/time/Duration;)Ljava/util/Map;} — both
 * start with {@code (Ljava/util/Set;} not
 * {@code (Lorg/apache/kafka/common/TopicPartition;}, so both are silent
 * for direct calls. For method-reference captures, Java's overload
 * resolution picks the {@code (Set)} overload when the SAM signature is
 * {@code Map apply(Set)} ({@code Function<Set<TopicPartition>,
 * Map<TopicPartition, OffsetAndMetadata>>}).
 *
 * <h2>Why the batched overload matters</h2>
 *
 * <p>Where the single-partition overload pays N × broker-RTT for N
 * partitions, the batched overload bundles every partition into ONE
 * {@code OFFSET_FETCH} request to the group coordinator. The improvement
 * is approximately N-fold at the application layer, and at the broker
 * layer it reduces request-rate pressure on the coordinator and lets
 * the SocketServer batch the response.
 *
 * <p>This fixture pins the boolean: zero
 * CONSUMER_COMMITTED_SINGLE_PARTITION_DEPRECATED violations.
 */
public final class GoodConsumerCommittedBatched {

    private static Properties consumerProps() {
        Properties p = new Properties();
        p.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "kafka-1:9092,kafka-2:9092,kafka-3:9092");
        p.put(ConsumerConfig.CLIENT_ID_CONFIG, "good-committed-batched");
        p.put(ConsumerConfig.GROUP_ID_CONFIG, "good-committed-batched");
        p.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        p.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        p.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, "60000");
        p.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "100");
        p.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        return p;
    }

    public void scrapeAllPartitionsInOneRoundTrip() {
        Set<TopicPartition> assignment = Set.of(
                new TopicPartition("topic", 0),
                new TopicPartition("topic", 1),
                new TopicPartition("topic", 2));
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(consumerProps())) {
            consumer.assign(List.copyOf(assignment));
            // SILENT — descriptor (Ljava/util/Set;)... does not start with
            // (Lorg/apache/kafka/common/TopicPartition;. One OFFSET_FETCH for the full set.
            Map<TopicPartition, OffsetAndMetadata> committed = consumer.committed(assignment);
            consumer.close(Duration.ofSeconds(5));
        }
    }

    public void scrapeAllPartitionsInOneRoundTripWithTimeout() {
        Set<TopicPartition> assignment = Set.of(
                new TopicPartition("topic", 0),
                new TopicPartition("topic", 1));
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(consumerProps())) {
            consumer.assign(List.copyOf(assignment));
            // SILENT — descriptor (Ljava/util/Set;Ljava/time/Duration;)... does not start
            // with (Lorg/apache/kafka/common/TopicPartition;.
            Map<TopicPartition, OffsetAndMetadata> committed =
                    consumer.committed(assignment, Duration.ofSeconds(2));
            consumer.close(Duration.ofSeconds(5));
        }
    }

    public void captureBatchedCommittedAsMethodReference() {
        Set<TopicPartition> assignment = Set.of(new TopicPartition("topic", 0));
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(consumerProps())) {
            consumer.assign(List.copyOf(assignment));
            // SILENT — Function<Set<TopicPartition>, Map<...>> SAM forces overload resolution
            // to the (Ljava/util/Set;) overload. The captured handle's descriptor does not
            // start with (Lorg/apache/kafka/common/TopicPartition;, so the rule's
            // INVOKEDYNAMIC walk skips it.
            Function<Set<TopicPartition>, Map<TopicPartition, OffsetAndMetadata>> deferred =
                    consumer::committed;
            Map<TopicPartition, OffsetAndMetadata> committed = deferred.apply(assignment);
            consumer.close(Duration.ofSeconds(5));
        }
    }
}
