package sample;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.List;
import java.util.Properties;
import java.util.function.Function;

/**
 * Silent: this fixture only calls
 * {@code poll(Duration)} — the KIP-266 replacement for the deprecated
 * {@code poll(long)} overload — and never the deprecated overload, neither
 * directly nor as a method-reference capture.
 *
 * <p>The rule discriminates by descriptor: only descriptors starting with
 * {@code (J)} fire. {@code poll(Duration)} has descriptor
 * {@code (Ljava/time/Duration;)Lorg/apache/kafka/clients/consumer/ConsumerRecords;}
 * and is silent for direct calls. For method-reference captures, Java's
 * overload resolution picks the overload that matches the SAM signature:
 * {@code consumer::poll} assigned to
 * {@code Function<Duration, ConsumerRecords<K,V>>} captures the
 * {@code (Ljava/time/Duration;)} overload — descriptor does NOT start with
 * {@code (J)}, so the rule does NOT fire.
 *
 * <h2>Why poll(Duration) is the safe replacement</h2>
 *
 * <p>Where {@code poll(long)} treats the argument as a fetch-wait timeout
 * that does NOT bound the initial group-coordinator join, {@code poll(Duration)}
 * treats it as a TOTAL deadline. A consumer that cannot join the group
 * within the budget gets returned an empty {@link ConsumerRecords} and
 * the caller learns about the stall, rather than parking inside poll
 * forever.
 *
 * <p>This fixture pins the boolean: zero CONSUMER_POLL_LONG_DEPRECATED
 * violations.
 */
public final class GoodConsumerPollDuration {

    private static Properties consumerProps() {
        Properties p = new Properties();
        p.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "kafka-1:9092,kafka-2:9092,kafka-3:9092");
        p.put(ConsumerConfig.CLIENT_ID_CONFIG, "good-poll-duration");
        p.put(ConsumerConfig.GROUP_ID_CONFIG, "good-poll-duration");
        p.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        p.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        p.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, "60000");
        p.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "100");
        p.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        return p;
    }

    public void pollDirectDuration() {
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(consumerProps())) {
            consumer.subscribe(List.of("topic"));
            // SILENT — descriptor (Ljava/time/Duration;)... does not start with (J).
            ConsumerRecords<String, String> batch = consumer.poll(Duration.ofMillis(500));
            consumer.close(Duration.ofSeconds(5));
        }
    }

    public void pollAsDurationMethodReferenceCapture() {
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(consumerProps())) {
            consumer.subscribe(List.of("topic"));
            // SILENT — Function<Duration, ConsumerRecords> SAM forces overload resolution
            // to the (Ljava/time/Duration;) overload. The captured handle's descriptor
            // does not start with (J), so the rule's INVOKEDYNAMIC walk skips it.
            Function<Duration, ConsumerRecords<String, String>> deferred = consumer::poll;
            ConsumerRecords<String, String> batch = deferred.apply(Duration.ofMillis(500));
            consumer.close(Duration.ofSeconds(5));
        }
    }
}
