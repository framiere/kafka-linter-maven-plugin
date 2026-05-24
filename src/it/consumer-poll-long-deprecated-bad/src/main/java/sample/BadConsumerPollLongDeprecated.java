package sample;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.List;
import java.util.Properties;
import java.util.function.LongFunction;

/**
 * RULE: CONSUMER_POLL_LONG_DEPRECATED.
 *
 * <p>Exercises the two shapes the rule must catch on
 * {@link KafkaConsumer#poll(long)} (or {@code Consumer#poll(long)}):
 *
 * <ol>
 *   <li>Direct {@code INVOKEVIRTUAL} on {@code KafkaConsumer.poll(long)} —
 *       a literal {@code consumer.poll(500L)} call. Bytecode descriptor
 *       {@code (J)Lorg/apache/kafka/clients/consumer/ConsumerRecords;}.</li>
 *   <li>{@code INVOKEDYNAMIC} method-reference capture
 *       {@code consumer::poll} stored into a
 *       {@code LongFunction<ConsumerRecords>} — the user-class bytecode
 *       contains zero {@code INVOKE*} targeting {@code poll}; the call
 *       lives inside a LambdaMetafactory-synthesized bridge whose
 *       {@code REF_invokeVirtual} handle the rule's {@code INVOKEDYNAMIC}
 *       walk recognises. Java's overload resolution picks the {@code (J)}
 *       overload because the SAM signature is
 *       {@code R apply(long)} — capturing {@code consumer::poll} into a
 *       {@code Function<Duration, ConsumerRecords>} would pick the
 *       {@code (Ljava/time/Duration;)} overload and would NOT fire (proven
 *       by the {@code -good} sibling fixture).</li>
 * </ol>
 *
 * <h2>Why this method is removal-bound</h2>
 *
 * <p>{@code poll(long)} carries a documented but easily-missed
 * &laquo;join the group first, then time the wait&raquo; semantics: if the
 * consumer is not yet a member of its consumer group (cold start,
 * coordinator failover, controller election, recent rebalance),
 * {@code poll(long)} blocks INDEFINITELY waiting for the initial
 * assignment, regardless of the supplied timeout — the timeout only
 * bounds the subsequent fetch wait. {@code poll(Duration)} (KIP-266,
 * Kafka 2.0) replaced this: the duration is a TOTAL deadline covering
 * both group-join and fetch, so a consumer that can't join within the
 * budget returns an empty {@link ConsumerRecords} batch and the caller
 * is given a chance to log / alert / back off, instead of silently
 * stalling on a wedged coordinator.
 *
 * <h2>Sibling-rule noise we explicitly OFF</h2>
 *
 * <p>The fixture pom OFFs {@code CONSUMER_POLL_INFINITE_DURATION},
 * {@code CONSUMER_POLL_ZERO}, {@code CONSUMER_POLL_RESULT_IGNORED},
 * {@code CONSUMER_CLOSE_NO_TIMEOUT}, {@code CONSUMER_AUTO_COMMIT_TRUE},
 * {@code CONSUMER_PROPERTIES_MAX_POLL_*_ABSENT},
 * {@code CONSUMER_GROUP_ID_RANDOM}, and {@code CONSUMER_NOT_CLOSED}
 * so the build failure is attributable to
 * {@code CONSUMER_POLL_LONG_DEPRECATED} alone.
 */
public final class BadConsumerPollLongDeprecated {

    private static Properties consumerProps() {
        Properties p = new Properties();
        p.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "kafka-1:9092,kafka-2:9092,kafka-3:9092");
        p.put(ConsumerConfig.CLIENT_ID_CONFIG, "bad-poll-long-deprecated");
        p.put(ConsumerConfig.GROUP_ID_CONFIG, "bad-poll-long-deprecated");
        p.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        p.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        p.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, "60000");
        p.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "100");
        p.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        return p;
    }

    public void pollDirectLong() {
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(consumerProps())) {
            consumer.subscribe(List.of("topic"));
            // FIRES — direct INVOKEVIRTUAL on the deprecated (J)... overload.
            // Will block INDEFINITELY on initial-assignment if the coordinator is down.
            ConsumerRecords<String, String> batch = consumer.poll(500L);
            consumer.close(Duration.ofSeconds(5));
        }
    }

    public void pollAsMethodReferenceCapture() {
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(consumerProps())) {
            consumer.subscribe(List.of("topic"));
            // FIRES — INVOKEDYNAMIC method-ref capture. SAM signature R apply(long)
            // forces overload resolution to pick (J)Lorg/.../ConsumerRecords; — the
            // deprecated overload. The user-class bytecode contains ZERO INVOKE*
            // instructions targeting poll; the rule's INVOKEDYNAMIC walk inspects
            // bsmArgs handles and detects the REF_invokeVirtual capture.
            LongFunction<ConsumerRecords<String, String>> deferred = consumer::poll;
            ConsumerRecords<String, String> batch = deferred.apply(500L);
            consumer.close(Duration.ofSeconds(5));
        }
    }
}
