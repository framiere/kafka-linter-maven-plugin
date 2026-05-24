package sample;

import java.time.Duration;
import java.util.Collection;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Consumer;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;

/**
 * RULE: CONSUMER_SUBSCRIBE_AND_ASSIGN_MIXED.
 *
 * <p>Fires when {@code KafkaConsumer.subscribe()} and {@code KafkaConsumer.assign()}
 * are invoked on the same local slot in the same method without an intervening
 * {@code unsubscribe()}. The two APIs are documented as mutually exclusive: the
 * consumer's internal {@code SubscriptionState} machine has three non-NONE
 * subscription types — {@code AUTO_TOPICS} (set by {@code subscribe(Collection)}),
 * {@code AUTO_PATTERN} (set by {@code subscribe(Pattern)}), {@code USER_ASSIGNED}
 * (set by {@code assign(Collection)}). Once one of those is set, attempting to
 * transition to a different non-NONE type throws
 * {@code IllegalStateException("Subscription to topics, partitions and pattern
 * are mutually exclusive")}. The only documented reset path is
 * {@code unsubscribe()}, which sets the state back to {@code NONE}.
 *
 * <p>The bug shape is structurally common in code that grew incrementally:
 * an initial {@code assign()} for testing or for a manual-control prototype
 * was left in production code after a migration to {@code subscribe()}-based
 * group management; both calls now execute on every init, the first sets the
 * mode, the second throws on the mode-mismatch check.
 *
 * <p>What this rule catches:
 * <ol>
 *   <li>{@code subscribeThenAssign} — group-managed code grew a stray
 *       {@code assign()} call after migrating from a single-instance prototype.
 *       subscribe() puts the state in AUTO_TOPICS; assign() throws because
 *       the state is no longer NONE.</li>
 *   <li>{@code assignThenSubscribe} — a manual-mode debug tool got 'upgraded'
 *       to use the consumer group and a subscribe() call was added without
 *       removing the assign(). assign() puts the state in USER_ASSIGNED;
 *       subscribe() throws because the state is no longer NONE.</li>
 *   <li>{@code subscribeThenCaptureAssignReference} — group-managed code
 *       captures {@code consumer::assign} for a 'rescue' executor callback
 *       intended to manually pin partitions during a rebalance storm. When
 *       the executor runs the captured reference, the second-mode call
 *       throws on the consumer's worker thread; the executor's
 *       uncaught-handler typically swallows it.</li>
 * </ol>
 *
 * <p>Correct pattern: pick ONE mode (subscribe for group-managed deployments,
 * assign for manual partition control) and remove the other call. If you
 * genuinely need to switch modes mid-lifecycle, call {@code unsubscribe()}
 * first to reset the SubscriptionState to NONE. The
 * {@code GoodConsumerSingleMode} silent controls cover this shape.
 */
public final class BadConsumerSubscribeAndAssignMixed {

    private static final String BROKERS = "kafka-1.prod.example.com:9092,kafka-2.prod.example.com:9092,kafka-3.prod.example.com:9092";

    private static Properties consumerProps(String groupId) {
        Properties p = new Properties();
        p.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BROKERS);
        p.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        p.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        p.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        p.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        p.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        p.put(ConsumerConfig.CLIENT_ID_CONFIG, "subscribe-assign-mixed-" + groupId);
        return p;
    }

    /** Anti-pattern: subscribe() then assign() on the same slot — FIRES. */
    public void subscribeThenAssign() {
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(consumerProps("bad-subscribe-then-assign"));
        try {
            consumer.subscribe(List.of("orders"));
            // Migration left this here: was the only setup before group-managed
            // mode was introduced; nobody noticed it after the subscribe was added.
            consumer.assign(List.of(new TopicPartition("orders", 0))); // FIRES — IllegalStateException
        } finally {
            consumer.close(Duration.ofSeconds(10));
        }
    }

    /** Anti-pattern: assign() then subscribe() on the same slot — FIRES. */
    public void assignThenSubscribe() {
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(consumerProps("bad-assign-then-subscribe"));
        try {
            consumer.assign(List.of(new TopicPartition("orders", 0)));
            // 'Upgrade' to consumer-group management was bolted on top of the
            // original manual-mode debug tool; the assign was never removed.
            consumer.subscribe(List.of("orders")); // FIRES — IllegalStateException
        } finally {
            consumer.close(Duration.ofSeconds(10));
        }
    }

    /** Anti-pattern: subscribe() then capture consumer::assign as a method reference — FIRES on indy. */
    public void subscribeThenCaptureAssignReference() {
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(consumerProps("bad-subscribe-then-capture-assign"));
        ExecutorService executor = Executors.newSingleThreadExecutor();
        try {
            consumer.subscribe(List.of("orders"));
            List<TopicPartition> rescue = List.of(new TopicPartition("orders", 0));
            // 'Rescue handler' that pins partitions manually during a perceived
            // rebalance storm — the captured method reference is invoked on the
            // executor's worker. The consumer is already in AUTO_TOPICS mode,
            // so assign() throws IllegalStateException; the executor's
            // uncaught-handler swallows it and the 'rescue' silently fails.
            Consumer<Collection<TopicPartition>> rescueAssign = consumer::assign; // FIRES — ::assign captured after subscribe()
            executor.submit(() -> rescueAssign.accept(rescue));
        } finally {
            executor.shutdownNow();
            consumer.close(Duration.ofSeconds(10));
        }
    }
}
