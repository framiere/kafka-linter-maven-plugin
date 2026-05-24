package sample;

import java.time.Duration;
import java.util.List;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;

/**
 * Control for CONSUMER_SUBSCRIBE_AND_ASSIGN_MIXED.
 *
 * <p>Three methods that each avoid the rule by following one of the only
 * correct shapes: pick ONE mode per consumer slot (subscribe XOR assign), or
 * if you really need to switch modes, call {@code unsubscribe()} in between
 * to reset the {@code SubscriptionState} back to NONE.
 *
 * <ol>
 *   <li>{@code subscribeOnly} — group-managed deployment, canonical shape:
 *       construct, subscribe, poll loop elided, close.</li>
 *   <li>{@code assignOnly} — manual-partition-control deployment (CDC,
 *       replay tool, debug harness): construct, assign, poll loop elided,
 *       close. No subscribe ever.</li>
 *   <li>{@code subscribeThenUnsubscribeThenAssign} — the documented mode-switch
 *       shape: subscribe puts state into AUTO_TOPICS; unsubscribe resets it
 *       to NONE; assign then sets it to USER_ASSIGNED. The rule honors
 *       unsubscribe() as the mode-reset call and does NOT fire on the
 *       subsequent assign.</li>
 * </ol>
 */
public final class GoodConsumerSingleMode {

    private static final String BROKERS = "kafka-1.prod.example.com:9092,kafka-2.prod.example.com:9092,kafka-3.prod.example.com:9092";

    private static Properties consumerProps(String groupId) {
        Properties p = new Properties();
        p.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BROKERS);
        p.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        p.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        p.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        p.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        p.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        p.put(ConsumerConfig.CLIENT_ID_CONFIG, "single-mode-" + groupId);
        return p;
    }

    /** Correct: group-managed mode only — subscribe, no assign. */
    public void subscribeOnly() {
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(consumerProps("good-subscribe-only"));
        try {
            consumer.subscribe(List.of("orders"));
        } finally {
            consumer.close(Duration.ofSeconds(10));
        }
    }

    /** Correct: manual partition control only — assign, no subscribe. */
    public void assignOnly() {
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(consumerProps("good-assign-only"));
        try {
            consumer.assign(List.of(new TopicPartition("orders", 0)));
        } finally {
            consumer.close(Duration.ofSeconds(10));
        }
    }

    /** Correct: documented mode-switch — subscribe, unsubscribe (reset), then assign. */
    public void subscribeThenUnsubscribeThenAssign() {
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(consumerProps("good-switch-modes"));
        try {
            consumer.subscribe(List.of("orders"));
            // Documented mode-reset: unsubscribe() puts SubscriptionState back
            // to NONE, allowing the next assign() to set USER_ASSIGNED cleanly.
            // Rare in practice (almost always indicates a design issue), but
            // it is the ONE documented way to switch modes on an open consumer.
            consumer.unsubscribe();
            consumer.assign(List.of(new TopicPartition("orders", 0)));
        } finally {
            consumer.close(Duration.ofSeconds(10));
        }
    }
}
