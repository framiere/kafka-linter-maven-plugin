package sample;

import java.time.Duration;
import java.util.Collections;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.springframework.kafka.annotation.KafkaListener;

/**
 * Control for CONSUMER_NO_WAKEUP_SHUTDOWN — @KafkaListener variant.
 *
 * <p>The rule's second suppression: ANY method in the class annotated
 * {@code @org.springframework.kafka.annotation.KafkaListener} is
 * treated as proof that the consumer is framework-managed. Spring's
 * ConcurrentMessageListenerContainer owns the lifecycle — it creates
 * the consumer, runs the poll loop, calls close() during context
 * shutdown. Application code does NOT touch the consumer directly,
 * so there is no application-level wakeup() to write.
 *
 * <p>This fixture declares a local stand-in for the annotation at
 * the exact FQN {@code org.springframework.kafka.annotation.KafkaListener}
 * — bytecode-indistinguishable from Spring's real annotation as far
 * as the rule (which checks descriptor only) is concerned. Saves the
 * spring-kafka transitive-dep tree.
 *
 * <p>One method here is annotated. The class ALSO has a hand-rolled
 * poll-in-loop method that would normally fire the rule. The
 * annotation makes the entire class silent — the suppression is
 * CLASS-SCOPE, not per-method.
 */
public final class GoodConsumerKafkaListener {

    /** Spring-style consumer entry point. Container manages the poll loop and close(). */
    @KafkaListener(topics = "orders")
    public void onMessage(String payload) {
        // Spring's listener container called us with one record. No poll/close in this method.
    }

    /** Even a hand-rolled poll loop in the SAME class is suppressed because @KafkaListener exists somewhere. */
    public void manualPollAlsoInThisClass(KafkaConsumer<String, String> consumer) {
        consumer.subscribe(Collections.singletonList("orders"));
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(30)); // silenced by @KafkaListener above
            records.forEach(r -> {
                // process (omitted)
            });
        }
    }
}
