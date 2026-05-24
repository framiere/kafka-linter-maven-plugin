package sample;

import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;

import java.time.Duration;
import java.util.List;

/**
 * RULE: CONSUMER_PAUSE_NO_RESUME.
 *
 * <p>Fires when {@code consumer.pause(...)} is called anywhere in the class
 * and {@code consumer.resume(...)} is called NOWHERE in the same class.
 *
 * <p>This file demonstrates two anti-patterns. Both fire because no method
 * in the class contains a {@code resume(...)} call.
 */
public final class BadConsumerPauseNoResume {

    private final KafkaConsumer<String, String> consumer;

    public BadConsumerPauseNoResume(KafkaConsumer<String, String> consumer) {
        this.consumer = consumer;
    }

    /**
     * Anti-pattern #1: backpressure-on-full handler that pauses but
     * never resumes. FIRES.
     */
    public void onDownstreamFull(List<TopicPartition> assignment) {
        consumer.pause(assignment); // FIRES — no resume anywhere in the class
    }

    /**
     * Anti-pattern #2: long-work-during-poll-interval pattern where the
     * developer remembered to pause but forgot to resume after the work
     * completed. FIRES.
     */
    public void doLongWork() {
        consumer.pause(consumer.assignment()); // FIRES — second pause site
        try {
            Thread.sleep(60_000);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        // No resume — partitions stay dark for the lifetime of this consumer.
    }

    /** Control: a normal poll loop on the same class — must not interfere. */
    public void pollLoop() {
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(1));
            if (records.isEmpty()) break;
        }
    }
}
