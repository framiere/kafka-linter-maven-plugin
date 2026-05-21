package sample;

import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.time.Duration;
import java.util.List;
import java.util.Properties;

/**
 * RULE: CONSUMER_CLOSE_ZERO_DURATION.
 *
 * Fires when {@code consumer.close(Duration.ZERO)} or {@code close(Duration.ofXxx(0))}
 * is called. The consumer skips the final commit, the onPartitionsRevoked broadcast,
 * and the LeaveGroup request — the group cannot rebalance for session.timeout.ms,
 * partitions sit idle, lag spikes, and any offsets not committed are reprocessed.
 */
public final class BadConsumerCloseZeroDuration {

    private KafkaConsumer<String, String> consumer(Properties p) {
        return new KafkaConsumer<>(p);
    }

    /** Anti-pattern: close(Duration.ZERO) — skip commit + LeaveGroup. */
    public void closeZeroField(Properties p) {
        KafkaConsumer<String, String> c = consumer(p);
        c.subscribe(List.of("orders"));
        ConsumerRecords<String, String> rs = c.poll(Duration.ofMillis(100));
        if (rs.isEmpty()) {
            c.close(Duration.ZERO); // FIRES — Duration.ZERO field
        }
    }

    /** Anti-pattern: close(Duration.ofMillis(0)) — same hazard, factory-method form. */
    public void closeOfMillisZero(Properties p) {
        KafkaConsumer<String, String> c = consumer(p);
        c.subscribe(List.of("orders"));
        c.poll(Duration.ofMillis(100));
        c.close(Duration.ofMillis(0)); // FIRES
    }

    /** Anti-pattern: close(Duration.ofSeconds(0)). */
    public void closeOfSecondsZero(Properties p) {
        KafkaConsumer<String, String> c = consumer(p);
        c.subscribe(List.of("orders"));
        c.poll(Duration.ofMillis(100));
        c.close(Duration.ofSeconds(0)); // FIRES
    }

    /** Anti-pattern: close(Duration.ofNanos(0L)). */
    public void closeOfNanosZero(Properties p) {
        KafkaConsumer<String, String> c = consumer(p);
        c.subscribe(List.of("orders"));
        c.poll(Duration.ofMillis(100));
        c.close(Duration.ofNanos(0L)); // FIRES
    }

    /** Control: close(Duration.ofSeconds(10)) — the conventional shape. Must NOT fire. */
    public void closeBounded(Properties p) {
        KafkaConsumer<String, String> c = consumer(p);
        c.subscribe(List.of("orders"));
        c.poll(Duration.ofMillis(100));
        c.close(Duration.ofSeconds(10)); // OK
    }

    /** Control: close(Duration.ofMillis(500)) — finite, well-bounded. Must NOT fire. */
    public void closeShortButNonZero(Properties p) {
        KafkaConsumer<String, String> c = consumer(p);
        c.subscribe(List.of("orders"));
        c.poll(Duration.ofMillis(100));
        c.close(Duration.ofMillis(500)); // OK
    }
}
