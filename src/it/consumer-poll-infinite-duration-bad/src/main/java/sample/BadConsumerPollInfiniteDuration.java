package sample;

import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.time.Duration;
import java.util.List;
import java.util.Properties;

/**
 * RULE: CONSUMER_POLL_INFINITE_DURATION.
 *
 * Fires when {@code consumer.poll(Duration.ofXxx(Long.MAX_VALUE))} is called.
 * The poll thread parks inside the fetcher's wait for an effectively unbounded
 * interval — SIGTERM cannot unblock it without an explicit consumer.wakeup()
 * from a shutdown hook, so the JVM hangs until the kubelet's grace period
 * elapses and SIGKILL fires mid-batch (uncommitted offsets reprocessed).
 */
public final class BadConsumerPollInfiniteDuration {

    private KafkaConsumer<String, String> consumer(Properties p) {
        return new KafkaConsumer<>(p);
    }

    /** Anti-pattern: poll(Duration.ofMillis(Long.MAX_VALUE)) — the textbook "wait forever" shape. */
    public void pollOfMillisMax(Properties p) {
        KafkaConsumer<String, String> c = consumer(p);
        c.subscribe(List.of("orders"));
        ConsumerRecords<String, String> rs = c.poll(Duration.ofMillis(Long.MAX_VALUE)); // FIRES
        if (rs.isEmpty()) {
            c.close();
        }
    }

    /** Anti-pattern: poll(Duration.ofSeconds(Long.MAX_VALUE)) — same hazard, different unit. */
    public void pollOfSecondsMax(Properties p) {
        KafkaConsumer<String, String> c = consumer(p);
        c.subscribe(List.of("orders"));
        ConsumerRecords<String, String> rs = c.poll(Duration.ofSeconds(Long.MAX_VALUE)); // FIRES
        if (rs.isEmpty()) {
            c.close();
        }
    }

    /** Anti-pattern: poll(Duration.ofNanos(Long.MAX_VALUE)) — same hazard, finest unit. */
    public void pollOfNanosMax(Properties p) {
        KafkaConsumer<String, String> c = consumer(p);
        c.subscribe(List.of("orders"));
        ConsumerRecords<String, String> rs = c.poll(Duration.ofNanos(Long.MAX_VALUE)); // FIRES
        if (rs.isEmpty()) {
            c.close();
        }
    }

    /** Anti-pattern: poll(Duration.ofDays(Long.MAX_VALUE)) — coarsest unit, same outcome. */
    public void pollOfDaysMax(Properties p) {
        KafkaConsumer<String, String> c = consumer(p);
        c.subscribe(List.of("orders"));
        ConsumerRecords<String, String> rs = c.poll(Duration.ofDays(Long.MAX_VALUE)); // FIRES
        if (rs.isEmpty()) {
            c.close();
        }
    }

    /** Anti-pattern: infinite poll inside a normal poll loop — both the per-poll wait and the shutdown story are broken. */
    public void pollLoopWithInfiniteWait(Properties p) {
        KafkaConsumer<String, String> c = consumer(p);
        c.subscribe(List.of("orders"));
        while (true) {
            ConsumerRecords<String, String> rs = c.poll(Duration.ofMillis(Long.MAX_VALUE)); // FIRES
            if (rs.isEmpty()) break;
        }
        c.close();
    }

    /** Control: bounded poll(100 ms) — the conventional shape. Must NOT fire. */
    public void pollBounded(Properties p) {
        KafkaConsumer<String, String> c = consumer(p);
        c.subscribe(List.of("orders"));
        while (true) {
            ConsumerRecords<String, String> rs = c.poll(Duration.ofMillis(100)); // OK
            if (rs.isEmpty()) break;
        }
        c.close();
    }

    /** Control: Duration.ofMillis(Long.MAX_VALUE) constructed but not passed to poll() — must NOT fire. */
    public void buildLargeDurationForOther() {
        Duration unused = Duration.ofMillis(Long.MAX_VALUE); // OK — never reaches poll()
        if (unused.isZero()) {
            System.out.println("unreachable");
        }
    }
}
