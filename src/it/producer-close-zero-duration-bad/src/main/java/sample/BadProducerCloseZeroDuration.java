package sample;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.time.Duration;
import java.util.Properties;

/**
 * RULE: PRODUCER_CLOSE_ZERO_DURATION.
 *
 * Fires when {@code producer.close(Duration.ZERO)} or {@code producer.close(Duration.ofXxx(0))}
 * is called. The Sender thread is told to stop without draining the accumulator —
 * every buffered or in-flight record is dropped without the registered callback ever
 * firing. Silent data loss with no diagnostic trail; for transactional producers, the
 * transactional.id stays fenced on the broker until transaction.timeout.ms elapses.
 */
public final class BadProducerCloseZeroDuration {

    private KafkaProducer<String, String> producer(Properties p) {
        return new KafkaProducer<>(p);
    }

    /** Anti-pattern: close(Duration.ZERO) — the canonical "drop everything" shape. */
    public void closeZeroField(Properties p) {
        KafkaProducer<String, String> producer = producer(p);
        producer.send(new ProducerRecord<>("orders", "k", "v"));
        producer.close(Duration.ZERO); // FIRES — Duration.ZERO field
    }

    /** Anti-pattern: close(Duration.ofMillis(0)) — same hazard, factory-method form. */
    public void closeOfMillisZero(Properties p) {
        KafkaProducer<String, String> producer = producer(p);
        producer.send(new ProducerRecord<>("orders", "k", "v"));
        producer.close(Duration.ofMillis(0)); // FIRES — Duration.ofMillis(0)
    }

    /** Anti-pattern: close(Duration.ofSeconds(0)). */
    public void closeOfSecondsZero(Properties p) {
        KafkaProducer<String, String> producer = producer(p);
        producer.send(new ProducerRecord<>("orders", "k", "v"));
        producer.close(Duration.ofSeconds(0)); // FIRES
    }

    /** Anti-pattern: close(Duration.ofNanos(0)). */
    public void closeOfNanosZero(Properties p) {
        KafkaProducer<String, String> producer = producer(p);
        producer.send(new ProducerRecord<>("orders", "k", "v"));
        producer.close(Duration.ofNanos(0L)); // FIRES
    }

    /** Anti-pattern: close(Duration.ofDays(0)). */
    public void closeOfDaysZero(Properties p) {
        KafkaProducer<String, String> producer = producer(p);
        producer.send(new ProducerRecord<>("orders", "k", "v"));
        producer.close(Duration.ofDays(0)); // FIRES
    }

    /** Control: close(Duration.ofSeconds(30)) — the conventional shutdown shape. Must NOT fire. */
    public void closeBounded(Properties p) {
        KafkaProducer<String, String> producer = producer(p);
        producer.send(new ProducerRecord<>("orders", "k", "v"));
        producer.flush();
        producer.close(Duration.ofSeconds(30)); // OK
    }

    /** Control: close(Duration.ofMillis(100)) — finite, well-bounded. Must NOT fire. */
    public void closeShortButNonZero(Properties p) {
        KafkaProducer<String, String> producer = producer(p);
        producer.send(new ProducerRecord<>("orders", "k", "v"));
        producer.close(Duration.ofMillis(100)); // OK
    }
}
