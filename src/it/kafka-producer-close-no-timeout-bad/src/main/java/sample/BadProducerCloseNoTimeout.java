package sample;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;

import java.time.Duration;

/**
 * RULE: PRODUCER_CLOSE_NO_TIMEOUT.
 *
 * Each of the three methods below calls the no-argument
 * {@link Producer#close()} — which internally delegates to
 * {@code close(Duration.ofMillis(Long.MAX_VALUE))}. A producer with
 * records still in the accumulator and an unreachable broker burns the
 * full {@code delivery.timeout.ms} per record before close() returns,
 * routinely pushing shutdown well past the Kubernetes
 * {@code terminationGracePeriodSeconds} and triggering SIGKILL mid-flush.
 *
 * The fourth method shows the correct shape: it passes a {@link Duration}
 * to the bounded overload — must NOT fire.
 *
 * Mix of receiver types deliberately exercises both INVOKEINTERFACE
 * (against {@code Producer}) and INVOKEVIRTUAL (against {@code KafkaProducer}).
 */
public final class BadProducerCloseNoTimeout {

    private final Producer<String, String> producer;

    public BadProducerCloseNoTimeout(Producer<String, String> producer) {
        this.producer = producer;
    }

    /** Anti-pattern: field on the Producer interface — INVOKEINTERFACE close(). */
    public void shutdownHook() {
        this.producer.close(); // FIRES
    }

    /** Anti-pattern: parameter typed as the concrete KafkaProducer — INVOKEVIRTUAL close(). */
    public static void closeConcrete(KafkaProducer<String, String> p) {
        p.close(); // FIRES
    }

    /** Anti-pattern: try/finally — the finally branch blocks indefinitely. */
    public static void sendAndClose(Producer<String, String> p) {
        try {
            p.flush();
        } finally {
            p.close(); // FIRES
        }
    }

    /** Control: bounded overload with a deadline — must NOT fire. */
    public void shutdownWithDeadline() {
        this.producer.close(Duration.ofSeconds(20));
    }
}
