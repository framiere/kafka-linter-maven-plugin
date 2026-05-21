package sample;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.time.Duration;

/**
 * RULE: CONSUMER_CLOSE_NO_TIMEOUT.
 *
 * Each of the three methods below calls the no-argument
 * {@link Consumer#close()} — which internally delegates to
 * {@code close(Duration.ofMillis(defaultApiTimeoutMs))} (60 s default).
 * The close sequence commits final offsets, sends the LeaveGroup, and
 * closes broker connections; against an unhealthy coordinator each
 * step's retry loop runs to budget — well past the default 30 s
 * Kubernetes terminationGracePeriodSeconds, after which SIGKILL fires
 * and the LeaveGroup never arrives, forcing the rest of the group to
 * wait session.timeout.ms (45 s) before rebalancing.
 *
 * The fourth method shows the correct shape: it passes a {@link Duration}
 * to the bounded overload — must NOT fire.
 *
 * Mix of receiver types deliberately exercises both INVOKEINTERFACE
 * (against {@code Consumer}) and INVOKEVIRTUAL (against {@code KafkaConsumer}).
 */
public final class BadConsumerCloseNoTimeout {

    private final Consumer<String, String> consumer;

    public BadConsumerCloseNoTimeout(Consumer<String, String> consumer) {
        this.consumer = consumer;
    }

    /** Anti-pattern: field on the Consumer interface — INVOKEINTERFACE close(). */
    public void shutdownHook() {
        this.consumer.close(); // FIRES
    }

    /** Anti-pattern: parameter typed as the concrete KafkaConsumer — INVOKEVIRTUAL close(). */
    public static void closeConcrete(KafkaConsumer<String, String> c) {
        c.close(); // FIRES
    }

    /** Anti-pattern: try/finally — the finally branch blocks for default.api.timeout.ms. */
    public static void pollAndClose(Consumer<String, String> c) {
        try {
            c.poll(Duration.ofMillis(100));
        } finally {
            c.close(); // FIRES
        }
    }

    /** Control: bounded overload with a deadline — must NOT fire. */
    public void shutdownWithDeadline() {
        this.consumer.close(Duration.ofSeconds(20));
    }
}
