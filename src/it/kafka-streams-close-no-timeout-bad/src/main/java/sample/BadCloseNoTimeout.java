package sample;

import org.apache.kafka.streams.KafkaStreams;

import java.time.Duration;

/**
 * RULE: STREAMS_CLOSE_NO_TIMEOUT.
 *
 * Each of the three methods below calls the no-argument
 * {@link KafkaStreams#close()} — which internally delegates to
 * {@code close(Duration.ofMillis(Long.MAX_VALUE))}. If any StreamThread is
 * wedged (slow Processor, blocked external call, stalled producer flush),
 * the caller parks forever; in Kubernetes the pod's
 * {@code terminationGracePeriodSeconds} expires, the kubelet sends SIGKILL,
 * and in-flight EOS transactions are torn mid-commit.
 *
 * The fourth method shows the correct shape: it passes a {@link Duration}
 * to the bounded overload — must NOT fire.
 */
public final class BadCloseNoTimeout {

    private final KafkaStreams streams;

    public BadCloseNoTimeout(KafkaStreams streams) {
        this.streams = streams;
    }

    /** Anti-pattern: shutdown hook closes with no timeout. */
    public void shutdownHook() {
        this.streams.close(); // FIRES
    }

    /** Anti-pattern: try/finally where the finally branch blocks indefinitely on close. */
    public void runAndClose() {
        try {
            this.streams.start();
        } finally {
            this.streams.close(); // FIRES
        }
    }

    /** Anti-pattern: parameter-passed instance, same hazard. */
    public static void closeNow(KafkaStreams s) {
        s.close(); // FIRES
    }

    /** Control: passes Duration to the bounded overload — must NOT fire. */
    public void shutdownWithDeadline() {
        if (!this.streams.close(Duration.ofSeconds(20))) {
            throw new IllegalStateException("forced shutdown — threads did not stop in 20 s");
        }
    }
}
