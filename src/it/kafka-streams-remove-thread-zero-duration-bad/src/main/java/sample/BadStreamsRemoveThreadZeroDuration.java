package sample;

import org.apache.kafka.streams.KafkaStreams;

import java.time.Duration;
import java.util.Optional;

/**
 * RULE: STREAMS_REMOVE_THREAD_ZERO_DURATION.
 *
 * Fires when {@code streams.removeStreamThread(Duration.ZERO)} or
 * {@code streams.removeStreamThread(Duration.ofXxx(0))} is called. The
 * StreamThread is interrupted mid-process(): its tasks leave behind
 * unflushed state-store caches, uncommitted source offsets, and (for
 * EOS-v2) an abandoned transaction. The reassignee replays records on
 * the next rebalance — duplicates downstream — and, for EOS-v2, hits
 * {@code ProducerFencedException} until {@code transaction.timeout.ms}
 * elapses.
 */
public final class BadStreamsRemoveThreadZeroDuration {

    /** Anti-pattern: removeStreamThread(Duration.ZERO) — skip the wind-down. */
    public void scaleDownZeroField(KafkaStreams s) {
        s.removeStreamThread(Duration.ZERO); // FIRES — Duration.ZERO field
    }

    /** Anti-pattern: removeStreamThread(Duration.ofMillis(0)) — factory-method form. */
    public void scaleDownOfMillisZero(KafkaStreams s) {
        s.removeStreamThread(Duration.ofMillis(0)); // FIRES
    }

    /** Anti-pattern: removeStreamThread(Duration.ofSeconds(0)). */
    public void scaleDownOfSecondsZero(KafkaStreams s) {
        s.removeStreamThread(Duration.ofSeconds(0)); // FIRES
    }

    /** Anti-pattern: removeStreamThread(Duration.ofNanos(0L)). */
    public void scaleDownOfNanosZero(KafkaStreams s) {
        s.removeStreamThread(Duration.ofNanos(0L)); // FIRES
    }

    /**
     * Control: removeStreamThread(Duration.ofSeconds(15)) — matches a typical
     * scale-down HTTP request budget. Must NOT fire.
     */
    public void scaleDownBounded(KafkaStreams s) {
        Optional<String> removed = s.removeStreamThread(Duration.ofSeconds(15));
        if (removed.isEmpty()) {
            throw new IllegalStateException("thread did not stop in 15 s — retry next cycle");
        }
    }

    /** Control: removeStreamThread(Duration.ofMillis(500)) — finite, well-bounded. Must NOT fire. */
    public void scaleDownShortButNonZero(KafkaStreams s) {
        s.removeStreamThread(Duration.ofMillis(500));
    }
}
