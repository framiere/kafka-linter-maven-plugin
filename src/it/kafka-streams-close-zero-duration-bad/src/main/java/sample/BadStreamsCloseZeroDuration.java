package sample;

import org.apache.kafka.streams.KafkaStreams;

import java.time.Duration;

/**
 * RULE: STREAMS_CLOSE_ZERO_DURATION.
 *
 * Fires when {@code streams.close(Duration.ZERO)} or
 * {@code streams.close(Duration.ofXxx(0))} is called. None of the StreamThreads
 * get a chance to: flush their state-store caches to RocksDB and the changelog
 * topic, drain the embedded producer's accumulator, commit the open EOS
 * transaction, or send LeaveGroup to the coordinator. The transactional.id
 * stays fenced on the broker until {@code transaction.timeout.ms} elapses,
 * blocking the next deploy with {@code ProducerFencedException}; the state
 * store comes back from a half-flushed changelog on restart.
 */
public final class BadStreamsCloseZeroDuration {

    /** Anti-pattern: close(Duration.ZERO) — skip the lifecycle. */
    public void closeZeroField(KafkaStreams s) {
        s.close(Duration.ZERO); // FIRES — Duration.ZERO field
    }

    /** Anti-pattern: close(Duration.ofMillis(0)) — same hazard, factory-method form. */
    public void closeOfMillisZero(KafkaStreams s) {
        s.close(Duration.ofMillis(0)); // FIRES
    }

    /** Anti-pattern: close(Duration.ofSeconds(0)). */
    public void closeOfSecondsZero(KafkaStreams s) {
        s.close(Duration.ofSeconds(0)); // FIRES
    }

    /** Anti-pattern: close(Duration.ofNanos(0L)). */
    public void closeOfNanosZero(KafkaStreams s) {
        s.close(Duration.ofNanos(0L)); // FIRES
    }

    /**
     * Control: close(Duration.ofSeconds(20)) — the conventional shape for a
     * 30 s {@code terminationGracePeriodSeconds}. Must NOT fire.
     */
    public void closeBounded(KafkaStreams s) {
        if (!s.close(Duration.ofSeconds(20))) {
            throw new IllegalStateException("forced shutdown — threads did not stop in 20 s");
        }
    }

    /** Control: close(Duration.ofMillis(500)) — finite, well-bounded. Must NOT fire. */
    public void closeShortButNonZero(KafkaStreams s) {
        s.close(Duration.ofMillis(500));
    }
}
