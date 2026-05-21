package sample;

import org.apache.kafka.streams.KafkaStreams;

import java.time.Duration;
import java.util.Optional;

/**
 * RULE: STREAMS_REMOVE_THREAD_NO_TIMEOUT.
 *
 * Each of the three methods below calls the no-argument
 * {@link KafkaStreams#removeStreamThread()} — which internally waits for
 * the selected thread to transition to DEAD with
 * {@code Duration.ofMillis(Long.MAX_VALUE)}. A thread stuck in a slow
 * {@code process()} call (DB lookup, HTTP fetch) or mid-commit on a
 * transactional topology parks the caller forever, deadlocking the
 * scale-down HTTP handler or the autoscaler reconciliation loop.
 *
 * The fourth method shows the correct shape: it passes a {@link Duration}
 * to the bounded overload and inspects {@link Optional#isEmpty()} — must
 * NOT fire.
 */
public final class BadRemoveThreadNoTimeout {

    private final KafkaStreams streams;

    public BadRemoveThreadNoTimeout(KafkaStreams streams) {
        this.streams = streams;
    }

    /** Anti-pattern: HTTP scale-down endpoint that blocks forever. */
    public Optional<String> scaleDown() {
        return this.streams.removeStreamThread(); // FIRES
    }

    /** Anti-pattern: autoscaler periodic call discards the result. */
    public void shrinkPool() {
        this.streams.removeStreamThread(); // FIRES
    }

    /** Anti-pattern: parameter-passed instance, same hazard. */
    public static Optional<String> removeOne(KafkaStreams s) {
        return s.removeStreamThread(); // FIRES
    }

    /** Control: bounded overload with deadline + empty-Optional escalation — must NOT fire. */
    public Optional<String> scaleDownWithDeadline() {
        Optional<String> removed = this.streams.removeStreamThread(Duration.ofSeconds(30));
        if (removed.isEmpty()) {
            // thread didn't stop — escalate (log, alert, retry next cycle) instead of waiting forever
            return Optional.empty();
        }
        return removed;
    }
}
