package sample;

import java.time.Duration;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

/**
 * RULE: CONSUMER_POLL_ZERO.
 *
 * <p>Fires when {@code consumer.poll(...)} is invoked with a zero
 * timeout. The rule covers every literal-zero form the JVM produces:
 * <ul>
 *   <li>{@code poll(0L)} — the deprecated long-millis overload, with
 *       a literal zero (bytecode: LCONST_0 + INVOKEVIRTUAL poll(J)).</li>
 *   <li>{@code poll(Duration.ZERO)} — the modern Duration overload
 *       with the static-field zero (bytecode: GETSTATIC Duration.ZERO).</li>
 *   <li>{@code poll(Duration.ofMillis(0))} — Duration constructed from
 *       a literal-zero long (LCONST_0 + INVOKESTATIC Duration.ofMillis).</li>
 *   <li>{@code poll(Duration.ofNanos(0))} — same shape, different
 *       factory.</li>
 *   <li>{@code poll(Duration.ofSeconds(0))} — same shape, different
 *       factory.</li>
 *   <li>{@code poll(Duration.ofMinutes(0))} /
 *       {@code poll(Duration.ofHours(0))} /
 *       {@code poll(Duration.ofDays(0))} — coarse-grain factories. Less
 *       common in practice but still bit-exact equivalents of
 *       {@code Duration.ZERO}; can creep in via inlined static-final
 *       constants ({@code static final long IDLE = 0;}), generated code,
 *       or refactors that erase a non-zero default. The rule covers them
 *       so detection stays complete and matches the sibling
 *       {@code CONSUMER_CLOSE_ZERO_DURATION} factory set.</li>
 * </ul>
 *
 * <p>Why poll(0) is wrong:
 * <ol>
 *   <li>{@code consumer.poll(timeout)} is the consumer's hot-path
 *       loop: it fetches records, processes coordinator heartbeats,
 *       drains the fetcher network buffers, and blocks UP TO
 *       {@code timeout} waiting for the broker to deliver records.
 *       If {@code timeout} is zero, the call returns IMMEDIATELY
 *       whether or not any records are available.</li>
 *   <li>The almost-universal calling pattern is
 *       {@code while (running) { records = consumer.poll(t); process(records); }}.
 *       With {@code t = 0}, that loop becomes a tight busy-spin:
 *       it makes thousands of poll() calls per second, almost all
 *       of which return zero records.</li>
 *   <li>Effect on the consumer process: a single thread pins one
 *       full CPU core, doing nothing useful. Each idle poll still
 *       updates internal state, sends heartbeats, and runs the
 *       fetcher, so the cost is not trivial — it's a hot loop in
 *       Java with no I/O backoff.</li>
 *   <li>Effect on the broker: each poll() may send a FetchRequest
 *       (or at minimum, exercises the coordinator with heartbeat
 *       traffic). A consumer in busy-spin can submit 50k+
 *       FetchRequests per second, slamming the broker's
 *       request-handler queue and pushing tail latency up for
 *       every other consumer group on the same coordinator.</li>
 * </ol>
 *
 * <p>What it looks like in production:
 * <ol>
 *   <li>Developer wants "non-blocking poll" semantics — "if there's
 *       nothing, return right away so my loop stays responsive."
 *       They write {@code consumer.poll(Duration.ZERO)} thinking it's
 *       the lighter form.</li>
 *   <li>App CPU goes to 100% on a single core. Container limits
 *       throttle the JVM, response latency spikes, the team blames
 *       "Kafka being slow."</li>
 *   <li>The fix is one line: pass a real timeout (typically 100ms
 *       to 1s for normal back-pressure-tolerant consumers).</li>
 * </ol>
 *
 * <p>What the rule catches: {@code poll(...)} on a Consumer owner
 * where the argument is one of the literal-zero forms above. Two
 * detection paths:
 * <ul>
 *   <li>For the long overload: the previous significant instruction
 *       is LCONST_0.</li>
 *   <li>For the Duration overload: the previous significant
 *       instruction is either GETSTATIC Duration.ZERO, or
 *       INVOKESTATIC Duration.ofMillis/ofNanos/ofSeconds preceded by
 *       LCONST_0.</li>
 * </ul>
 *
 * <p>The rule does NOT fire on non-literal forms (e.g. a Duration
 * pulled from a config) — those are out of scope by design (false
 * positives are worse than missing a dynamic-zero case). The
 * literal-zero detection covers the overwhelming majority of
 * real-world occurrences.
 */
public final class BadConsumerPollZero {

    /** Anti-pattern: poll(Duration.ZERO) — FIRES (GETSTATIC Duration.ZERO). */
    public ConsumerRecords<String, String> pollDurationZero(Consumer<String, String> consumer) {
        return consumer.poll(Duration.ZERO); // FIRES
    }

    /** Anti-pattern: poll(Duration.ofMillis(0)) — FIRES (Duration.ofMillis preceded by LCONST_0). */
    public ConsumerRecords<String, String> pollOfMillisZero(Consumer<String, String> consumer) {
        return consumer.poll(Duration.ofMillis(0)); // FIRES
    }

    /** Anti-pattern: poll(Duration.ofNanos(0)) — FIRES. */
    public ConsumerRecords<String, String> pollOfNanosZero(Consumer<String, String> consumer) {
        return consumer.poll(Duration.ofNanos(0)); // FIRES
    }

    /** Anti-pattern: poll(Duration.ofSeconds(0)) — FIRES. */
    public ConsumerRecords<String, String> pollOfSecondsZero(Consumer<String, String> consumer) {
        return consumer.poll(Duration.ofSeconds(0)); // FIRES
    }

    /** Anti-pattern: poll(Duration.ofMinutes(0)) — FIRES (coarse factory still folds to zero). */
    public ConsumerRecords<String, String> pollOfMinutesZero(Consumer<String, String> consumer) {
        return consumer.poll(Duration.ofMinutes(0)); // FIRES
    }

    /** Anti-pattern: poll(Duration.ofHours(0)) — FIRES (coarse factory still folds to zero). */
    public ConsumerRecords<String, String> pollOfHoursZero(Consumer<String, String> consumer) {
        return consumer.poll(Duration.ofHours(0)); // FIRES
    }

    /** Anti-pattern: poll(Duration.ofDays(0)) — FIRES (coarse factory still folds to zero). */
    public ConsumerRecords<String, String> pollOfDaysZero(Consumer<String, String> consumer) {
        return consumer.poll(Duration.ofDays(0)); // FIRES
    }

    /** Anti-pattern: poll(0L) on KafkaConsumer concrete — FIRES (deprecated long overload, LCONST_0). */
    @SuppressWarnings("deprecation")
    public ConsumerRecords<String, String> pollZeroLong(KafkaConsumer<String, String> consumer) {
        return consumer.poll(0L); // FIRES
    }

    /** Control: poll(Duration.ofMillis(500)) — non-zero literal, must NOT fire. */
    public ConsumerRecords<String, String> pollMillis500(Consumer<String, String> consumer) {
        return consumer.poll(Duration.ofMillis(500));
    }

    /** Control: poll(Duration.ofSeconds(1)) — non-zero literal, must NOT fire. */
    public ConsumerRecords<String, String> pollSecondsOne(Consumer<String, String> consumer) {
        return consumer.poll(Duration.ofSeconds(1));
    }

    /** Control: poll(Duration) where the Duration is a parameter — out of scope, must NOT fire. */
    public ConsumerRecords<String, String> pollDynamicDuration(Consumer<String, String> consumer,
                                                              Duration timeout) {
        return consumer.poll(timeout);
    }
}
