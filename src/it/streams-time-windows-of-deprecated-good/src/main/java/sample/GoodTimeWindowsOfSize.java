package sample;

import org.apache.kafka.streams.kstream.TimeWindows;

import java.time.Duration;
import java.util.function.BiFunction;
import java.util.function.Function;

/**
 * RULE: STREAMS_TIME_WINDOWS_OF_DEPRECATED — must NOT fire.
 *
 * <p>The four methods below exercise the supported migration targets:
 *
 * <ol>
 *   <li>Direct {@code INVOKESTATIC} on
 *       {@code TimeWindows.ofSizeWithNoGrace(Duration)} — the
 *       grace-free 1-arg replacement. Same arity as the legacy
 *       factory, distinct method name, so the rule's name filter
 *       rejects the site.</li>
 *   <li>Direct {@code INVOKESTATIC} on
 *       {@code TimeWindows.ofSizeAndGrace(Duration, Duration)} — the
 *       explicit-grace 2-arg replacement. Distinct method name from
 *       the legacy factory, so the rule's name filter rejects the
 *       site.</li>
 *   <li>{@code INVOKEDYNAMIC} method-ref capture
 *       {@code TimeWindows::ofSizeWithNoGrace} bound to a
 *       {@code Function<Duration, TimeWindows>} — the bsm-arg handle's
 *       name is {@code ofSizeWithNoGrace}, not {@code of}, so the
 *       rule's name filter rejects the site.</li>
 *   <li>{@code INVOKEDYNAMIC} method-ref capture
 *       {@code TimeWindows::ofSizeAndGrace} bound to a
 *       {@code BiFunction<Duration, Duration, TimeWindows>} — the
 *       bsm-arg handle's name is {@code ofSizeAndGrace}, not
 *       {@code of}, so the rule's name filter rejects the site.</li>
 * </ol>
 *
 * <h2>Why this is the right migration</h2>
 *
 * <p>KIP-633 (Kafka Streams 3.0) split the legacy 1-arg
 * {@code of(Duration)} factory in two precisely so the grace period
 * cannot be silently inherited from a stale default:
 *
 * <ul>
 *   <li>{@code ofSizeWithNoGrace(Duration size)} pins grace to zero.
 *       Late events are dropped at the window boundary; state-store
 *       retention equals the window size exactly. This is the right
 *       call for any topology that wants bounded state and
 *       deterministic emission timing.</li>
 *   <li>{@code ofSizeAndGrace(Duration size, Duration grace)} takes
 *       an explicit grace argument. Set this to the smallest value
 *       that absorbs your real upstream-clock skew — typically
 *       seconds or minutes, never the legacy default's hours.</li>
 * </ul>
 *
 * <p>The replacement factories have distinct method names from the
 * legacy {@code of(Duration)}, so the rule's name filter discriminates
 * — no descriptor magic is required to keep the supported migration
 * targets safe.
 */
public final class GoodTimeWindowsOfSize {

    public TimeWindows buildNoGrace() {
        // DOES NOT FIRE — ofSizeWithNoGrace is a distinct method name
        // from the legacy of, so the name filter rejects the site.
        return TimeWindows.ofSizeWithNoGrace(Duration.ofMinutes(1));
    }

    public TimeWindows buildWithGrace() {
        // DOES NOT FIRE — ofSizeAndGrace is a distinct method name
        // from the legacy of, so the name filter rejects the site.
        return TimeWindows.ofSizeAndGrace(Duration.ofMinutes(1), Duration.ofSeconds(30));
    }

    public Function<Duration, TimeWindows> capturedNoGraceFactory() {
        // DOES NOT FIRE — INVOKEDYNAMIC method-ref capture resolving
        // to ofSizeWithNoGrace. The bsm-arg handle's name is
        // ofSizeWithNoGrace, not of, so the rule's name filter
        // rejects the site.
        return TimeWindows::ofSizeWithNoGrace;
    }

    public BiFunction<Duration, Duration, TimeWindows> capturedWithGraceFactory() {
        // DOES NOT FIRE — INVOKEDYNAMIC method-ref capture resolving
        // to ofSizeAndGrace. The bsm-arg handle's name is
        // ofSizeAndGrace, not of, so the rule's name filter rejects
        // the site.
        return TimeWindows::ofSizeAndGrace;
    }
}
