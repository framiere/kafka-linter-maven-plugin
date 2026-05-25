package sample;

import org.apache.kafka.streams.kstream.SessionWindows;

import java.time.Duration;
import java.util.function.BiFunction;
import java.util.function.Function;

/**
 * RULE: STREAMS_SESSION_WINDOWS_WITH_DEPRECATED — must NOT fire.
 *
 * <p>The four methods below exercise the supported migration targets:
 *
 * <ol>
 *   <li>Direct {@code INVOKESTATIC} on
 *       {@code SessionWindows.ofInactivityGapWithNoGrace(Duration)} —
 *       the grace-free 1-arg replacement. Same arity as the legacy
 *       factory, distinct method name, so the rule's name filter
 *       rejects the site.</li>
 *   <li>Direct {@code INVOKESTATIC} on
 *       {@code SessionWindows.ofInactivityGapAndGrace(Duration, Duration)}
 *       — the explicit-grace 2-arg replacement. Distinct method name
 *       from the legacy factory, so the rule's name filter rejects
 *       the site.</li>
 *   <li>{@code INVOKEDYNAMIC} method-ref capture
 *       {@code SessionWindows::ofInactivityGapWithNoGrace} bound to a
 *       {@code Function<Duration, SessionWindows>} — the bsm-arg
 *       handle's name is {@code ofInactivityGapWithNoGrace}, not
 *       {@code with}, so the rule's name filter rejects the site.</li>
 *   <li>{@code INVOKEDYNAMIC} method-ref capture
 *       {@code SessionWindows::ofInactivityGapAndGrace} bound to a
 *       {@code BiFunction<Duration, Duration, SessionWindows>} — the
 *       bsm-arg handle's name is {@code ofInactivityGapAndGrace},
 *       not {@code with}, so the rule's name filter rejects the
 *       site.</li>
 * </ol>
 *
 * <h2>Why this is the right migration</h2>
 *
 * <p>KIP-633 (Kafka Streams 3.0) split the legacy 1-arg
 * {@code with(Duration)} factory in two precisely so the grace
 * period cannot be silently inherited from a stale 24h default:
 *
 * <ul>
 *   <li>{@code ofInactivityGapWithNoGrace(Duration inactivityGap)}
 *       pins grace to zero. Late events open a new session instead
 *       of fusing onto the prior one; the session-windowed store
 *       evicts each session exactly {@code inactivityGap} after the
 *       last event on that key. This is the right call for any
 *       sessionization topology that wants bounded session-state and
 *       deterministic session-close timing.</li>
 *   <li>{@code ofInactivityGapAndGrace(Duration inactivityGap,
 *       Duration grace)} takes an explicit grace argument. Set this
 *       to the smallest value that absorbs your real upstream-clock
 *       skew — typically seconds or minutes, never the legacy
 *       default's hours.</li>
 * </ul>
 *
 * <p>The replacement factories have distinct method names from the
 * legacy {@code with(Duration)}, so the rule's name filter
 * discriminates — no descriptor magic is required to keep the
 * supported migration targets safe.
 */
public final class GoodSessionWindowsOfInactivityGap {

    public SessionWindows buildNoGrace() {
        // DOES NOT FIRE — ofInactivityGapWithNoGrace is a distinct
        // method name from the legacy with, so the name filter
        // rejects the site.
        return SessionWindows.ofInactivityGapWithNoGrace(Duration.ofMinutes(5));
    }

    public SessionWindows buildWithGrace() {
        // DOES NOT FIRE — ofInactivityGapAndGrace is a distinct
        // method name from the legacy with, so the name filter
        // rejects the site.
        return SessionWindows.ofInactivityGapAndGrace(Duration.ofMinutes(5), Duration.ofSeconds(30));
    }

    public Function<Duration, SessionWindows> capturedNoGraceFactory() {
        // DOES NOT FIRE — INVOKEDYNAMIC method-ref capture resolving
        // to ofInactivityGapWithNoGrace. The bsm-arg handle's name
        // is ofInactivityGapWithNoGrace, not with, so the rule's
        // name filter rejects the site.
        return SessionWindows::ofInactivityGapWithNoGrace;
    }

    public BiFunction<Duration, Duration, SessionWindows> capturedWithGraceFactory() {
        // DOES NOT FIRE — INVOKEDYNAMIC method-ref capture resolving
        // to ofInactivityGapAndGrace. The bsm-arg handle's name is
        // ofInactivityGapAndGrace, not with, so the rule's name
        // filter rejects the site.
        return SessionWindows::ofInactivityGapAndGrace;
    }
}
