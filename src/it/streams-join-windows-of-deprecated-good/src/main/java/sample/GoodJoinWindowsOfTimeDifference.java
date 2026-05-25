package sample;

import org.apache.kafka.streams.kstream.JoinWindows;

import java.time.Duration;
import java.util.function.BiFunction;
import java.util.function.Function;

/**
 * RULE: STREAMS_JOIN_WINDOWS_OF_DEPRECATED — must NOT fire.
 *
 * <p>The four methods below exercise the supported migration targets:
 *
 * <ol>
 *   <li>Direct {@code INVOKESTATIC} on
 *       {@code JoinWindows.ofTimeDifferenceWithNoGrace(Duration)} —
 *       the grace-free 1-arg replacement. Same arity as the legacy
 *       factory, distinct method name, so the rule's name filter
 *       rejects the site.</li>
 *   <li>Direct {@code INVOKESTATIC} on
 *       {@code JoinWindows.ofTimeDifferenceAndGrace(Duration, Duration)}
 *       — the explicit-grace 2-arg replacement. Distinct method name
 *       from the legacy factory, so the rule's name filter rejects
 *       the site.</li>
 *   <li>{@code INVOKEDYNAMIC} method-ref capture
 *       {@code JoinWindows::ofTimeDifferenceWithNoGrace} bound to a
 *       {@code Function<Duration, JoinWindows>} — the bsm-arg
 *       handle's name is {@code ofTimeDifferenceWithNoGrace}, not
 *       {@code of}, so the rule's name filter rejects the site.</li>
 *   <li>{@code INVOKEDYNAMIC} method-ref capture
 *       {@code JoinWindows::ofTimeDifferenceAndGrace} bound to a
 *       {@code BiFunction<Duration, Duration, JoinWindows>} — the
 *       bsm-arg handle's name is {@code ofTimeDifferenceAndGrace},
 *       not {@code of}, so the rule's name filter rejects the
 *       site.</li>
 * </ol>
 *
 * <h2>Why this is the right migration</h2>
 *
 * <p>KIP-633 (Kafka Streams 3.0) split the legacy 1-arg
 * {@code of(Duration)} factory in two precisely so the grace period
 * cannot be silently inherited from a stale 24h default:
 *
 * <ul>
 *   <li>{@code ofTimeDifferenceWithNoGrace(Duration timeDifference)}
 *       pins grace to zero. Late join candidates are dropped at the
 *       window boundary; both sides' state-store retention equals the
 *       symmetric join-window width exactly. This is the right call
 *       for any stream-stream join that wants bounded join-state and
 *       deterministic outer-join null-emission timing.</li>
 *   <li>{@code ofTimeDifferenceAndGrace(Duration timeDifference,
 *       Duration grace)} takes an explicit grace argument. Set this
 *       to the smallest value that absorbs your real upstream-clock
 *       skew between the two streams — typically seconds or minutes,
 *       never the legacy default's hours.</li>
 * </ul>
 *
 * <p>The replacement factories have distinct method names from the
 * legacy {@code of(Duration)}, so the rule's name filter
 * discriminates — no descriptor magic is required to keep the
 * supported migration targets safe.
 */
public final class GoodJoinWindowsOfTimeDifference {

    public JoinWindows buildNoGrace() {
        // DOES NOT FIRE — ofTimeDifferenceWithNoGrace is a distinct
        // method name from the legacy of, so the name filter rejects
        // the site.
        return JoinWindows.ofTimeDifferenceWithNoGrace(Duration.ofMinutes(1));
    }

    public JoinWindows buildWithGrace() {
        // DOES NOT FIRE — ofTimeDifferenceAndGrace is a distinct
        // method name from the legacy of, so the name filter rejects
        // the site.
        return JoinWindows.ofTimeDifferenceAndGrace(Duration.ofMinutes(1), Duration.ofSeconds(30));
    }

    public Function<Duration, JoinWindows> capturedNoGraceFactory() {
        // DOES NOT FIRE — INVOKEDYNAMIC method-ref capture resolving
        // to ofTimeDifferenceWithNoGrace. The bsm-arg handle's name
        // is ofTimeDifferenceWithNoGrace, not of, so the rule's name
        // filter rejects the site.
        return JoinWindows::ofTimeDifferenceWithNoGrace;
    }

    public BiFunction<Duration, Duration, JoinWindows> capturedWithGraceFactory() {
        // DOES NOT FIRE — INVOKEDYNAMIC method-ref capture resolving
        // to ofTimeDifferenceAndGrace. The bsm-arg handle's name is
        // ofTimeDifferenceAndGrace, not of, so the rule's name
        // filter rejects the site.
        return JoinWindows::ofTimeDifferenceAndGrace;
    }
}
