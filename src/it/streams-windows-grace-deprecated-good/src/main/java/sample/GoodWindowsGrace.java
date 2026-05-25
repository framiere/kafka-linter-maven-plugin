package sample;

import org.apache.kafka.streams.kstream.JoinWindows;
import org.apache.kafka.streams.kstream.SessionWindows;
import org.apache.kafka.streams.kstream.TimeWindows;

import java.time.Duration;

/**
 * RULE: STREAMS_WINDOWS_GRACE_DEPRECATED — must NOT fire on any of
 * the six methods below.
 *
 * <p>Each method uses one of the post-KIP-633 static factories that
 * make grace explicit at construction time. None of the six call sites
 * reaches the legacy chained {@code .grace(Duration)} virtual method
 * on {@code TimeWindows}, {@code JoinWindows}, or {@code SessionWindows}:
 *
 * <ol>
 *   <li>{@code TimeWindows.ofSizeAndGrace(size, grace)} — explicit
 *       grace at construction. INVOKESTATIC on the new factory; the
 *       legacy chained grace method is not reached.</li>
 *   <li>{@code TimeWindows.ofSizeWithNoGrace(size)} — explicit
 *       "no grace" at construction. INVOKESTATIC on the new factory.</li>
 *   <li>{@code JoinWindows.ofTimeDifferenceAndGrace(diff, grace)} —
 *       explicit grace at construction.</li>
 *   <li>{@code JoinWindows.ofTimeDifferenceWithNoGrace(diff)} —
 *       explicit "no grace" at construction.</li>
 *   <li>{@code SessionWindows.ofInactivityGapAndGrace(gap, grace)} —
 *       explicit grace at construction.</li>
 *   <li>{@code SessionWindows.ofInactivityGapWithNoGrace(gap)} —
 *       explicit "no grace" at construction.</li>
 * </ol>
 *
 * <p>The new factories close the four KIP-633 incident classes that
 * justified the deprecation:
 *
 * <ul>
 *   <li>No more silent 24-hour grace default — the grace decision is
 *       made at the call site and is visible in source.</li>
 *   <li>No chained-order fragility — a single static call replaces the
 *       order-dependent {@code of(size).advanceBy(advance).grace(grace)}
 *       chain.</li>
 *   <li>No runtime {@code IllegalStateException} from mixing new
 *       factories with a stray {@code .grace()} — the legacy chained
 *       method is simply absent from the new flow.</li>
 *   <li>No INVOKEDYNAMIC method-ref capture path to the deprecated
 *       method — there is no method-ref equivalent of a static factory
 *       chain.</li>
 * </ul>
 */
public final class GoodWindowsGrace {

    public TimeWindows newTimeWindowsExplicitGrace() {
        // DOES NOT FIRE — TimeWindows.ofSizeAndGrace(size, grace) is
        // the KIP-633 replacement: explicit grace at construction, no
        // chained .grace() and no hidden 24-hour default.
        return TimeWindows.ofSizeAndGrace(Duration.ofMinutes(5), Duration.ofSeconds(30));
    }

    public TimeWindows newTimeWindowsNoGrace() {
        // DOES NOT FIRE — TimeWindows.ofSizeWithNoGrace(size) is the
        // KIP-633 replacement for "I really mean zero grace". Forces
        // the caller to acknowledge the grace decision.
        return TimeWindows.ofSizeWithNoGrace(Duration.ofMinutes(5));
    }

    public JoinWindows newJoinWindowsExplicitGrace() {
        // DOES NOT FIRE — JoinWindows.ofTimeDifferenceAndGrace(diff,
        // grace) is the KIP-633 replacement.
        return JoinWindows.ofTimeDifferenceAndGrace(Duration.ofMinutes(2), Duration.ofSeconds(15));
    }

    public JoinWindows newJoinWindowsNoGrace() {
        // DOES NOT FIRE — JoinWindows.ofTimeDifferenceWithNoGrace(diff)
        // is the KIP-633 "zero grace" replacement.
        return JoinWindows.ofTimeDifferenceWithNoGrace(Duration.ofMinutes(2));
    }

    public SessionWindows newSessionWindowsExplicitGrace() {
        // DOES NOT FIRE — SessionWindows.ofInactivityGapAndGrace(gap,
        // grace) is the KIP-633 replacement.
        return SessionWindows.ofInactivityGapAndGrace(Duration.ofMinutes(10), Duration.ofMinutes(1));
    }

    public SessionWindows newSessionWindowsNoGrace() {
        // DOES NOT FIRE — SessionWindows.ofInactivityGapWithNoGrace(gap)
        // is the KIP-633 "zero grace" replacement.
        return SessionWindows.ofInactivityGapWithNoGrace(Duration.ofMinutes(10));
    }
}
