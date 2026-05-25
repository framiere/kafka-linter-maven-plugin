package sample;

import org.apache.kafka.streams.kstream.JoinWindows;
import org.apache.kafka.streams.kstream.SessionWindows;
import org.apache.kafka.streams.kstream.TimeWindows;

import java.time.Duration;
import java.util.function.BiFunction;

/**
 * RULE: STREAMS_WINDOWS_GRACE_DEPRECATED — must fire on all six methods
 * below.
 *
 * <p>The six methods exercise the six distinct bytecode shapes the rule
 * is required to catch — three direct calls (one per owner) plus three
 * {@code INVOKEDYNAMIC} method-ref captures (one per owner):
 *
 * <ol>
 *   <li>Direct {@code INVOKEVIRTUAL} on
 *       {@code TimeWindows.grace(Duration)}. The static factory
 *       {@code TimeWindows.of(Duration)} returns a {@code TimeWindows}
 *       whose {@code grace(Duration)} virtual method we then chain.</li>
 *   <li>Direct {@code INVOKEVIRTUAL} on
 *       {@code JoinWindows.grace(Duration)}. The static factory
 *       {@code JoinWindows.of(Duration)} returns a {@code JoinWindows}
 *       whose {@code grace(Duration)} virtual method we then chain.</li>
 *   <li>Direct {@code INVOKEVIRTUAL} on
 *       {@code SessionWindows.grace(Duration)}. The static factory
 *       {@code SessionWindows.with(Duration)} returns a
 *       {@code SessionWindows} whose {@code grace(Duration)} virtual
 *       method we then chain.</li>
 *   <li>{@code INVOKEDYNAMIC} unbound method-ref capture
 *       {@code TimeWindows::grace} bound to a
 *       {@code BiFunction<TimeWindows, Duration, TimeWindows>}. javac
 *       resolves the method-ref to {@code grace(Duration)} by matching
 *       the SAM's (receiver, arg, return-erasure) triple. The
 *       user-class bytecode at this site contains ZERO direct
 *       INVOKEVIRTUAL on the legacy method — only the INVOKEDYNAMIC +
 *       LambdaMetafactory bridge whose bsm-args contain a
 *       REF_invokeVirtual handle on TimeWindows.grace.</li>
 *   <li>{@code INVOKEDYNAMIC} unbound method-ref capture
 *       {@code JoinWindows::grace} bound to a
 *       {@code BiFunction<JoinWindows, Duration, JoinWindows>}.</li>
 *   <li>{@code INVOKEDYNAMIC} unbound method-ref capture
 *       {@code SessionWindows::grace} bound to a
 *       {@code BiFunction<SessionWindows, Duration, SessionWindows>}.</li>
 * </ol>
 *
 * <h2>Why these methods are deprecated</h2>
 *
 * <p>{@code TimeWindows.grace(Duration)},
 * {@code JoinWindows.grace(Duration)}, and
 * {@code SessionWindows.grace(Duration)} were deprecated by KIP-633
 * (Kafka Streams 3.0, September 2021) along with the legacy
 * {@code Windows.of(...)} static factories themselves. The legacy
 * factories baked in a 24-hour default grace when {@code .grace()} was
 * omitted, silently buffering late events for up to a day in heap and
 * in the changelog topic. The new static factories make grace explicit
 * at construction:
 *
 * <ul>
 *   <li>{@code TimeWindows.ofSizeAndGrace(size, grace)} /
 *       {@code TimeWindows.ofSizeWithNoGrace(size)}</li>
 *   <li>{@code JoinWindows.ofTimeDifferenceAndGrace(diff, grace)} /
 *       {@code JoinWindows.ofTimeDifferenceWithNoGrace(diff)}</li>
 *   <li>{@code SessionWindows.ofInactivityGapAndGrace(gap, grace)} /
 *       {@code SessionWindows.ofInactivityGapWithNoGrace(gap)}</li>
 * </ul>
 */
public final class BadWindowsGrace {

    @SuppressWarnings("deprecation")
    public TimeWindows directTimeWindowsGrace() {
        // MUST FIRE — chained .grace(Duration) on TimeWindows. The
        // legacy TimeWindows.of(size) factory silently baked a 24-hour
        // default grace; this chained .grace(...) call site is the
        // exact migration target the rule must surface.
        return TimeWindows.of(Duration.ofMinutes(5)).grace(Duration.ofSeconds(30));
    }

    @SuppressWarnings("deprecation")
    public JoinWindows directJoinWindowsGrace() {
        // MUST FIRE — chained .grace(Duration) on JoinWindows.
        return JoinWindows.of(Duration.ofMinutes(2)).grace(Duration.ofSeconds(15));
    }

    @SuppressWarnings("deprecation")
    public SessionWindows directSessionWindowsGrace() {
        // MUST FIRE — chained .grace(Duration) on SessionWindows.
        return SessionWindows.with(Duration.ofMinutes(10)).grace(Duration.ofMinutes(1));
    }

    @SuppressWarnings("deprecation")
    public BiFunction<TimeWindows, Duration, TimeWindows> capturedTimeWindowsGrace() {
        // MUST FIRE — INVOKEDYNAMIC method-ref capture targeting the
        // deprecated TimeWindows.grace(Duration). The bsm-args contain
        // a REF_invokeVirtual handle on
        // (Ljava/time/Duration;)Lorg/apache/kafka/streams/kstream/TimeWindows;.
        return TimeWindows::grace;
    }

    @SuppressWarnings("deprecation")
    public BiFunction<JoinWindows, Duration, JoinWindows> capturedJoinWindowsGrace() {
        // MUST FIRE — INVOKEDYNAMIC method-ref capture targeting the
        // deprecated JoinWindows.grace(Duration). The bsm-args contain
        // a REF_invokeVirtual handle on
        // (Ljava/time/Duration;)Lorg/apache/kafka/streams/kstream/JoinWindows;.
        return JoinWindows::grace;
    }

    @SuppressWarnings("deprecation")
    public BiFunction<SessionWindows, Duration, SessionWindows> capturedSessionWindowsGrace() {
        // MUST FIRE — INVOKEDYNAMIC method-ref capture targeting the
        // deprecated SessionWindows.grace(Duration). The bsm-args
        // contain a REF_invokeVirtual handle on
        // (Ljava/time/Duration;)Lorg/apache/kafka/streams/kstream/SessionWindows;.
        return SessionWindows::grace;
    }
}
