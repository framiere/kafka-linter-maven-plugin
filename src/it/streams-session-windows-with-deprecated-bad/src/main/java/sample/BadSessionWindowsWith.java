package sample;

import org.apache.kafka.streams.kstream.SessionWindows;

import java.time.Duration;
import java.util.function.Function;

/**
 * RULE: STREAMS_SESSION_WINDOWS_WITH_DEPRECATED — must fire on both
 * methods.
 *
 * <p>The two methods below exercise the two distinct bytecode shapes
 * the rule is required to catch for the deprecated 1-arg static
 * factory {@code SessionWindows.with(Duration)}:
 *
 * <ol>
 *   <li>Direct {@code INVOKESTATIC} on the legacy factory — the
 *       classic call site, where the user-class bytecode contains an
 *       explicit {@code INVOKESTATIC
 *       org/apache/kafka/streams/kstream/SessionWindows.with(Ljava/time/Duration;)Lorg/apache/kafka/streams/kstream/SessionWindows;}.</li>
 *   <li>{@code INVOKEDYNAMIC} method-ref capture
 *       {@code SessionWindows::with} bound to a
 *       {@code Function<Duration, SessionWindows>} SAM — javac
 *       resolves the method-ref by arity to the legacy single-arg
 *       overload (no other static {@code with} on
 *       {@code SessionWindows} takes a {@code Duration} and returns
 *       a {@code SessionWindows}). javac emits an
 *       {@code INVOKEDYNAMIC} site whose bsm-args contain a
 *       {@code REF_invokeStatic} handle pointing at
 *       {@code SessionWindows.with(Ljava/time/Duration;)Lorg/apache/kafka/streams/kstream/SessionWindows;}.
 *       The user-class bytecode at this site contains ZERO direct
 *       {@code INVOKESTATIC} on the legacy factory — only the
 *       {@code INVOKEDYNAMIC} + {@code LambdaMetafactory} bridge. A
 *       name-only MethodInsnNode walk misses this case entirely; the
 *       rule's bsm-arg walk via {@code AsmUtil.indyTargetHandle}
 *       catches it.</li>
 * </ol>
 *
 * <h2>Why this factory is dangerous</h2>
 *
 * <p>{@code SessionWindows.with(Duration inactivityGap)} accepts only
 * the inactivity gap and silently inherits the legacy default grace
 * period of {@code 24h - inactivityGap}. Two failure modes that are
 * specific to session windows (and not shared with the time-window
 * factories): late events silently fuse sessions that should be
 * separate (a new event arriving up to 24h after the previous one on
 * the same key is treated as belonging to the same session), and
 * session-close suppression fires ~24h late. KIP-633 (Kafka Streams
 * 3.0) split the factory into
 * {@code ofInactivityGapWithNoGrace(Duration)} and
 * {@code ofInactivityGapAndGrace(Duration, Duration)} so grace is
 * always explicit at the call site.
 */
public final class BadSessionWindowsWith {

    @SuppressWarnings("deprecation")
    public SessionWindows buildDirect() {
        // MUST FIRE — direct INVOKESTATIC on the deprecated factory
        // SessionWindows.with(Duration). javac emits an INVOKESTATIC
        // with descriptor
        // (Ljava/time/Duration;)Lorg/apache/kafka/streams/kstream/SessionWindows;.
        return SessionWindows.with(Duration.ofMinutes(5));
    }

    @SuppressWarnings("deprecation")
    public Function<Duration, SessionWindows> capturedFactory() {
        // MUST FIRE — INVOKEDYNAMIC method-ref capture targeting the
        // deprecated factory. javac resolves `SessionWindows::with`
        // by matching the SAM's argument arity (1 Duration) and
        // return type against the available static factories,
        // picking the legacy with(Duration). javac emits an
        // INVOKEDYNAMIC site whose bsm-args contain a
        // REF_invokeStatic handle on
        // (Ljava/time/Duration;)Lorg/apache/kafka/streams/kstream/SessionWindows;.
        // The user-class bytecode here contains ZERO direct
        // INVOKESTATIC on the legacy factory — only the
        // INVOKEDYNAMIC + LambdaMetafactory bridge. A name-only
        // MethodInsnNode walk misses this entirely; the rule's
        // bsm-arg walk catches it.
        return SessionWindows::with;
    }
}
