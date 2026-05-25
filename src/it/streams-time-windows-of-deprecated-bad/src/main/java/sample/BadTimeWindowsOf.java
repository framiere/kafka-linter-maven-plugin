package sample;

import org.apache.kafka.streams.kstream.TimeWindows;

import java.time.Duration;
import java.util.function.Function;

/**
 * RULE: STREAMS_TIME_WINDOWS_OF_DEPRECATED — must fire on both methods.
 *
 * <p>The two methods below exercise the two distinct bytecode shapes
 * the rule is required to catch for the deprecated 1-arg static
 * factory {@code TimeWindows.of(Duration)}:
 *
 * <ol>
 *   <li>Direct {@code INVOKESTATIC} on the legacy factory — the
 *       classic call site, where the user-class bytecode contains an
 *       explicit {@code INVOKESTATIC
 *       org/apache/kafka/streams/kstream/TimeWindows.of(Ljava/time/Duration;)Lorg/apache/kafka/streams/kstream/TimeWindows;}.</li>
 *   <li>{@code INVOKEDYNAMIC} method-ref capture {@code TimeWindows::of}
 *       bound to a {@code Function<Duration, TimeWindows>} SAM —
 *       javac resolves the method-ref by arity to the legacy
 *       single-arg overload (there is no other static {@code of} on
 *       {@code TimeWindows} that takes a {@code Duration} and returns
 *       a {@code TimeWindows}). javac emits an {@code INVOKEDYNAMIC}
 *       site whose bsm-args contain a {@code REF_invokeStatic} handle
 *       pointing at
 *       {@code TimeWindows.of(Ljava/time/Duration;)Lorg/apache/kafka/streams/kstream/TimeWindows;}.
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
 * <p>{@code TimeWindows.of(Duration size)} accepts only the window
 * size and silently inherits the legacy default grace period of
 * {@code 24h - size} — for a 1-minute window that is ~24 hours of
 * grace, ~1440× the user-visible window width. State stores hold
 * every window open for {@code size + grace = 24h} instead of the
 * user-expected ~few minutes, blowing up RocksDB footprint and
 * delaying suppression by a full day. KIP-633 (Kafka Streams 3.0)
 * split the factory into {@code ofSizeWithNoGrace(Duration)} and
 * {@code ofSizeAndGrace(Duration, Duration)} so grace is always
 * explicit at the call site.
 */
public final class BadTimeWindowsOf {

    @SuppressWarnings("deprecation")
    public TimeWindows buildDirect() {
        // MUST FIRE — direct INVOKESTATIC on the deprecated factory
        // TimeWindows.of(Duration). javac emits an INVOKESTATIC with
        // descriptor (Ljava/time/Duration;)Lorg/apache/kafka/streams/kstream/TimeWindows;.
        return TimeWindows.of(Duration.ofMinutes(1));
    }

    @SuppressWarnings("deprecation")
    public Function<Duration, TimeWindows> capturedFactory() {
        // MUST FIRE — INVOKEDYNAMIC method-ref capture targeting the
        // deprecated factory. javac resolves `TimeWindows::of` by
        // matching the SAM's argument arity (1 Duration) and return
        // type against the available static factories, picking the
        // legacy of(Duration). javac emits an INVOKEDYNAMIC site whose
        // bsm-args contain a REF_invokeStatic handle on
        // (Ljava/time/Duration;)Lorg/apache/kafka/streams/kstream/TimeWindows;.
        // The user-class bytecode here contains ZERO direct
        // INVOKESTATIC on the legacy factory — only the INVOKEDYNAMIC
        // + LambdaMetafactory bridge. A name-only MethodInsnNode walk
        // misses this entirely; the rule's bsm-arg walk catches it.
        return TimeWindows::of;
    }
}
