package sample;

import org.apache.kafka.streams.kstream.JoinWindows;

import java.time.Duration;
import java.util.function.Function;

/**
 * RULE: STREAMS_JOIN_WINDOWS_OF_DEPRECATED — must fire on both methods.
 *
 * <p>The two methods below exercise the two distinct bytecode shapes
 * the rule is required to catch for the deprecated 1-arg static
 * factory {@code JoinWindows.of(Duration)}:
 *
 * <ol>
 *   <li>Direct {@code INVOKESTATIC} on the legacy factory — the
 *       classic call site, where the user-class bytecode contains an
 *       explicit {@code INVOKESTATIC
 *       org/apache/kafka/streams/kstream/JoinWindows.of(Ljava/time/Duration;)Lorg/apache/kafka/streams/kstream/JoinWindows;}.</li>
 *   <li>{@code INVOKEDYNAMIC} method-ref capture {@code JoinWindows::of}
 *       bound to a {@code Function<Duration, JoinWindows>} SAM —
 *       javac resolves the method-ref by arity to the legacy
 *       single-arg overload (no other static {@code of} on
 *       {@code JoinWindows} takes a {@code Duration} and returns a
 *       {@code JoinWindows}). javac emits an {@code INVOKEDYNAMIC}
 *       site whose bsm-args contain a {@code REF_invokeStatic} handle
 *       pointing at
 *       {@code JoinWindows.of(Ljava/time/Duration;)Lorg/apache/kafka/streams/kstream/JoinWindows;}.
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
 * <p>{@code JoinWindows.of(Duration timeDifference)} accepts only the
 * time-difference and silently inherits the legacy default grace
 * period of {@code 24h - timeDifference} — for a 1-minute symmetric
 * join window that is ~24 hours of grace, ~1440× the user-visible
 * join-window width. Stream-stream joins are typically the single
 * largest state-store load on a Streams application, so a 1440×
 * retention multiplier translates directly into terabyte-scale
 * RocksDB shards, 24h changelog replays on rebalance, and outer-join
 * null-emission delays of approximately one day. KIP-633 (Kafka
 * Streams 3.0) split the factory into
 * {@code ofTimeDifferenceWithNoGrace(Duration)} and
 * {@code ofTimeDifferenceAndGrace(Duration, Duration)} so grace is
 * always explicit at the call site.
 */
public final class BadJoinWindowsOf {

    @SuppressWarnings("deprecation")
    public JoinWindows buildDirect() {
        // MUST FIRE — direct INVOKESTATIC on the deprecated factory
        // JoinWindows.of(Duration). javac emits an INVOKESTATIC with
        // descriptor (Ljava/time/Duration;)Lorg/apache/kafka/streams/kstream/JoinWindows;.
        return JoinWindows.of(Duration.ofMinutes(1));
    }

    @SuppressWarnings("deprecation")
    public Function<Duration, JoinWindows> capturedFactory() {
        // MUST FIRE — INVOKEDYNAMIC method-ref capture targeting the
        // deprecated factory. javac resolves `JoinWindows::of` by
        // matching the SAM's argument arity (1 Duration) and return
        // type against the available static factories, picking the
        // legacy of(Duration). javac emits an INVOKEDYNAMIC site
        // whose bsm-args contain a REF_invokeStatic handle on
        // (Ljava/time/Duration;)Lorg/apache/kafka/streams/kstream/JoinWindows;.
        // The user-class bytecode here contains ZERO direct
        // INVOKESTATIC on the legacy factory — only the INVOKEDYNAMIC
        // + LambdaMetafactory bridge. A name-only MethodInsnNode walk
        // misses this entirely; the rule's bsm-arg walk catches it.
        return JoinWindows::of;
    }
}
