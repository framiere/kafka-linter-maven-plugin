package io.conductor.kafkalinter.rules.streams;

import io.conductor.kafkalinter.RuleId;
import io.conductor.kafkalinter.Severity;
import io.conductor.kafkalinter.Violation;
import io.conductor.kafkalinter.rules.Rule;
import io.conductor.kafkalinter.scanner.AsmUtil;
import io.conductor.kafkalinter.scanner.KafkaTypes;
import io.conductor.kafkalinter.scanner.RuleContext;
import org.objectweb.asm.Handle;
import org.objectweb.asm.tree.AbstractInsnNode;
import org.objectweb.asm.tree.InvokeDynamicInsnNode;
import org.objectweb.asm.tree.MethodInsnNode;
import org.objectweb.asm.tree.MethodNode;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

/**
 * Fires for every reach of the deprecated 1-arg static factory
 * {@link org.apache.kafka.streams.kstream.JoinWindows#of(java.time.Duration)}
 * — whether the call lands directly via {@code INVOKESTATIC} or
 * indirectly through an {@code INVOKEDYNAMIC} method-reference capture
 * (e.g. {@code JoinWindows::of} bound to a
 * {@code Function<Duration, JoinWindows>} factory used by a
 * join-window builder, parameterized-test source, topology-DSL helper
 * or fixture-driven join test harness).
 *
 * <h2>Why this factory is dangerous, not merely cosmetic</h2>
 *
 * <p>{@code JoinWindows.of(Duration timeDifference)} is the legacy
 * one-line shortcut for symmetric stream-stream join windows
 * ({@code [t - timeDifference, t + timeDifference]}). It accepts only
 * the time-difference and silently inherits the legacy default grace
 * period of <b>{@code 24h - timeDifference}</b> — for a typical
 * 1-minute symmetric join window that is approximately 24 hours of
 * grace on each side, ~1440× the user-visible join window width.
 *
 * <p>Stream-stream joins are the single highest state-store load on
 * a typical streams application — every joined record requires both
 * sides of the symmetric window to be held in a windowed RocksDB
 * store until the join window plus grace closes. The legacy grace
 * default therefore costs four concrete things:
 *
 * <ul>
 *   <li><b>Join-side state-store footprint blows up by ~1440×.</b>
 *       Each side of the join holds every record open for
 *       {@code timeDifference + grace = ~24h} instead of the
 *       user-expected ~few minutes. A 10k key/s pair of input streams
 *       with 1-minute symmetric join windows carries terabytes of
 *       per-side state per shard rather than the user's intended
 *       gigabytes, and rebalancing a join task involves replaying 24h
 *       of changelog rather than minutes.</li>
 *   <li><b>Spurious joins from out-of-band records.</b> Records
 *       arriving up to 24h after the symmetric window's nominal close
 *       still feed the join, producing match pairs that disagree with
 *       the user's mental model of "join within N minutes of the
 *       counterpart record". Downstream consumers see joins whose
 *       timestamps differ by hours even though the topology was
 *       configured for a minute-scale window.</li>
 *   <li><b>Outer/left-outer null-emission timing is wrong.</b>
 *       Left/outer joins emit null on the un-matched side only after
 *       the window plus grace closes. With grace silently set to 24h,
 *       a null on the right side is emitted ~24h after the left
 *       record's timestamp — operators wired to "emit unmatched join
 *       after N minutes" effectively never fire in real-time
 *       topologies.</li>
 *   <li><b>{@code INVOKEDYNAMIC JoinWindows::of} captures silently
 *       bind to the deprecated factory.</b> A join-window builder
 *       using a {@code Function<Duration, JoinWindows>} resolves the
 *       method-ref by arity to the deprecated overload — the
 *       user-class bytecode contains zero direct {@code INVOKESTATIC}
 *       on the legacy factory and a name-only MethodInsnNode walk
 *       misses the call.</li>
 * </ul>
 *
 * <h2>The replacement APIs</h2>
 *
 * <p>KIP-633 split the factory in two so the grace period is always
 * explicit at the call site:
 *
 * <ul>
 *   <li>{@link org.apache.kafka.streams.kstream.JoinWindows#ofTimeDifferenceWithNoGrace(java.time.Duration)}
 *       — pin grace to zero. Late join candidates are dropped, both
 *       sides' state-store retention is exactly the join window
 *       width. This is the right call for stream-stream joins where
 *       you want bounded join-state and deterministic outer-join
 *       null-emission timing.</li>
 *   <li>{@link org.apache.kafka.streams.kstream.JoinWindows#ofTimeDifferenceAndGrace(java.time.Duration, java.time.Duration)}
 *       — explicit grace argument. Use the smallest grace that
 *       absorbs your real upstream-clock skew between the two
 *       streams (typically seconds to minutes, not hours).</li>
 * </ul>
 *
 * <p>Both replacement factories have distinct method names from the
 * legacy {@code of(Duration)}, so the name filter alone discriminates
 * — the descriptor pin below is kept for consistency with the rest
 * of the dedicated rules and to insulate the rule from any future
 * overload of {@code of} on {@code JoinWindows}.
 *
 * <h2>Method-reference capture path</h2>
 *
 * <p>{@code JoinWindows::of} bound to a 1-arg functional interface
 * (e.g. {@code Function<Duration, JoinWindows>}) compiles to
 * {@code INVOKEDYNAMIC} whose bsm-args contain a
 * {@code REF_invokeStatic} handle pointing at the resolved factory
 * method. The rule's bsm-arg walk catches this case by checking the
 * handle's {@code (owner, name, desc)} triple against the same
 * filter used for direct calls.
 */
public final class StreamsJoinWindowsOfDeprecatedRule implements Rule {

    private static final Set<String> OWNERS = Set.of(KafkaTypes.JOIN_WINDOWS);
    private static final String METHOD_NAME = "of";
    private static final String LEGACY_DESC =
            "(Ljava/time/Duration;)Lorg/apache/kafka/streams/kstream/JoinWindows;";

    private final Severity severity;

    public StreamsJoinWindowsOfDeprecatedRule(Severity severity) {
        this.severity = severity;
    }

    @Override
    public RuleId id() {
        return RuleId.STREAMS_JOIN_WINDOWS_OF_DEPRECATED;
    }

    @Override
    public List<Violation> check(RuleContext ctx) {
        List<Violation> out = new ArrayList<>();
        for (MethodNode mn : ctx.classNode().methods) {
            for (AbstractInsnNode insn : mn.instructions) {
                if (insn instanceof MethodInsnNode mi
                        && OWNERS.contains(mi.owner)
                        && METHOD_NAME.equals(mi.name)
                        && LEGACY_DESC.equals(mi.desc)) {
                    out.add(violation(ctx, mn, insn));
                    continue;
                }
                if (insn instanceof InvokeDynamicInsnNode indy) {
                    Handle h = AsmUtil.indyTargetHandle(indy, OWNERS, METHOD_NAME, LEGACY_DESC);
                    if (h != null) {
                        out.add(violation(ctx, mn, insn));
                    }
                }
            }
        }
        return out;
    }

    private Violation violation(RuleContext ctx, MethodNode mn, AbstractInsnNode insn) {
        return new Violation(
                RuleId.STREAMS_JOIN_WINDOWS_OF_DEPRECATED, severity,
                ctx.classNode().name, mn.name, AsmUtil.lineOf(insn),
                "JoinWindows.of(Duration) 1-arg static factory is reached here — "
                        + "either as a direct call or as an INVOKEDYNAMIC "
                        + "method-reference capture (e.g. `JoinWindows::of` bound to a "
                        + "Function<Duration, JoinWindows> factory used by a "
                        + "join-window builder, parameterized-test source, topology-DSL "
                        + "helper, or fixture-driven join test harness). This factory is "
                        + "deprecated since Kafka Streams 3.0 (KIP-633) because it "
                        + "accepts only the time-difference and silently inherits the "
                        + "legacy default grace period of `24h - timeDifference` — for "
                        + "a 1-minute symmetric join window that is ~24h of grace, "
                        + "~1440× the user-visible join-window width. Stream-stream "
                        + "joins are typically the single largest state-store load on a "
                        + "Streams application; four downstream consequences for any "
                        + "join using sub-day windows: (1) each side's state-store "
                        + "footprint blows up by ~1440× because every record is held "
                        + "open for timeDifference + grace = 24h instead of the "
                        + "user-expected ~few minutes — rebalancing a join task involves "
                        + "replaying 24h of changelog rather than minutes; "
                        + "(2) spurious joins from out-of-band records — records "
                        + "arriving up to 24h after the symmetric window's nominal "
                        + "close still feed the join, producing match pairs whose "
                        + "timestamps differ by hours even though the topology was "
                        + "configured for a minute-scale window; (3) outer/left-outer "
                        + "null-emission timing is wrong — null on the un-matched side "
                        + "is emitted ~24h after the counterpart record's timestamp, so "
                        + "operators wired to `emit unmatched join after N minutes` "
                        + "effectively never fire in real-time topologies; "
                        + "(4) INVOKEDYNAMIC `JoinWindows::of` captures silently bind to "
                        + "the deprecated factory whenever the SAM arity is 1 — the "
                        + "user-class bytecode contains zero direct INVOKESTATIC on the "
                        + "legacy factory and a name-only walk misses it. Migrate to "
                        + "`JoinWindows.ofTimeDifferenceWithNoGrace(timeDifference)` to "
                        + "pin grace to zero (bounded join-state, deterministic "
                        + "outer-join null-emission timing) or "
                        + "`JoinWindows.ofTimeDifferenceAndGrace(timeDifference, grace)` "
                        + "to set grace explicitly to the smallest value that absorbs "
                        + "your real upstream-clock skew between the two streams "
                        + "(typically seconds to minutes, not hours).");
    }
}
