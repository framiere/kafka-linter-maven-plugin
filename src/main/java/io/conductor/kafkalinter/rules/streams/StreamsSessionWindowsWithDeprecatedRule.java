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
 * {@link org.apache.kafka.streams.kstream.SessionWindows#with(java.time.Duration)}
 * — whether the call lands directly via {@code INVOKESTATIC} or
 * indirectly through an {@code INVOKEDYNAMIC} method-reference capture
 * (e.g. {@code SessionWindows::with} bound to a
 * {@code Function<Duration, SessionWindows>} factory used by a
 * session-window builder, parameterized-test source, topology-DSL
 * helper or fixture-driven sessionization test harness).
 *
 * <h2>Why this factory is dangerous, not merely cosmetic</h2>
 *
 * <p>{@code SessionWindows.with(Duration inactivityGap)} is the legacy
 * one-line shortcut for session-window aggregations. It accepts only
 * the inactivity gap and silently inherits the legacy default grace
 * period of <b>{@code 24h - inactivityGap}</b> — for a typical
 * 5-minute inactivity gap that is approximately 24 hours of grace,
 * ~288× the user-visible session window's inactivity threshold (and
 * approaches ~1440× as the inactivity gap shrinks toward 1 minute).
 *
 * <p>Session windows differ from time windows in one important way:
 * they do not have a fixed width. Each session opens on the first
 * key event and closes after {@code inactivityGap + grace}
 * milliseconds of silence on that key — so the legacy grace default
 * has two distinct consequences beyond raw retention:
 *
 * <ul>
 *   <li><b>Session-store footprint blows up.</b> Each open session
 *       (one per key, plus all sessions that have not yet been
 *       evicted) is held in the session-windowed RocksDB store for
 *       {@code lastEvent + inactivityGap + grace = lastEvent + ~24h}
 *       instead of the user-expected {@code lastEvent + ~few
 *       minutes}. On a topology with millions of distinct keys per
 *       day, the session store carries a day of session candidates
 *       per shard rather than the user's intended few minutes.</li>
 *   <li><b>Late events silently fuse sessions that should be
 *       separate.</b> A new event arriving up to 24h after the
 *       previous event on the same key is treated as belonging to
 *       the same session — instead of opening a fresh session as the
 *       user expects from a "5-minute inactivity gap" configuration.
 *       The aggregator output shows fewer, longer sessions than the
 *       intended sessionization model.</li>
 *   <li><b>{@code Suppressed.untilWindowCloses(...)} on a session
 *       window fires ~24h late.</b> Session close is computed as
 *       {@code lastEvent + inactivityGap + grace}; with grace silently
 *       set to {@code 24h - inactivityGap}, suppressed downstream
 *       emissions are delayed by approximately one day after the
 *       user-visible session close. Operators wired to "emit on
 *       session close" effectively never fire in real-time
 *       topologies.</li>
 *   <li><b>{@code INVOKEDYNAMIC SessionWindows::with} captures
 *       silently bind to the deprecated factory.</b> A session-window
 *       builder using a {@code Function<Duration, SessionWindows>}
 *       resolves the method-ref by arity to the deprecated overload
 *       — the user-class bytecode contains zero direct
 *       {@code INVOKESTATIC} on the legacy factory and a name-only
 *       MethodInsnNode walk misses the call.</li>
 * </ul>
 *
 * <h2>The replacement APIs</h2>
 *
 * <p>KIP-633 (ratified in Kafka 3.0; the legacy {@code with} was
 * already deprecated in Kafka 2.7) split the factory in two so the
 * grace period is always explicit at the call site:
 *
 * <ul>
 *   <li>{@link org.apache.kafka.streams.kstream.SessionWindows#ofInactivityGapWithNoGrace(java.time.Duration)}
 *       — pin grace to zero. Late events open a new session, the
 *       session store evicts each session exactly
 *       {@code inactivityGap} after the last event. This is the
 *       right call for sessionization where you want bounded
 *       session-state and deterministic session-close timing.</li>
 *   <li>{@link org.apache.kafka.streams.kstream.SessionWindows#ofInactivityGapAndGrace(java.time.Duration, java.time.Duration)}
 *       — explicit grace argument. Use the smallest grace that
 *       absorbs your real upstream-clock skew (typically seconds
 *       to minutes, not hours).</li>
 * </ul>
 *
 * <p>Both replacement factories have distinct method names from the
 * legacy {@code with(Duration)}, so the name filter alone
 * discriminates — the descriptor pin below is kept for consistency
 * with the rest of the dedicated rules and to insulate the rule
 * from any future overload of {@code with} on {@code SessionWindows}.
 *
 * <h2>Method-reference capture path</h2>
 *
 * <p>{@code SessionWindows::with} bound to a 1-arg functional
 * interface (e.g. {@code Function<Duration, SessionWindows>})
 * compiles to {@code INVOKEDYNAMIC} whose bsm-args contain a
 * {@code REF_invokeStatic} handle pointing at the resolved factory
 * method. The rule's bsm-arg walk catches this case by checking the
 * handle's {@code (owner, name, desc)} triple against the same
 * filter used for direct calls.
 */
public final class StreamsSessionWindowsWithDeprecatedRule implements Rule {

    private static final Set<String> OWNERS = Set.of(KafkaTypes.SESSION_WINDOWS);
    private static final String METHOD_NAME = "with";
    private static final String LEGACY_DESC =
            "(Ljava/time/Duration;)Lorg/apache/kafka/streams/kstream/SessionWindows;";

    private final Severity severity;

    public StreamsSessionWindowsWithDeprecatedRule(Severity severity) {
        this.severity = severity;
    }

    @Override
    public RuleId id() {
        return RuleId.STREAMS_SESSION_WINDOWS_WITH_DEPRECATED;
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
                RuleId.STREAMS_SESSION_WINDOWS_WITH_DEPRECATED, severity,
                ctx.classNode().name, mn.name, AsmUtil.lineOf(insn),
                "SessionWindows.with(Duration) 1-arg static factory is reached here "
                        + "— either as a direct call or as an INVOKEDYNAMIC "
                        + "method-reference capture (e.g. `SessionWindows::with` bound to "
                        + "a Function<Duration, SessionWindows> factory used by a "
                        + "session-window builder, parameterized-test source, "
                        + "topology-DSL helper, or fixture-driven sessionization test "
                        + "harness). This factory is deprecated since Kafka Streams 2.7 "
                        + "(ratified by KIP-633 in 3.0) because it accepts only the "
                        + "inactivity gap and silently inherits the legacy default grace "
                        + "period of `24h - inactivityGap` — for a 5-minute gap that is "
                        + "~24h of grace, ~288× the user-visible inactivity threshold. "
                        + "Four downstream consequences for sessionization topologies: "
                        + "(1) session-store footprint blows up — each open session is "
                        + "held for lastEvent + inactivityGap + grace = lastEvent + ~24h "
                        + "instead of the user-expected lastEvent + ~few minutes, so the "
                        + "session-windowed RocksDB store carries a day of session "
                        + "candidates per shard rather than the user's intended few "
                        + "minutes; (2) late events silently fuse sessions that should "
                        + "be separate — a new event arriving up to 24h after the "
                        + "previous event on the same key is treated as belonging to "
                        + "the same session instead of opening a fresh one, so the "
                        + "aggregator emits fewer, longer sessions than the intended "
                        + "model; (3) Suppressed.untilWindowCloses fires ~24h late "
                        + "because session close = lastEvent + inactivityGap + grace; "
                        + "operators wired to `emit on session close` effectively never "
                        + "fire in real-time topologies; (4) INVOKEDYNAMIC "
                        + "`SessionWindows::with` captures silently bind to the "
                        + "deprecated factory whenever the SAM arity is 1 — the "
                        + "user-class bytecode contains zero direct INVOKESTATIC on the "
                        + "legacy factory and a name-only walk misses it. Migrate to "
                        + "`SessionWindows.ofInactivityGapWithNoGrace(inactivityGap)` to "
                        + "pin grace to zero (bounded session-state, deterministic "
                        + "session-close timing) or "
                        + "`SessionWindows.ofInactivityGapAndGrace(inactivityGap, grace)` "
                        + "to set grace explicitly to the smallest value that absorbs "
                        + "your real upstream-clock skew (typically seconds to minutes, "
                        + "not hours).");
    }
}
