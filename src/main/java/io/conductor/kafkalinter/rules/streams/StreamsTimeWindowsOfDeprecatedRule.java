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
 * {@link org.apache.kafka.streams.kstream.TimeWindows#of(java.time.Duration)}
 * — whether the call lands directly via {@code INVOKESTATIC} or
 * indirectly through an {@code INVOKEDYNAMIC} method-reference capture
 * (e.g. {@code TimeWindows::of} bound to a
 * {@code Function<Duration, TimeWindows>} factory used by a
 * window-factory builder, parameterized-test source, topology-DSL
 * helper, or fixture-driven aggregation test harness that fabricates
 * {@code TimeWindows} instances from a per-test window size).
 *
 * <h2>Why this factory is dangerous, not merely cosmetic</h2>
 *
 * <p>{@code TimeWindows.of(Duration size)} is the legacy one-line
 * shortcut for tumbling or hopping time windows. It accepts only the
 * window <em>size</em>, leaving the grace period to a default. The
 * default is <b>{@code 24 hours minus the window size}</b> (the legacy
 * {@code TimeWindows} default grace clamp), so a topology that uses
 * sub-minute windows pays for state-store retention equivalent to
 * <b>~1440× the user-visible window width</b>. Concretely, for a
 * 1-minute tumbling window:
 *
 * <ul>
 *   <li><b>State-store footprint blows up by ~1440×.</b> The store
 *       keeps every window open for {@code size + grace = 24h} so it
 *       can absorb late events, instead of the user-expected
 *       {@code size + grace = ~few minutes}. On a 10k key/s topology
 *       with 1-minute windows, the RocksDB state store carries ~24h of
 *       windowed-key entries rather than the user's intended ~few
 *       minutes — that is the difference between gigabytes and
 *       terabytes of state per shard, and the difference between
 *       sub-second and minute-scale interactive-query latencies.</li>
 *   <li><b>Late-event admission policy is silently wrong.</b> Records
 *       arriving up to 24h after the window's end are still aggregated
 *       into the closed window, producing aggregations that disagree
 *       with the user's mental model of "1-minute tumbling window with
 *       no late events". The aggregator output looks correct but the
 *       windowed numbers are subtly off because they include 24h of
 *       drift.</li>
 *   <li><b>{@code Suppressed.untilWindowCloses(...)} fires 24h late.</b>
 *       Suppression compares record timestamp against
 *       {@code window.end() + grace}; with grace silently set to
 *       {@code 24h - size}, suppressed downstream emissions are delayed
 *       by approximately one day after the user-visible window
 *       boundary. Operators wired to "emit on close" effectively never
 *       fire in real-time topologies.</li>
 *   <li><b>{@code INVOKEDYNAMIC TimeWindows::of} captures silently bind
 *       to the deprecated factory.</b> A window-factory builder using a
 *       {@code Function<Duration, TimeWindows>} resolves the method-ref
 *       by arity to the deprecated overload — the user-class bytecode
 *       contains zero direct {@code INVOKESTATIC} on the legacy factory
 *       and a name-only MethodInsnNode walk misses the call.</li>
 * </ul>
 *
 * <h2>The replacement APIs</h2>
 *
 * <p>KIP-633 split the factory in two so the grace period is always
 * explicit at the call site:
 *
 * <ul>
 *   <li>{@link org.apache.kafka.streams.kstream.TimeWindows#ofSizeWithNoGrace(java.time.Duration)}
 *       — pin grace to zero. Late events are dropped, state retention
 *       is exactly the window size. This is the right call for stream
 *       processing where you want bounded state and deterministic
 *       emission timing.</li>
 *   <li>{@link org.apache.kafka.streams.kstream.TimeWindows#ofSizeAndGrace(java.time.Duration, java.time.Duration)}
 *       — explicit grace argument. Use the smallest grace that absorbs
 *       your real upstream-clock skew (typically seconds to minutes,
 *       not hours), and size your state store accordingly.</li>
 * </ul>
 *
 * <p>The two replacement factories have distinct method names from the
 * legacy {@code of(Duration)}, so the name filter alone discriminates
 * — the descriptor pin below is kept for consistency with the rest of
 * the dedicated rules and to insulate the rule from any future
 * overload of {@code of} on {@code TimeWindows}.
 *
 * <h2>Method-reference capture path</h2>
 *
 * <p>{@code TimeWindows::of} bound to a 1-arg functional interface
 * (e.g. {@code Function<Duration, TimeWindows>}) compiles to
 * {@code INVOKEDYNAMIC} whose bsm-args contain a
 * {@code REF_invokeStatic} handle pointing at the resolved factory
 * method. The rule's bsm-arg walk catches this case by checking the
 * handle's {@code (owner, name, desc)} triple against the same filter
 * used for direct calls.
 */
public final class StreamsTimeWindowsOfDeprecatedRule implements Rule {

    private static final Set<String> OWNERS = Set.of(KafkaTypes.TIME_WINDOWS);
    private static final String METHOD_NAME = "of";
    private static final String LEGACY_DESC =
            "(Ljava/time/Duration;)Lorg/apache/kafka/streams/kstream/TimeWindows;";

    private final Severity severity;

    public StreamsTimeWindowsOfDeprecatedRule(Severity severity) {
        this.severity = severity;
    }

    @Override
    public RuleId id() {
        return RuleId.STREAMS_TIME_WINDOWS_OF_DEPRECATED;
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
                RuleId.STREAMS_TIME_WINDOWS_OF_DEPRECATED, severity,
                ctx.classNode().name, mn.name, AsmUtil.lineOf(insn),
                "TimeWindows.of(Duration) 1-arg static factory is reached here — "
                        + "either as a direct call or as an INVOKEDYNAMIC "
                        + "method-reference capture (e.g. `TimeWindows::of` bound to a "
                        + "Function<Duration, TimeWindows> factory used by a "
                        + "window-factory builder, parameterized-test source, topology-DSL "
                        + "helper, or fixture-driven aggregation test harness). This "
                        + "factory is deprecated since Kafka Streams 3.0 (KIP-633) "
                        + "because it accepts only the window size and silently inherits "
                        + "the legacy default grace period of `24h - size` — for a "
                        + "1-minute window that is ~24 hours of grace, ~1440× the "
                        + "user-visible window width. Four downstream consequences for "
                        + "any topology using sub-day windows: (1) state-store footprint "
                        + "blows up by ~1440× because every window is held open for "
                        + "size + grace = 24h instead of the user-expected ~few minutes "
                        + "— RocksDB shards carry terabytes rather than gigabytes of "
                        + "state and interactive-query latencies degrade from sub-second "
                        + "to minute-scale; (2) late-event admission policy is silently "
                        + "wrong — records arriving up to 24h after the window's end "
                        + "still feed the aggregator, producing windowed numbers that "
                        + "disagree with the user's mental model of `N-minute tumbling "
                        + "window with no late events`; (3) Suppressed.untilWindowCloses "
                        + "fires ~24h late because suppression compares record timestamp "
                        + "against window.end() + grace — emit-on-close operators "
                        + "effectively never fire in real-time topologies; "
                        + "(4) INVOKEDYNAMIC `TimeWindows::of` captures silently bind to "
                        + "the deprecated factory whenever the SAM arity is 1 — the "
                        + "user-class bytecode contains zero direct INVOKESTATIC on the "
                        + "legacy factory and a name-only walk misses it. Migrate to "
                        + "`TimeWindows.ofSizeWithNoGrace(size)` to pin grace to zero "
                        + "(bounded state, deterministic emission timing) or "
                        + "`TimeWindows.ofSizeAndGrace(size, grace)` to set grace "
                        + "explicitly to the smallest value that absorbs your real "
                        + "upstream-clock skew (typically seconds to minutes, not "
                        + "hours).");
    }
}
