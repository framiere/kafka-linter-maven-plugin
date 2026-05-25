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
import java.util.Map;

/**
 * Fires for every reach of the deprecated chained
 * {@code grace(Duration)} on
 * {@link org.apache.kafka.streams.kstream.TimeWindows},
 * {@link org.apache.kafka.streams.kstream.JoinWindows}, and
 * {@link org.apache.kafka.streams.kstream.SessionWindows} — whether
 * the call lands directly via {@code INVOKEVIRTUAL} or indirectly
 * through an {@code INVOKEDYNAMIC} method-reference capture (e.g.
 * {@code TimeWindows::grace} bound to a SAM).
 *
 * <h2>Why these methods are deprecated</h2>
 *
 * <p>{@code TimeWindows.grace(Duration)},
 * {@code JoinWindows.grace(Duration)}, and
 * {@code SessionWindows.grace(Duration)} are the pre-KIP-633 way to
 * attach a grace period (allowed lateness) to a window specification.
 * The chained-instance shape
 * {@code TimeWindows.of(size).grace(grace)} (and its
 * {@code JoinWindows.of(diff).grace(grace)} /
 * {@code SessionWindows.with(inactivityGap).grace(grace)} siblings)
 * was deprecated by KIP-633 (Kafka Streams 3.0, September 2021) in
 * favour of static factories that make grace explicit at construction
 * time:
 *
 * <ul>
 *   <li>{@code TimeWindows.ofSizeAndGrace(size, grace)} /
 *       {@code TimeWindows.ofSizeWithNoGrace(size)}</li>
 *   <li>{@code JoinWindows.ofTimeDifferenceAndGrace(diff, grace)} /
 *       {@code JoinWindows.ofTimeDifferenceWithNoGrace(diff)}</li>
 *   <li>{@code SessionWindows.ofInactivityGapAndGrace(gap, grace)} /
 *       {@code SessionWindows.ofInactivityGapWithNoGrace(gap)}</li>
 * </ul>
 *
 * <p>The deprecation tracks four concrete incident classes that the
 * legacy chained-grace API made silent or unrecoverable:
 *
 * <ul>
 *   <li><b>Silent 24-hour grace default when {@code .grace()} is
 *       omitted.</b> Before KIP-633, the legacy factories
 *       ({@code TimeWindows.of(size)},
 *       {@code JoinWindows.of(diff)},
 *       {@code SessionWindows.with(gap)}) returned a window
 *       specification with grace defaulting to {@code 24 * 60 * 60 *
 *       1000} ms. The default was undocumented in the method's
 *       Javadoc summary and undiscoverable from the call site; a
 *       developer who wrote {@code TimeWindows.of(Duration.ofMinutes(5))}
 *       believed they had a five-minute window with zero grace but
 *       in fact had a five-minute window with a 24-hour grace
 *       period. Practical effect: late events older than 5 minutes
 *       but younger than 24 hours buffered in heap and in the
 *       changelog topic, accumulating up to a day of state per
 *       windowed key. State-store size on rocksdb grew linearly with
 *       the load curve over the past 24 hours. Aggregations were
 *       silently still being updated by records that arrived a day
 *       after the window closed. The new factories require an
 *       explicit decision: {@code ofSizeWithNoGrace} for "no grace"
 *       or {@code ofSizeAndGrace(size, grace)} for an explicit
 *       grace, and the legacy 24-hour default is gone.</li>
 *   <li><b>Chained-grace is order-dependent and silently broken
 *       when refactored.</b> {@code TimeWindows.of(size).grace(grace)}
 *       returns a new {@code TimeWindows} from the {@code grace}
 *       call, but {@code TimeWindows.of(size).advanceBy(advance).grace(grace)}
 *       also requires the {@code .grace(grace)} to come last because
 *       intermediate calls return new builders. A refactor that
 *       reorders the chain (for example, extracting
 *       {@code TimeWindows.of(size).advanceBy(advance)} to a
 *       constant and chaining {@code .grace(grace)} later) silently
 *       changes the grace because the legacy {@code .of(...)}
 *       constructor already baked in the 24-hour default. The new
 *       factory shape collapses all of this into one call with no
 *       chained mutation.</li>
 *   <li><b>Calling {@code .grace()} after one of the new factories
 *       throws {@code IllegalStateException} at runtime.</b> Some
 *       codebases that started a partial migration (mixing
 *       {@code TimeWindows.ofSizeAndGrace(...)} with a stray
 *       {@code .grace(...)} call) fail only at topology-build time
 *       with an unhelpful "grace period already set" exception
 *       message. The lint catches the call-site shape before the
 *       runtime check.</li>
 *   <li><b>INVOKEDYNAMIC {@code TimeWindows::grace} captures silently
 *       bind to the deprecated method.</b> A windowing-factory
 *       abstraction (e.g. a generic
 *       {@code BiFunction<TimeWindows, Duration, TimeWindows>} that
 *       configures grace policy externally) resolves the method-ref
 *       by arity and erased argument types. The user-class bytecode
 *       contains zero direct {@code INVOKEVIRTUAL} on the legacy
 *       method — only the {@code INVOKEDYNAMIC} +
 *       {@code LambdaMetafactory} bridge.</li>
 * </ul>
 *
 * <h2>Multi-owner dispatch — three owners, one method name</h2>
 *
 * <p>The deprecated method has the same name {@code grace} on three
 * unrelated owners. The descriptor is owner-typed (each
 * {@code grace(Duration)} returns its own owner type), so each
 * (owner, name) pair has a distinct descriptor:
 *
 * <ul>
 *   <li>{@code TimeWindows.grace} →
 *       {@code (Ljava/time/Duration;)Lorg/apache/kafka/streams/kstream/TimeWindows;}</li>
 *   <li>{@code JoinWindows.grace} →
 *       {@code (Ljava/time/Duration;)Lorg/apache/kafka/streams/kstream/JoinWindows;}</li>
 *   <li>{@code SessionWindows.grace} →
 *       {@code (Ljava/time/Duration;)Lorg/apache/kafka/streams/kstream/SessionWindows;}</li>
 * </ul>
 *
 * <p>The rule uses {@code Map<String, String>} pairing each owner
 * with its single descriptor; the check loop iterates the map at
 * every candidate instruction.
 *
 * <h2>Method-reference capture path</h2>
 *
 * <p>{@code TimeWindows::grace} bound to a SAM whose erased argument
 * is {@code Duration} compiles to {@code INVOKEDYNAMIC} whose
 * bsm-args contain a {@code REF_invokeVirtual} handle pointing at
 * the legacy method. The rule's bsm-arg walk catches this case by
 * checking the handle's {@code (owner, name, desc)} triple against
 * the same filter used for direct calls, iterated over all three
 * (owner, descriptor) pairs.
 */
public final class StreamsWindowsGraceDeprecatedRule implements Rule {

    private static final String METHOD_NAME = "grace";

    private static final Map<String, String> OWNER_TO_LEGACY_DESC = Map.of(
            KafkaTypes.TIME_WINDOWS,
            "(Ljava/time/Duration;)Lorg/apache/kafka/streams/kstream/TimeWindows;",
            KafkaTypes.JOIN_WINDOWS,
            "(Ljava/time/Duration;)Lorg/apache/kafka/streams/kstream/JoinWindows;",
            KafkaTypes.SESSION_WINDOWS,
            "(Ljava/time/Duration;)Lorg/apache/kafka/streams/kstream/SessionWindows;");

    private final Severity severity;

    public StreamsWindowsGraceDeprecatedRule(Severity severity) {
        this.severity = severity;
    }

    @Override
    public RuleId id() {
        return RuleId.STREAMS_WINDOWS_GRACE_DEPRECATED;
    }

    @Override
    public List<Violation> check(RuleContext ctx) {
        List<Violation> out = new ArrayList<>();
        for (MethodNode mn : ctx.classNode().methods) {
            for (AbstractInsnNode insn : mn.instructions) {
                if (insn instanceof MethodInsnNode mi && METHOD_NAME.equals(mi.name)) {
                    String expectedDesc = OWNER_TO_LEGACY_DESC.get(mi.owner);
                    if (expectedDesc != null && expectedDesc.equals(mi.desc)) {
                        out.add(violation(ctx, mn, insn));
                        continue;
                    }
                }
                if (insn instanceof InvokeDynamicInsnNode indy) {
                    for (Map.Entry<String, String> e : OWNER_TO_LEGACY_DESC.entrySet()) {
                        Handle h = AsmUtil.indyTargetHandle(indy, java.util.Set.of(e.getKey()), METHOD_NAME, e.getValue());
                        if (h != null) {
                            out.add(violation(ctx, mn, insn));
                            break;
                        }
                    }
                }
            }
        }
        return out;
    }

    private Violation violation(RuleContext ctx, MethodNode mn, AbstractInsnNode insn) {
        return new Violation(
                RuleId.STREAMS_WINDOWS_GRACE_DEPRECATED, severity,
                ctx.classNode().name, mn.name, AsmUtil.lineOf(insn),
                "TimeWindows.grace(Duration) / JoinWindows.grace(Duration) / "
                        + "SessionWindows.grace(Duration) is reached here — either as a "
                        + "direct call or as an INVOKEDYNAMIC method-reference capture "
                        + "(e.g. `TimeWindows::grace` bound to a windowing-factory SAM). "
                        + "These chained-instance methods are deprecated since Kafka "
                        + "Streams 3.0 (KIP-633, September 2021) because the legacy "
                        + "`Windows.of(size)` factories baked in a 24-hour default "
                        + "grace when `.grace()` was omitted, which silently "
                        + "accumulated up to a day of late events in heap buffers and "
                        + "in the changelog topic. Four concrete failure modes follow: "
                        + "(1) silent 24-hour grace default — `TimeWindows.of("
                        + "Duration.ofMinutes(5))` looked like a five-minute window "
                        + "with zero grace but in fact had a 24-hour grace, so late "
                        + "events arriving up to a day after the window closed kept "
                        + "updating aggregations and growing state-store size on "
                        + "rocksdb linearly with the past 24 hours of load; (2) "
                        + "chained-grace is order-dependent and silently broken when "
                        + "refactored — extracting `TimeWindows.of(size).advanceBy("
                        + "advance)` to a constant and chaining `.grace(grace)` later "
                        + "silently changes the grace because the legacy `.of(...)` "
                        + "already baked in the 24-hour default; (3) calling `.grace()` "
                        + "after one of the new factories throws "
                        + "IllegalStateException at topology-build time with an "
                        + "unhelpful 'grace period already set' message — partial "
                        + "migrations break only at runtime; (4) INVOKEDYNAMIC "
                        + "`TimeWindows::grace` captures silently bind to the "
                        + "deprecated method — the user-class bytecode contains zero "
                        + "direct INVOKEVIRTUAL on the legacy method and a name-only "
                        + "walk misses it. Migrate to the new static factories that "
                        + "force the grace decision at construction time: "
                        + "TimeWindows.ofSizeAndGrace(size, grace) / "
                        + "TimeWindows.ofSizeWithNoGrace(size); "
                        + "JoinWindows.ofTimeDifferenceAndGrace(diff, grace) / "
                        + "JoinWindows.ofTimeDifferenceWithNoGrace(diff); "
                        + "SessionWindows.ofInactivityGapAndGrace(gap, grace) / "
                        + "SessionWindows.ofInactivityGapWithNoGrace(gap). After the "
                        + "migration the grace value is visible at the call site and "
                        + "the 24-hour default is gone.");
    }
}
