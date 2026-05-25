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
 * Fires for every reach of the deprecated 1-arg
 * {@link org.apache.kafka.streams.kstream.TimeWindowedDeserializer}
 * constructor {@code (Deserializer<T> inner)} — whether the call lands
 * directly via {@code INVOKESPECIAL} or indirectly through an
 * {@code INVOKEDYNAMIC} constructor-reference capture (e.g.
 * {@code TimeWindowedDeserializer::new} bound to a
 * {@code Function<Deserializer, TimeWindowedDeserializer>} factory used
 * by parameterized-test sources, fixture-driven Serde mocks, or
 * windowed-store recovery test harnesses that fabricate
 * {@code TimeWindowedDeserializer} instances from a per-test inner
 * deserializer).
 *
 * <h2>Why this constructor is dangerous, not merely cosmetic</h2>
 *
 * <p>{@code TimeWindowedDeserializer<T>} is the wire-format reader for
 * {@code Windowed<T>} keys on changelog topics and on the output side of
 * {@code KTable.toStream()} when materialised through a windowed store.
 * Every deserialised {@code Windowed<T>} carries a {@link
 * org.apache.kafka.streams.kstream.Window} pair {@code (windowStart,
 * windowEnd)} that downstream operators read to make routing decisions:
 * windowed joins compare {@code window.end()} against the join-window
 * size, suppress operators discard records past {@code window.end() +
 * grace}, and interactive-query routers fetch from
 * {@code window.start()} to {@code window.end()}.
 *
 * <p>KIP-659 (Kafka Streams 2.8) introduced the 2-arg constructor
 * {@code (Deserializer<T> inner, Long windowSize)} so callers must
 * supply the topology's window size at deserializer construction. The
 * legacy 1-arg constructor predates KIP-659 and defaults
 * {@code windowSize} to {@link Long#MAX_VALUE}; consequently every
 * reconstructed {@code Windowed<T>.window().end()} is computed as
 * {@code windowStart + Long.MAX_VALUE}, which overflows to a wrap-around
 * negative {@code long}. Four downstream consequences follow:
 *
 * <ul>
 *   <li><b>Suppress operators retain late events forever.</b>
 *       {@code Suppressed.untilWindowCloses()} compares the record
 *       timestamp against {@code window.end() + grace}; with
 *       {@code window.end()} overflowing negative, the comparison
 *       always concludes the window has not closed, so suppressed
 *       records sit in the in-memory buffer until the JVM OOMs.</li>
 *   <li><b>Windowed-store range queries silently return wrong rows.</b>
 *       {@code ReadOnlyWindowStore.fetch(key, fromTime, toTime)}
 *       under the hood compares the windowed-key's {@code window.end()}
 *       against {@code toTime}; an overflowed-negative end is less
 *       than every plausible {@code toTime}, so the store reports the
 *       window as "before the range" and elides it from the result —
 *       interactive-query endpoints return empty/incomplete pages with
 *       no diagnostic.</li>
 *   <li><b>Punctuators wired to {@code window.end()} reschedule wrong.</b>
 *       A processor that sets the next punctuation to
 *       {@code window.end() + interval} reschedules to a negative
 *       timestamp, which the streams runtime clamps to
 *       {@code Long.MAX_VALUE} → the punctuator fires once at startup
 *       then never again.</li>
 *   <li><b>INVOKEDYNAMIC {@code TimeWindowedDeserializer::new}
 *       captures silently bind to the deprecated 1-arg ctor whenever
 *       the factory SAM arity is 1.</b> A test harness using a
 *       {@code Function<Deserializer<String>, TimeWindowedDeserializer<String>>}
 *       factory resolves the constructor-ref by arity to the
 *       deprecated overload — the user-class bytecode contains zero
 *       direct {@code INVOKESPECIAL} on the legacy ctor and a name-only
 *       MethodInsnNode walk misses the call.</li>
 * </ul>
 *
 * <h2>The replacement API</h2>
 *
 * <p>The non-deprecated 2-arg constructor takes
 * {@code (Deserializer<T> inner, Long windowSize)}. The {@code windowSize}
 * argument must match the topology's {@code TimeWindows.ofSizeAndGrace(...)}
 * width — e.g. {@code Duration.ofMinutes(5).toMillis()} for a 5-minute
 * tumbling window. Mismatched window sizes here produce a different bug
 * (wrong-but-consistent {@code window.end()}), which is easier to detect
 * than the legacy ctor's silent overflow.
 *
 * <h2>Descriptor discrimination</h2>
 *
 * <p>{@code <init>} is overloaded on {@code TimeWindowedDeserializer} three
 * ways: the legacy 1-arg ctor has descriptor
 * {@code (Lorg/apache/kafka/common/serialization/Deserializer;)V}, the
 * modern 2-arg ctor has
 * {@code (Lorg/apache/kafka/common/serialization/Deserializer;Ljava/lang/Long;)V},
 * and a no-arg ctor (used by reflective Serde framework instantiation,
 * never directly by user code) has {@code ()V}. The rule matches the
 * legacy 1-arg descriptor exactly. The modern descriptor differs by an
 * appended {@code Ljava/lang/Long;}, so it never matches — descriptor
 * discrimination is mandatory because a name-only filter would
 * false-positive on the supported migration target.
 *
 * <h2>Constructor-reference capture path</h2>
 *
 * <p>{@code TimeWindowedDeserializer::new} bound to a 1-arg factory
 * functional interface compiles to {@code INVOKEDYNAMIC} whose bsm-args
 * contain a {@code REF_newInvokeSpecial} handle pointing at the resolved
 * constructor. The rule's bsm-arg walk catches this case by checking the
 * handle's {@code (owner, name, desc)} triple against the same filter
 * used for direct calls.
 */
public final class StreamsTimeWindowedDeserializerNoSizeDeprecatedRule implements Rule {

    private static final Set<String> OWNERS = Set.of(KafkaTypes.TIME_WINDOWED_DESERIALIZER);
    private static final String CTOR_NAME = "<init>";
    private static final String LEGACY_DESC = "(Lorg/apache/kafka/common/serialization/Deserializer;)V";

    private final Severity severity;

    public StreamsTimeWindowedDeserializerNoSizeDeprecatedRule(Severity severity) {
        this.severity = severity;
    }

    @Override
    public RuleId id() {
        return RuleId.STREAMS_TIME_WINDOWED_DESERIALIZER_NO_SIZE_DEPRECATED;
    }

    @Override
    public List<Violation> check(RuleContext ctx) {
        List<Violation> out = new ArrayList<>();
        for (MethodNode mn : ctx.classNode().methods) {
            for (AbstractInsnNode insn : mn.instructions) {
                if (insn instanceof MethodInsnNode mi
                        && OWNERS.contains(mi.owner)
                        && CTOR_NAME.equals(mi.name)
                        && LEGACY_DESC.equals(mi.desc)) {
                    out.add(violation(ctx, mn, insn));
                    continue;
                }
                if (insn instanceof InvokeDynamicInsnNode indy) {
                    Handle h = AsmUtil.indyTargetHandle(indy, OWNERS, CTOR_NAME, LEGACY_DESC);
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
                RuleId.STREAMS_TIME_WINDOWED_DESERIALIZER_NO_SIZE_DEPRECATED, severity,
                ctx.classNode().name, mn.name, AsmUtil.lineOf(insn),
                "TimeWindowedDeserializer(Deserializer<T> inner) 1-arg constructor is "
                        + "reached here — either as a direct call or as an INVOKEDYNAMIC "
                        + "constructor-reference capture (e.g. `TimeWindowedDeserializer::new` "
                        + "bound to a Function<Deserializer, TimeWindowedDeserializer> factory "
                        + "used by a parameterized-test source, fixture-driven Serde mock, or "
                        + "windowed-store recovery test harness). This constructor is "
                        + "deprecated since Kafka Streams 2.8 (KIP-659) because it predates "
                        + "the windowSize parameter and defaults windowSize to Long.MAX_VALUE; "
                        + "every reconstructed Windowed<T>.window().end() is computed as "
                        + "windowStart + Long.MAX_VALUE, which overflows to a wrap-around "
                        + "negative long. Four downstream consequences for any topology that "
                        + "reads Windowed<T> keys back: (1) Suppressed.untilWindowCloses() "
                        + "compares record-timestamp against window.end() + grace; with "
                        + "window.end() overflowing negative, the comparison always concludes "
                        + "the window has not closed, so suppressed records sit in the "
                        + "in-memory buffer until the JVM OOMs; (2) ReadOnlyWindowStore.fetch "
                        + "compares the windowed-key's window.end() against the caller's "
                        + "toTime — an overflowed-negative end is less than every plausible "
                        + "toTime, so the store reports the window as `before the range` and "
                        + "elides it from interactive-query results with no diagnostic; "
                        + "(3) Punctuators wired to window.end() reschedule to a negative "
                        + "timestamp clamped to Long.MAX_VALUE — they fire once at startup "
                        + "then never again; (4) INVOKEDYNAMIC `TimeWindowedDeserializer::new` "
                        + "captures silently bind to the deprecated 1-arg ctor whenever the "
                        + "factory SAM arity is 1 — the user-class bytecode contains zero "
                        + "direct INVOKESPECIAL on the legacy ctor and a name-only walk "
                        + "misses it. Migrate to the non-deprecated 2-arg constructor "
                        + "`new TimeWindowedDeserializer<>(inner, windowSizeMs)` where "
                        + "windowSizeMs matches the topology's TimeWindows width "
                        + "(e.g. `Duration.ofMinutes(5).toMillis()` for a 5-minute tumbling "
                        + "window). Mismatched window sizes here produce a wrong-but-consistent "
                        + "window.end(), which is easier to detect than the legacy ctor's "
                        + "silent overflow.");
    }
}
