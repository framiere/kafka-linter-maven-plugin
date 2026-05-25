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
 * {@link org.apache.kafka.streams.kstream.WindowedSerdes#timeWindowedSerdeFrom(Class)}
 * — whether the call lands directly via {@code INVOKESTATIC} or
 * indirectly through an {@code INVOKEDYNAMIC} method-reference capture
 * (e.g. {@code WindowedSerdes::timeWindowedSerdeFrom} bound to a
 * {@code Function<Class<?>, Serde<?>>} factory used by parameterized-test
 * sources, fixture-driven serde-registry builders, or
 * interactive-query routing test harnesses that fabricate
 * {@code Serde<Windowed<T>>} instances from a per-test inner-key class).
 *
 * <h2>Why this factory is dangerous, not merely cosmetic</h2>
 *
 * <p>{@code WindowedSerdes.timeWindowedSerdeFrom(Class)} is the standard
 * one-line shortcut for getting a {@code Serde<Windowed<T>>} when the
 * caller has a {@code Class<T>} for the inner key type. The returned
 * {@code Serde}'s deserializer side is a {@link
 * org.apache.kafka.streams.kstream.TimeWindowedDeserializer}; the
 * factory <em>wires it without a {@code windowSize}</em>. Internally
 * this is identical to calling the deprecated 1-arg
 * {@code TimeWindowedDeserializer(Deserializer)} constructor — the
 * deserializer carries {@code windowSize = Long.MAX_VALUE}, so every
 * reconstructed {@code Windowed<T>.window().end()} is computed as
 * {@code windowStart + Long.MAX_VALUE} and overflows to a wrap-around
 * negative {@code long}.
 *
 * <p>The overflow cascades into the same four downstream failures as
 * the underlying {@code TimeWindowedDeserializer} 1-arg ctor:
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
 *       compares the windowed-key's {@code window.end()} against
 *       {@code toTime}; an overflowed-negative end is less than every
 *       plausible {@code toTime}, so the store reports the window as
 *       "before the range" and elides it from interactive-query
 *       results with no diagnostic.</li>
 *   <li><b>Punctuators wired to {@code window.end()} reschedule wrong.</b>
 *       A processor that sets the next punctuation to
 *       {@code window.end() + interval} reschedules to a negative
 *       timestamp, which the streams runtime clamps to
 *       {@code Long.MAX_VALUE} → the punctuator fires once at startup
 *       then never again.</li>
 *   <li><b>INVOKEDYNAMIC {@code WindowedSerdes::timeWindowedSerdeFrom}
 *       captures silently bind to the deprecated 1-arg factory whenever
 *       the SAM arity is 1.</b> A test harness using a
 *       {@code Function<Class<?>, Serde<?>>} factory resolves the
 *       method-ref by arity to the deprecated overload — the user-class
 *       bytecode contains zero direct {@code INVOKESTATIC} on the
 *       legacy factory and a name-only MethodInsnNode walk misses the
 *       call.</li>
 * </ul>
 *
 * <h2>The replacement API</h2>
 *
 * <p>The non-deprecated 2-arg static factory takes
 * {@code (Class<T> innerClass, long windowSize)}. The {@code windowSize}
 * argument must match the topology's {@code TimeWindows.ofSizeAndGrace(...)}
 * width — e.g. {@code Duration.ofMinutes(5).toMillis()} for a 5-minute
 * tumbling window. With a concrete window size the underlying
 * {@code TimeWindowedDeserializer} computes window boundaries
 * consistently with the rest of the topology.
 *
 * <h2>Descriptor discrimination</h2>
 *
 * <p>{@code timeWindowedSerdeFrom} is overloaded on
 * {@code WindowedSerdes}: the legacy 1-arg factory has descriptor
 * {@code (Ljava/lang/Class;)Lorg/apache/kafka/common/serialization/Serde;}
 * and the modern 2-arg factory has descriptor
 * {@code (Ljava/lang/Class;J)Lorg/apache/kafka/common/serialization/Serde;}.
 * The rule matches the legacy descriptor exactly. The modern descriptor
 * differs by an inserted primitive {@code J} (long), so it never matches
 * — descriptor discrimination is mandatory because a name-only filter
 * would false-positive on the supported migration target.
 *
 * <h2>Method-reference capture path</h2>
 *
 * <p>{@code WindowedSerdes::timeWindowedSerdeFrom} bound to a 1-arg
 * functional interface compiles to {@code INVOKEDYNAMIC} whose bsm-args
 * contain a {@code REF_invokeStatic} handle pointing at the resolved
 * factory method. The rule's bsm-arg walk catches this case by checking
 * the handle's {@code (owner, name, desc)} triple against the same
 * filter used for direct calls.
 */
public final class StreamsWindowedSerdesTimeFromClassDeprecatedRule implements Rule {

    private static final Set<String> OWNERS = Set.of(KafkaTypes.WINDOWED_SERDES);
    private static final String METHOD_NAME = "timeWindowedSerdeFrom";
    private static final String LEGACY_DESC =
            "(Ljava/lang/Class;)Lorg/apache/kafka/common/serialization/Serde;";

    private final Severity severity;

    public StreamsWindowedSerdesTimeFromClassDeprecatedRule(Severity severity) {
        this.severity = severity;
    }

    @Override
    public RuleId id() {
        return RuleId.STREAMS_WINDOWED_SERDES_TIME_FROM_CLASS_DEPRECATED;
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
                RuleId.STREAMS_WINDOWED_SERDES_TIME_FROM_CLASS_DEPRECATED, severity,
                ctx.classNode().name, mn.name, AsmUtil.lineOf(insn),
                "WindowedSerdes.timeWindowedSerdeFrom(Class) 1-arg static factory is "
                        + "reached here — either as a direct call or as an INVOKEDYNAMIC "
                        + "method-reference capture (e.g. "
                        + "`WindowedSerdes::timeWindowedSerdeFrom` bound to a "
                        + "Function<Class, Serde<Windowed<?>>> factory used by a "
                        + "parameterized-test source, fixture-driven serde-registry "
                        + "builder, or interactive-query routing test harness). This "
                        + "factory is deprecated since Kafka Streams 2.8 (KIP-659) "
                        + "because it wires a TimeWindowedDeserializer internally without "
                        + "a windowSize — equivalent to calling the deprecated 1-arg "
                        + "TimeWindowedDeserializer(Deserializer) ctor. The deserializer "
                        + "carries windowSize=Long.MAX_VALUE, so every reconstructed "
                        + "Windowed<T>.window().end() = windowStart + Long.MAX_VALUE "
                        + "overflows to a wrap-around negative long. Four downstream "
                        + "consequences for any topology that reads Windowed<T> keys "
                        + "back: (1) Suppressed.untilWindowCloses() compares "
                        + "record-timestamp against window.end() + grace; with "
                        + "window.end() overflowing negative, the comparison always "
                        + "concludes the window has not closed, so suppressed records "
                        + "sit in the in-memory buffer until the JVM OOMs; "
                        + "(2) ReadOnlyWindowStore.fetch compares the windowed-key's "
                        + "window.end() against the caller's toTime — an "
                        + "overflowed-negative end is less than every plausible toTime, "
                        + "so the store reports the window as `before the range` and "
                        + "elides it from interactive-query results with no diagnostic; "
                        + "(3) Punctuators wired to window.end() reschedule to a "
                        + "negative timestamp clamped to Long.MAX_VALUE — they fire "
                        + "once at startup then never again; (4) INVOKEDYNAMIC "
                        + "`WindowedSerdes::timeWindowedSerdeFrom` captures silently "
                        + "bind to the deprecated 1-arg factory whenever the SAM arity "
                        + "is 1 — the user-class bytecode contains zero direct "
                        + "INVOKESTATIC on the legacy factory and a name-only walk "
                        + "misses it. Migrate to the non-deprecated 2-arg factory "
                        + "`WindowedSerdes.timeWindowedSerdeFrom(InnerKey.class, "
                        + "windowSizeMs)` where windowSizeMs matches the topology's "
                        + "TimeWindows width (e.g. `Duration.ofMinutes(5).toMillis()` "
                        + "for a 5-minute tumbling window).");
    }
}
