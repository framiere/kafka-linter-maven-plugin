package sample;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.kstream.WindowedSerdes;

import java.util.function.Function;

/**
 * RULE: STREAMS_WINDOWED_SERDES_TIME_FROM_CLASS_DEPRECATED — must fire on
 * both methods.
 *
 * <p>The two methods below exercise the two distinct bytecode shapes the
 * rule is required to catch for the deprecated 1-arg static factory
 * {@code WindowedSerdes.timeWindowedSerdeFrom(Class<T>)}:
 *
 * <ol>
 *   <li>Direct {@code INVOKESTATIC} on the 1-arg factory — the classic
 *       call site, where the user-class bytecode contains an explicit
 *       {@code INVOKESTATIC
 *       org/apache/kafka/streams/kstream/WindowedSerdes.timeWindowedSerdeFrom(Ljava/lang/Class;)Lorg/apache/kafka/common/serialization/Serde;}.</li>
 *   <li>{@code INVOKEDYNAMIC} method-ref capture
 *       {@code WindowedSerdes::timeWindowedSerdeFrom} resolved against a
 *       1-arg {@code Function<Class<?>, Serde<?>>} SAM — javac compiles
 *       the method-ref through {@code LambdaMetafactory} into an
 *       {@code INVOKEDYNAMIC} site whose bsm-args contain a
 *       {@code REF_invokeStatic} handle pointing at
 *       {@code WindowedSerdes.timeWindowedSerdeFrom(Ljava/lang/Class;)Lorg/apache/kafka/common/serialization/Serde;}.
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
 * <p>{@code WindowedSerdes.timeWindowedSerdeFrom(Class)} wires a
 * {@code TimeWindowedDeserializer} without a {@code windowSize} — same
 * defect as the deprecated 1-arg {@code TimeWindowedDeserializer(Deserializer)}
 * ctor. The deserializer defaults {@code windowSize} to
 * {@code Long.MAX_VALUE}, so every reconstructed
 * {@code Windowed<T>.window().end()} overflows to a wrap-around
 * negative {@code long}, producing four downstream failures:
 * {@code Suppressed.untilWindowCloses()} buffers indefinitely (OOM),
 * {@code ReadOnlyWindowStore.fetch} silently elides windowed keys,
 * punctuators wired to {@code window.end()} fire once at startup then
 * never again, and {@code WindowedSerdes::timeWindowedSerdeFrom}
 * captures into a 1-arg SAM silently bind to the deprecated overload.
 *
 * <p>KIP-659 (Kafka Streams 2.8) introduced the 2-arg overload
 * {@code timeWindowedSerdeFrom(Class<T>, long windowSize)} — the
 * replacement that wires the deserializer with the topology's
 * actual window size.
 */
public final class BadWindowedSerdesTimeFromClass {

    @SuppressWarnings("deprecation")
    public Serde<Windowed<String>> buildDirect() {
        // MUST FIRE — direct INVOKESTATIC on the deprecated 1-arg factory
        // WindowedSerdes.timeWindowedSerdeFrom(Class). Descriptor:
        // (Ljava/lang/Class;)Lorg/apache/kafka/common/serialization/Serde;.
        // The returned Serde's deserializer carries windowSize=Long.MAX_VALUE
        // — every reconstructed Windowed<String>.window().end() overflows.
        return WindowedSerdes.timeWindowedSerdeFrom(String.class);
    }

    @SuppressWarnings("deprecation")
    public Function<Class<?>, Serde<? extends Windowed<?>>> capturedLegacyFactory() {
        // MUST FIRE — INVOKEDYNAMIC method-ref capture targeting the
        // deprecated 1-arg factory. javac resolves
        // `WindowedSerdes::timeWindowedSerdeFrom` to the (Class) overload
        // by SAM arity (Function<T, R> is 1-arg), emitting an
        // INVOKEDYNAMIC site whose bsm-args contain a REF_invokeStatic
        // handle on (Ljava/lang/Class;)Lorg/apache/kafka/common/serialization/Serde;.
        // The user-class bytecode here contains ZERO direct INVOKESTATIC
        // on the legacy factory — only the INVOKEDYNAMIC +
        // LambdaMetafactory bridge. A name-only MethodInsnNode walk
        // misses this entirely; the rule's bsm-arg walk catches it.
        return WindowedSerdes::timeWindowedSerdeFrom;
    }
}
