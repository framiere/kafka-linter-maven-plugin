package sample;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.streams.kstream.TimeWindowedDeserializer;

import java.util.function.Function;

/**
 * RULE: STREAMS_TIME_WINDOWED_DESERIALIZER_NO_SIZE_DEPRECATED — must fire
 * on both methods.
 *
 * <p>The two methods below exercise the two distinct bytecode shapes the
 * rule is required to catch for the deprecated 1-arg
 * {@code TimeWindowedDeserializer(Deserializer<T> inner)} constructor:
 *
 * <ol>
 *   <li>Direct {@code INVOKESPECIAL} on the 1-arg constructor — the
 *       classic call site, where the user-class bytecode contains an
 *       explicit {@code NEW
 *       org/apache/kafka/streams/kstream/TimeWindowedDeserializer}
 *       followed by {@code INVOKESPECIAL
 *       <init>(Lorg/apache/kafka/common/serialization/Deserializer;)V}.</li>
 *   <li>{@code INVOKEDYNAMIC} constructor-ref capture
 *       {@code TimeWindowedDeserializer::new} resolved against a 1-arg
 *       {@code Function<Deserializer<String>, TimeWindowedDeserializer<String>>}
 *       — javac compiles the constructor-ref through
 *       {@code LambdaMetafactory} into an {@code INVOKEDYNAMIC} site
 *       whose bsm-args contain a {@code REF_newInvokeSpecial} handle
 *       pointing at {@code TimeWindowedDeserializer.<init>(Lorg/apache/kafka/common/serialization/Deserializer;)V}.
 *       The user-class bytecode contains ZERO direct
 *       {@code INVOKESPECIAL} on the legacy ctor at this site — a
 *       name-only MethodInsnNode walk misses it entirely. The rule's
 *       bsm-arg walk via {@code AsmUtil.indyTargetHandle} catches this
 *       case.</li>
 * </ol>
 *
 * <h2>Why this constructor is dangerous</h2>
 *
 * <p>{@code TimeWindowedDeserializer} reconstructs {@code Windowed<T>}
 * keys from changelog-topic bytes; each {@code Windowed<T>} carries a
 * {@code (windowStart, windowEnd)} pair downstream operators read to
 * make routing decisions. KIP-659 (Kafka Streams 2.8) introduced the
 * 2-arg constructor {@code (Deserializer<T> inner, Long windowSize)}
 * so callers must supply the topology's window size. The legacy 1-arg
 * constructor predates KIP-659 and defaults windowSize to
 * {@code Long.MAX_VALUE} — every reconstructed
 * {@code Windowed<T>.window().end()} is computed as
 * {@code windowStart + Long.MAX_VALUE}, which overflows to a
 * wrap-around negative {@code long}.
 *
 * <p>Four downstream consequences flow from the overflow:
 * {@code Suppressed.untilWindowCloses()} retains records forever
 * (buffer grows until JVM OOMs); {@code ReadOnlyWindowStore.fetch(key,
 * fromTime, toTime)} silently elides every windowed key (overflowed
 * negative end is less than every plausible toTime); punctuators wired
 * to {@code window.end()} reschedule to a negative timestamp clamped
 * to {@code Long.MAX_VALUE} and never fire after startup; and
 * {@code TimeWindowedDeserializer::new} captures into a 1-arg SAM
 * silently bind to the deprecated ctor with no direct INVOKESPECIAL
 * visible at the call site.
 */
public final class BadTimeWindowedDeserializerNoSize {

    @SuppressWarnings("deprecation")
    public TimeWindowedDeserializer<String> buildDirect() {
        // MUST FIRE — direct INVOKESPECIAL on the deprecated 1-arg ctor
        // (Deserializer). Descriptor:
        // (Lorg/apache/kafka/common/serialization/Deserializer;)V.
        // The resulting deserializer carries windowSize=Long.MAX_VALUE
        // — every reconstructed Windowed<String>.window().end()
        // overflows negative.
        return new TimeWindowedDeserializer<>(new StringDeserializer());
    }

    @SuppressWarnings("deprecation")
    public Function<Deserializer<String>, TimeWindowedDeserializer<String>> capturedLegacyFactory() {
        // MUST FIRE — INVOKEDYNAMIC constructor-ref capture targeting
        // the deprecated 1-arg ctor. javac resolves
        // `TimeWindowedDeserializer::new` to the (Deserializer) overload
        // by SAM arity (Function<T, R> is 1-arg), emitting an
        // INVOKEDYNAMIC site whose bsm-args contain a
        // REF_newInvokeSpecial handle on
        // <init>(Lorg/apache/kafka/common/serialization/Deserializer;)V.
        // The user-class bytecode here contains ZERO direct
        // INVOKESPECIAL on the legacy ctor — only the INVOKEDYNAMIC +
        // LambdaMetafactory bridge. A name-only MethodInsnNode walk
        // misses this entirely; the rule's bsm-arg walk catches it.
        return TimeWindowedDeserializer::new;
    }
}
