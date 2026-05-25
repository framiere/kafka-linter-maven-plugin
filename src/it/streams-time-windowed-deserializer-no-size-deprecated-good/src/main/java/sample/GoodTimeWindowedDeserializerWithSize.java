package sample;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.streams.kstream.TimeWindowedDeserializer;

import java.time.Duration;
import java.util.function.BiFunction;

/**
 * RULE: STREAMS_TIME_WINDOWED_DESERIALIZER_NO_SIZE_DEPRECATED — must NOT
 * fire.
 *
 * <p>The two methods below exercise the supported 2-arg modern
 * {@code TimeWindowedDeserializer(Deserializer<T> inner, Long windowSize)}
 * constructor:
 *
 * <ol>
 *   <li>Direct {@code INVOKESPECIAL} on the 2-arg constructor whose
 *       descriptor is
 *       {@code (Lorg/apache/kafka/common/serialization/Deserializer;Ljava/lang/Long;)V}.
 *       The rule's filter matches the legacy descriptor
 *       {@code (Lorg/apache/kafka/common/serialization/Deserializer;)V}
 *       exactly, so the 2-arg descriptor — which differs by a trailing
 *       {@code Ljava/lang/Long;} — never matches.</li>
 *   <li>{@code INVOKEDYNAMIC} constructor-ref capture
 *       {@code TimeWindowedDeserializer::new} resolved against a
 *       {@code BiFunction<Deserializer<String>, Long, TimeWindowedDeserializer<String>>}
 *       SAM. The bsm-arg handle's descriptor is
 *       {@code (Lorg/apache/kafka/common/serialization/Deserializer;Ljava/lang/Long;)V}
 *       — not the legacy descriptor — so the rule's descriptor filter
 *       rejects the site even though the name {@code <init>} and the
 *       owner {@code TimeWindowedDeserializer} both match.</li>
 * </ol>
 *
 * <h2>Why this is safe</h2>
 *
 * <p>The non-deprecated 2-arg constructor takes
 * {@code (Deserializer<T> inner, Long windowSize)}. The {@code windowSize}
 * must match the topology's {@code TimeWindows.ofSizeAndGrace(...)} width
 * — e.g. {@code Duration.ofMinutes(5).toMillis()} for a 5-minute tumbling
 * window. With a concrete window size, every reconstructed
 * {@code Windowed<T>.window().end()} is computed as
 * {@code windowStart + windowSize}: a real timestamp that lines up with
 * the topology's grace-period math, suppress-operator window-closed
 * checks, and windowed-store range scans.
 *
 * <p>Descriptor discrimination is mandatory here because {@code <init>}
 * is overloaded on {@code TimeWindowedDeserializer}: a name-only filter
 * would false-positive on the supported migration target. The 0-arg
 * ctor (used by reflective Serde framework instantiation, never directly
 * by user code) and the modern 2-arg ctor share the legacy ctor's name
 * but have distinct descriptors.
 */
public final class GoodTimeWindowedDeserializerWithSize {

    private static final long FIVE_MIN_MS = Duration.ofMinutes(5).toMillis();

    public TimeWindowedDeserializer<String> buildModern() {
        // DOES NOT FIRE — 2-arg modern constructor. Descriptor:
        // (Lorg/apache/kafka/common/serialization/Deserializer;Ljava/lang/Long;)V.
        // The trailing Long parameter (boxed because the ctor declares
        // Long, not long) places the descriptor outside the rule's
        // exact-match filter on the legacy 1-arg descriptor.
        return new TimeWindowedDeserializer<>(new StringDeserializer(), FIVE_MIN_MS);
    }

    public BiFunction<Deserializer<String>, Long, TimeWindowedDeserializer<String>> capturedModernFactory() {
        // DOES NOT FIRE — INVOKEDYNAMIC constructor-ref capture targeting
        // the SUPPORTED 2-arg modern constructor. javac resolves
        // `TimeWindowedDeserializer::new` to the 2-arg overload by SAM
        // arity (BiFunction<T, U, R> is 2-arg). The bsm-arg handle's
        // descriptor is
        // (Lorg/apache/kafka/common/serialization/Deserializer;Ljava/lang/Long;)V,
        // not the legacy
        // (Lorg/apache/kafka/common/serialization/Deserializer;)V, so the
        // rule's descriptor filter rejects this site. This is precisely
        // why descriptor discrimination is mandatory: the name <init>
        // and owner TimeWindowedDeserializer both match, but the
        // descriptor difference protects the supported migration target.
        return TimeWindowedDeserializer::new;
    }
}
