package sample;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.kstream.WindowedSerdes;

import java.time.Duration;

/**
 * RULE: STREAMS_WINDOWED_SERDES_TIME_FROM_CLASS_DEPRECATED — must NOT
 * fire.
 *
 * <p>The two methods below exercise the supported 2-arg modern
 * {@code WindowedSerdes.timeWindowedSerdeFrom(Class<T>, long windowSize)}
 * static factory:
 *
 * <ol>
 *   <li>Direct {@code INVOKESTATIC} on the 2-arg factory whose
 *       descriptor is
 *       {@code (Ljava/lang/Class;J)Lorg/apache/kafka/common/serialization/Serde;}.
 *       The rule's filter matches the legacy descriptor
 *       {@code (Ljava/lang/Class;)Lorg/apache/kafka/common/serialization/Serde;}
 *       exactly, so the 2-arg descriptor — which differs by an inserted
 *       primitive {@code J} (long) — never matches.</li>
 *   <li>{@code INVOKEDYNAMIC} method-ref capture
 *       {@code WindowedSerdes::timeWindowedSerdeFrom} resolved against a
 *       2-arg lambda that takes a {@code Class} and a primitive
 *       {@code long}. To express this capture shape Java's stdlib
 *       requires a custom 2-arg {@code @FunctionalInterface} because no
 *       built-in functional interface accepts {@code (Class<?>, long)}
 *       and returns {@code Serde<?>} — {@code ObjLongFunction} doesn't
 *       exist in {@code java.util.function}. The bsm-arg handle's
 *       descriptor is
 *       {@code (Ljava/lang/Class;J)Lorg/apache/kafka/common/serialization/Serde;},
 *       not the legacy descriptor, so the rule's descriptor filter
 *       rejects the site.</li>
 * </ol>
 *
 * <h2>Why this is safe</h2>
 *
 * <p>The non-deprecated 2-arg factory takes
 * {@code (Class<T> innerClass, long windowSize)}. The
 * {@code windowSize} must match the topology's
 * {@code TimeWindows.ofSizeAndGrace(...)} width — e.g.
 * {@code Duration.ofMinutes(5).toMillis()} for a 5-minute tumbling
 * window. With a concrete window size the underlying
 * {@code TimeWindowedDeserializer} computes window boundaries
 * consistently with the rest of the topology.
 *
 * <p>Descriptor discrimination is mandatory here because
 * {@code timeWindowedSerdeFrom} is overloaded on {@code WindowedSerdes}:
 * a name-only filter would false-positive on the supported migration
 * target.
 */
public final class GoodWindowedSerdesTimeFromClass {

    @FunctionalInterface
    interface ClassLongToSerde {
        Serde<? extends Windowed<?>> apply(Class<?> innerClass, long windowSize);
    }

    private static final long FIVE_MIN_MS = Duration.ofMinutes(5).toMillis();

    public Serde<Windowed<String>> buildModern() {
        // DOES NOT FIRE — 2-arg modern factory. Descriptor:
        // (Ljava/lang/Class;J)Lorg/apache/kafka/common/serialization/Serde;.
        // The inserted primitive J (long) places the descriptor outside
        // the rule's exact-match filter on the legacy 1-arg descriptor.
        return WindowedSerdes.timeWindowedSerdeFrom(String.class, FIVE_MIN_MS);
    }

    public ClassLongToSerde capturedModernFactory() {
        // DOES NOT FIRE — INVOKEDYNAMIC method-ref capture targeting the
        // SUPPORTED 2-arg modern factory. javac resolves
        // `WindowedSerdes::timeWindowedSerdeFrom` to the (Class, long)
        // overload by SAM arity. The bsm-arg handle's descriptor is
        // (Ljava/lang/Class;J)Lorg/apache/kafka/common/serialization/Serde;,
        // not the legacy (Ljava/lang/Class;)Lorg/apache/kafka/common/serialization/Serde;,
        // so the rule's descriptor filter rejects this site. This is
        // precisely why descriptor discrimination is mandatory: the name
        // timeWindowedSerdeFrom and owner WindowedSerdes both match, but
        // the descriptor difference protects the supported migration
        // target.
        return WindowedSerdes::timeWindowedSerdeFrom;
    }
}
