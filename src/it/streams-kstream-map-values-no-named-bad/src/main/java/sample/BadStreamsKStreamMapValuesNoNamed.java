package sample;

import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.ValueMapper;

import java.util.List;
import java.util.function.Function;
import java.util.stream.Stream;

/**
 * RULE: STREAMS_KSTREAM_MAP_VALUES_NO_NAMED — must fire
 * EXACTLY 8 times across this class (one per method below).
 *
 * <p>Exercises the unsafe overload descriptors on KStream
 * ({@code mapValues(ValueMapper)},
 * {@code mapValues(ValueMapperWithKey)}) via direct
 * INVOKEINTERFACE and via INVOKEDYNAMIC method-reference
 * captures.
 */
public final class BadStreamsKStreamMapValuesNoNamed {

    /** 1-arg SAM matching {@code (ValueMapper)KStream}. */
    @FunctionalInterface
    interface MapValuesFactory<V, V2> {
        KStream<String, V2> apply(ValueMapper<? super V, ? extends V2> mapper);
    }

    // ===== Direct INVOKEINTERFACE on the unsafe overloads =====

    /** MUST FIRE — direct INVOKEINTERFACE on
     *  KStream.mapValues(ValueMapper). */
    public KStream<String, Integer> projectLength(KStream<String, String> stream) {
        return stream.mapValues((ValueMapper<String, Integer>) v -> v == null ? 0 : v.length());
    }

    /** MUST FIRE — direct INVOKEINTERFACE on the unsafe
     *  overload in a chain. */
    public KStream<String, String> uppercaseValues(KStream<String, String> stream) {
        return stream.mapValues((ValueMapper<String, String>) v -> v == null ? "" : v.toUpperCase());
    }

    /** MUST FIRE — direct INVOKEINTERFACE on the unsafe
     *  overload, distinct call site. */
    public KStream<String, String> trimValues(KStream<String, String> stream) {
        return stream.mapValues((ValueMapper<String, String>) v -> v == null ? "" : v.trim());
    }

    // ===== INVOKEDYNAMIC method-reference captures =====

    /** MUST FIRE — {@code stream::mapValues} bound to
     *  {@link Function Function&lt;ValueMapper, KStream&gt;}.
     *  Indy bsm-arg points at {@code mapValues(ValueMapper)KStream}. */
    public Function<ValueMapper<String, Integer>, KStream<String, Integer>>
            buildProjectionFactory(KStream<String, String> stream) {
        return stream::mapValues;
    }

    /** MUST FIRE — {@code stream::mapValues} bound to a
     *  generic Function at a second site. */
    public Function<ValueMapper<String, String>, KStream<String, String>>
            buildIdentityFactory(KStream<String, String> stream) {
        return stream::mapValues;
    }

    /** MUST FIRE — {@code stream::mapValues} bound to a custom
     *  1-arg generic SAM. Indy implMethod handle descriptor is
     *  the no-Named overload exactly. */
    public <V2> MapValuesFactory<String, V2> buildCustomFactory(KStream<String, String> stream) {
        return stream::mapValues;
    }

    /** MUST FIRE — local Function binding via method
     *  reference, applied inside the same method. */
    public KStream<String, Integer> useLocalFactory(KStream<String, String> stream,
            ValueMapper<String, Integer> mapper) {
        Function<ValueMapper<String, Integer>, KStream<String, Integer>> factory = stream::mapValues;
        return factory.apply(mapper);
    }

    /** MUST FIRE — explicit lambda body that invokes
     *  {@code s.mapValues(mapper)}. */
    public Stream<KStream<String, Integer>> projectAll(
            List<KStream<String, String>> streams,
            ValueMapper<String, Integer> mapper) {
        return streams.stream().map(s -> s.mapValues(mapper));
    }

    public static void main(String[] args) {
        new BadStreamsKStreamMapValuesNoNamed();
    }
}
