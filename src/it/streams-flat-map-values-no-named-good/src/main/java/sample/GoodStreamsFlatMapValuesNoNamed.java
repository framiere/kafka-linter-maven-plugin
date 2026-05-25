package sample;

import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Named;
import org.apache.kafka.streams.kstream.ValueMapper;

import java.util.Collections;
import java.util.List;
import java.util.function.BiFunction;
import java.util.stream.Stream;

/**
 * MUST NOT FIRE — every call site passes an explicit
 * {@link Named} pinning the flatMapValues processor-node name.
 */
public final class GoodStreamsFlatMapValuesNoNamed {

    /** 2-arg SAM matching {@code (ValueMapper, Named)KStream}. */
    @FunctionalInterface
    interface FlatMapValuesFactory<V, V2> {
        KStream<String, V2> apply(ValueMapper<? super V, ? extends Iterable<? extends V2>> mapper, Named named);
    }

    public KStream<String, String> explode(KStream<String, String> stream) {
        return stream.flatMapValues((ValueMapper<String, Iterable<String>>) v ->
                v == null ? Collections.<String>emptyList() : Collections.singletonList(v),
                Named.as("explode-values"));
    }

    public KStream<String, Integer> explodeToLengths(KStream<String, String> stream) {
        return stream.flatMapValues((ValueMapper<String, Iterable<Integer>>) v ->
                v == null ? Collections.<Integer>emptyList() : Collections.singletonList(v.length()),
                Named.as("explode-to-lengths"));
    }

    public KStream<String, String> identityExplode(KStream<String, String> stream) {
        return stream.flatMapValues((ValueMapper<String, Iterable<String>>) v ->
                Collections.singletonList(v),
                Named.as("identity-explode-values"));
    }

    public BiFunction<ValueMapper<String, Iterable<String>>, Named, KStream<String, String>>
            buildExpandFactory(KStream<String, String> stream) {
        return stream::flatMapValues;
    }

    public BiFunction<ValueMapper<String, Iterable<Integer>>, Named, KStream<String, Integer>>
            buildLengthExpandFactory(KStream<String, String> stream) {
        return stream::flatMapValues;
    }

    public <V2> FlatMapValuesFactory<String, V2> buildCustomFactory(KStream<String, String> stream) {
        return stream::flatMapValues;
    }

    public KStream<String, String> useLocalFactory(KStream<String, String> stream,
            ValueMapper<String, Iterable<String>> mapper) {
        BiFunction<ValueMapper<String, Iterable<String>>, Named, KStream<String, String>> factory =
                stream::flatMapValues;
        return factory.apply(mapper, Named.as("local-factory-flat-map-values"));
    }

    public Stream<KStream<String, String>> explodeAll(
            List<KStream<String, String>> streams,
            ValueMapper<String, Iterable<String>> mapper) {
        return streams.stream().map(s -> s.flatMapValues(mapper, Named.as("explode-all-values")));
    }

    public static void main(String[] args) {
        new GoodStreamsFlatMapValuesNoNamed();
    }
}
