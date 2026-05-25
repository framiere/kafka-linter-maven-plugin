package sample;

import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Named;
import org.apache.kafka.streams.kstream.ValueMapper;

import java.util.List;
import java.util.function.BiFunction;
import java.util.stream.Stream;

/**
 * MUST NOT FIRE — every call site passes an explicit
 * {@link Named} pinning the mapValues processor-node name.
 */
public final class GoodStreamsKStreamMapValuesNoNamed {

    /** 2-arg SAM matching {@code (ValueMapper, Named)KStream}. */
    @FunctionalInterface
    interface MapValuesFactory<V, V2> {
        KStream<String, V2> apply(ValueMapper<? super V, ? extends V2> mapper, Named named);
    }

    public KStream<String, Integer> projectLength(KStream<String, String> stream) {
        return stream.mapValues((ValueMapper<String, Integer>) v -> v == null ? 0 : v.length(),
                Named.as("project-length"));
    }

    public KStream<String, String> uppercaseValues(KStream<String, String> stream) {
        return stream.mapValues((ValueMapper<String, String>) v -> v == null ? "" : v.toUpperCase(),
                Named.as("project-uppercase"));
    }

    public KStream<String, String> trimValues(KStream<String, String> stream) {
        return stream.mapValues((ValueMapper<String, String>) v -> v == null ? "" : v.trim(),
                Named.as("trim-values"));
    }

    public BiFunction<ValueMapper<String, Integer>, Named, KStream<String, Integer>>
            buildProjectionFactory(KStream<String, String> stream) {
        return stream::mapValues;
    }

    public BiFunction<ValueMapper<String, String>, Named, KStream<String, String>>
            buildIdentityFactory(KStream<String, String> stream) {
        return stream::mapValues;
    }

    public <V2> MapValuesFactory<String, V2> buildCustomFactory(KStream<String, String> stream) {
        return stream::mapValues;
    }

    public KStream<String, Integer> useLocalFactory(KStream<String, String> stream,
            ValueMapper<String, Integer> mapper) {
        BiFunction<ValueMapper<String, Integer>, Named, KStream<String, Integer>> factory =
                stream::mapValues;
        return factory.apply(mapper, Named.as("local-factory-mapvalues"));
    }

    public Stream<KStream<String, Integer>> projectAll(
            List<KStream<String, String>> streams,
            ValueMapper<String, Integer> mapper) {
        return streams.stream().map(s -> s.mapValues(mapper, Named.as("project-all")));
    }

    public static void main(String[] args) {
        new GoodStreamsKStreamMapValuesNoNamed();
    }
}
