package sample;

import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KeyValueMapper;
import org.apache.kafka.streams.kstream.Named;

import java.util.Collections;
import java.util.List;
import java.util.function.BiFunction;
import java.util.stream.Stream;

/**
 * MUST NOT FIRE — every call site passes an explicit
 * {@link Named} pinning the flatMap processor-node name (and,
 * transitively, the downstream auto-repartition topic name).
 */
public final class GoodStreamsFlatMapNoNamed {

    /** 2-arg SAM matching {@code (KeyValueMapper, Named)KStream}. */
    @FunctionalInterface
    interface FlatMapFactory<K, V, K2, V2> {
        KStream<K2, V2> apply(KeyValueMapper<? super K, ? super V, ? extends Iterable<? extends KeyValue<? extends K2, ? extends V2>>> mapper, Named named);
    }

    public KStream<String, String> explode(KStream<String, String> stream) {
        return stream.flatMap((k, v) -> v == null
                ? Collections.<KeyValue<String, String>>emptyList()
                : Collections.singletonList(KeyValue.pair(v, v)), Named.as("explode"));
    }

    public KStream<String, Integer> explodeToLength(KStream<String, String> stream) {
        return stream.flatMap((k, v) -> v == null
                ? Collections.<KeyValue<String, Integer>>emptyList()
                : Collections.singletonList(KeyValue.pair(v, v.length())), Named.as("explode-to-length"));
    }

    public KStream<String, String> identityExplode(KStream<String, String> stream) {
        return stream.flatMap((k, v) -> Collections.singletonList(KeyValue.pair(k, v)),
                Named.as("identity-explode"));
    }

    public BiFunction<KeyValueMapper<String, String, Iterable<KeyValue<String, String>>>, Named, KStream<String, String>>
            buildExpandFactory(KStream<String, String> stream) {
        return stream::flatMap;
    }

    public BiFunction<KeyValueMapper<String, String, Iterable<KeyValue<String, Integer>>>, Named, KStream<String, Integer>>
            buildLengthExpandFactory(KStream<String, String> stream) {
        return stream::flatMap;
    }

    public <K2, V2> FlatMapFactory<String, String, K2, V2> buildCustomFactory(KStream<String, String> stream) {
        return stream::flatMap;
    }

    public KStream<String, String> useLocalFactory(KStream<String, String> stream,
            KeyValueMapper<String, String, Iterable<KeyValue<String, String>>> mapper) {
        BiFunction<KeyValueMapper<String, String, Iterable<KeyValue<String, String>>>, Named, KStream<String, String>> factory =
                stream::flatMap;
        return factory.apply(mapper, Named.as("local-factory-flat-map"));
    }

    public Stream<KStream<String, String>> explodeAll(
            List<KStream<String, String>> streams,
            KeyValueMapper<String, String, Iterable<KeyValue<String, String>>> mapper) {
        return streams.stream().map(s -> s.flatMap(mapper, Named.as("explode-all")));
    }

    public static void main(String[] args) {
        new GoodStreamsFlatMapNoNamed();
    }
}
