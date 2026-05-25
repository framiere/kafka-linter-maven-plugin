package sample;

import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KeyValueMapper;
import org.apache.kafka.streams.kstream.Named;

import java.util.List;
import java.util.function.BiFunction;
import java.util.stream.Stream;

/**
 * MUST NOT FIRE — every call site passes an explicit
 * {@link Named} pinning the map processor-node name (and,
 * transitively, the downstream auto-repartition topic name).
 */
public final class GoodStreamsMapNoNamed {

    /** 2-arg SAM matching {@code (KeyValueMapper, Named)KStream}. */
    @FunctionalInterface
    interface MapFactory<K, V, K2, V2> {
        KStream<K2, V2> apply(KeyValueMapper<? super K, ? super V, ? extends KeyValue<? extends K2, ? extends V2>> mapper, Named named);
    }

    public KStream<String, String> rekeyByValue(KStream<String, String> stream) {
        return stream.map((k, v) -> KeyValue.pair(v == null ? "" : v, v),
                Named.as("rekey-by-value"));
    }

    public KStream<String, Integer> rekeyToLength(KStream<String, String> stream) {
        return stream.map((k, v) -> KeyValue.pair(v == null ? "" : v, v == null ? 0 : v.length()),
                Named.as("rekey-to-length"));
    }

    public KStream<String, String> identityRekey(KStream<String, String> stream) {
        return stream.map((k, v) -> KeyValue.pair(k, v),
                Named.as("identity-rekey"));
    }

    public BiFunction<KeyValueMapper<String, String, KeyValue<String, String>>, Named, KStream<String, String>>
            buildRekeyFactory(KStream<String, String> stream) {
        return stream::map;
    }

    public BiFunction<KeyValueMapper<String, String, KeyValue<String, Integer>>, Named, KStream<String, Integer>>
            buildLengthRekeyFactory(KStream<String, String> stream) {
        return stream::map;
    }

    public <K2, V2> MapFactory<String, String, K2, V2> buildCustomFactory(KStream<String, String> stream) {
        return stream::map;
    }

    public KStream<String, String> useLocalFactory(KStream<String, String> stream,
            KeyValueMapper<String, String, KeyValue<String, String>> mapper) {
        BiFunction<KeyValueMapper<String, String, KeyValue<String, String>>, Named, KStream<String, String>> factory =
                stream::map;
        return factory.apply(mapper, Named.as("local-factory-map"));
    }

    public Stream<KStream<String, String>> rekeyAll(
            List<KStream<String, String>> streams,
            KeyValueMapper<String, String, KeyValue<String, String>> mapper) {
        return streams.stream().map(s -> s.map(mapper, Named.as("rekey-all")));
    }

    public static void main(String[] args) {
        new GoodStreamsMapNoNamed();
    }
}
