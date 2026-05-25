package sample;

import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KeyValueMapper;

import java.util.Collections;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Stream;

/**
 * RULE: STREAMS_FLAT_MAP_NO_NAMED — must fire EXACTLY 8 times
 * across this class (one per method below).
 *
 * <p>Exercises the unsafe overload descriptor on KStream
 * ({@code flatMap(KeyValueMapper)}) via direct INVOKEINTERFACE
 * and via INVOKEDYNAMIC method-reference captures.
 *
 * <p>KStream.flatMap is KEY-CHANGING AND fan-out — every fire is
 * a latent auto-repartition-topic-carrying-N*-throughput hazard.
 */
public final class BadStreamsFlatMapNoNamed {

    /** 1-arg SAM matching {@code (KeyValueMapper)KStream}. */
    @FunctionalInterface
    interface FlatMapFactory<K, V, K2, V2> {
        KStream<K2, V2> apply(KeyValueMapper<? super K, ? super V, ? extends Iterable<? extends KeyValue<? extends K2, ? extends V2>>> mapper);
    }

    // ===== Direct INVOKEINTERFACE on the unsafe overload =====

    /** MUST FIRE — direct INVOKEINTERFACE on
     *  KStream.flatMap(KeyValueMapper). */
    public KStream<String, String> explode(KStream<String, String> stream) {
        return stream.flatMap((k, v) -> v == null
                ? Collections.<KeyValue<String, String>>emptyList()
                : Collections.singletonList(KeyValue.pair(v, v)));
    }

    /** MUST FIRE — direct INVOKEINTERFACE on the unsafe
     *  overload in a chain. */
    public KStream<String, Integer> explodeToLength(KStream<String, String> stream) {
        return stream.flatMap((k, v) -> v == null
                ? Collections.<KeyValue<String, Integer>>emptyList()
                : Collections.singletonList(KeyValue.pair(v, v.length())));
    }

    /** MUST FIRE — direct INVOKEINTERFACE on the unsafe
     *  overload, distinct call site. */
    public KStream<String, String> identityExplode(KStream<String, String> stream) {
        return stream.flatMap((k, v) -> Collections.singletonList(KeyValue.pair(k, v)));
    }

    // ===== INVOKEDYNAMIC method-reference captures =====

    /** MUST FIRE — {@code stream::flatMap} bound to
     *  {@link Function Function&lt;KeyValueMapper, KStream&gt;}. */
    public Function<KeyValueMapper<String, String, Iterable<KeyValue<String, String>>>, KStream<String, String>>
            buildExpandFactory(KStream<String, String> stream) {
        return stream::flatMap;
    }

    /** MUST FIRE — {@code stream::flatMap} bound to a generic
     *  Function at a second site. */
    public Function<KeyValueMapper<String, String, Iterable<KeyValue<String, Integer>>>, KStream<String, Integer>>
            buildLengthExpandFactory(KStream<String, String> stream) {
        return stream::flatMap;
    }

    /** MUST FIRE — {@code stream::flatMap} bound to a custom
     *  1-arg generic SAM. */
    public <K2, V2> FlatMapFactory<String, String, K2, V2> buildCustomFactory(KStream<String, String> stream) {
        return stream::flatMap;
    }

    /** MUST FIRE — local Function binding via method
     *  reference, applied inside the same method. */
    public KStream<String, String> useLocalFactory(KStream<String, String> stream,
            KeyValueMapper<String, String, Iterable<KeyValue<String, String>>> mapper) {
        Function<KeyValueMapper<String, String, Iterable<KeyValue<String, String>>>, KStream<String, String>> factory =
                stream::flatMap;
        return factory.apply(mapper);
    }

    /** MUST FIRE — explicit lambda body that invokes
     *  {@code s.flatMap(mapper)}. */
    public Stream<KStream<String, String>> explodeAll(
            List<KStream<String, String>> streams,
            KeyValueMapper<String, String, Iterable<KeyValue<String, String>>> mapper) {
        return streams.stream().map(s -> s.flatMap(mapper));
    }

    public static void main(String[] args) {
        new BadStreamsFlatMapNoNamed();
    }
}
