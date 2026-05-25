package sample;

import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KeyValueMapper;

import java.util.List;
import java.util.function.Function;
import java.util.stream.Stream;

/**
 * RULE: STREAMS_MAP_NO_NAMED — must fire EXACTLY 8 times
 * across this class (one per method below).
 *
 * <p>Exercises the unsafe overload descriptor on KStream
 * ({@code map(KeyValueMapper)}) via direct INVOKEINTERFACE
 * and via INVOKEDYNAMIC method-reference captures.
 *
 * <p>KStream.map is KEY-CHANGING — every fire here is a
 * latent auto-repartition-topic hazard.
 */
public final class BadStreamsMapNoNamed {

    /** 1-arg SAM matching {@code (KeyValueMapper)KStream}. */
    @FunctionalInterface
    interface MapFactory<K, V, K2, V2> {
        KStream<K2, V2> apply(KeyValueMapper<? super K, ? super V, ? extends KeyValue<? extends K2, ? extends V2>> mapper);
    }

    // ===== Direct INVOKEINTERFACE on the unsafe overload =====

    /** MUST FIRE — direct INVOKEINTERFACE on
     *  KStream.map(KeyValueMapper). */
    public KStream<String, String> rekeyByValue(KStream<String, String> stream) {
        return stream.map((k, v) -> KeyValue.pair(v == null ? "" : v, v));
    }

    /** MUST FIRE — direct INVOKEINTERFACE on the unsafe
     *  overload in a chain. */
    public KStream<String, Integer> rekeyToLength(KStream<String, String> stream) {
        return stream.map((k, v) -> KeyValue.pair(v == null ? "" : v, v == null ? 0 : v.length()));
    }

    /** MUST FIRE — direct INVOKEINTERFACE on the unsafe
     *  overload, distinct call site. */
    public KStream<String, String> identityRekey(KStream<String, String> stream) {
        return stream.map((k, v) -> KeyValue.pair(k, v));
    }

    // ===== INVOKEDYNAMIC method-reference captures =====

    /** MUST FIRE — {@code stream::map} bound to
     *  {@link Function Function&lt;KeyValueMapper, KStream&gt;}.
     *  Indy bsm-arg points at {@code map(KeyValueMapper)KStream}. */
    public Function<KeyValueMapper<String, String, KeyValue<String, String>>, KStream<String, String>>
            buildRekeyFactory(KStream<String, String> stream) {
        return stream::map;
    }

    /** MUST FIRE — {@code stream::map} bound to a generic
     *  Function at a second site. */
    public Function<KeyValueMapper<String, String, KeyValue<String, Integer>>, KStream<String, Integer>>
            buildLengthRekeyFactory(KStream<String, String> stream) {
        return stream::map;
    }

    /** MUST FIRE — {@code stream::map} bound to a custom
     *  1-arg generic SAM. Indy implMethod handle descriptor is
     *  the no-Named overload exactly. */
    public <K2, V2> MapFactory<String, String, K2, V2> buildCustomFactory(KStream<String, String> stream) {
        return stream::map;
    }

    /** MUST FIRE — local Function binding via method
     *  reference, applied inside the same method. */
    public KStream<String, String> useLocalFactory(KStream<String, String> stream,
            KeyValueMapper<String, String, KeyValue<String, String>> mapper) {
        Function<KeyValueMapper<String, String, KeyValue<String, String>>, KStream<String, String>> factory =
                stream::map;
        return factory.apply(mapper);
    }

    /** MUST FIRE — explicit lambda body that invokes
     *  {@code s.map(mapper)}. */
    public Stream<KStream<String, String>> rekeyAll(
            List<KStream<String, String>> streams,
            KeyValueMapper<String, String, KeyValue<String, String>> mapper) {
        return streams.stream().map(s -> s.map(mapper));
    }

    public static void main(String[] args) {
        new BadStreamsMapNoNamed();
    }
}
