package sample;

import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KeyValueMapper;

import java.util.List;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Stream;

/**
 * RULE: STREAMS_SELECT_KEY_NO_NAMED — must fire EXACTLY 8 times
 * across this class (one per method below).
 *
 * <p>Each method exercises one of the bytecode shapes the rule is
 * required to catch — single unsafe overload descriptor
 * {@code (Lorg/apache/kafka/streams/kstream/KeyValueMapper;)Lorg/
 * apache/kafka/streams/kstream/KStream;}. Catches both direct
 * {@code INVOKEINTERFACE} calls (KStream is an interface) and
 * {@code INVOKEDYNAMIC} method-reference captures bound to a SAM
 * whose erased implMethod descriptor matches the unsafe overload.
 */
public final class BadStreamsSelectKeyNoNamed {

    /** 1-arg selectKey SAM matching the unsafe overload. */
    @FunctionalInterface
    interface SelectKeyFactory<K, V, KR> {
        KStream<KR, V> apply(KeyValueMapper<? super K, ? super V, ? extends KR> mapper);
    }

    /** Non-generic 1-arg selectKey SAM. */
    @FunctionalInterface
    interface StringSelectKeyFactory {
        KStream<String, String> apply(KeyValueMapper<? super String, ? super String, String> mapper);
    }

    // ===== Direct INVOKEINTERFACE on the unsafe overload =====

    /** MUST FIRE — direct INVOKEINTERFACE. The SelectKey node and
     *  any downstream repartition topic are auto-named from the
     *  topology graph index. */
    public KStream<String, String> rekeyByUser(KStream<String, String> events) {
        return events.selectKey((k, v) -> v);
    }

    /** MUST FIRE — direct INVOKEINTERFACE in a chain feeding a
     *  groupByKey, where the rename has cascading effect on the
     *  downstream repartition topic. */
    public KStream<String, String> rekeyAndGroup(KStream<String, String> events) {
        return events.selectKey((KeyValueMapper<String, String, String>) (k, v) -> v + "-suffix");
    }

    /** MUST FIRE — direct INVOKEINTERFACE applied to a KStream
     *  passed as a parameter, downstream of an upstream filter
     *  that the team will eventually edit. */
    public KStream<String, String> rekeyFiltered(KStream<String, String> events) {
        return events.filter((k, v) -> v != null)
                .selectKey((k, v) -> v.toLowerCase());
    }

    // ===== INVOKEDYNAMIC method-reference captures =====

    /** MUST FIRE — {@code stream::selectKey} bound to
     *  {@link Function Function&lt;KeyValueMapper, KStream&gt;}.
     *  INVOKEDYNAMIC bsm-args contain a REF_invokeInterface Handle
     *  pointing at {@code KStream.selectKey(KeyValueMapper)KStream}. */
    public Function<KeyValueMapper<? super String, ? super String, String>,
                    KStream<String, String>>
            buildFunctionFactory(KStream<String, String> events) {
        return events::selectKey;
    }

    /** MUST FIRE — {@code stream::selectKey} bound to a custom
     *  1-arg generic SAM. Indy implMethod handle descriptor is
     *  the unsafe overload exactly. */
    public SelectKeyFactory<String, String, String>
            buildSelectKeyFactory(KStream<String, String> events) {
        return events::selectKey;
    }

    /** MUST FIRE — {@code stream::selectKey} bound to a custom
     *  non-generic 1-arg SAM. */
    public StringSelectKeyFactory buildStringSelectKeyFactory(KStream<String, String> events) {
        return events::selectKey;
    }

    /** MUST FIRE — local SAM binding applied immediately. The
     *  indy site is in this method's bytecode; the synthetic
     *  lambda body goes through the SAM handle, not directly
     *  through selectKey. */
    public KStream<String, String> useLocalFactory(KStream<String, String> events,
            KeyValueMapper<? super String, ? super String, String> mapper) {
        Function<KeyValueMapper<? super String, ? super String, String>,
                 KStream<String, String>> factory = events::selectKey;
        return factory.apply(mapper);
    }

    /** MUST FIRE — explicit lambda body that invokes
     *  {@code stream.selectKey(mapper)}. The synthetic lambda
     *  method's bytecode contains a direct INVOKEINTERFACE on
     *  the unsafe overload — the rule walks all methods on the
     *  class including synthetic lambda bodies and fires there. */
    public Stream<KStream<String, String>> rekeyAll(
            KStream<String, String> events,
            List<KeyValueMapper<String, String, String>> mappers) {
        return mappers.stream().map(mapper -> events.selectKey(mapper));
    }

    public static void main(String[] args) {
        StreamsBuilder builder = new StreamsBuilder();
        // Reference BiFunction to keep the import non-trivial (no fires here).
        BiFunction<String, String, String> identity = (k, v) -> v;
        System.out.println(builder + " " + identity);
        new BadStreamsSelectKeyNoNamed();
    }
}
