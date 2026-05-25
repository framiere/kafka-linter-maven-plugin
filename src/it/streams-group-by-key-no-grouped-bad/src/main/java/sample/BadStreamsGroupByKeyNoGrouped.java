package sample;

import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KGroupedStream;
import org.apache.kafka.streams.kstream.KStream;

import java.util.List;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Stream;

/**
 * RULE: STREAMS_GROUP_BY_KEY_NO_GROUPED — must fire EXACTLY 8
 * times across this class (one per method below).
 *
 * <p>Single unsafe overload descriptor:
 * {@code ()Lorg/apache/kafka/streams/kstream/KGroupedStream;}.
 */
public final class BadStreamsGroupByKeyNoGrouped {

    /** 0-arg groupByKey SAM matching {@code ()KGroupedStream}. */
    @FunctionalInterface
    interface GroupByKeyFactory<K, V> {
        KGroupedStream<K, V> apply();
    }

    // ===== Direct INVOKEINTERFACE on the unsafe overload =====

    /** MUST FIRE — direct INVOKEINTERFACE on the 0-arg overload. */
    public KGroupedStream<String, String> groupOrders(KStream<String, String> orders) {
        return orders.groupByKey();
    }

    /** MUST FIRE — direct INVOKEINTERFACE after a selectKey chain. */
    public KGroupedStream<String, String> groupAfterSelectKey(KStream<String, String> events) {
        return events.selectKey((k, v) -> v).groupByKey();
    }

    /** MUST FIRE — direct INVOKEINTERFACE after a filter chain. */
    public KGroupedStream<String, String> groupAfterFilter(KStream<String, String> events) {
        return events.filter((k, v) -> v != null).groupByKey();
    }

    // ===== INVOKEDYNAMIC method-reference captures =====

    /** MUST FIRE — {@code stream::groupByKey} bound to
     *  {@link Supplier Supplier&lt;KGroupedStream&gt;}. INVOKEDYNAMIC
     *  bsm-args contain a REF_invokeInterface Handle pointing at
     *  {@code KStream.groupByKey()KGroupedStream}. */
    public Supplier<KGroupedStream<String, String>>
            buildSupplierFactory(KStream<String, String> stream) {
        return stream::groupByKey;
    }

    /** MUST FIRE — {@code stream::groupByKey} bound to a custom
     *  0-arg generic SAM. Indy implMethod handle descriptor is
     *  the 0-arg unsafe overload exactly. */
    public GroupByKeyFactory<String, String>
            buildNullaryFactory(KStream<String, String> stream) {
        return stream::groupByKey;
    }

    /** MUST FIRE — {@code stream::groupByKey} bound to
     *  {@link Function Function&lt;Object, KGroupedStream&gt;} —
     *  here Function captures stream and ignores its argument,
     *  but the bsm-arg Handle still points at the 0-arg unsafe
     *  overload because the compiler captures `stream` and uses
     *  it as the receiver. */
    public Supplier<KGroupedStream<String, String>>
            buildAnotherSupplier(KStream<String, String> stream) {
        Supplier<KGroupedStream<String, String>> factory = stream::groupByKey;
        return factory;
    }

    /** MUST FIRE — local Supplier binding applied immediately.
     *  The indy site is in this method's bytecode. */
    public KGroupedStream<String, String> useLocalFactory(KStream<String, String> stream) {
        Supplier<KGroupedStream<String, String>> factory = stream::groupByKey;
        return factory.get();
    }

    /** MUST FIRE — explicit lambda body that invokes
     *  {@code stream.groupByKey()}. The synthetic lambda method's
     *  bytecode contains a direct INVOKEINTERFACE on the 0-arg
     *  unsafe overload — the rule walks all methods on the class
     *  including synthetic lambda bodies and fires there. */
    public Stream<KGroupedStream<String, String>> groupAll(
            List<KStream<String, String>> streams) {
        return streams.stream().map(s -> s.groupByKey());
    }

    public static void main(String[] args) {
        StreamsBuilder builder = new StreamsBuilder();
        System.out.println(builder);
        new BadStreamsGroupByKeyNoGrouped();
    }
}
