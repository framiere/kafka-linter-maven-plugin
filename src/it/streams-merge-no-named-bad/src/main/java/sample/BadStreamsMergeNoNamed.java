package sample;

import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KStream;

import java.util.List;
import java.util.function.Function;
import java.util.stream.Stream;

/**
 * RULE: STREAMS_MERGE_NO_NAMED — must fire EXACTLY 8 times across
 * this class (one per method below).
 *
 * <p>Each method exercises one of the bytecode shapes the rule
 * is required to catch — single unsafe overload descriptor
 * {@code (Lorg/apache/kafka/streams/kstream/KStream;)Lorg/apache/
 * kafka/streams/kstream/KStream;}. Catches both direct
 * {@code INVOKEINTERFACE} calls (KStream is an interface) and
 * {@code INVOKEDYNAMIC} method-reference captures bound to a SAM
 * whose erased implMethod descriptor matches the unsafe overload.
 */
public final class BadStreamsMergeNoNamed {

    /** Custom 1-arg merge SAM matching the unsafe overload. */
    @FunctionalInterface
    interface MergeFactory<K, V> {
        KStream<K, V> apply(KStream<K, V> other);
    }

    /** Non-generic 1-arg merge SAM. */
    @FunctionalInterface
    interface StringMergeFactory {
        KStream<String, String> apply(KStream<String, String> other);
    }

    // ===== Direct INVOKEINTERFACE on the unsafe overload =====

    /** MUST FIRE — direct INVOKEINTERFACE merging two streams. */
    public KStream<String, String> mergeOrdersAndReturns(
            KStream<String, String> orders, KStream<String, String> returns) {
        return orders.merge(returns);
    }

    /** MUST FIRE — direct INVOKEINTERFACE in a chain after a
     *  filter; the filter is the upstream edit the team will add. */
    public KStream<String, String> mergeFiltered(
            KStream<String, String> primary, KStream<String, String> secondary) {
        return primary.filter((k, v) -> v != null).merge(secondary);
    }

    /** MUST FIRE — direct INVOKEINTERFACE merging a chained
     *  {@code mapValues} result into another stream. */
    public KStream<String, String> mergeMapped(
            KStream<String, String> primary, KStream<String, String> secondary) {
        return primary.mapValues((k, v) -> v.toUpperCase()).merge(secondary);
    }

    // ===== INVOKEDYNAMIC method-reference captures =====

    /** MUST FIRE — {@code primary::merge} bound to
     *  {@link Function Function&lt;KStream, KStream&gt;}.
     *  INVOKEDYNAMIC bsm-args contain a REF_invokeInterface Handle
     *  pointing at {@code KStream.merge(KStream)KStream}. */
    public Function<KStream<String, String>, KStream<String, String>>
            buildFunctionFactory(KStream<String, String> primary) {
        return primary::merge;
    }

    /** MUST FIRE — {@code primary::merge} bound to a custom
     *  1-arg generic SAM. Indy implMethod handle descriptor is the
     *  unsafe overload exactly. */
    public MergeFactory<String, String> buildMergeFactory(KStream<String, String> primary) {
        return primary::merge;
    }

    /** MUST FIRE — {@code primary::merge} bound to a custom
     *  non-generic 1-arg SAM. */
    public StringMergeFactory buildStringMergeFactory(KStream<String, String> primary) {
        return primary::merge;
    }

    /** MUST FIRE — local Function binding applied immediately.
     *  The indy site is in this method's bytecode. */
    public KStream<String, String> useLocalFactory(
            KStream<String, String> primary, KStream<String, String> secondary) {
        Function<KStream<String, String>, KStream<String, String>> factory = primary::merge;
        return factory.apply(secondary);
    }

    /** MUST FIRE — explicit lambda body that invokes
     *  {@code primary.merge(other)}. The synthetic lambda method's
     *  bytecode contains a direct INVOKEINTERFACE on the unsafe
     *  overload — the rule walks all methods on the class
     *  including synthetic lambda bodies and fires there. */
    public Stream<KStream<String, String>> mergeAll(
            KStream<String, String> primary, List<KStream<String, String>> others) {
        return others.stream().map(other -> primary.merge(other));
    }

    public static void main(String[] args) {
        StreamsBuilder builder = new StreamsBuilder();
        System.out.println(builder);
        new BadStreamsMergeNoNamed();
    }
}
