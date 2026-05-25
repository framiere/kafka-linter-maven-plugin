package sample;

import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Named;

import java.util.List;
import java.util.function.BiFunction;
import java.util.stream.Stream;

/**
 * RULE: STREAMS_MERGE_NO_NAMED — must NOT fire on ANY method
 * below.
 *
 * <p>Mirror of {@code BadStreamsMergeNoNamed} that uses the
 * {@code merge(KStream, Named)} safe overload everywhere the BAD
 * class used the unsafe overload. The safe-overload descriptor
 * is strictly different from the unsafe descriptor the rule
 * predicate matches against — so neither the direct
 * INVOKEINTERFACE sites nor the {@code primary::merge}
 * INVOKEDYNAMIC method-reference captures (which carry the
 * resolved safe-overload descriptor in their bsm-args
 * REF_invokeInterface Handle) match the rule predicate.
 *
 * <p>Every Named argument carries a stable domain-meaningful
 * processor node name so that per-node JMX metrics, trace span
 * names, and topology.describe() runbook references survive
 * topology edits.
 */
public final class GoodStreamsMergeNoNamed {

    @FunctionalInterface
    interface MergeWithNamedFactory<K, V> {
        KStream<K, V> apply(KStream<K, V> other, Named named);
    }

    @FunctionalInterface
    interface StringMergeWithNamedFactory {
        KStream<String, String> apply(KStream<String, String> other, Named named);
    }

    // ===== Direct INVOKEINTERFACE on the safe overload =====

    public KStream<String, String> mergeOrdersAndReturns(
            KStream<String, String> orders, KStream<String, String> returns) {
        return orders.merge(returns, Named.as("merge-orders-returns"));
    }

    public KStream<String, String> mergeFiltered(
            KStream<String, String> primary, KStream<String, String> secondary) {
        return primary.filter((k, v) -> v != null)
                .merge(secondary, Named.as("merge-filtered"));
    }

    public KStream<String, String> mergeMapped(
            KStream<String, String> primary, KStream<String, String> secondary) {
        return primary.mapValues((k, v) -> v.toUpperCase())
                .merge(secondary, Named.as("merge-mapped"));
    }

    // ===== INVOKEDYNAMIC method-reference captures bound to safe
    //       SAMs — implMethod handle desc is (KStream, Named)
    //       KStream — does NOT match rule predicate =====

    public BiFunction<KStream<String, String>, Named, KStream<String, String>>
            buildBiFunctionFactory(KStream<String, String> primary) {
        return primary::merge;
    }

    public MergeWithNamedFactory<String, String> buildMergeFactory(KStream<String, String> primary) {
        return primary::merge;
    }

    public StringMergeWithNamedFactory buildStringMergeFactory(KStream<String, String> primary) {
        return primary::merge;
    }

    public KStream<String, String> useLocalFactory(
            KStream<String, String> primary, KStream<String, String> secondary) {
        BiFunction<KStream<String, String>, Named, KStream<String, String>> factory = primary::merge;
        return factory.apply(secondary, Named.as("local-merge"));
    }

    public Stream<KStream<String, String>> mergeAll(
            KStream<String, String> primary, List<KStream<String, String>> others) {
        return others.stream().map(other ->
                primary.merge(other, Named.as("merge-all-" + System.identityHashCode(other))));
    }

    public static void main(String[] args) {
        StreamsBuilder builder = new StreamsBuilder();
        System.out.println(builder);
        new GoodStreamsMergeNoNamed();
    }
}
