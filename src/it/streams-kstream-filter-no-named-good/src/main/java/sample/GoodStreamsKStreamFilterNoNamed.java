package sample;

import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Named;
import org.apache.kafka.streams.kstream.Predicate;

import java.util.List;
import java.util.function.BiFunction;
import java.util.stream.Stream;

/**
 * MUST NOT FIRE — every call site passes an explicit
 * {@link Named} pinning the filter / filterNot processor-node
 * name, so the topology-graph index does not auto-name the
 * node and per-node drop-rate alerts, runbook ids, trace
 * spans, and dashboards remain stable across upstream edits.
 */
public final class GoodStreamsKStreamFilterNoNamed {

    /** 2-arg SAM matching {@code (Predicate, Named)KStream}. */
    @FunctionalInterface
    interface FilterFactory<K, V> {
        KStream<K, V> apply(Predicate<? super K, ? super V> predicate, Named named);
    }

    public KStream<String, String> rejectFraud(KStream<String, String> txns) {
        return txns.filter((k, v) -> v != null && !v.startsWith("FRAUD-"),
                Named.as("fraud-rejection-filter"));
    }

    public KStream<String, String> dropEmpty(KStream<String, String> stream) {
        return stream.filterNot((k, v) -> v == null || v.isBlank(),
                Named.as("drop-empty-filter"));
    }

    public KStream<String, String> filteredAndChained(KStream<String, String> stream) {
        return stream.filter((k, v) -> v != null && v.length() > 4,
                Named.as("min-length-filter"));
    }

    public BiFunction<Predicate<String, String>, Named, KStream<String, String>>
            buildFilterFactory(KStream<String, String> stream) {
        return stream::filter;
    }

    public BiFunction<Predicate<String, String>, Named, KStream<String, String>>
            buildFilterNotFactory(KStream<String, String> stream) {
        return stream::filterNot;
    }

    public <K, V> FilterFactory<K, V> buildCustomFactory(KStream<K, V> stream) {
        return stream::filter;
    }

    public KStream<String, String> useLocalFactory(KStream<String, String> stream,
            Predicate<String, String> p) {
        BiFunction<Predicate<String, String>, Named, KStream<String, String>> factory =
                stream::filter;
        return factory.apply(p, Named.as("local-factory-filter"));
    }

    public Stream<KStream<String, String>> filterAll(
            List<KStream<String, String>> streams,
            Predicate<String, String> p) {
        return streams.stream().map(s -> s.filter(p, Named.as("filter-all")));
    }

    public static void main(String[] args) {
        new GoodStreamsKStreamFilterNoNamed();
    }
}
