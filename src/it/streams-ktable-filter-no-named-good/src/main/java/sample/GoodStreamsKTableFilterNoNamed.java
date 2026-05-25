package sample;

import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Named;
import org.apache.kafka.streams.kstream.Predicate;

import java.util.List;
import java.util.function.BiFunction;
import java.util.stream.Stream;

/**
 * MUST NOT FIRE — every call site passes an explicit
 * {@link Named} pinning the filter / filterNot processor-node
 * name, so the topology-graph index does not auto-name the node
 * and the artifact (metric panel, runbook node id, trace span,
 * Materialized state-store) is stable across upstream edits.
 */
public final class GoodStreamsKTableFilterNoNamed {

    /** 2-arg SAM matching {@code (Predicate, Named)KTable}. */
    @FunctionalInterface
    interface FilterFactory<K, V> {
        KTable<K, V> apply(Predicate<? super K, ? super V> predicate, Named named);
    }

    public KTable<String, String> filterHighValue(KTable<String, String> orders) {
        return orders.filter((k, v) -> v != null && v.length() > 10,
                Named.as("high-value-orders-filter"));
    }

    public KTable<String, String> rejectNulls(KTable<String, String> orders) {
        return orders.filterNot((k, v) -> v == null,
                Named.as("reject-nulls-filter"));
    }

    public KTable<String, String> filteredAndChained(KTable<String, String> orders) {
        return orders.filter((k, v) -> v.startsWith("hv-"),
                Named.as("hv-prefix-filter"));
    }

    public BiFunction<Predicate<String, String>, Named, KTable<String, String>>
            buildFilterFactory(KTable<String, String> table) {
        return table::filter;
    }

    public BiFunction<Predicate<String, String>, Named, KTable<String, String>>
            buildFilterNotFactory(KTable<String, String> table) {
        return table::filterNot;
    }

    public <K, V> FilterFactory<K, V> buildCustomFactory(KTable<K, V> table) {
        return table::filter;
    }

    public KTable<String, String> useLocalFactory(KTable<String, String> orders,
            Predicate<String, String> p) {
        BiFunction<Predicate<String, String>, Named, KTable<String, String>> factory =
                orders::filter;
        return factory.apply(p, Named.as("local-factory-filter"));
    }

    public Stream<KTable<String, String>> filterAll(
            List<KTable<String, String>> tables,
            Predicate<String, String> p) {
        return tables.stream().map(t -> t.filter(p, Named.as("filter-all")));
    }

    public static void main(String[] args) {
        new GoodStreamsKTableFilterNoNamed();
    }
}
