package sample;

import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Named;
import org.apache.kafka.streams.kstream.ValueMapper;

import java.util.List;
import java.util.function.BiFunction;
import java.util.stream.Stream;

/**
 * MUST NOT FIRE — every call site passes an explicit
 * {@link Named} pinning the mapValues processor-node name, so
 * the topology-graph index does not auto-name the node and the
 * artifact (metric panel, runbook node id, trace span,
 * Materialized state-store) is stable across upstream edits.
 */
public final class GoodStreamsKTableMapValuesNoNamed {

    /** 2-arg SAM matching {@code (ValueMapper, Named)KTable}. */
    @FunctionalInterface
    interface MapValuesFactory<V, V2> {
        KTable<String, V2> apply(ValueMapper<? super V, ? extends V2> mapper, Named named);
    }

    public KTable<String, Integer> projectLength(KTable<String, String> orders) {
        return orders.mapValues((ValueMapper<String, Integer>) v -> v == null ? 0 : v.length(),
                Named.as("project-length"));
    }

    public KTable<String, String> projectAndChain(KTable<String, String> orders) {
        return orders.mapValues((ValueMapper<String, String>) v -> v == null ? "" : v.toUpperCase(),
                Named.as("project-uppercase"));
    }

    public KTable<String, String> truncateValue(KTable<String, String> orders) {
        return orders.mapValues((ValueMapper<String, String>) v ->
                        v == null ? "" : v.substring(0, Math.min(8, v.length())),
                Named.as("truncate-value"));
    }

    public BiFunction<ValueMapper<String, Integer>, Named, KTable<String, Integer>>
            buildProjectionFactory(KTable<String, String> table) {
        return table::mapValues;
    }

    public BiFunction<ValueMapper<String, String>, Named, KTable<String, String>>
            buildIdentityFactory(KTable<String, String> table) {
        return table::mapValues;
    }

    public <V2> MapValuesFactory<String, V2> buildCustomFactory(KTable<String, String> table) {
        return table::mapValues;
    }

    public KTable<String, Integer> useLocalFactory(KTable<String, String> orders,
            ValueMapper<String, Integer> mapper) {
        BiFunction<ValueMapper<String, Integer>, Named, KTable<String, Integer>> factory =
                orders::mapValues;
        return factory.apply(mapper, Named.as("local-factory-mapvalues"));
    }

    public Stream<KTable<String, Integer>> projectAll(
            List<KTable<String, String>> tables,
            ValueMapper<String, Integer> mapper) {
        return tables.stream().map(t -> t.mapValues(mapper, Named.as("project-all")));
    }

    public static void main(String[] args) {
        new GoodStreamsKTableMapValuesNoNamed();
    }
}
