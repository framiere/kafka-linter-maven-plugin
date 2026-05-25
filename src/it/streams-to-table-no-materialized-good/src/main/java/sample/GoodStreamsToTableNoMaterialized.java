package sample;

import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Named;
import org.apache.kafka.streams.state.KeyValueStore;

import java.util.List;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Stream;

/**
 * MUST NOT FIRE — every call site uses a {@code toTable}
 * overload that takes a {@link Materialized} so the backing
 * state store and changelog topic carry a stable name across
 * topology edits.
 *
 * <p>Safe overload descriptors used here:
 *
 * <ol>
 *   <li>{@code (Lorg/apache/kafka/streams/kstream/Materialized;)
 *       Lorg/apache/kafka/streams/kstream/KTable;} —
 *       {@code toTable(Materialized)};</li>
 *   <li>{@code (Lorg/apache/kafka/streams/kstream/Named;
 *       Lorg/apache/kafka/streams/kstream/Materialized;)
 *       Lorg/apache/kafka/streams/kstream/KTable;} —
 *       {@code toTable(Named, Materialized)}.</li>
 * </ol>
 */
public final class GoodStreamsToTableNoMaterialized {

    /** 1-arg Materialized SAM matching {@code (Materialized)KTable}. */
    @FunctionalInterface
    interface ToTableWithMaterializedFactory<K, V> {
        KTable<K, V> apply(Materialized<K, V, KeyValueStore<Bytes, byte[]>> mat);
    }

    /** 2-arg (Named, Materialized) SAM. */
    @FunctionalInterface
    interface ToTableWithNamedAndMaterializedFactory<K, V> {
        KTable<K, V> apply(Named named,
                Materialized<K, V, KeyValueStore<Bytes, byte[]>> mat);
    }

    private static Materialized<String, String, KeyValueStore<Bytes, byte[]>>
            mat(String name) {
        return Materialized.<String, String, KeyValueStore<Bytes, byte[]>>as(name);
    }

    // ===== Direct INVOKEINTERFACE on the safe overloads =====

    public KTable<String, String> eventsTable(KStream<String, String> events) {
        return events.toTable(mat("events-table"));
    }

    public KTable<String, String> ordersTable(KStream<String, String> orders) {
        return orders.toTable(Named.as("orders-promotion"), mat("orders-table"));
    }

    public KTable<String, String> filteredTable(KStream<String, String> stream) {
        return stream.filter((k, v) -> v != null).toTable(mat("filtered-table"));
    }

    // ===== INVOKEDYNAMIC method-reference captures bound to safe overloads =====

    public Function<Materialized<String, String, KeyValueStore<Bytes, byte[]>>,
                    KTable<String, String>>
            buildSupplierFactory(KStream<String, String> events) {
        return events::toTable;
    }

    public BiFunction<Named,
                      Materialized<String, String, KeyValueStore<Bytes, byte[]>>,
                      KTable<String, String>>
            buildFunctionFactory(KStream<String, String> events) {
        return events::toTable;
    }

    public ToTableWithMaterializedFactory<String, String>
            buildNullaryFactory(KStream<String, String> events) {
        return events::toTable;
    }

    public KTable<String, String> useLocalFactory(KStream<String, String> events) {
        ToTableWithMaterializedFactory<String, String> factory = events::toTable;
        return factory.apply(mat("events-table-local"));
    }

    public Stream<KTable<String, String>> tableAll(
            KStream<String, String> stream, List<String> names) {
        return names.stream().map(name -> stream.toTable(
                Named.as(name), mat(name + "-store")));
    }

    public static void main(String[] args) {
        StreamsBuilder builder = new StreamsBuilder();
        System.out.println(builder);
        new GoodStreamsToTableNoMaterialized();
    }
}
