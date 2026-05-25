package sample;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.GlobalKTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.state.KeyValueStore;

import java.util.List;
import java.util.function.BiFunction;
import java.util.stream.Stream;

/**
 * RULE: STREAMS_GLOBAL_TABLE_NO_MATERIALIZED — must NOT fire on ANY
 * method below.
 *
 * <p>Mirror of {@code BadStreamsGlobalTableNoMaterialized} that uses
 * the 2-arg {@code globalTable(topic, Materialized)} and 3-arg
 * {@code globalTable(topic, Consumed, Materialized)} overloads
 * everywhere the BAD class used the unsafe no-Materialized
 * overloads. The safe-overload descriptors are strictly different
 * from the two unsafe descriptors the rule predicate matches
 * against — so neither the direct {@code INVOKEVIRTUAL} sites nor
 * the {@code builder::globalTable} {@code INVOKEDYNAMIC}
 * method-reference captures (which carry the resolved safe-overload
 * descriptor in their bsm-args {@code REF_invokeVirtual} Handle
 * because the target SAM has the matching parameter slots) match
 * the rule predicate.
 *
 * <p>Every {@link Materialized} argument carries a stable
 * domain-meaningful global-store name so that the global state
 * store survives arbitrary topology edits and remains queryable
 * via interactive queries — crucial for GlobalKTable because the
 * cold-restart cost is multiplied by every Streams instance in the
 * cluster.
 */
public final class GoodStreamsGlobalTableNoMaterialized {

    @FunctionalInterface
    interface MaterializedGlobalTableFactory<K, V> {
        GlobalKTable<K, V> create(String topic,
                Materialized<K, V, KeyValueStore<Bytes, byte[]>> materialized);
    }

    @FunctionalInterface
    interface ProductsGlobalTableFactory {
        GlobalKTable<String, String> create(String topic,
                Materialized<String, String, KeyValueStore<Bytes, byte[]>> materialized);
    }

    // ===== Direct INVOKEVIRTUAL on the safe overloads =====

    public GlobalKTable<String, String> productsGlobalTable(StreamsBuilder builder) {
        return builder.globalTable("products",
                Materialized.<String, String, KeyValueStore<Bytes, byte[]>>as("products-store"));
    }

    public GlobalKTable<String, String> customersGlobalTable(StreamsBuilder builder) {
        return builder.globalTable("customers",
                Consumed.with(Serdes.String(), Serdes.String()),
                Materialized.<String, String, KeyValueStore<Bytes, byte[]>>as("customers-store"));
    }

    public void buildTopology(StreamsBuilder builder) {
        GlobalKTable<String, String> reference = builder.globalTable("reference",
                Materialized.<String, String, KeyValueStore<Bytes, byte[]>>as("reference-store"));
        System.out.println(reference);
    }

    // ===== INVOKEDYNAMIC method-reference captures bound to safe
    //       SAMs — implMethod handle desc is (String, Materialized)
    //       GlobalKTable or (String, Consumed, Materialized)
    //       GlobalKTable — does NOT match rule predicate =====

    public BiFunction<String,
                      Materialized<String, String, KeyValueStore<Bytes, byte[]>>,
                      GlobalKTable<String, String>>
            buildBiFunctionFactory(StreamsBuilder builder) {
        return builder::globalTable;
    }

    public <K, V> MaterializedGlobalTableFactory<K, V> buildMaterializedFactory(
            StreamsBuilder builder) {
        return builder::globalTable;
    }

    public ProductsGlobalTableFactory buildProductsGlobalTableFactory(StreamsBuilder builder) {
        return builder::globalTable;
    }

    public GlobalKTable<String, String> useLocalFactory(StreamsBuilder builder, String topic) {
        BiFunction<String,
                Materialized<String, String, KeyValueStore<Bytes, byte[]>>,
                GlobalKTable<String, String>> factory = builder::globalTable;
        return factory.apply(topic,
                Materialized.<String, String, KeyValueStore<Bytes, byte[]>>as("local-store"));
    }

    public Stream<GlobalKTable<String, String>> globalTableAll(StreamsBuilder builder, List<String> topics) {
        return topics.stream().map(name -> builder.globalTable(name,
                Materialized.<String, String, KeyValueStore<Bytes, byte[]>>as(name + "-store")));
    }

    public static void main(String[] args) {
        StreamsBuilder b = new StreamsBuilder();
        new GoodStreamsGlobalTableNoMaterialized().buildTopology(b);
    }
}
