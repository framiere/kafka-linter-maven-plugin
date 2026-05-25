package sample;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.state.KeyValueStore;

import java.util.List;
import java.util.function.BiFunction;
import java.util.stream.Stream;

/**
 * RULE: STREAMS_TABLE_NO_MATERIALIZED — must NOT fire on ANY method
 * below.
 *
 * <p>Mirror of {@code BadStreamsTableNoMaterialized} that uses the
 * 2-arg {@code table(topic, Materialized)} and 3-arg
 * {@code table(topic, Consumed, Materialized)} overloads everywhere
 * the BAD class used the unsafe no-Materialized overloads. The
 * safe-overload descriptors are strictly different from the two
 * unsafe descriptors the rule predicate matches against — so
 * neither the direct {@code INVOKEVIRTUAL} sites nor the
 * {@code builder::table} {@code INVOKEDYNAMIC} method-reference
 * captures (which carry the resolved safe-overload descriptor in
 * their bsm-args {@code REF_invokeVirtual} Handle because the
 * target SAM has the matching parameter slots) match the rule
 * predicate.
 *
 * <p>Every {@link Materialized} argument carries a domain-meaningful
 * store name so that the local state store + changelog topic
 * survive arbitrary topology edits and remain queryable via
 * interactive queries.
 */
public final class GoodStreamsTableNoMaterialized {

    @FunctionalInterface
    interface MaterializedTableFactory<K, V> {
        KTable<K, V> create(String topic,
                Materialized<K, V, KeyValueStore<Bytes, byte[]>> materialized);
    }

    @FunctionalInterface
    interface UsersTableFactory {
        KTable<String, String> create(String topic,
                Materialized<String, String, KeyValueStore<Bytes, byte[]>> materialized);
    }

    // ===== Direct INVOKEVIRTUAL on the safe overloads =====

    public KTable<String, String> usersTable(StreamsBuilder builder) {
        return builder.table("users",
                Materialized.<String, String, KeyValueStore<Bytes, byte[]>>as("users-store"));
    }

    public KTable<String, String> customersTable(StreamsBuilder builder) {
        return builder.table("customers",
                Consumed.with(Serdes.String(), Serdes.String()),
                Materialized.<String, String, KeyValueStore<Bytes, byte[]>>as("customers-store"));
    }

    public void buildTopology(StreamsBuilder builder) {
        KTable<String, String> inventory = builder.table("inventory",
                Materialized.<String, String, KeyValueStore<Bytes, byte[]>>as("inventory-store"));
        inventory.toStream().to("inventory-out");
    }

    // ===== INVOKEDYNAMIC method-reference captures bound to safe
    //       SAMs — implMethod handle desc is (String, Materialized)
    //       KTable or (String, Consumed, Materialized)KTable — does
    //       NOT match rule predicate =====

    public BiFunction<String,
                      Materialized<String, String, KeyValueStore<Bytes, byte[]>>,
                      KTable<String, String>>
            buildBiFunctionFactory(StreamsBuilder builder) {
        return builder::table;
    }

    public <K, V> MaterializedTableFactory<K, V> buildMaterializedFactory(
            StreamsBuilder builder) {
        return builder::table;
    }

    public UsersTableFactory buildUsersTableFactory(StreamsBuilder builder) {
        return builder::table;
    }

    public KTable<String, String> useLocalFactory(StreamsBuilder builder, String topic) {
        BiFunction<String,
                Materialized<String, String, KeyValueStore<Bytes, byte[]>>,
                KTable<String, String>> factory = builder::table;
        return factory.apply(topic,
                Materialized.<String, String, KeyValueStore<Bytes, byte[]>>as("local-store"));
    }

    public Stream<KTable<String, String>> tableAll(StreamsBuilder builder, List<String> topics) {
        return topics.stream().map(name -> builder.table(name,
                Materialized.<String, String, KeyValueStore<Bytes, byte[]>>as(name + "-store")));
    }

    public static void main(String[] args) {
        StreamsBuilder b = new StreamsBuilder();
        new GoodStreamsTableNoMaterialized().buildTopology(b);
    }
}
